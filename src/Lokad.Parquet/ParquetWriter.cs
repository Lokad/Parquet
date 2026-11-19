using System.Buffers.Binary;
using System.Numerics;
using System.Text;
using System.Text.Unicode;
using Lokad.Parquet.Internal;

namespace Lokad.Parquet;

/// <summary>Writes flat, required Parquet columns to a caller-owned stream in schema order.</summary>
public sealed class ParquetWriter : IDisposable
{
    private const int MaximumColumns = 4_096;
    private const int MaximumRowsPerGroup = 1_048_576;
    private const int MaximumRowGroups = 65_536;
    private const int MaximumNameBytes = 4_096;
    private const int MaximumMetadataStringBytes = 16 * 1024 * 1024;
    private const int MaximumFooterBytes = 32 * 1024 * 1024;
    private const int MaximumPageBytes = 16 * 1024 * 1024;
    private static readonly UTF8Encoding StrictUtf8 = new(false, true);
    private static readonly byte[] Magic = "PAR1"u8.ToArray();

    private readonly Stream _destination;
    private readonly WriterColumn[] _columns;
    private readonly ParquetWriterOptions _options;
    private readonly int _namesBytes;
    private readonly int _groupFooterBound;
    private readonly byte[] _pageHeader = new byte[64];
    private List<WrittenRowGroup>? _groups = [];
    private WrittenColumnChunk[]? _groupChunks;
    private int _footerBound;
    private int _metadataStringBytes;
    private int _rowGroupRows;
    private int _columnOrdinal;
    private int _columnRows;
    private int _pageValues;
    private int _pageBytes;
    private byte[]? _page;
    private byte[]? _compressed;
    private int[]? _hashPositions;
    private long _offset;
    private long _rows;
    private long _chunkOffset;
    private long _chunkUncompressedBytes;
    private int _busy;
    private WriterState _state;
    private List<WrittenRowGroup> Groups => _groups ?? throw new InvalidOperationException("The writer is closed.");

    private enum WriterState
    {
        Open,
        Completed,
        Faulted,
        Disposed,
    }

    private delegate void EncodeFixed<T>(ReadOnlySpan<T> source, Span<byte> destination, int valueOffset);

    private ParquetWriter(Stream destination, WriterColumn[] columns, ParquetWriterOptions options, int namesBytes)
    {
        _destination = destination;
        _columns = columns;
        _options = options;
        _namesBytes = namesBytes;
        _metadataStringBytes = "schema"u8.Length + namesBytes;

        // Bound the retained chunk descriptors and provide a conservative capacity
        // for Compact Thrift serialization, including the eight-byte trailer.
        _footerBound = 128 + columns.Length * 64 + namesBytes;
        _groupFooterBound = 64 + columns.Length * 96 + namesBytes;
    }

    /// <summary>Starts a new Snappy-compressed Parquet file with the default page targets.</summary>
    public static ValueTask<ParquetWriter> CreateAsync(Stream destination, IReadOnlyList<ParquetWriteColumn> columns) =>
        CreateAsync(destination, columns, ParquetWriterOptions.Default, CancellationToken.None);

    /// <summary>Starts a new Snappy-compressed Parquet file with the supplied page targets.</summary>
    public static ValueTask<ParquetWriter> CreateAsync(
        Stream destination, IReadOnlyList<ParquetWriteColumn> columns, ParquetWriterOptions options) =>
        CreateAsync(destination, columns, options, CancellationToken.None);

    /// <summary>Validates the supplied schema and starts a new Snappy-compressed Parquet file.</summary>
    /// <remarks>The destination must be empty and positioned at its start. The caller owns the stream.</remarks>
    public static async ValueTask<ParquetWriter> CreateAsync(
        Stream destination,
        IReadOnlyList<ParquetWriteColumn> columns,
        ParquetWriterOptions options,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(destination);
        ArgumentNullException.ThrowIfNull(columns);
        ArgumentNullException.ThrowIfNull(options);
        if (!destination.CanWrite || (destination.CanSeek && (destination.Position != 0 || destination.Length != 0)))
            throw new ArgumentException("The destination must be a writable stream at the start of a new file.", nameof(destination));

        if (options.TargetPageBytes is < sizeof(long) or > MaximumPageBytes ||
            options.TargetPageValues is < 1 or > MaximumRowsPerGroup)
            throw new ArgumentOutOfRangeException(nameof(options), "A page target is outside the supported range.");
        if (columns.Count is < 1 or > MaximumColumns)
            throw new ArgumentException("A writer schema must contain between 1 and 4,096 columns.", nameof(columns));

        var schema = new WriterColumn[columns.Count];
        var distinctNames = new HashSet<string>(StringComparer.Ordinal);
        var namesBytes = 0;
        for (var i = 0; i < schema.Length; i++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var column = columns[i];
            if (column is null || string.IsNullOrEmpty(column.Name) || !distinctNames.Add(column.Name))
                throw new ArgumentException("Column names must be nonempty and unique.", nameof(columns));
            if (column.PhysicalType is not (ParquetPhysicalType.Boolean or ParquetPhysicalType.Float or
                ParquetPhysicalType.Int64 or ParquetPhysicalType.ByteArray or ParquetPhysicalType.Int32))
                throw new ArgumentException($"Column '{column.Name}' has an unsupported physical type.", nameof(columns));
            var nameBytes = StrictUtf8.GetByteCount(column.Name);
            namesBytes += nameBytes;
            if (nameBytes > MaximumNameBytes || namesBytes > MaximumMetadataStringBytes - "schema"u8.Length)
                throw new ArgumentException("The schema exceeds the UTF-8 column name limits.", nameof(columns));
            schema[i] = new WriterColumn(column.Name, StrictUtf8.GetBytes(column.Name), column.PhysicalType);
        }

        var writer = new ParquetWriter(destination, schema, options, namesBytes);
        await writer.WriteDestinationAsync(Magic, cancellationToken).ConfigureAwait(false);
        return writer;
    }

    /// <summary>Starts a row group containing between 1 and 1,048,576 rows.</summary>
    /// <remarks>Complete each column in schema order before starting the next row group.</remarks>
    public void BeginRowGroup(int rowCount)
    {
        Enter();
        try
        {
            RequireOpen();
            if (_groupChunks is not null)
                throw new InvalidOperationException($"{DescribeColumn()} is still open.");
            if (rowCount is < 1 or > MaximumRowsPerGroup)
                throw new ArgumentOutOfRangeException(nameof(rowCount));
            if (Groups.Count >= MaximumRowGroups ||
                _groupFooterBound > MaximumFooterBytes - _footerBound ||
                _namesBytes > MaximumMetadataStringBytes - _metadataStringBytes)
                throw new InvalidOperationException("An additional row group would exceed the writer's footer limits.");

            _groupChunks = new WrittenColumnChunk[_columns.Length];
            _rowGroupRows = rowCount;
            _columnOrdinal = 0;
            _chunkOffset = _offset;
        }
        finally
        {
            Exit();
        }
    }

    /// <summary>Appends boolean values to the active BOOLEAN column.</summary>
    public ValueTask WriteBooleansAsync(ReadOnlyMemory<bool> values) =>
        WriteBooleansAsync(values, CancellationToken.None);

    /// <summary>Appends boolean values to the active BOOLEAN column with cancellation.</summary>
    public ValueTask WriteBooleansAsync(ReadOnlyMemory<bool> values, CancellationToken cancellationToken) =>
        WriteFixedAsync(values, ParquetPhysicalType.Boolean, 0, static (source, destination, offset) =>
        {
            var i = 0;
            // The first partial byte may already contain values from an earlier slice.
            for (; i < source.Length && (offset & 7) != 0; i++, offset++)
            {
                if (source[i])
                    destination[offset >> 3] |= (byte)(1 << (offset & 7));
            }

            for (; i <= source.Length - 8; i += 8, offset += 8)
            {
                destination[offset >> 3] = (byte)(
                    (source[i] ? 1 : 0) |
                    (source[i + 1] ? 2 : 0) |
                    (source[i + 2] ? 4 : 0) |
                    (source[i + 3] ? 8 : 0) |
                    (source[i + 4] ? 16 : 0) |
                    (source[i + 5] ? 32 : 0) |
                    (source[i + 6] ? 64 : 0) |
                    (source[i + 7] ? 128 : 0));
            }

            if (i < source.Length)
            {
                var packed = 0;
                for (var bit = 0; i < source.Length; i++, bit++)
                    if (source[i])
                        packed |= 1 << bit;
                destination[offset >> 3] = (byte)packed;
            }
        }, cancellationToken);

    /// <summary>Appends binary32 values to the active FLOAT column, preserving their bits.</summary>
    public ValueTask WriteFloatsAsync(ReadOnlyMemory<float> values) =>
        WriteFloatsAsync(values, CancellationToken.None);

    /// <summary>Appends binary32 values to the active FLOAT column with cancellation, preserving their bits.</summary>
    public ValueTask WriteFloatsAsync(ReadOnlyMemory<float> values, CancellationToken cancellationToken) =>
        WriteFixedAsync(values, ParquetPhysicalType.Float, sizeof(float), static (source, destination, offset) =>
        {
            var i = CopyLittleEndianVectors(source, destination, offset, sizeof(float));
            for (; i < source.Length; i++)
                BinaryPrimitives.WriteInt32LittleEndian(destination[((offset + i) * sizeof(float))..],
                    BitConverter.SingleToInt32Bits(source[i]));
        }, cancellationToken);

    /// <summary>Appends signed values to the active INT64 column.</summary>
    public ValueTask WriteInt64Async(ReadOnlyMemory<long> values) =>
        WriteInt64Async(values, CancellationToken.None);

    /// <summary>Appends signed values to the active INT64 column with cancellation.</summary>
    public ValueTask WriteInt64Async(ReadOnlyMemory<long> values, CancellationToken cancellationToken) =>
        WriteFixedAsync(values, ParquetPhysicalType.Int64, sizeof(long), static (source, destination, offset) =>
        {
            var i = CopyLittleEndianVectors(source, destination, offset, sizeof(long));
            for (; i < source.Length; i++)
                BinaryPrimitives.WriteInt64LittleEndian(destination[((offset + i) * sizeof(long))..], source[i]);
        }, cancellationToken);

    /// <summary>Appends days since 1970-01-01 to the active INT32 DATE column.</summary>
    public ValueTask WriteDatesAsync(ReadOnlyMemory<int> days) =>
        WriteDatesAsync(days, CancellationToken.None);

    /// <summary>Appends days since 1970-01-01 to the active INT32 DATE column with cancellation.</summary>
    public ValueTask WriteDatesAsync(ReadOnlyMemory<int> days, CancellationToken cancellationToken) =>
        WriteFixedAsync(days, ParquetPhysicalType.Int32, sizeof(int), static (source, destination, offset) =>
        {
            var i = CopyLittleEndianVectors(source, destination, offset, sizeof(int));
            for (; i < source.Length; i++)
                BinaryPrimitives.WriteInt32LittleEndian(destination[((offset + i) * sizeof(int))..], source[i]);
        }, cancellationToken);

    /// <summary>Appends UTF-8 values delineated by offsets to the active BYTE_ARRAY STRING column.</summary>
    public ValueTask WriteUtf8Async(ReadOnlyMemory<byte> payload, ReadOnlyMemory<int> offsets) =>
        WriteUtf8Async(payload, offsets, CancellationToken.None);

    /// <summary>Appends UTF-8 values to the active BYTE_ARRAY STRING column with cancellation.</summary>
    /// <remarks>Each adjacent pair of offsets describes one value in the supplied payload.
    /// Both buffers may be reused when the operation completes.</remarks>
    public async ValueTask WriteUtf8Async(
        ReadOnlyMemory<byte> payload,
        ReadOnlyMemory<int> offsets,
        CancellationToken cancellationToken)
    {
        Enter();
        try
        {
            RequireColumn();
            if (offsets.IsEmpty)
                throw new ArgumentException($"{DescribeColumn()} needs at least one UTF-8 offset.", nameof(offsets));
            ValidateSlice(ParquetPhysicalType.ByteArray, offsets.Length - 1);
            try
            {
                ValidateUtf8(payload.Span, offsets.Span, cancellationToken);
            }
            catch (OperationCanceledException)
            {
                Fail();
                throw;
            }

            try
            {
                for (var i = 0; i < offsets.Length - 1; i++)
                {
                    var start = offsets.Span[i];
                    var length = offsets.Span[i + 1] - start;
                    var encodedBytes = sizeof(int) + length;
                    if (_pageValues > 0 &&
                        (_pageValues == _options.TargetPageValues || encodedBytes > _options.TargetPageBytes - _pageBytes))
                        await FlushPageAsync(cancellationToken).ConfigureAwait(false);
                    var page = EnsureBuffer(ref _page, _pageBytes + encodedBytes, _pageBytes);
                    BinaryPrimitives.WriteInt32LittleEndian(page.AsSpan(_pageBytes), length);
                    payload.Span.Slice(start, length).CopyTo(page.AsSpan(_pageBytes + sizeof(int)));
                    _pageBytes += encodedBytes;
                    _pageValues++;
                }
                cancellationToken.ThrowIfCancellationRequested();
                _columnRows += offsets.Length - 1;
            }
            catch
            {
                Fail();
                throw;
            }
        }
        finally
        {
            Exit();
        }

        void ValidateUtf8(ReadOnlySpan<byte> bytes, ReadOnlySpan<int> boundaries, CancellationToken token)
        {
            var start = boundaries[0];
            if ((uint)start > (uint)bytes.Length)
                throw new ArgumentException($"{DescribeColumn()} has an offset outside the supplied payload.", nameof(offsets));
            for (var i = 1; i < boundaries.Length; i++)
            {
                if ((i & 4095) == 1)
                    token.ThrowIfCancellationRequested();
                var end = boundaries[i];
                if (end < start || end > bytes.Length)
                    throw new ArgumentException($"{DescribeColumn()} has decreasing or out-of-range UTF-8 offsets.", nameof(offsets));
                var length = end - start;
                if (length > MaximumPageBytes - sizeof(int))
                    throw new ArgumentException($"{DescribeColumn()} has a UTF-8 value exceeding the hard page limit.", nameof(payload));
                if (!Utf8.IsValid(bytes.Slice(start, length)))
                    throw new ArgumentException($"{DescribeColumn()} has an invalid UTF-8 value.", nameof(payload));
                start = end;
            }
        }
    }

    /// <summary>Finishes the active column and advances to the next column or ends the row group.</summary>
    /// <remarks>The active column must have received exactly the declared row count.</remarks>
    public ValueTask CompleteColumnAsync() => CompleteColumnAsync(CancellationToken.None);

    /// <summary>Finishes the active column with cancellation and advances the writer.</summary>
    public async ValueTask CompleteColumnAsync(CancellationToken cancellationToken)
    {
        Enter();
        try
        {
            var chunks = RequireColumn();
            if (_columnRows != _rowGroupRows)
                throw new InvalidOperationException($"{DescribeColumn()} has {_columnRows} values, expected {_rowGroupRows}.");
            try
            {
                cancellationToken.ThrowIfCancellationRequested();
                await FlushPageAsync(cancellationToken).ConfigureAwait(false);
                chunks[_columnOrdinal++] = new WrittenColumnChunk(
                    _chunkOffset, _chunkUncompressedBytes, _offset - _chunkOffset);
                _columnRows = 0;
                _chunkUncompressedBytes = 0;
                _chunkOffset = _offset;
                if (_columnOrdinal == _columns.Length)
                {
                    Groups.Add(new WrittenRowGroup(_rowGroupRows, chunks));
                    _rows = checked(_rows + _rowGroupRows);
                    _footerBound += _groupFooterBound;
                    _metadataStringBytes += _namesBytes;
                    _groupChunks = null;
                }
            }
            catch
            {
                Fail();
                throw;
            }
        }
        finally
        {
            Exit();
        }
    }

    /// <summary>Writes the footer and completes the file, including a schema-only file with zero rows.</summary>
    /// <remarks>The caller retains responsibility for flushing and disposing the destination.</remarks>
    public ValueTask CompleteAsync() => CompleteAsync(CancellationToken.None);

    /// <summary>Writes the footer and completes the file with cancellation.</summary>
    public async ValueTask CompleteAsync(CancellationToken cancellationToken)
    {
        Enter();
        try
        {
            RequireOpen();
            if (_groupChunks is not null)
                throw new InvalidOperationException($"{DescribeColumn()} is still open.");
            try
            {
                cancellationToken.ThrowIfCancellationRequested();
                ReleasePageBuffers();
                var footer = ParquetArrayPool.Rent<byte>(_footerBound);
                try
                {
                    var length = ParquetWriterWire.WriteFooter(footer, _columns, Groups, _rows, cancellationToken);
                    await WriteDestinationAsync(footer.AsMemory(0, length), cancellationToken).ConfigureAwait(false);
                }
                finally
                {
                    ParquetArrayPool.Return(footer);
                }
                _groups = null;
                _state = WriterState.Completed;
            }
            catch
            {
                Fail();
                throw;
            }
        }
        finally
        {
            Exit();
        }
    }

    /// <summary>Returns working buffers and leaves the destination open.</summary>
    /// <remarks>Call <see cref="CompleteAsync()"/> explicitly to finish the file before disposal.</remarks>
    public void Dispose()
    {
        Enter();
        try
        {
            _state = WriterState.Disposed;
            ReleaseResources();
        }
        finally
        {
            Exit();
        }
    }

    private static int CopyLittleEndianVectors<T>(ReadOnlySpan<T> source, Span<byte> destination, int offset, int width)
        where T : struct
    {
        var count = Vector<T>.Count;
        if (source.Length < count || ParquetRuntime.ForceScalar || !Vector.IsHardwareAccelerated || !BitConverter.IsLittleEndian)
            return 0;

        var i = 0;
        for (; i <= source.Length - count; i += count)
            Vector.AsVectorByte(new Vector<T>(source[i..])).CopyTo(destination[((offset + i) * width)..]);
        return i;
    }

    private async ValueTask WriteFixedAsync<T>(
        ReadOnlyMemory<T> values, ParquetPhysicalType type, int width, EncodeFixed<T> encode, CancellationToken cancellationToken)
    {
        Enter();
        try
        {
            RequireColumn();
            ValidateSlice(type, values.Length);
            try
            {
                var pageLimit = Math.Min(_options.TargetPageValues,
                    width == 0 ? _options.TargetPageBytes * 8 : _options.TargetPageBytes / width);
                while (!values.IsEmpty)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    if (_pageValues == pageLimit)
                        await FlushPageAsync(cancellationToken).ConfigureAwait(false);
                    var count = Math.Min(values.Length, pageLimit - _pageValues);
                    var nextPageValues = _pageValues + count;
                    var nextPageBytes = width == 0 ? (nextPageValues + 7) / 8 : nextPageValues * width;
                    var page = EnsureBuffer(ref _page, nextPageBytes, _pageBytes);
                    encode(values.Span[..count], page, _pageValues);
                    _pageValues = nextPageValues;
                    _pageBytes = nextPageBytes;
                    _columnRows += count;
                    values = values[count..];
                }
                cancellationToken.ThrowIfCancellationRequested();
            }
            catch
            {
                Fail();
                throw;
            }
        }
        finally
        {
            Exit();
        }
    }

    private async ValueTask FlushPageAsync(CancellationToken cancellationToken)
    {
        if (_pageValues == 0)
            return;
        var compressed = EnsureBuffer(ref _compressed, SnappyBlockEncoder.MaximumCompressedLength(_pageBytes), 0);
        _hashPositions ??= ParquetArrayPool.Rent<int>(SnappyBlockEncoder.HashEntries);
        var compressedBytes = SnappyBlockEncoder.Compress(
            _page.AsSpan(0, _pageBytes), compressed, _hashPositions, cancellationToken);
        var headerBytes = ParquetWriterWire.WritePageHeader(_pageHeader, _pageValues, _pageBytes, compressedBytes);
        await WriteDestinationAsync(_pageHeader.AsMemory(0, headerBytes), cancellationToken).ConfigureAwait(false);
        await WriteDestinationAsync(compressed.AsMemory(0, compressedBytes), cancellationToken).ConfigureAwait(false);
        _chunkUncompressedBytes = checked(_chunkUncompressedBytes + headerBytes + _pageBytes);
        _pageBytes = 0;
        _pageValues = 0;
    }

    private async ValueTask WriteDestinationAsync(ReadOnlyMemory<byte> bytes, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var nextOffset = checked(_offset + bytes.Length);
        await _destination.WriteAsync(bytes, cancellationToken).ConfigureAwait(false);
        cancellationToken.ThrowIfCancellationRequested();
        _offset = nextOffset;
    }

    private void ValidateSlice(ParquetPhysicalType type, int values)
    {
        if (_columns[_columnOrdinal].Type != type)
            throw new ArgumentException($"{DescribeColumn()} does not accept {type} input.");
        if (values > _rowGroupRows - _columnRows)
            throw new ArgumentException($"{DescribeColumn()} would exceed the declared row count of {_rowGroupRows}.");
    }

    private WrittenColumnChunk[] RequireColumn()
    {
        RequireOpen();
        return _groupChunks ?? throw new InvalidOperationException("Begin a row group before writing a column.");
    }

    private string DescribeColumn() => $"Column '{_columns[_columnOrdinal].Name}' in row group {Groups.Count}";

    private void RequireOpen()
    {
        ObjectDisposedException.ThrowIf(_state == WriterState.Disposed, this);
        if (_state != WriterState.Open)
            throw new InvalidOperationException("The writer has completed or faulted.");
    }

    private void Enter()
    {
        if (Interlocked.CompareExchange(ref _busy, 1, 0) != 0)
            throw new InvalidOperationException("Another operation is active on the writer.");
    }

    private void Exit() => Volatile.Write(ref _busy, 0);

    private void Fail()
    {
        _state = WriterState.Faulted;
        ReleaseResources();
    }

    private void ReleaseResources()
    {
        _groups = null;
        _groupChunks = null;
        ReleasePageBuffers();
    }

    private void ReleasePageBuffers()
    {
        try
        {
            ReturnBuffer(ref _page);
        }
        finally
        {
            try { ReturnBuffer(ref _compressed); }
            finally { ReturnBuffer(ref _hashPositions); }
        }

        static void ReturnBuffer<T>(ref T[]? buffer)
        {
            var previous = buffer;
            buffer = null;
            if (previous is not null)
                ParquetArrayPool.Return(previous);
        }
    }

    private static byte[] EnsureBuffer(ref byte[]? buffer, int length, int preserveBytes)
    {
        var previous = buffer;
        if (previous is not null && previous.Length >= length)
            return previous;
        var replacement = ParquetArrayPool.Rent<byte>(length);
        previous.AsSpan(0, preserveBytes).CopyTo(replacement);
        buffer = replacement;
        if (previous is not null)
            ParquetArrayPool.Return(previous);
        return replacement;
    }
}
