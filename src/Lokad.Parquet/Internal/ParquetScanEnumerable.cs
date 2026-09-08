using System.Buffers.Binary;
using System.Diagnostics.CodeAnalysis;
using System.Numerics;
using System.Runtime.CompilerServices;

namespace Lokad.Parquet.Internal;

internal sealed class ParquetScanEnumerable : IAsyncEnumerable<ParquetBatch>
{
    private readonly ParquetFile _file;
    private readonly ParquetScanOptions _options;
    private readonly CancellationToken _cancellationToken;

    public ParquetScanEnumerable(
        ParquetFile file,
        ParquetScanOptions options,
        CancellationToken cancellationToken)
    {
        _file = file;
        _options = options;
        _cancellationToken = cancellationToken;
    }

    public IAsyncEnumerator<ParquetBatch> GetAsyncEnumerator(CancellationToken cancellationToken)
    {
        ScanPlan BuildPlan()
        {
            if (_options.Columns.Count == 0)
                throw new ArgumentException("At least one column must be projected.", "options");
            if (_options.Columns.Count != 1)
                throw new ParquetUnsupportedFeatureException("This scan path currently supports one projected column.");
            var selected = _options.Columns[0];
            var columnOrdinal = selected.Ordinal;
            if ((uint)columnOrdinal >= (uint)_file.Metadata.Schema.Columns.Count)
                throw new ArgumentOutOfRangeException("options", "A projected column ordinal is outside the schema.");
            var column = _file.Metadata.Schema.Columns[columnOrdinal];
            if (!ReferenceEquals(selected, column))
                throw new ArgumentException("A projected column descriptor belongs to another file.", "options");
            if (!column.IsReadable)
                throw new ParquetUnsupportedFeatureException(
                    column.UnsupportedReason ?? "The projected column is unsupported.", ParquetErrorLocation.AtColumn(column.Ordinal));
            ValidateTargetBatchRowCount(_file, _options);
            return new ScanPlan(column, BuildRowGroups(_file, _options));
        }

        var plan = BuildPlan();
        _file.EvictIdleColumnCaches(new int[] { plan.Column.Ordinal });
        return new Enumerator(new ColumnCursor(
                new ScanCursorServices(_file, _options, _file.ScanMemoryBudget, _file.PagePayloadCache),
                plan.Column,
                plan.RowGroups,
                _cancellationToken,
                cancellationToken,
                ScanLifetimeOwnership.Enumerator));
    }

    internal static void ValidateTargetBatchRowCount(ParquetFile file, ParquetScanOptions options)
    {
        if (options.TargetBatchRowCount <= 0 || options.TargetBatchRowCount > file.Options.MaximumRowsPerBatch)
            throw new ArgumentOutOfRangeException("options", "The target batch size is outside the reader limits.");
    }

    internal static ScanRowGroup[] BuildRowGroups(ParquetFile file, ParquetScanOptions options)
    {
        HashSet<int>? selection = null;
        if (options.RowGroups is not null)
        {
            if (options.RowGroups.Count == 0)
                return [];
            selection = [];
            foreach (var rowGroup in options.RowGroups)
            {
                var ordinal = rowGroup.Ordinal;
                if ((uint)ordinal >= (uint)file.Metadata.RowGroups.Count)
                    throw new ArgumentOutOfRangeException(nameof(options), "A row-group ordinal is outside the file.");
                if (!ReferenceEquals(file.Metadata.RowGroups[ordinal], rowGroup))
                    throw new ArgumentException("A selected row-group descriptor belongs to another file.", nameof(options));
                if (!selection.Add(ordinal))
                    throw new ArgumentException("Duplicate row-group descriptors are not permitted.", nameof(options));
            }
        }

        var range = options.RowRange ?? new ParquetRowRange(0, file.Metadata.RowCount);
        if (range.End > file.Metadata.RowCount)
            throw new ArgumentOutOfRangeException(nameof(options), "The row range lies outside the file.");
        if (range.Count == 0)
            return [];
        // Size the plan to the selection with two cheap metadata passes and no payload I/O.
        var matchCount = 0;
        foreach (var rowGroup in file.Metadata.RowGroups)
        {
            if (selection is not null && !selection.Contains(rowGroup.Ordinal))
                continue;
            var start = Math.Max(range.Start, rowGroup.RowOffset);
            var end = Math.Min(range.End, checked(rowGroup.RowOffset + rowGroup.RowCount));
            if (start < end)
                matchCount++;
        }
        if (matchCount == 0)
            return [];
        var plans = new ScanRowGroup[matchCount];
        var planCount = 0;
        foreach (var rowGroup in file.Metadata.RowGroups)
        {
            if (selection is not null && !selection.Contains(rowGroup.Ordinal))
                continue;
            var start = Math.Max(range.Start, rowGroup.RowOffset);
            var end = Math.Min(range.End, checked(rowGroup.RowOffset + rowGroup.RowCount));
            if (start < end)
                plans[planCount++] = new ScanRowGroup(rowGroup, start - rowGroup.RowOffset, end - start);
        }
        return plans;
    }

    private sealed class Enumerator(ColumnCursor cursor) : IAsyncEnumerator<ParquetBatch>
    {
        private ParquetBatch? _current;

        public ParquetBatch Current => _current ??
            throw new InvalidOperationException("The enumerator has no current batch.");

        public async ValueTask<bool> MoveNextAsync()
        {
            var moving = cursor.MoveNextAsync();
            var moved = moving.IsCompletedSuccessfully
                ? moving.Result
                : await moving.ConfigureAwait(false);
            if (!moved)
            {
                _current = null;
                return false;
            }
            var decoded = cursor.Current;
            var lifetime = new BatchLifetime();
            _current = new ParquetBatch(
                decoded.RowOffset,
                decoded.RowGroupOrdinal,
                decoded.RowOffsetInGroup,
                decoded.RowCount,
                [decoded.Column],
                lifetime,
                [decoded]);
            return true;
        }

        public ValueTask DisposeAsync()
        {
            cursor.Dispose();
            return ValueTask.CompletedTask;
        }
    }

    // Concrete internal cursor: disposal is synchronous, so coordinators release it directly without blocking fallback waits.
    internal sealed class ColumnCursor
    {
        private readonly ParquetFile _file;
        private readonly ParquetScanOptions _options;
        private readonly ScanRowGroup[] _rowGroups;
        private readonly ParquetColumn _column;
        private readonly CancellationTokenSource? _userLinkedCancellation;
        private readonly CancellationTokenSource? _linkedCancellation;
        private readonly CancellationToken _cancellationToken;
        private readonly ParquetScanMemoryBudget _memoryBudget;
        private readonly PooledArrayOwnerCache<byte> _pagePayloadCache;
        private readonly ScanLifetimeOwnership _lifetimeOwnership;

        private int _planIndex = -1;
        private ScanRowGroup _plan;
        private long _pageOffset;
        private long _chunkEnd;
        private long _rowsSeenInGroup;
        private int _pageOrdinal;
        // Fixed-width raw payload lease: an owned pool buffer or borrowed immutable memory. Kind distinguishes empty borrowed from none.
        private PagePayloadLease _pagePayloadLease;
        private IDisposable? _pageRowValuesOwner;
        private Array? _pageRowValues;
        // Explicit validity: None means no page loaded, AllValid means implicitly all-valid, Explicit holds the bitmap.
        private PageValidityState _pageValidity;
        private PooledArrayOwner<int>? _pageBinaryOffsets;
        private PooledArrayOwner<byte>? _pageBinaryPayload;
        private PooledArrayOwner<byte>? _pageFixedPayload;
        private int _pageFixedWidth;
        private IDisposable? _dictionaryOwner;
        private Array? _dictionaryValues;
        private PooledArrayOwner<int>? _dictionaryBinaryOffsets;
        private PooledArrayOwner<byte>? _dictionaryBinaryPayload;
        private PooledArrayOwner<byte>? _dictionaryFixedPayload;
        private int _dictionaryFixedWidth;
        private int _dictionaryCount;
        private bool _seenDataPage;
        private int _pageValueCount;
        private int _pageValueIndex;
        private long _pageRowOffset;
        private DecodedColumnBatch? _current;
        private bool _terminated;
        private bool _fileDisposed;
        private bool _disposed;

        private delegate void PlainDecode<T>(
            ReadOnlySpan<byte> source,
            Span<T> destination,
            CancellationToken cancellationToken)
            where T : unmanaged;

        public ColumnCursor(
            ScanCursorServices services,
            ParquetColumn column,
            ScanRowGroup[] rowGroups,
            CancellationToken scanCancellation,
            CancellationToken enumerationCancellation,
            ScanLifetimeOwnership lifetimeOwnership)
        {
            void OnFileDisposed()
            {
                _fileDisposed = true;
                _terminated = true;
                DisposePage();
                DisposeDictionary();
                _linkedCancellation?.Dispose();
                _userLinkedCancellation?.Dispose();
            }

            _file = services.File;
            _options = services.Options;
            _memoryBudget = services.MemoryBudget;
            _pagePayloadCache = services.PagePayloadCache;
            _lifetimeOwnership = lifetimeOwnership;
            _column = column;
            _rowGroups = rowGroups;
            if (lifetimeOwnership == ScanLifetimeOwnership.Coordinator)
            {
                _userLinkedCancellation = null;
                _linkedCancellation = null;
                _cancellationToken = scanCancellation;
            }
            else
            {
                if (scanCancellation.CanBeCanceled && enumerationCancellation.CanBeCanceled)
                {
                    _userLinkedCancellation = CancellationTokenSource.CreateLinkedTokenSource(
                        scanCancellation,
                        enumerationCancellation);
                }
                var userCancellation = _userLinkedCancellation?.Token ??
                    (scanCancellation.CanBeCanceled ? scanCancellation : enumerationCancellation);
                if (userCancellation.CanBeCanceled)
                {
                    _linkedCancellation = CancellationTokenSource.CreateLinkedTokenSource(
                        userCancellation,
                        _file.DisposalToken);
                    _cancellationToken = _linkedCancellation.Token;
                }
                else
                {
                    _linkedCancellation = null;
                    _cancellationToken = _file.DisposalToken;
                }
            }
            try
            {
                if (lifetimeOwnership == ScanLifetimeOwnership.Enumerator)
                    _file.RegisterScan(OnFileDisposed);
            }
            catch
            {
                _linkedCancellation?.Dispose();
                _userLinkedCancellation?.Dispose();
                throw;
            }
        }

        public DecodedColumnBatch Current => _current ?? throw new InvalidOperationException("The enumerator has no current batch.");

        public async ValueTask<bool> MoveNextAsync()
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            ObjectDisposedException.ThrowIf(_fileDisposed || _file.IsDisposed, _file);
            if (_terminated)
                return false;
            if (_current is not null && !_current.IsDisposed)
                throw new InvalidOperationException("Dispose the current Parquet batch before advancing the enumerator.");
            _current = null;

            if (_lifetimeOwnership == ScanLifetimeOwnership.Enumerator)
                _file.EnterScanOperation();
            try
            {
                _cancellationToken.ThrowIfCancellationRequested();
                while (true)
                {
                    if ((_pagePayloadLease.HasPayload || _pageRowValuesOwner is not null ||
                        _pageBinaryOffsets is not null || _pageFixedPayload is not null) &&
                        TryCreateBatchFromPage(out var batch))
                    {
                        _current = batch;
                        return true;
                    }

                    DisposePage();
                    if (_planIndex < 0 || _rowsSeenInGroup >= _plan.RowGroup.RowCount)
                    {
                        if (_planIndex >= 0 && _pageOffset != _chunkEnd)
                            throw new ParquetFormatException("A column chunk contains trailing pages or bytes after its declared rows.", ParquetErrorLocation.AtPage(_pageOffset, _plan.RowGroup.Ordinal, _column.Ordinal, _pageOrdinal));

                        if (!MoveToNextRowGroup())
                        {
                            Terminate();
                            return false;
                        }
                    }

                    await LoadNextPageAsync().ConfigureAwait(false);
                }
            }
            catch (OperationCanceledException) when (_file.IsDisposed)
            {
                Terminate();
                throw new ObjectDisposedException(nameof(ParquetFile));
            }
            catch
            {
                Terminate();
                throw;
            }
            finally
            {
                if (_lifetimeOwnership == ScanLifetimeOwnership.Enumerator)
                    _file.ExitScanOperation();
            }

            bool MoveToNextRowGroup()
            {
                DisposeDictionary();
                while (++_planIndex < _rowGroups.Length)
                {
                    _plan = _rowGroups[_planIndex];
                    if (_plan.Count == 0)
                        continue;
                    var chunk = _plan.RowGroup.Columns[_column.Ordinal];
                    if (chunk.ExternalFilePath is not null)
                        throw new ParquetUnsupportedFeatureException(
                            "External column chunks are unsupported.", ParquetErrorLocation.AtRowGroupColumn(_plan.RowGroup.Ordinal, _column.Ordinal));
                    if (chunk.HasCryptoMetadata || chunk.HasEncryptedMetadata || _file.Metadata.IsEncrypted)
                        throw new ParquetUnsupportedFeatureException(
                            "Encrypted column chunks are unsupported.", ParquetErrorLocation.AtRowGroupColumn(_plan.RowGroup.Ordinal, _column.Ordinal));
                    if (chunk.CompressionCodec is not ParquetCompressionCodec.Uncompressed and not ParquetCompressionCodec.Snappy)
                        throw new ParquetUnsupportedFeatureException(
                            "This scan path currently requires an uncompressed or Snappy column chunk.", ParquetErrorLocation.AtRowGroupColumn(_plan.RowGroup.Ordinal, _column.Ordinal));

                    _pageOffset = chunk.DictionaryPageOffset.HasValue
                        ? Math.Min(chunk.DictionaryPageOffset.Value, chunk.DataPageOffset)
                        : chunk.DataPageOffset;
                    _chunkEnd = checked(_pageOffset + chunk.TotalCompressedSize);
                    _rowsSeenInGroup = 0;
                    _pageOrdinal = 0;
                    _seenDataPage = false;
                    return true;
                }
                return false;
            }

            async ValueTask LoadNextPageAsync()
            {
                if (_pageOffset >= _chunkEnd)
                    throw new ParquetFormatException(
                        "A column chunk ended before its declared row count.", ParquetErrorLocation.AtPage(_pageOffset, _plan.RowGroup.Ordinal, _column.Ordinal, _pageOrdinal));

                if (_pageOrdinal >= _file.Options.MaximumPagesPerColumnChunk)
                    throw new ParquetLimitExceededException("A column chunk exceeds the configured page-count limit.", ParquetErrorLocation.AtPage(_pageOffset, _plan.RowGroup.Ordinal, _column.Ordinal, _pageOrdinal));


                var parsed = await ScanPageReader.ReadPageHeaderAsync(_file.Source, _file.Length, _file.Options, _pagePayloadCache, _cancellationToken, _pageOffset, _chunkEnd, _plan.RowGroup.Ordinal, _column.Ordinal, _pageOrdinal).ConfigureAwait(false);
                _cancellationToken.ThrowIfCancellationRequested();
                var header = parsed.Header;
                var payloadOffset = checked(_pageOffset + parsed.HeaderByteCount);
                var nextPageOffset = checked(payloadOffset + header.CompressedSize);
                if (nextPageOffset > _chunkEnd)
                    throw new ParquetFormatException(
                        "A page payload exceeds its enclosing column chunk.", ParquetErrorLocation.AtPage(_pageOffset, _plan.RowGroup.Ordinal, _column.Ordinal, _pageOrdinal));


                PooledArrayOwner<byte>? compressedPayload = null;
                PooledArrayOwner<byte>? decodedPayload = null;
                try
                {
                    ReadOnlyMemory<byte> compressedMemory;
                    if (ScanPageReader.TryGetSourceMemory(_file.Source, _file.Length, payloadOffset, header.CompressedSize, out var sourceMemory))
                    {
                        compressedMemory = sourceMemory;
                    }
                    else
                    {
                        compressedPayload = _pagePayloadCache.Rent(header.CompressedSize);
                        await ScanPageReader.ReadExactlyAsync(_file.Source, _cancellationToken, payloadOffset, new ArraySegment<byte>(compressedPayload.Array, 0, compressedPayload.Memory.Length), _plan.RowGroup.Ordinal, _column.Ordinal, _pageOrdinal)
                            .ConfigureAwait(false);
                        compressedMemory = compressedPayload.Memory;
                    }
                    _cancellationToken.ThrowIfCancellationRequested();
                    if (header.Crc is int expected && ScanPageReader.ComputeCrc32(compressedMemory.Span, _cancellationToken) != unchecked((uint)expected))
                        throw new ParquetFormatException(
                            "A page CRC does not match its serialized payload.", ParquetErrorLocation.AtPage(_pageOffset, _plan.RowGroup.Ordinal, _column.Ordinal, _pageOrdinal));

                    if (header.PageType is not ValidatedPageType.DataV1 and not ValidatedPageType.Dictionary and not ValidatedPageType.DataV2)
                        throw new ParquetUnsupportedFeatureException("This scan path requires Data Page V1 or V2.", ParquetErrorLocation.AtPage(_pageOffset, _plan.RowGroup.Ordinal, _column.Ordinal, _pageOrdinal));

                    var codec = _plan.RowGroup.Columns[_column.Ordinal].CompressionCodec;
                    ReadOnlyMemory<byte> decodedMemory;
                    if (header.PageType == ValidatedPageType.DataV2)
                    {
                        if (V2ValueSectionIsUncompressed(header, codec, out _))
                        {
                            if (header.CompressedSize != header.UncompressedSize)
                                throw PageFormat("An uncompressed V2 value section has inconsistent sizes.");
                            if (compressedPayload is null)
                            {
                                decodedMemory = compressedMemory;
                            }
                            else
                            {
                                decodedPayload = compressedPayload;
                                compressedPayload = null;
                                decodedMemory = decodedPayload.Memory;
                            }
                        }
                        else
                        {
                            decodedPayload = DecodeCompressedV2Payload(header, compressedMemory.Span);
                            decodedMemory = decodedPayload.Memory;
                        }
                    }
                    else switch (codec)
                        {
                            case ParquetCompressionCodec.Uncompressed:
                                if (header.UncompressedSize != header.CompressedSize)
                                    throw new ParquetFormatException(
                                        "An uncompressed page declares different compressed and uncompressed sizes.", ParquetErrorLocation.AtPage(_pageOffset, _plan.RowGroup.Ordinal, _column.Ordinal, _pageOrdinal));

                                if (compressedPayload is null)
                                {
                                    decodedMemory = compressedMemory;
                                }
                                else
                                {
                                    decodedPayload = compressedPayload;
                                    compressedPayload = null;
                                    decodedMemory = decodedPayload.Memory;
                                }
                                break;
                            case ParquetCompressionCodec.Snappy:
                                decodedPayload = PooledArrayOwner<byte>.Rent(header.UncompressedSize, _memoryBudget);
                                SnappyBlockDecoder.Decompress(
                                    compressedMemory.Span,
                                    decodedPayload.Memory.Span,
                                    _cancellationToken);
                                decodedMemory = decodedPayload.Memory;
                                break;
                            default:
                                throw new ParquetUnsupportedFeatureException("The page compression codec is unsupported.", ParquetErrorLocation.AtPage(_pageOffset, _plan.RowGroup.Ordinal, _column.Ordinal, _pageOrdinal));

                        }
                    var physicalType = _column.SchemaElement.PhysicalType;
                    if (physicalType is not ParquetPhysicalType.Boolean and
                        not ParquetPhysicalType.Int32 and
                        not ParquetPhysicalType.Int64 and
                        not ParquetPhysicalType.Float and
                        not ParquetPhysicalType.Double and
                        not ParquetPhysicalType.ByteArray and
                        not ParquetPhysicalType.FixedLengthByteArray ||
                        _column.SchemaElement.Repetition is not ParquetRepetition.Required and not ParquetRepetition.Optional)
                        throw new ParquetUnsupportedFeatureException("This scan path currently supports only required or optional flat Core 0.1 leaves.", ParquetErrorLocation.AtPage(_pageOffset, _plan.RowGroup.Ordinal, _column.Ordinal, _pageOrdinal));


                    if (header.PageType == ValidatedPageType.Dictionary)
                    {
                        if (_seenDataPage || HasDictionary)
                            throw PageFormat("A dictionary page is duplicated or appears after data pages.");
                        if (header.Dictionary.EncodingCode is not (int)ParquetEncoding.Plain and
                            not (int)ParquetEncoding.PlainDictionary)
                            throw new ParquetUnsupportedFeatureException("Dictionary values require the PLAIN or legacy PLAIN_DICTIONARY marker.", ParquetErrorLocation.AtPage(_pageOffset, _plan.RowGroup.Ordinal, _column.Ordinal, _pageOrdinal));

                        if (header.Dictionary.ValueCount > _file.Options.MaximumDictionaryEntries ||
                            header.UncompressedSize > _file.Options.MaximumDictionaryBytes)
                            throw new ParquetLimitExceededException("A dictionary exceeds the configured entry or byte limit.", ParquetErrorLocation.AtPage(_pageOffset, _plan.RowGroup.Ordinal, _column.Ordinal, _pageOrdinal));

                        DecodeDictionaryPage(decodedMemory.Span, header.Dictionary.ValueCount, physicalType.Value);
                        _pageOffset = nextPageOffset;
                        _pageOrdinal++;
                        return;
                    }

                    _seenDataPage = true;
                    var valueCount = header.PageType switch
                    {
                        ValidatedPageType.DataV1 => header.DataV1.ValueCount,
                        ValidatedPageType.DataV2 => header.DataV2.ValueCount,
                        _ => throw new InvalidOperationException("A validated data page has no data header."),
                    };
                    var encodingCode = header.PageType switch
                    {
                        ValidatedPageType.DataV1 => header.DataV1.EncodingCode,
                        ValidatedPageType.DataV2 => header.DataV2.EncodingCode,
                        _ => throw new InvalidOperationException("A validated data page has no data header."),
                    };
                    if (encodingCode is not (int)ParquetEncoding.Plain and
                        not (int)ParquetEncoding.PlainDictionary and
                        not (int)ParquetEncoding.RunLengthDictionary)
                        throw new ParquetUnsupportedFeatureException("The data-page value encoding is unsupported.", ParquetErrorLocation.AtPage(_pageOffset, _plan.RowGroup.Ordinal, _column.Ordinal, _pageOrdinal));

                    if (valueCount > _file.Options.MaximumValuesPerPage)
                        throw new ParquetLimitExceededException("A data page exceeds the configured value-count limit.", ParquetErrorLocation.AtPage(_pageOffset, _plan.RowGroup.Ordinal, _column.Ordinal, _pageOrdinal));

                    var dictionaryEncoded = encodingCode is (int)ParquetEncoding.PlainDictionary or
                        (int)ParquetEncoding.RunLengthDictionary;
                    if (dictionaryEncoded && !HasDictionary)
                        throw PageFormat("A dictionary-encoded data page has no preceding dictionary page.");

                    if (dictionaryEncoded)
                    {
                        DecodeDictionaryDataPage(header, decodedMemory.Span, valueCount, physicalType.Value);
                    }
                    else if (physicalType == ParquetPhysicalType.ByteArray)
                    {
                        DecodeBinaryPage(header, decodedMemory.Span, valueCount);
                    }
                    else if (physicalType == ParquetPhysicalType.FixedLengthByteArray)
                    {
                        DecodeFixedLengthByteArrayPage(header, decodedMemory.Span, valueCount);
                    }
                    else if (_column.SchemaElement.Repetition == ParquetRepetition.Required)
                    {
                        var valueOffset = header.PageType == ValidatedPageType.DataV2 ? DefinitionLevelCodec.GetV2LevelByteCount(header.DataV2, CurrentPageLocation()) : 0;
                        if (header.PageType == ValidatedPageType.DataV2 &&
                            (header.DataV2.NullCount != 0 || header.DataV2.RowCount != valueCount || valueOffset != 0))
                            throw PageFormat("A required flat V2 page has inconsistent row, null, or level fields.");
                        var expectedBytes = GetPlainByteCount(physicalType.Value, valueCount);
                        if (decodedMemory.Length - valueOffset != expectedBytes)
                            throw new ParquetFormatException(
                                "A PLAIN fixed-width page payload length does not match its value count.", ParquetErrorLocation.AtPage(_pageOffset, _plan.RowGroup.Ordinal, _column.Ordinal, _pageOrdinal));

                        if (physicalType == ParquetPhysicalType.Boolean)
                        {
                            DecodeRequiredBooleanPage(decodedMemory.Span[valueOffset..], valueCount);
                        }
                        else
                        {
                            if (valueOffset == 0)
                            {
                                if (decodedPayload is null)
                                {
                                    _pagePayloadLease.SetBorrowed(decodedMemory);
                                    _pageValidity.SetAllValid();
                                }
                                else
                                {
                                    _pagePayloadLease.SetOwned(decodedPayload);
                                    decodedPayload = null;
                                    _pageValidity.SetAllValid();
                                }
                            }
                            else
                            {
                                var valuesOnly = PooledArrayOwner<byte>.Rent(expectedBytes, _memoryBudget);
                                decodedMemory.Span[valueOffset..].CopyTo(valuesOnly.Memory.Span);
                                _pagePayloadLease.SetOwned(valuesOnly);
                                _pageValidity.SetAllValid();
                            }
                        }
                    }
                    else
                    {
                        if (header.PageType == ValidatedPageType.DataV1 && header.DataV1.DefinitionEncodingCode != (int)ParquetEncoding.RunLength)
                            throw new ParquetUnsupportedFeatureException("Optional definition levels require RLE/bit-packed hybrid encoding.", ParquetErrorLocation.AtPage(_pageOffset, _plan.RowGroup.Ordinal, _column.Ordinal, _pageOrdinal));

                        if (header.PageType == ValidatedPageType.DataV1)
                        {
                            DecodeOptionalPrimitivePageV1(decodedMemory.Span, valueCount, physicalType.Value);
                        }
                        else
                        {
                            if (header.PageType != ValidatedPageType.DataV2)
                                throw new InvalidOperationException("A validated V2 page has no V2 header.");
                            var v2 = header.DataV2;
                            if (v2.RowCount != valueCount)
                                throw PageFormat("A flat V2 page has different row and value counts.");
                            if (v2.RepetitionLevelsByteLength != 0)
                                throw PageFormat("A flat V2 page contains repetition-level bytes.");
                            DecodeOptionalPrimitivePage(
                                decodedMemory.Span,
                                valueCount,
                                physicalType.Value,
                                0,
                                v2.DefinitionLevelsByteLength,
                                v2.DefinitionLevelsByteLength,
                                v2.NullCount);
                        }
                    }
                    if (valueCount > _plan.RowGroup.RowCount - _rowsSeenInGroup)
                        throw new ParquetFormatException(
                            "Data pages contain more rows than the row group declares.", ParquetErrorLocation.AtPage(_pageOffset, _plan.RowGroup.Ordinal, _column.Ordinal, _pageOrdinal));


                    _pageValueCount = valueCount;
                    _pageValueIndex = 0;
                    _pageRowOffset = _rowsSeenInGroup;
                    _rowsSeenInGroup += _pageValueCount;
                    _pageOffset = nextPageOffset;
                    _pageOrdinal++;
                }
                finally
                {
                    decodedPayload?.Dispose();
                    compressedPayload?.Dispose();
                }
            }

            bool TryCreateBatchFromPage([NotNullWhen(true)] out DecodedColumnBatch? batch)
            {
                var pageEnd = checked(_pageRowOffset + _pageValueCount);
                var selectedStart = _plan.StartInGroup;
                var selectedEnd = checked(selectedStart + _plan.Count);
                var candidate = Math.Max(checked(_pageRowOffset + _pageValueIndex), selectedStart);
                if (candidate >= pageEnd || candidate >= selectedEnd)
                {
                    _pageValueIndex = _pageValueCount;
                    batch = null;
                    return false;
                }

                var available = Math.Min(pageEnd, selectedEnd) - candidate;
                var count = checked((int)Math.Min(available, _options.TargetBatchRowCount));
                var sourceIndex = checked((int)(candidate - _pageRowOffset));
                while (true)
                {
                    var emittedCount = count;
                    try
                    {
                        batch = _column.SchemaElement.PhysicalType switch
                        {
                            ParquetPhysicalType.Boolean => CreatePrimitiveBatch<bool>(
                                candidate, sourceIndex, count, 0, null),
                            ParquetPhysicalType.Int32 => CreatePrimitiveBatch<int>(
                                candidate, sourceIndex, count, sizeof(int), PlainDecoder.DecodeInt32),
                            ParquetPhysicalType.Int64 => CreatePrimitiveBatch<long>(
                                candidate, sourceIndex, count, sizeof(long), PlainDecoder.DecodeInt64),
                            ParquetPhysicalType.Float => CreatePrimitiveBatch<float>(
                                candidate, sourceIndex, count, sizeof(float), PlainDecoder.DecodeFloat),
                            ParquetPhysicalType.Double => CreatePrimitiveBatch<double>(
                                candidate, sourceIndex, count, sizeof(double), PlainDecoder.DecodeDouble),
                            ParquetPhysicalType.ByteArray => CreateBinaryBatch(
                                candidate, sourceIndex, ref emittedCount),
                            ParquetPhysicalType.FixedLengthByteArray => CreateFixedLengthByteArrayBatch(
                                candidate, sourceIndex, count),
                            _ => throw new ParquetUnsupportedFeatureException(
                                "The projected physical type is not available in the Core 0.1 scan path.", ParquetErrorLocation.AtRowGroupColumn(_plan.RowGroup.Ordinal, _column.Ordinal)),
                        };
                        _pageValueIndex = checked(sourceIndex + emittedCount);
                        return true;
                    }
                    catch (ParquetLimitExceededException) when (count > 1)
                    {
                        count = (count + 1) >> 1;
                    }
                }
            }

            DecodedColumnBatch CreatePrimitiveBatch<T>(
            long candidate,
            int sourceIndex,
            int count,
            int byteWidth,
            PlainDecode<T>? decoder)
            where T : unmanaged
            {
                if (sourceIndex == 0 && count == _pageValueCount &&
                    _pageRowValuesOwner is PooledArrayOwner<T> pageOwner)
                {
                    var lifetime = new BatchLifetime();
                    var takenValidity = _pageValidity.Take();
                    var validity = new ParquetValidity(
                        lifetime,
                        count,
                        takenValidity is null ? ReadOnlyMemory<byte>.Empty : takenValidity.Memory,
                        takenValidity is null);
                    var values = new ParquetPrimitiveColumnBatch<T>(
                        lifetime,
                        _column,
                        pageOwner.Memory,
                        validity);
                    IDisposable[] owners = takenValidity is null ? [pageOwner] : [pageOwner, takenValidity];
                    var transferredBatch = new DecodedColumnBatch(
                        checked(_plan.RowGroup.RowOffset + candidate),
                        _plan.RowGroup.Ordinal,
                        candidate,
                        count,
                        values,
                        lifetime,
                        owners);
                    _pageRowValuesOwner = null;
                    _pageRowValues = null;
                    return transferredBatch;
                }

                PooledArrayOwner<T>? owner = _file.RentColumnValues<T>(_column, count);
                PooledArrayOwner<byte>? validityOwner = null;
                try
                {
                    var output = owner.Memory.Span;
                    ReadOnlyMemory<byte> validityBits;
                    bool allValid;
                    if (_pageRowValues is T[] pageValues)
                    {
                        pageValues.AsSpan(sourceIndex, count).CopyTo(output);
                        validityOwner = CreateBatchValidity(
                            sourceIndex,
                            count,
                            out validityBits,
                            out allValid);
                    }
                    else
                    {
                        if (decoder is null || byteWidth <= 0)
                            throw new InvalidOperationException("The decoded page representation does not match its physical type.");
                        var input = _pagePayloadLease.Span;
                        if (!_pagePayloadLease.HasPayload || input.IsEmpty)
                            throw new InvalidOperationException("A decoded fixed-width page has no payload.");
                        decoder(
                            input.Slice(checked(sourceIndex * byteWidth), checked(count * byteWidth)),
                            output,
                            _cancellationToken);
                        if (sourceIndex == 0 && count == _pageValueCount)
                        {
                            _pagePayloadLease.Dispose();
                        }
                        validityBits = ReadOnlyMemory<byte>.Empty;
                        allValid = true;
                    }

                    var lifetime = new BatchLifetime();
                    var validity = new ParquetValidity(lifetime, count, validityBits, allValid);
                    var values = new ParquetPrimitiveColumnBatch<T>(lifetime, _column, owner.Memory, validity);
                    IDisposable[] owners = validityOwner is null ? [owner] : [owner, validityOwner];
                    var batch = new DecodedColumnBatch(
                        checked(_plan.RowGroup.RowOffset + candidate),
                        _plan.RowGroup.Ordinal,
                        candidate,
                        count,
                        values,
                        lifetime,
                        owners);
                    owner = null;
                    validityOwner = null;
                    return batch;
                }
                finally
                {
                    owner?.Dispose();
                    validityOwner?.Dispose();
                }
            }

            DecodedColumnBatch CreateBinaryBatch(long candidate, int sourceIndex, ref int count)
            {
                var sourceOffsets = (_pageBinaryOffsets ??
                    throw new InvalidOperationException("A decoded BYTE_ARRAY page has no offsets.")).Memory.Span;
                var baseOffset = sourceOffsets[sourceIndex];
                var low = 0;
                var high = count;
                while (low < high)
                {
                    var middle = low + ((high - low + 1) >> 1);
                    if (sourceOffsets[sourceIndex + middle] - baseOffset <= _file.Options.MaximumBinaryBatchBytes)
                        low = middle;
                    else
                        high = middle - 1;
                }
                if (low == 0)
                    throw new ParquetLimitExceededException(
                        "One binary value exceeds the configured batch payload limit.",
                        ParquetErrorLocation.AtRowGroupColumnPage(_plan.RowGroup.Ordinal, _column.Ordinal, _pageOrdinal - 1));
                count = low;
                var payloadLength = sourceOffsets[sourceIndex + count] - baseOffset;

                // A complete page transfers decoded storage into the batch without copying;
                // partial selections use the copy path below.
                if (sourceIndex == 0 && count == _pageValueCount && _pageBinaryOffsets is not null)
                {
                    var transferredLifetime = new BatchLifetime();
                    var takenBinaryValidity = _pageValidity.Take();
                    var transferredValidity = new ParquetValidity(
                        transferredLifetime,
                        count,
                        takenBinaryValidity is null ? ReadOnlyMemory<byte>.Empty : takenBinaryValidity.Memory,
                        takenBinaryValidity is null);
                    var transferredValues = new ParquetBinaryColumnBatch(
                        transferredLifetime,
                        _column,
                        count,
                        _pageBinaryPayload?.Memory ?? ReadOnlyMemory<byte>.Empty,
                        _pageBinaryOffsets.Memory,
                        transferredValidity);
                    var transferredOwnerCount = 1 + (_pageBinaryPayload is not null ? 1 : 0) + (takenBinaryValidity is not null ? 1 : 0);
                    var transferredOwners = new IDisposable[transferredOwnerCount];
                    transferredOwners[0] = _pageBinaryOffsets;
                    var transferredOwnerIndex = 1;
                    if (_pageBinaryPayload is not null)
                        transferredOwners[transferredOwnerIndex++] = _pageBinaryPayload;
                    if (takenBinaryValidity is not null)
                        transferredOwners[transferredOwnerIndex++] = takenBinaryValidity;
                    var transferredBatch = new DecodedColumnBatch(
                        checked(_plan.RowGroup.RowOffset + candidate),
                        _plan.RowGroup.Ordinal,
                        candidate,
                        count,
                        transferredValues,
                        transferredLifetime,
                        transferredOwners);
                    _pageBinaryOffsets = null;
                    _pageBinaryPayload = null;
                    return transferredBatch;
                }

                PooledArrayOwner<int>? offsets = null;
                PooledArrayOwner<byte>? payload = null;
                PooledArrayOwner<byte>? validityOwner = null;
                try
                {
                    offsets = PooledArrayOwner<int>.Rent(checked(count + 1), _memoryBudget);
                    payload = payloadLength == 0
                        ? null
                        : PooledArrayOwner<byte>.Rent(payloadLength, _memoryBudget);
                    for (var i = 0; i <= count; i++)
                        offsets.Memory.Span[i] = sourceOffsets[sourceIndex + i] - baseOffset;
                    if (payload is not null)
                        (_pageBinaryPayload ??
                            throw new InvalidOperationException("A decoded BYTE_ARRAY page has no payload."))
                        .Memory.Span.Slice(baseOffset, payloadLength).CopyTo(payload.Memory.Span);
                    validityOwner = CreateBatchValidity(sourceIndex, count, out var validityBits, out var allValid);

                    var lifetime = new BatchLifetime();
                    var validity = new ParquetValidity(lifetime, count, validityBits, allValid);
                    var values = new ParquetBinaryColumnBatch(
                        lifetime,
                        _column,
                        count,
                        payload?.Memory ?? ReadOnlyMemory<byte>.Empty,
                        offsets.Memory,
                        validity);
                    var ownerCount = 1 + (payload is not null ? 1 : 0) + (validityOwner is not null ? 1 : 0);
                    var owners = new IDisposable[ownerCount];
                    owners[0] = offsets;
                    var ownerIndex = 1;
                    if (payload is not null)
                        owners[ownerIndex++] = payload;
                    if (validityOwner is not null)
                        owners[ownerIndex++] = validityOwner;
                    var batch = new DecodedColumnBatch(
                        checked(_plan.RowGroup.RowOffset + candidate),
                        _plan.RowGroup.Ordinal,
                        candidate,
                        count,
                        values,
                        lifetime,
                        owners);
                    offsets = null;
                    payload = null;
                    validityOwner = null;
                    return batch;
                }
                finally
                {
                    offsets?.Dispose();
                    payload?.Dispose();
                    validityOwner?.Dispose();
                }
            }

            DecodedColumnBatch CreateFixedLengthByteArrayBatch(long candidate, int sourceIndex, int count)
            {
                // A complete page transfers decoded storage into the batch without copying;
                // partial selections use the copy path below.
                if (sourceIndex == 0 && count == _pageValueCount && _pageFixedPayload is not null)
                {
                    var transferredLifetime = new BatchLifetime();
                    var takenFixedValidity = _pageValidity.Take();
                    var transferredValidity = new ParquetValidity(
                        transferredLifetime,
                        count,
                        takenFixedValidity is null ? ReadOnlyMemory<byte>.Empty : takenFixedValidity.Memory,
                        takenFixedValidity is null);
                    var transferredValues = new ParquetFixedLengthByteArrayColumnBatch(
                        transferredLifetime,
                        _column,
                        count,
                        _pageFixedWidth,
                        _pageFixedPayload.Memory,
                        transferredValidity);
                    IDisposable[] transferredOwners = takenFixedValidity is null
                        ? [_pageFixedPayload]
                        : [_pageFixedPayload, takenFixedValidity];
                    var transferredBatch = new DecodedColumnBatch(
                        checked(_plan.RowGroup.RowOffset + candidate),
                        _plan.RowGroup.Ordinal,
                        candidate,
                        count,
                        transferredValues,
                        transferredLifetime,
                        transferredOwners);
                    _pageFixedPayload = null;
                    return transferredBatch;
                }

                var byteCount = checked(count * _pageFixedWidth);
                PooledArrayOwner<byte>? payload = PooledArrayOwner<byte>.Rent(byteCount, _memoryBudget);
                PooledArrayOwner<byte>? validityOwner = null;
                try
                {
                    (_pageFixedPayload ??
                        throw new InvalidOperationException("A decoded FIXED_LEN_BYTE_ARRAY page has no payload."))
                        .Memory.Span.Slice(checked(sourceIndex * _pageFixedWidth), byteCount)
                        .CopyTo(payload.Memory.Span);
                    validityOwner = CreateBatchValidity(sourceIndex, count, out var validityBits, out var allValid);
                    var lifetime = new BatchLifetime();
                    var validity = new ParquetValidity(lifetime, count, validityBits, allValid);
                    var values = new ParquetFixedLengthByteArrayColumnBatch(
                        lifetime,
                        _column,
                        count,
                        _pageFixedWidth,
                        payload.Memory,
                        validity);
                    IDisposable[] owners = validityOwner is null ? [payload] : [payload, validityOwner];
                    var batch = new DecodedColumnBatch(
                        checked(_plan.RowGroup.RowOffset + candidate),
                        _plan.RowGroup.Ordinal,
                        candidate,
                        count,
                        values,
                        lifetime,
                        owners);
                    payload = null;
                    validityOwner = null;
                    return batch;
                }
                finally
                {
                    payload?.Dispose();
                    validityOwner?.Dispose();
                }
            }

            PooledArrayOwner<byte>? CreateBatchValidity(
            int sourceIndex,
            int count,
            out ReadOnlyMemory<byte> bits,
            out bool allValid)
            {
                if (_column.SchemaElement.Repetition == ParquetRepetition.Required)
                {
                    bits = ReadOnlyMemory<byte>.Empty;
                    allValid = true;
                    return null;
                }

                // AllValid means the page was loaded with implicitly all-valid rows (for example the specialized optional INT32 path drops its bitmap).
                if (_pageValidity.Kind == PageValidityKind.AllValid)
                {
                    bits = ReadOnlyMemory<byte>.Empty;
                    allValid = true;
                    return null;
                }

                if (_pageValidity.Kind != PageValidityKind.Explicit)
                    throw new InvalidOperationException("No page validity is loaded.");
                return ValidityBitmap.CopySlice(
                    _pageValidity.Bits,
                    sourceIndex,
                    count,
                    _memoryBudget,
                    _cancellationToken,
                    out bits,
                    out allValid);
            }

            void DecodeBinaryPage(ValidatedPageHeader header, ReadOnlySpan<byte> payload, int rowCount)
            {
                using var levels = DefinitionLevelCodec.DecodeSection(header, payload, rowCount, _column.SchemaElement.Repetition, _memoryBudget, _cancellationToken, CurrentPageLocation(), out var physicalOffset);
                var levelSpan = levels is null ? ReadOnlySpan<int>.Empty : levels.Memory.Span;
                var offsetBytes = checked((rowCount + 1L) * sizeof(int));
                if (offsetBytes > _file.Options.MaximumScanPooledBytes)
                    throw new ParquetLimitExceededException("A BYTE_ARRAY page's offsets exceed the configured scan-memory limit.", ParquetErrorLocation.AtPage(_pageOffset, _plan.RowGroup.Ordinal, _column.Ordinal, _pageOrdinal));

                PooledArrayOwner<int>? offsets = PooledArrayOwner<int>.Rent(checked(rowCount + 1), _memoryBudget);
                PooledArrayOwner<byte>? data = null;
                PooledArrayOwner<byte>? validity = null;
                try
                {
                    offsets.Memory.Span.Clear();
                    var inputOffset = physicalOffset;
                    var aggregateLength = 0;
                    for (var row = 0; row < rowCount; row++)
                    {
                        if ((row & 1023) == 0)
                            _cancellationToken.ThrowIfCancellationRequested();
                        if (levels is not null && levelSpan[row] == 0)
                        {
                            offsets.Memory.Span[row + 1] = aggregateLength;
                            continue;
                        }
                        if (payload.Length - inputOffset < sizeof(int))
                            throw PageFormat("A PLAIN BYTE_ARRAY length is truncated.");
                        var length = BinaryPrimitives.ReadInt32LittleEndian(payload[inputOffset..]);
                        inputOffset += sizeof(int);
                        if (length < 0 || length > payload.Length - inputOffset)
                            throw PageFormat("A PLAIN BYTE_ARRAY length is invalid.");
                        if (length > _file.Options.MaximumBinaryValueBytes)
                            throw new ParquetLimitExceededException("A BYTE_ARRAY value exceeds the configured byte limit.", ParquetErrorLocation.AtPage(_pageOffset, _plan.RowGroup.Ordinal, _column.Ordinal, _pageOrdinal));

                        aggregateLength += length;
                        inputOffset += length;
                        offsets.Memory.Span[row + 1] = aggregateLength;
                    }
                    if (inputOffset != payload.Length)
                        throw PageFormat("A PLAIN BYTE_ARRAY page has trailing physical bytes.");
                    if (offsetBytes + aggregateLength > _file.Options.MaximumScanPooledBytes)
                        throw new ParquetLimitExceededException("A BYTE_ARRAY page exceeds the configured scan-memory limit.", ParquetErrorLocation.AtPage(_pageOffset, _plan.RowGroup.Ordinal, _column.Ordinal, _pageOrdinal));


                    if (aggregateLength != 0)
                        data = PooledArrayOwner<byte>.Rent(aggregateLength, _memoryBudget);
                    inputOffset = physicalOffset;
                    var outputOffset = 0;
                    for (var row = 0; row < rowCount; row++)
                    {
                        if ((row & 1023) == 0)
                            _cancellationToken.ThrowIfCancellationRequested();
                        if (levels is not null && levelSpan[row] == 0)
                            continue;
                        var length = BinaryPrimitives.ReadInt32LittleEndian(payload[inputOffset..]);
                        inputOffset += sizeof(int);
                        if (length != 0)
                            payload.Slice(inputOffset, length).CopyTo(
                                (data ?? throw new InvalidOperationException("A non-empty BYTE_ARRAY page has no payload buffer."))
                                .Memory.Span[outputOffset..]);
                        inputOffset += length;
                        outputOffset += length;
                    }
                    validity = DefinitionLevelCodec.CreateBitmap(levelSpan, _memoryBudget, _cancellationToken);
                    _pageBinaryOffsets = offsets;
                    _pageBinaryPayload = data;
                    _pageValidity.SetFromNullable(validity);
                    offsets = null;
                    data = null;
                    validity = null;
                }
                finally
                {
                    offsets?.Dispose();
                    data?.Dispose();
                    validity?.Dispose();
                }
            }

            void DecodeFixedLengthByteArrayPage(ValidatedPageHeader header, ReadOnlySpan<byte> payload, int rowCount)
            {
                var width = _column.SchemaElement.TypeLength ?? 0;
                if (width <= 0)
                    throw PageFormat("A FIXED_LEN_BYTE_ARRAY column has an invalid width.");
                using var levels = DefinitionLevelCodec.DecodeSection(header, payload, rowCount, _column.SchemaElement.Repetition, _memoryBudget, _cancellationToken, CurrentPageLocation(), out var physicalOffset);
                var levelSpan = levels is null ? ReadOnlySpan<int>.Empty : levels.Memory.Span;
                var physicalCount = levels is null ? rowCount : rowCount - DefinitionLevelCodec.CountLevel(levelSpan, 0);
                var physicalBytesAvailable = payload.Length - physicalOffset;
                if (physicalCount > physicalBytesAvailable / width || physicalCount * width != physicalBytesAvailable)
                    throw PageFormat("A PLAIN FIXED_LEN_BYTE_ARRAY payload length is inconsistent.");
                var physicalByteCount = physicalCount * width;
                var rowByteCount = checked((long)rowCount * width);
                if (rowByteCount > int.MaxValue || rowByteCount > _file.Options.MaximumScanPooledBytes)
                    throw new ParquetLimitExceededException("A FIXED_LEN_BYTE_ARRAY page exceeds the configured scan-memory limit.", ParquetErrorLocation.AtPage(_pageOffset, _plan.RowGroup.Ordinal, _column.Ordinal, _pageOrdinal));


                PooledArrayOwner<byte>? values = PooledArrayOwner<byte>.Rent((int)rowByteCount, _memoryBudget);
                PooledArrayOwner<byte>? validity = null;
                try
                {
                    values.Memory.Span.Clear();
                    var physicalIndex = 0;
                    for (var row = 0; row < rowCount; row++)
                    {
                        if ((row & 1023) == 0)
                            _cancellationToken.ThrowIfCancellationRequested();
                        if (levels is not null && levelSpan[row] == 0)
                            continue;
                        payload.Slice(physicalOffset + physicalIndex * width, width)
                            .CopyTo(values.Memory.Span.Slice(row * width, width));
                        physicalIndex++;
                    }
                    validity = DefinitionLevelCodec.CreateBitmap(levelSpan, _memoryBudget, _cancellationToken);
                    _pageFixedPayload = values;
                    _pageFixedWidth = width;
                    _pageValidity.SetFromNullable(validity);
                    values = null;
                    validity = null;
                }
                finally
                {
                    values?.Dispose();
                    validity?.Dispose();
                }
            }

            void DecodeOptionalPrimitivePageV1(ReadOnlySpan<byte> payload, int rowCount, ParquetPhysicalType physicalType)
            {
                if (payload.Length < sizeof(int))
                    throw PageFormat("An optional V1 page is missing its definition-level length.");
                var levelByteCount = BinaryPrimitives.ReadInt32LittleEndian(payload);
                if (levelByteCount < 0 || levelByteCount > payload.Length - sizeof(int))
                    throw PageFormat("An optional V1 page has an invalid definition-level length.");

                DecodeOptionalPrimitivePage(
                    payload,
                    rowCount,
                    physicalType,
                    sizeof(int),
                    levelByteCount,
                    checked(sizeof(int) + levelByteCount),
                    null);
            }

            void DecodeOptionalPrimitivePage(
            ReadOnlySpan<byte> payload,
            int rowCount,
            ParquetPhysicalType physicalType,
            int levelOffset,
            int levelByteCount,
            int physicalOffset,
            int? expectedNullCount)
            {
                if (levelOffset < 0 || levelByteCount < 0 || physicalOffset < 0 ||
                    levelOffset > payload.Length || levelByteCount > payload.Length - levelOffset ||
                    physicalOffset > payload.Length)
                    throw PageFormat("An optional page has invalid level or value boundaries.");

                if (physicalType == ParquetPhysicalType.Int32)
                {
                    PooledArrayOwner<int>? rowValues = null;
                    PooledArrayOwner<byte>? validity = null;
                    try
                    {
                        rowValues = _file.RentColumnValues<int>(_column, rowCount);
                        validity = PooledArrayOwner<byte>.Rent(
                            checked((rowCount + 7) / 8),
                            _memoryBudget);
                        var int32LevelInput = payload.Slice(levelOffset, levelByteCount);
                        int int32Consumed;
                        int validCount;
                        try
                        {
                            int32Consumed = RleBitPackedHybridDecoder.DecodeBitWidthOneToBitmap(
                                int32LevelInput,
                                rowCount,
                                validity.Memory.Span,
                                _cancellationToken,
                                out validCount);
                        }
                        catch (ParquetFormatException exception) when (exception.ByteOffset is null)
                        {
                            throw new ParquetFormatException(exception.Message, exception, ParquetErrorLocation.AtPage(_pageOffset, _plan.RowGroup.Ordinal, _column.Ordinal, _pageOrdinal));

                        }
                        if (int32Consumed != int32LevelInput.Length)
                            throw PageFormat("An optional page has trailing definition-level bytes.");
                        if (expectedNullCount is int int32NullCount && rowCount - validCount != int32NullCount)
                            throw PageFormat("A V2 page null count does not match its definition levels.");

                        var int32PhysicalByteCount = payload.Length - physicalOffset;
                        if ((int32PhysicalByteCount & (sizeof(int) - 1)) != 0 ||
                            int32PhysicalByteCount / sizeof(int) != validCount)
                            throw PageFormat("An optional fixed-width page has an inconsistent physical-value length.");
                        var int32PhysicalPayload = payload.Slice(physicalOffset, int32PhysicalByteCount);
                        var physicalByteOffset = 0;
                        var int32Output = rowValues.Memory.Span;
                        if (validCount == rowCount)
                        {
                            PlainDecoder.DecodeInt32(int32PhysicalPayload, int32Output, _cancellationToken);
                            physicalByteOffset = int32PhysicalPayload.Length;
                        }
                        else
                        {
                            int32Output.Clear();
                            var validityBytes = validity.Memory.Span;
                            for (var byteIndex = 0; byteIndex < validityBytes.Length; byteIndex++)
                            {
                                if ((byteIndex & 511) == 0)
                                    _cancellationToken.ThrowIfCancellationRequested();
                                var remainingBits = (uint)validityBytes[byteIndex];
                                while (remainingBits != 0)
                                {
                                    var bitIndex = BitOperations.TrailingZeroCount(remainingBits);
                                    int32Output[(byteIndex << 3) + bitIndex] =
                                        BinaryPrimitives.ReadInt32LittleEndian(
                                            int32PhysicalPayload.Slice(physicalByteOffset, sizeof(int)));
                                    physicalByteOffset += sizeof(int);
                                    remainingBits &= remainingBits - 1;
                                }
                            }
                        }
                        if (physicalByteOffset != int32PhysicalPayload.Length)
                            throw PageFormat("Definition levels do not match the physical-value count.");

                        _pageRowValuesOwner = rowValues;
                        _pageRowValues = rowValues.Array;
                        rowValues = null;
                        if (validCount == rowCount)
                        {
                            validity.Dispose();
                            validity = null;
                            _pageValidity.SetAllValid();
                        }
                        else
                        {
                            _pageValidity.SetExplicit(validity);
                            validity = null;
                        }
                    }
                    finally
                    {
                        rowValues?.Dispose();
                        validity?.Dispose();
                    }
                    return;
                }

                using var levels = PooledArrayOwner<int>.Rent(rowCount, _memoryBudget);
                var levelInput = payload.Slice(levelOffset, levelByteCount);
                var physicalCount = DefinitionLevelCodec.DecodeValidated(levelInput, levels.Memory.Span, expectedNullCount, _cancellationToken, CurrentPageLocation());
                var physicalByteCount = GetPlainByteCount(physicalType, physicalCount);
                if (physicalByteCount != payload.Length - physicalOffset)
                    throw PageFormat("An optional fixed-width page has an inconsistent physical-value length.");

                var physicalPayload = payload.Slice(physicalOffset, physicalByteCount);
                switch (physicalType)
                {
                    case ParquetPhysicalType.Boolean:
                        DecodeOptionalValues<bool>(physicalPayload, levels.Memory.Span, physicalCount, PlainDecoder.DecodeBoolean);
                        break;
                    case ParquetPhysicalType.Int64:
                        DecodeOptionalValues<long>(physicalPayload, levels.Memory.Span, physicalCount, PlainDecoder.DecodeInt64);
                        break;
                    case ParquetPhysicalType.Float:
                        DecodeOptionalValues<float>(physicalPayload, levels.Memory.Span, physicalCount, PlainDecoder.DecodeFloat);
                        break;
                    case ParquetPhysicalType.Double:
                        DecodeOptionalValues<double>(physicalPayload, levels.Memory.Span, physicalCount, PlainDecoder.DecodeDouble);
                        break;
                    default:
                        throw new ParquetUnsupportedFeatureException("The optional physical type is unsupported.", ParquetErrorLocation.AtPage(_pageOffset, _plan.RowGroup.Ordinal, _column.Ordinal, _pageOrdinal));

                }
            }

            // Validates the V2 level section and reports whether the value section is
            // stored uncompressed, in which case the serialized payload is reused
            // without copying. Snappy value sections still need output storage.
            bool V2ValueSectionIsUncompressed(
            ValidatedPageHeader header,
            ParquetCompressionCodec? codec,
            out int levelByteCount)
            {
                _cancellationToken.ThrowIfCancellationRequested();
                if (header.PageType != ValidatedPageType.DataV2)
                    throw new InvalidOperationException("A validated V2 page has no V2 header.");
                var v2 = header.DataV2;
                levelByteCount = DefinitionLevelCodec.GetV2LevelByteCount(v2, CurrentPageLocation());
                if (levelByteCount > header.CompressedSize || levelByteCount > header.UncompressedSize)
                    throw PageFormat("A V2 level section exceeds its page payload.");
                if (!v2.IsCompressed || codec == ParquetCompressionCodec.Uncompressed)
                    return true;
                if (codec == ParquetCompressionCodec.Snappy)
                    return false;
                throw new ParquetUnsupportedFeatureException("The V2 value-section compression codec is unsupported.", ParquetErrorLocation.AtPage(_pageOffset, _plan.RowGroup.Ordinal, _column.Ordinal, _pageOrdinal));

            }

            PooledArrayOwner<byte> DecodeCompressedV2Payload(
            ValidatedPageHeader header,
            ReadOnlySpan<byte> serializedPayload)
            {
                _cancellationToken.ThrowIfCancellationRequested();
                if (header.PageType != ValidatedPageType.DataV2)
                    throw new InvalidOperationException("A validated V2 page has no V2 header.");
                var levelByteCount = DefinitionLevelCodec.GetV2LevelByteCount(header.DataV2, CurrentPageLocation());
                PooledArrayOwner<byte>? decoded = PooledArrayOwner<byte>.Rent(header.UncompressedSize, _memoryBudget);
                try
                {
                    serializedPayload[..levelByteCount].CopyTo(decoded.Memory.Span);
                    SnappyBlockDecoder.Decompress(
                        serializedPayload[levelByteCount..],
                        decoded.Memory.Span[levelByteCount..],
                        _cancellationToken);
                    var result = decoded;
                    decoded = null;
                    return result;
                }
                finally
                {
                    decoded?.Dispose();
                }
            }

            void DecodeOptionalValues<T>(
            ReadOnlySpan<byte> physicalPayload,
            ReadOnlySpan<int> levels,
            int physicalCount,
            PlainDecode<T> decoder)
            where T : unmanaged
            {
                using var physicalValues = PooledArrayOwner<T>.Rent(physicalCount, _memoryBudget);
                decoder(physicalPayload, physicalValues.Memory.Span, _cancellationToken);

                PooledArrayOwner<T>? rowValues = null;
                PooledArrayOwner<byte>? validity = null;
                try
                {
                    rowValues = _file.RentColumnValues<T>(_column, levels.Length);
                    validity = PooledArrayOwner<byte>.Rent(
                        checked((levels.Length + 7) / 8),
                        _memoryBudget);
                    rowValues.Memory.Span.Clear();
                    validity.Memory.Span.Clear();
                    var physicalIndex = 0;
                    for (var row = 0; row < levels.Length; row++)
                    {
                        if ((row & 1023) == 0)
                            _cancellationToken.ThrowIfCancellationRequested();
                        if (levels[row] == 0)
                            continue;
                        rowValues.Memory.Span[row] = physicalValues.Memory.Span[physicalIndex++];
                        validity.Memory.Span[row >> 3] |= (byte)(1 << (row & 7));
                    }
                    if (physicalIndex != physicalCount)
                        throw PageFormat("Definition levels do not match the physical-value count.");
                    _pageRowValuesOwner = rowValues;
                    _pageRowValues = rowValues.Array;
                    _pageValidity.SetExplicit(validity);
                    rowValues = null;
                    validity = null;
                }
                finally
                {
                    rowValues?.Dispose();
                    validity?.Dispose();
                }
            }

            void DecodeDictionaryPage(ReadOnlySpan<byte> payload, int valueCount, ParquetPhysicalType physicalType)
            {
                if (physicalType == ParquetPhysicalType.ByteArray)
                {
                    SetBinaryDictionary(payload, valueCount);
                    return;
                }
                if (physicalType == ParquetPhysicalType.FixedLengthByteArray)
                {
                    SetFixedDictionary(payload, valueCount);
                    return;
                }

                var expectedByteCount = GetPlainByteCount(physicalType, valueCount);
                if (payload.Length != expectedByteCount)
                    throw PageFormat("A PLAIN dictionary payload length does not match its entry count.");

                switch (physicalType)
                {
                    case ParquetPhysicalType.Boolean:
                        SetDictionary<bool>(payload, valueCount, PlainDecoder.DecodeBoolean);
                        break;
                    case ParquetPhysicalType.Int32:
                        SetDictionary<int>(payload, valueCount, PlainDecoder.DecodeInt32);
                        break;
                    case ParquetPhysicalType.Int64:
                        SetDictionary<long>(payload, valueCount, PlainDecoder.DecodeInt64);
                        break;
                    case ParquetPhysicalType.Float:
                        SetDictionary<float>(payload, valueCount, PlainDecoder.DecodeFloat);
                        break;
                    case ParquetPhysicalType.Double:
                        SetDictionary<double>(payload, valueCount, PlainDecoder.DecodeDouble);
                        break;
                    default:
                        throw new ParquetUnsupportedFeatureException("The dictionary physical type is unsupported.", ParquetErrorLocation.AtPage(_pageOffset, _plan.RowGroup.Ordinal, _column.Ordinal, _pageOrdinal));

                }
            }

            void SetBinaryDictionary(ReadOnlySpan<byte> payload, int valueCount)
            {
                // Serialized bytes were checked against MaximumDictionaryBytes at the page header; decoded layout is checked after lengths are known below.
                // Compact decoded layout is offsets plus payload bytes without length prefixes.
                PooledArrayOwner<int>? offsets = PooledArrayOwner<int>.Rent(checked(valueCount + 1), _memoryBudget);
                PooledArrayOwner<byte>? values = null;
                try
                {
                    var inputOffset = 0;
                    var aggregateLength = 0;
                    offsets.Memory.Span[0] = 0;
                    for (var index = 0; index < valueCount; index++)
                    {
                        if (payload.Length - inputOffset < sizeof(int))
                            throw PageFormat("A PLAIN BYTE_ARRAY dictionary length is truncated.");
                        var length = BinaryPrimitives.ReadInt32LittleEndian(payload[inputOffset..]);
                        inputOffset += sizeof(int);
                        if (length < 0 || length > payload.Length - inputOffset)
                            throw PageFormat("A PLAIN BYTE_ARRAY dictionary length is invalid.");
                        if (length > _file.Options.MaximumBinaryValueBytes)
                            throw new ParquetLimitExceededException("A BYTE_ARRAY dictionary value exceeds the configured byte limit.", ParquetErrorLocation.AtPage(_pageOffset, _plan.RowGroup.Ordinal, _column.Ordinal, _pageOrdinal));

                        aggregateLength += length;
                        inputOffset += length;
                        offsets.Memory.Span[index + 1] = aggregateLength;
                    }
                    if (inputOffset != payload.Length)
                        throw PageFormat("A PLAIN BYTE_ARRAY dictionary has trailing bytes.");
                    var decodedBytes = checked((valueCount + 1L) * sizeof(int) + (long)aggregateLength);
                    if (decodedBytes > _file.Options.MaximumDictionaryBytes)
                        throw new ParquetLimitExceededException("A decoded BYTE_ARRAY dictionary exceeds its configured byte limit.", ParquetErrorLocation.AtPage(_pageOffset, _plan.RowGroup.Ordinal, _column.Ordinal, _pageOrdinal));


                    values = PooledArrayOwner<byte>.Rent(aggregateLength, _memoryBudget);
                    inputOffset = 0;
                    var outputOffset = 0;
                    for (var index = 0; index < valueCount; index++)
                    {
                        var length = BinaryPrimitives.ReadInt32LittleEndian(payload[inputOffset..]);
                        inputOffset += sizeof(int);
                        payload.Slice(inputOffset, length).CopyTo(values.Memory.Span[outputOffset..]);
                        inputOffset += length;
                        outputOffset += length;
                    }

                    _dictionaryBinaryOffsets = offsets;
                    _dictionaryBinaryPayload = values;
                    _dictionaryCount = valueCount;
                    offsets = null;
                    values = null;
                }
                finally
                {
                    offsets?.Dispose();
                    values?.Dispose();
                }
            }

            void SetFixedDictionary(ReadOnlySpan<byte> payload, int valueCount)
            {
                var width = _column.SchemaElement.TypeLength ?? 0;
                if (width <= 0 || valueCount > payload.Length / width || valueCount * width != payload.Length)
                    throw PageFormat("A PLAIN FIXED_LEN_BYTE_ARRAY dictionary payload length is inconsistent.");
                var values = PooledArrayOwner<byte>.Rent(payload.Length, _memoryBudget);
                payload.CopyTo(values.Memory.Span);
                _dictionaryFixedPayload = values;
                _dictionaryFixedWidth = width;
                _dictionaryCount = valueCount;
            }

            void SetDictionary<T>(ReadOnlySpan<byte> payload, int valueCount, PlainDecode<T> decoder)
            where T : unmanaged
            {
                long decodedBytes = typeof(T) == typeof(bool) ? valueCount : typeof(T) == typeof(int) || typeof(T) == typeof(float) ? checked((long)valueCount * sizeof(int)) : checked((long)valueCount * sizeof(long));
                if (decodedBytes > _file.Options.MaximumDictionaryBytes)
                    throw new ParquetLimitExceededException("A decoded dictionary exceeds its configured byte limit.", ParquetErrorLocation.AtPage(_pageOffset, _plan.RowGroup.Ordinal, _column.Ordinal, _pageOrdinal));

                PooledArrayOwner<T>? values = PooledArrayOwner<T>.Rent(valueCount, _memoryBudget);
                try
                {
                    decoder(payload, values.Memory.Span, _cancellationToken);
                    _dictionaryOwner = values;
                    _dictionaryValues = values.Array;
                    _dictionaryCount = valueCount;
                    values = null;
                }
                finally
                {
                    values?.Dispose();
                }
            }

            void DecodeDictionaryDataPage(
            ValidatedPageHeader header,
            ReadOnlySpan<byte> payload,
            int rowCount,
            ParquetPhysicalType physicalType)
            {
                using var levels = DefinitionLevelCodec.DecodeSection(header, payload, rowCount, _column.SchemaElement.Repetition, _memoryBudget, _cancellationToken, CurrentPageLocation(), out var physicalOffset);
                var levelSpan = levels is null ? ReadOnlySpan<int>.Empty : levels.Memory.Span;
                var physicalCount = levels is null ? rowCount : rowCount - DefinitionLevelCodec.CountLevel(levelSpan, 0);

                if (physicalOffset >= payload.Length)
                    throw PageFormat("A dictionary data page is missing its index bit width.");
                var bitWidth = payload[physicalOffset++];
                using var indices = PooledArrayOwner<int>.Rent(physicalCount, _memoryBudget);
                var encodedIndices = payload[physicalOffset..];
                int indexConsumed;
                try
                {
                    indexConsumed = RleBitPackedHybridDecoder.Decode(
                        encodedIndices,
                        bitWidth,
                        indices.Memory.Span,
                        _cancellationToken);
                }
                catch (ParquetFormatException exception) when (exception.ByteOffset is null)
                {
                    throw new ParquetFormatException(exception.Message, exception, ParquetErrorLocation.AtPage(_pageOffset, _plan.RowGroup.Ordinal, _column.Ordinal, _pageOrdinal));

                }
                if (indexConsumed != encodedIndices.Length)
                    throw PageFormat("A dictionary data page has trailing index bytes.");
                foreach (var index in indices.Memory.Span)
                {
                    if ((uint)index >= (uint)_dictionaryCount)
                        throw PageFormat("A dictionary index is outside the dictionary.");
                }

                switch (physicalType)
                {
                    case ParquetPhysicalType.Boolean:
                        ExpandDictionary<bool>(indices.Memory.Span, levelSpan);
                        break;
                    case ParquetPhysicalType.Int32:
                        ExpandDictionary<int>(indices.Memory.Span, levelSpan);
                        break;
                    case ParquetPhysicalType.Int64:
                        ExpandDictionary<long>(indices.Memory.Span, levelSpan);
                        break;
                    case ParquetPhysicalType.Float:
                        ExpandDictionary<float>(indices.Memory.Span, levelSpan);
                        break;
                    case ParquetPhysicalType.Double:
                        ExpandDictionary<double>(indices.Memory.Span, levelSpan);
                        break;
                    case ParquetPhysicalType.ByteArray:
                        ExpandBinaryDictionary(indices.Memory.Span, levelSpan);
                        break;
                    case ParquetPhysicalType.FixedLengthByteArray:
                        ExpandFixedDictionary(indices.Memory.Span, levelSpan);
                        break;
                    default:
                        throw new ParquetUnsupportedFeatureException("The dictionary physical type is unsupported.", ParquetErrorLocation.AtPage(_pageOffset, _plan.RowGroup.Ordinal, _column.Ordinal, _pageOrdinal));

                }
            }

            void ExpandBinaryDictionary(ReadOnlySpan<int> indices, ReadOnlySpan<int> levels)
            {
                if (_dictionaryBinaryOffsets is null || _dictionaryBinaryPayload is null)
                    throw new InvalidOperationException("The dictionary representation does not match its physical type.");

                var optional = _column.SchemaElement.Repetition == ParquetRepetition.Optional;
                var rowCount = optional ? levels.Length : indices.Length;
                var dictionaryOffsets = _dictionaryBinaryOffsets.Memory.Span;
                long aggregateLength = 0;
                var physicalIndex = 0;
                _cancellationToken.ThrowIfCancellationRequested();
                for (var row = 0; row < rowCount; row++)
                {
                    if ((row & 1023) == 0)
                        _cancellationToken.ThrowIfCancellationRequested();
                    if (optional && levels[row] == 0)
                        continue;
                    var dictionaryIndex = indices[physicalIndex++];
                    aggregateLength += dictionaryOffsets[dictionaryIndex + 1] - dictionaryOffsets[dictionaryIndex];
                }
                if (physicalIndex != indices.Length)
                    throw PageFormat("Dictionary indices do not match the definition levels.");
                var retainedBytes = aggregateLength + checked((rowCount + 1L) * sizeof(int));
                if (aggregateLength > int.MaxValue || retainedBytes > _file.Options.MaximumScanPooledBytes)
                    throw new ParquetLimitExceededException("An expanded BYTE_ARRAY dictionary page exceeds the configured scan-memory limit.", ParquetErrorLocation.AtPage(_pageOffset, _plan.RowGroup.Ordinal, _column.Ordinal, _pageOrdinal));


                PooledArrayOwner<int>? offsets = null;
                PooledArrayOwner<byte>? payload = null;
                PooledArrayOwner<byte>? validity = null;
                try
                {
                    offsets = PooledArrayOwner<int>.Rent(checked(rowCount + 1), _memoryBudget);
                    payload = PooledArrayOwner<byte>.Rent((int)aggregateLength, _memoryBudget);
                    offsets.Memory.Span[0] = 0;
                    var outputOffset = 0;
                    physicalIndex = 0;
                    for (var row = 0; row < rowCount; row++)
                    {
                        if ((row & 1023) == 0)
                            _cancellationToken.ThrowIfCancellationRequested();
                        if (!optional || levels[row] != 0)
                        {
                            var dictionaryIndex = indices[physicalIndex++];
                            var start = dictionaryOffsets[dictionaryIndex];
                            var length = dictionaryOffsets[dictionaryIndex + 1] - start;
                            _dictionaryBinaryPayload.Memory.Span.Slice(start, length)
                                .CopyTo(payload.Memory.Span[outputOffset..]);
                            outputOffset += length;
                        }
                        offsets.Memory.Span[row + 1] = outputOffset;
                    }
                    validity = DefinitionLevelCodec.CreateBitmap(levels, _memoryBudget, _cancellationToken);
                    _pageBinaryOffsets = offsets;
                    _pageBinaryPayload = payload;
                    _pageValidity.SetFromNullable(validity);
                    offsets = null;
                    payload = null;
                    validity = null;
                }
                finally
                {
                    offsets?.Dispose();
                    payload?.Dispose();
                    validity?.Dispose();
                }
            }

            void ExpandFixedDictionary(ReadOnlySpan<int> indices, ReadOnlySpan<int> levels)
            {
                if (_dictionaryFixedPayload is null || _dictionaryFixedWidth <= 0)
                    throw new InvalidOperationException("The dictionary representation does not match its physical type.");

                var optional = _column.SchemaElement.Repetition == ParquetRepetition.Optional;
                var rowCount = optional ? levels.Length : indices.Length;
                var byteCount = checked((long)rowCount * _dictionaryFixedWidth);
                if (byteCount > int.MaxValue || byteCount > _file.Options.MaximumScanPooledBytes)
                    throw new ParquetLimitExceededException("An expanded FIXED_LEN_BYTE_ARRAY dictionary page exceeds the configured scan-memory limit.", ParquetErrorLocation.AtPage(_pageOffset, _plan.RowGroup.Ordinal, _column.Ordinal, _pageOrdinal));


                PooledArrayOwner<byte>? payload = PooledArrayOwner<byte>.Rent((int)byteCount, _memoryBudget);
                PooledArrayOwner<byte>? validity = null;
                try
                {
                    payload.Memory.Span.Clear();
                    _cancellationToken.ThrowIfCancellationRequested();
                    var physicalIndex = 0;
                    for (var row = 0; row < rowCount; row++)
                    {
                        if ((row & 1023) == 0)
                            _cancellationToken.ThrowIfCancellationRequested();
                        if (optional && levels[row] == 0)
                            continue;
                        var dictionaryIndex = indices[physicalIndex++];
                        _dictionaryFixedPayload.Memory.Span
                            .Slice(dictionaryIndex * _dictionaryFixedWidth, _dictionaryFixedWidth)
                            .CopyTo(payload.Memory.Span.Slice(row * _dictionaryFixedWidth, _dictionaryFixedWidth));
                    }
                    if (physicalIndex != indices.Length)
                        throw PageFormat("Dictionary indices do not match the definition levels.");
                    validity = DefinitionLevelCodec.CreateBitmap(levels, _memoryBudget, _cancellationToken);
                    _pageFixedPayload = payload;
                    _pageFixedWidth = _dictionaryFixedWidth;
                    _pageValidity.SetFromNullable(validity);
                    payload = null;
                    validity = null;
                }
                finally
                {
                    payload?.Dispose();
                    validity?.Dispose();
                }
            }

            void ExpandDictionary<T>(ReadOnlySpan<int> indices, ReadOnlySpan<int> levels)
            where T : unmanaged
            {
                if (_dictionaryValues is not T[] dictionary)
                    throw new InvalidOperationException("The dictionary representation does not match its physical type.");

                var optional = _column.SchemaElement.Repetition == ParquetRepetition.Optional;
                var rowCount = optional ? levels.Length : indices.Length;
                PooledArrayOwner<T>? rowValues = null;
                PooledArrayOwner<byte>? validity = null;
                try
                {
                    rowValues = _file.RentColumnValues<T>(_column, rowCount);
                    validity = optional
                        ? PooledArrayOwner<byte>.Rent(checked((rowCount + 7) / 8), _memoryBudget)
                        : null;
                    rowValues.Memory.Span.Clear();
                    if (validity is not null)
                        validity.Memory.Span.Clear();
                    _cancellationToken.ThrowIfCancellationRequested();
                    var physicalIndex = 0;
                    for (var row = 0; row < rowCount; row++)
                    {
                        if ((row & 1023) == 0)
                            _cancellationToken.ThrowIfCancellationRequested();
                        if (optional && levels[row] == 0)
                            continue;
                        rowValues.Memory.Span[row] = dictionary[indices[physicalIndex++]];
                        if (optional)
                            (validity ?? throw new InvalidOperationException("An optional dictionary page has no validity buffer."))
                                .Memory.Span[row >> 3] |= (byte)(1 << (row & 7));
                    }
                    if (physicalIndex != indices.Length)
                        throw PageFormat("Dictionary indices do not match the definition levels.");
                    _pageRowValuesOwner = rowValues;
                    _pageRowValues = rowValues.Array;
                    _pageValidity.SetFromNullable(validity);
                    rowValues = null;
                    validity = null;
                }
                finally
                {
                    rowValues?.Dispose();
                    validity?.Dispose();
                }
            }

            void DecodeRequiredBooleanPage(ReadOnlySpan<byte> payload, int rowCount)
            {
                PooledArrayOwner<bool>? values = _file.RentColumnValues<bool>(_column, rowCount);
                try
                {
                    PlainDecoder.DecodeBoolean(payload, values.Memory.Span, _cancellationToken);
                    _pageRowValuesOwner = values;
                    _pageRowValues = values.Array;
                    values = null;
                    _pageValidity.SetAllValid();
                }
                finally
                {
                    values?.Dispose();
                }
            }

            int GetPlainByteCount(ParquetPhysicalType physicalType, int valueCount)
            {
                try
                {
                    return physicalType switch
                    {
                        ParquetPhysicalType.Boolean => checked((valueCount + 7) / 8),
                        ParquetPhysicalType.Int32 or ParquetPhysicalType.Float => checked(valueCount * 4),
                        ParquetPhysicalType.Int64 or ParquetPhysicalType.Double => checked(valueCount * 8),
                        _ => throw new ParquetUnsupportedFeatureException("The PLAIN physical type is unsupported by this scan path.", ParquetErrorLocation.AtPage(_pageOffset, _plan.RowGroup.Ordinal, _column.Ordinal, _pageOrdinal)),
                    };
                }
                catch (OverflowException exception)
                {
                    throw new ParquetFormatException(
                        "A PLAIN fixed-width byte count overflows.", exception, ParquetErrorLocation.AtPage(_pageOffset, _plan.RowGroup.Ordinal, _column.Ordinal, _pageOrdinal));

                }
            }

            ParquetFormatException PageFormat(string message) => new(
            message,
            ParquetErrorLocation.AtPage(_pageOffset, _plan.RowGroup.Ordinal, _column.Ordinal, _pageOrdinal));

            ParquetErrorLocation CurrentPageLocation() => ParquetErrorLocation.AtPage(_pageOffset, _plan.RowGroup.Ordinal, _column.Ordinal, _pageOrdinal);

        }
        public void Dispose()
        {
            if (!_disposed)
            {
                _disposed = true;
                Terminate();
            }
        }

        private void DisposePage()
        {
            Exception? failure = null;
            try { _pagePayloadLease.Dispose(); } catch (Exception exception) when (failure is null) { failure = exception; } catch (Exception) { }
            try { _pageRowValuesOwner?.Dispose(); } catch (Exception exception) when (failure is null) { failure = exception; } catch (Exception) { }
            _pageRowValuesOwner = null;
            _pageRowValues = null;
            try { _pageValidity.Dispose(); } catch (Exception exception) when (failure is null) { failure = exception; } catch (Exception) { }
            try { _pageBinaryOffsets?.Dispose(); } catch (Exception exception) when (failure is null) { failure = exception; } catch (Exception) { }
            _pageBinaryOffsets = null;
            try { _pageBinaryPayload?.Dispose(); } catch (Exception exception) when (failure is null) { failure = exception; } catch (Exception) { }
            _pageBinaryPayload = null;
            try { _pageFixedPayload?.Dispose(); } catch (Exception exception) when (failure is null) { failure = exception; } catch (Exception) { }
            _pageFixedPayload = null;
            _pageFixedWidth = 0;
            _pageValueCount = 0;
            _pageValueIndex = 0;
            if (failure is not null)
                System.Runtime.ExceptionServices.ExceptionDispatchInfo.Capture(failure).Throw();
        }

        private void DisposeDictionary()
        {
            Exception? failure = null;
            try { _dictionaryOwner?.Dispose(); } catch (Exception exception) when (failure is null) { failure = exception; } catch (Exception) { }
            _dictionaryOwner = null;
            _dictionaryValues = null;
            try { _dictionaryBinaryOffsets?.Dispose(); } catch (Exception exception) when (failure is null) { failure = exception; } catch (Exception) { }
            _dictionaryBinaryOffsets = null;
            try { _dictionaryBinaryPayload?.Dispose(); } catch (Exception exception) when (failure is null) { failure = exception; } catch (Exception) { }
            _dictionaryBinaryPayload = null;
            try { _dictionaryFixedPayload?.Dispose(); } catch (Exception exception) when (failure is null) { failure = exception; } catch (Exception) { }
            _dictionaryFixedPayload = null;
            _dictionaryFixedWidth = 0;
            _dictionaryCount = 0;
            if (failure is not null)
                System.Runtime.ExceptionServices.ExceptionDispatchInfo.Capture(failure).Throw();
        }

        private bool HasDictionary =>
            _dictionaryOwner is not null ||
            _dictionaryBinaryOffsets is not null ||
            _dictionaryFixedPayload is not null;

        private void Terminate()
        {
            if (_terminated)
                return;
            _terminated = true;
            Exception? failure = null;
            try { DisposePage(); } catch (Exception exception) when (failure is null) { failure = exception; } catch (Exception) { }
            try { DisposeDictionary(); } catch (Exception exception) when (failure is null) { failure = exception; } catch (Exception) { }
            // Unregistration belongs to the single termination.
            if (_lifetimeOwnership == ScanLifetimeOwnership.Enumerator)
            {
                try { _file.UnregisterScan(); } catch (Exception exception) when (failure is null) { failure = exception; } catch (Exception) { }
            }

            try { _linkedCancellation?.Dispose(); } catch (Exception exception) when (failure is null) { failure = exception; } catch (Exception) { }
            try { _userLinkedCancellation?.Dispose(); } catch (Exception exception) when (failure is null) { failure = exception; } catch (Exception) { }
            if (failure is not null)
                System.Runtime.ExceptionServices.ExceptionDispatchInfo.Capture(failure).Throw();
        }

    }

    private readonly record struct ScanPlan(ParquetColumn Column, ScanRowGroup[] RowGroups);

    // Narrowed cursor inputs: the shared scan services travel as one value together with the per-column plan.
    internal readonly record struct ScanCursorServices(ParquetFile File, ParquetScanOptions Options, ParquetScanMemoryBudget MemoryBudget, PooledArrayOwnerCache<byte> PagePayloadCache);
    internal enum ScanLifetimeOwnership { Enumerator, Coordinator }
    internal readonly record struct ScanRowGroup(ParquetRowGroup RowGroup, long StartInGroup, long Count);
}
