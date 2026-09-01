namespace Lokad.Parquet.Internal;

internal sealed class ParquetProjectedScanEnumerable : IAsyncEnumerable<ParquetBatch>
{
    private readonly ParquetFile _file;
    private readonly ParquetScanOptions _options;
    private readonly CancellationToken _cancellationToken;

    public ParquetProjectedScanEnumerable(
        ParquetFile file,
        ParquetScanOptions options,
        CancellationToken cancellationToken)
    {
        _file = file;
        _options = options;
        _cancellationToken = cancellationToken;
    }

    public IAsyncEnumerator<ParquetBatch> GetAsyncEnumerator(CancellationToken cancellationToken) =>
        new Enumerator(
            _file,
            _options,
            _cancellationToken,
            cancellationToken,
            _file.ScanMemoryBudget);

    private sealed class Enumerator : IAsyncEnumerator<ParquetBatch>
    {
        private enum SourceBatchDisposition
        {
            Retained,
            Transferred,
        }

        private readonly ParquetFile _file;
        private readonly IAsyncEnumerator<DecodedColumnBatch>[] _enumerators;
        private readonly DecodedColumnBatch?[] _sourceBatches;
        private readonly int[] _sourceOffsets;
        private readonly ParquetScanMemoryBudget _memoryBudget;
        private readonly PooledArrayOwnerCache<byte> _pagePayloadCache;
        private readonly CancellationTokenSource? _userLinkedCancellation;
        private readonly CancellationTokenSource? _linkedCancellation;
        private readonly CancellationToken _cancellationToken;
        private ParquetBatch? _current;
        private int _terminationStarted;
        private bool _terminated;
        private bool _fileDisposed;
        private bool _disposed;

        public Enumerator(
            ParquetFile file,
            ParquetScanOptions options,
            CancellationToken scanCancellation,
            CancellationToken enumerationCancellation,
            ParquetScanMemoryBudget memoryBudget)
        {
            static int[] ResolveProjection(ParquetFile file, ParquetScanOptions options)
            {
                var ordinals = new int[options.Columns.Count];
                if (ordinals.Length < 2)
                    throw new InvalidOperationException("The projected scan coordinator requires at least two columns.");
                for (var index = 0; index < ordinals.Length; index++)
                {
                    var selected = options.Columns[index];
                    var ordinal = selected.Ordinal;
                    ordinals[index] = ordinal;
                    for (var previous = 0; previous < index; previous++)
                    {
                        if (ordinals[previous] == ordinal)
                            throw new ArgumentException("Duplicate projected columns are not permitted.", nameof(options));
                    }
                    if ((uint)ordinal >= (uint)file.Metadata.Schema.Columns.Count)
                        throw new ArgumentOutOfRangeException(nameof(options), "A projected column ordinal is outside the schema.");
                    var column = file.Metadata.Schema.Columns[ordinal];
                    if (!ReferenceEquals(selected, column))
                        throw new ArgumentException("A projected column descriptor belongs to another file.", nameof(options));
                    if (!column.IsReadable)
                        throw new ParquetUnsupportedFeatureException(
                            column.UnsupportedReason ?? "The projected column is unsupported.",
                            columnOrdinal: ordinal);
                }
                return ordinals;
            }

            _file = file;
            _memoryBudget = memoryBudget;
            _pagePayloadCache = file.PagePayloadCache;
            var ordinals = ResolveProjection(file, options);
            _enumerators = new IAsyncEnumerator<DecodedColumnBatch>[ordinals.Length];
            _sourceBatches = new DecodedColumnBatch?[ordinals.Length];
            _sourceOffsets = new int[ordinals.Length];
            var selectedRowGroups = ParquetScanEnumerable.BuildRowGroups(file, options);
            try
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
                        file.DisposalToken);
                    _cancellationToken = _linkedCancellation.Token;
                }
                else
                {
                    _linkedCancellation = null;
                    _cancellationToken = file.DisposalToken;
                }
                for (var index = 0; index < ordinals.Length; index++)
                {
                    var column = file.Metadata.Schema.Columns[ordinals[index]];
                    _enumerators[index] = new ParquetScanEnumerable.ColumnCursor(
                        file,
                        options,
                        column,
                        selectedRowGroups,
                        _cancellationToken,
                        CancellationToken.None,
                        memoryBudget,
                        _pagePayloadCache,
                        ParquetScanEnumerable.ScanLifetimeOwnership.Coordinator);
                }
                file.RegisterScan(OnFileDisposed);
            }
            catch
            {
                foreach (var enumerator in _enumerators)
                {
                    if (enumerator is null)
                        continue;
                    var disposal = enumerator.DisposeAsync();
                    if (!disposal.IsCompletedSuccessfully)
                        disposal.AsTask().GetAwaiter().GetResult();
                }
                _linkedCancellation?.Dispose();
                _userLinkedCancellation?.Dispose();
                throw;
            }

            void OnFileDisposed()
            {
                _fileDisposed = true;
                Terminate();
            }
        }

        public ParquetBatch Current => _current ??
            throw new InvalidOperationException("The enumerator has no current batch.");

        public async ValueTask<bool> MoveNextAsync()
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            ObjectDisposedException.ThrowIf(_fileDisposed, _file);
            if (_terminated)
                return false;
            if (_current is not null && !_current.IsDisposed)
                throw new InvalidOperationException("Dispose the current Parquet batch before advancing the enumerator.");
            _current = null;

            _file.EnterScanOperation();
            try
            {
                for (var index = 0; index < _enumerators.Length; index++)
                {
                    if (_sourceBatches[index] is not null)
                        continue;
                    var moving = _enumerators[index].MoveNextAsync();
                    var moved = moving.IsCompletedSuccessfully
                        ? moving.Result
                        : await moving.ConfigureAwait(false);
                    if (!moved)
                        continue;
                    _sourceBatches[index] = _enumerators[index].Current;
                    _sourceOffsets[index] = 0;
                }

                var hasRows = false;
                var hasMissingColumn = false;
                for (var index = 0; index < _sourceBatches.Length; index++)
                {
                    if (_sourceBatches[index] is null)
                        hasMissingColumn = true;
                    else
                        hasRows = true;
                }
                if (!hasRows)
                {
                    TerminateAndUnregister();
                    return false;
                }
                if (hasMissingColumn)
                    throw new ParquetFormatException("Projected columns do not contain the same number of rows.");

                var first = _sourceBatches[0] ??
                    throw new InvalidOperationException("An aligned source batch is missing.");
                var rowOffset = checked(first.RowOffset + _sourceOffsets[0]);
                var rowOffsetInGroup = checked(first.RowOffsetInGroup + _sourceOffsets[0]);
                var rowGroupOrdinal = first.RowGroupOrdinal;
                var count = first.RowCount - _sourceOffsets[0];
                for (var index = 1; index < _sourceBatches.Length; index++)
                {
                    var source = _sourceBatches[index] ??
                        throw new InvalidOperationException("An aligned source batch is missing.");
                    if (source.RowOffset + _sourceOffsets[index] != rowOffset ||
                        source.RowOffsetInGroup + _sourceOffsets[index] != rowOffsetInGroup ||
                        source.RowGroupOrdinal != rowGroupOrdinal)
                        throw new ParquetFormatException("Projected column batches are not row-aligned.");
                    count = Math.Min(count, source.RowCount - _sourceOffsets[index]);
                }
                count = FitBinaryPayload(count);
                var sourceBatchDisposition = SourceBatchDisposition.Retained;
                while (true)
                {
                    try
                    {
                        _current = CreateBatch(
                            rowOffset,
                            rowGroupOrdinal,
                            rowOffsetInGroup,
                            count,
                            out sourceBatchDisposition);
                        break;
                    }
                    catch (ParquetLimitExceededException) when (count > 1)
                    {
                        count = (count + 1) >> 1;
                    }
                }

                for (var index = 0; index < _sourceBatches.Length; index++)
                {
                    if (sourceBatchDisposition == SourceBatchDisposition.Transferred)
                    {
                        _sourceBatches[index] = null;
                        _sourceOffsets[index] = 0;
                        continue;
                    }
                    _sourceOffsets[index] += count;
                    var source = _sourceBatches[index] ??
                        throw new InvalidOperationException("An aligned source batch is missing.");
                    if (_sourceOffsets[index] != source.RowCount)
                        continue;
                    source.Dispose();
                    _sourceBatches[index] = null;
                    _sourceOffsets[index] = 0;
                }
                return true;
            }
            catch
            {
                TerminateAndUnregister();
                throw;
            }
            finally
            {
                _file.ExitScanOperation();
            }

            int FitBinaryPayload(int maximumCount)
            {
                var hasBinary = false;
                for (var index = 0; index < _sourceBatches.Length; index++)
                {
                    var source = _sourceBatches[index] ??
                        throw new InvalidOperationException("An aligned source batch is missing.");
                    if (source.Column is ParquetBinaryColumnBatch)
                    {
                        hasBinary = true;
                        break;
                    }
                }
                if (!hasBinary)
                    return maximumCount;

                var low = 0;
                var high = maximumCount;
                while (low < high)
                {
                    var middle = low + ((high - low + 1) >> 1);
                    long payloadBytes = 0;
                    for (var index = 0; index < _sourceBatches.Length; index++)
                    {
                        var source = _sourceBatches[index] ??
                            throw new InvalidOperationException("An aligned source batch is missing.");
                        if (source.Column is not ParquetBinaryColumnBatch binary)
                            continue;
                        var offsets = binary.Offsets.Span;
                        var start = _sourceOffsets[index];
                        payloadBytes += offsets[start + middle] - offsets[start];
                    }
                    if (payloadBytes <= _file.Options.MaximumBinaryBatchBytes)
                        low = middle;
                    else
                        high = middle - 1;
                }
                if (low == 0)
                    throw new ParquetLimitExceededException("One aligned row exceeds the configured binary batch limit.");
                return low;
            }

            ParquetBatch CreateBatch(
            long rowOffset,
            int rowGroupOrdinal,
            long rowOffsetInGroup,
            int rowCount,
            out SourceBatchDisposition sourceBatchDisposition)
            {
                if (CanTransferSourceBatches(rowCount))
                {
                    // Whole aligned source batches become owners of the public batch; their buffers are not copied.
                    sourceBatchDisposition = SourceBatchDisposition.Transferred;
                    var transferredLifetime = new BatchLifetime();
                    var transferredColumns = new ParquetColumnBatch[_sourceBatches.Length];
                    var transferredOwners = new IDisposable[_sourceBatches.Length];
                    for (var index = 0; index < transferredColumns.Length; index++)
                    {
                        var source = _sourceBatches[index] ??
                            throw new InvalidOperationException("An aligned source batch is missing.");
                        transferredColumns[index] = source.Column;
                        transferredOwners[index] = source;
                    }
                    return new ParquetBatch(
                        rowOffset,
                        rowGroupOrdinal,
                        rowOffsetInGroup,
                        rowCount,
                        transferredColumns,
                        transferredLifetime,
                        transferredOwners);
                }

                sourceBatchDisposition = SourceBatchDisposition.Retained;
                // Misaligned page boundaries require fresh aligned buffers owned solely by the projected batch.
                var lifetime = new BatchLifetime();
                var owners = new List<IDisposable>();
                var columns = new ParquetColumnBatch[_sourceBatches.Length];
                try
                {
                    for (var index = 0; index < columns.Length; index++)
                    {
                        var source = _sourceBatches[index] ??
                            throw new InvalidOperationException("An aligned source batch is missing.");
                        columns[index] = CopyColumn(
                            source.Column,
                            _sourceOffsets[index],
                            rowCount,
                            lifetime,
                            owners);
                    }
                    return new ParquetBatch(
                        rowOffset,
                        rowGroupOrdinal,
                        rowOffsetInGroup,
                        rowCount,
                        columns,
                        lifetime,
                        owners.ToArray());
                }
                catch
                {
                    lifetime.Dispose();
                    foreach (var owner in owners)
                        owner.Dispose();
                    throw;
                }

                bool CanTransferSourceBatches(int count)
                {
                    for (var index = 0; index < _sourceBatches.Length; index++)
                    {
                        var source = _sourceBatches[index] ??
                            throw new InvalidOperationException("An aligned source batch is missing.");
                        if (_sourceOffsets[index] != 0 || source.RowCount != count)
                            return false;
                    }
                    return true;
                }
            }

            ParquetColumnBatch CopyColumn(
            ParquetColumnBatch source,
            int sourceOffset,
            int rowCount,
            BatchLifetime lifetime,
            List<IDisposable> owners)
            {
                var validity = CopyValidity(source.Validity, sourceOffset, rowCount, lifetime, owners);
                return source switch
                {
                    ParquetPrimitiveColumnBatch<bool> typed => CopyPrimitive(typed, sourceOffset, rowCount, validity, lifetime, owners),
                    ParquetPrimitiveColumnBatch<int> typed => CopyPrimitive(typed, sourceOffset, rowCount, validity, lifetime, owners),
                    ParquetPrimitiveColumnBatch<long> typed => CopyPrimitive(typed, sourceOffset, rowCount, validity, lifetime, owners),
                    ParquetPrimitiveColumnBatch<float> typed => CopyPrimitive(typed, sourceOffset, rowCount, validity, lifetime, owners),
                    ParquetPrimitiveColumnBatch<double> typed => CopyPrimitive(typed, sourceOffset, rowCount, validity, lifetime, owners),
                    ParquetBinaryColumnBatch binary => CopyBinary(binary, sourceOffset, rowCount, validity, lifetime, owners),
                    ParquetFixedLengthByteArrayColumnBatch fixedBytes =>
                        CopyFixed(fixedBytes, sourceOffset, rowCount, validity, lifetime, owners),
                    _ => throw new ParquetUnsupportedFeatureException("A projected batch representation is unsupported."),
                };
            }

            ParquetPrimitiveColumnBatch<T> CopyPrimitive<T>(
            ParquetPrimitiveColumnBatch<T> source,
            int sourceOffset,
            int rowCount,
            ParquetValidity validity,
            BatchLifetime lifetime,
            List<IDisposable> owners)
            where T : unmanaged
            {
                var owner = PooledArrayOwner<T>.Rent(rowCount, _memoryBudget);
                source.Values.Span.Slice(sourceOffset, rowCount).CopyTo(owner.Memory.Span);
                owners.Add(owner);
                return new ParquetPrimitiveColumnBatch<T>(lifetime, source.Column, owner.Memory, validity);
            }

            ParquetBinaryColumnBatch CopyBinary(
            ParquetBinaryColumnBatch source,
            int sourceOffset,
            int rowCount,
            ParquetValidity validity,
            BatchLifetime lifetime,
            List<IDisposable> owners)
            {
                var sourceOffsets = source.Offsets.Span;
                var baseOffset = sourceOffsets[sourceOffset];
                var payloadLength = sourceOffsets[sourceOffset + rowCount] - baseOffset;
                var offsets = PooledArrayOwner<int>.Rent(checked(rowCount + 1), _memoryBudget);
                owners.Add(offsets);
                for (var index = 0; index <= rowCount; index++)
                    offsets.Memory.Span[index] = sourceOffsets[sourceOffset + index] - baseOffset;

                ReadOnlyMemory<byte> payloadMemory = ReadOnlyMemory<byte>.Empty;
                if (payloadLength != 0)
                {
                    var payload = PooledArrayOwner<byte>.Rent(payloadLength, _memoryBudget);
                    owners.Add(payload);
                    source.Payload.Span.Slice(baseOffset, payloadLength).CopyTo(payload.Memory.Span);
                    payloadMemory = payload.Memory;
                }
                return new ParquetBinaryColumnBatch(
                    lifetime,
                    source.Column,
                    rowCount,
                    payloadMemory,
                    offsets.Memory,
                    validity);
            }

            ParquetFixedLengthByteArrayColumnBatch CopyFixed(
            ParquetFixedLengthByteArrayColumnBatch source,
            int sourceOffset,
            int rowCount,
            ParquetValidity validity,
            BatchLifetime lifetime,
            List<IDisposable> owners)
            {
                var byteCount = checked(rowCount * source.TypeWidth);
                var payload = PooledArrayOwner<byte>.Rent(byteCount, _memoryBudget);
                owners.Add(payload);
                source.Payload.Span.Slice(checked(sourceOffset * source.TypeWidth), byteCount)
                    .CopyTo(payload.Memory.Span);
                return new ParquetFixedLengthByteArrayColumnBatch(
                    lifetime,
                    source.Column,
                    rowCount,
                    source.TypeWidth,
                    payload.Memory,
                    validity);
            }

            ParquetValidity CopyValidity(
            ParquetValidity source,
            int sourceOffset,
            int rowCount,
            BatchLifetime lifetime,
            List<IDisposable> owners)
            {
                if (source.IsAllValid)
                    return new ParquetValidity(lifetime, rowCount, ReadOnlyMemory<byte>.Empty, true);
                var bits = PooledArrayOwner<byte>.Rent(checked((rowCount + 7) / 8), _memoryBudget);
                bits.Memory.Span.Clear();
                var allValid = true;
                for (var row = 0; row < rowCount; row++)
                {
                    if (source.IsValid(sourceOffset + row))
                        bits.Memory.Span[row >> 3] |= (byte)(1 << (row & 7));
                    else
                        allValid = false;
                }
                if (allValid)
                {
                    bits.Dispose();
                    return new ParquetValidity(lifetime, rowCount, ReadOnlyMemory<byte>.Empty, true);
                }
                owners.Add(bits);
                return new ParquetValidity(lifetime, rowCount, bits.Memory, false);
            }
        }

        public ValueTask DisposeAsync()
        {
            if (_disposed)
                return ValueTask.CompletedTask;
            _disposed = true;
            TerminateAndUnregister();
            return ValueTask.CompletedTask;
        }

        private void TerminateAndUnregister()
        {
            Terminate();
            _file.UnregisterScan();
        }

        private void Terminate()
        {
            if (Interlocked.Exchange(ref _terminationStarted, 1) != 0)
                return;
            _terminated = true;
            for (var index = 0; index < _sourceBatches.Length; index++)
            {
                _sourceBatches[index]?.Dispose();
                _sourceBatches[index] = null;
            }
            foreach (var enumerator in _enumerators)
            {
                var disposal = enumerator.DisposeAsync();
                if (!disposal.IsCompletedSuccessfully)
                    disposal.AsTask().GetAwaiter().GetResult();
            }
            _linkedCancellation?.Dispose();
            _userLinkedCancellation?.Dispose();
        }

    }
}
