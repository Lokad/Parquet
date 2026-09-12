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
            cancellationToken);

    private sealed class Enumerator : IAsyncEnumerator<ParquetBatch>
    {
        private enum SourceBatchDisposition
        {
            Retained,
            Transferred,
            Mixed,
        }

        private readonly ParquetFile _file;
        private readonly ParquetScanEnumerable.ColumnCursor[] _enumerators;
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
            CancellationToken enumerationCancellation)
        {
            static int[] ResolveProjection(ParquetFile file, ParquetScanOptions options)
            {
                var ordinals = new int[options.Columns.Count];
                if (ordinals.Length < 2)
                    throw new InvalidOperationException("The projected scan coordinator requires at least two columns.");
                // Linear duplicate detection for wide projections; the nested loop keeps small projections allocation-free.
                HashSet<int>? seen = ordinals.Length > 16 ? new HashSet<int>(ordinals.Length) : null;
                for (var index = 0; index < ordinals.Length; index++)
                {
                    var selected = options.Columns[index];
                    var ordinal = selected.Ordinal;
                    ordinals[index] = ordinal;
                    if (seen is not null)
                    {
                        if (!seen.Add(ordinal))
                            throw new ArgumentException("Duplicate projected columns are not permitted.", nameof(options));
                    }
                    else
                    {
                        for (var previous = 0; previous < index; previous++)
                        {
                            if (ordinals[previous] == ordinal)
                                throw new ArgumentException("Duplicate projected columns are not permitted.", nameof(options));
                        }
                    }
                    if ((uint)ordinal >= (uint)file.Metadata.Schema.Columns.Count)
                        throw new ArgumentOutOfRangeException(nameof(options), "A projected column ordinal is outside the schema.");
                    var column = file.Metadata.Schema.Columns[ordinal];
                    if (!ReferenceEquals(selected, column))
                        throw new ArgumentException("A projected column descriptor belongs to another file.", nameof(options));
                    if (!column.IsReadable)
                        throw new ParquetUnsupportedFeatureException(
                            column.UnsupportedReason ?? "The projected column is unsupported.", ParquetErrorLocation.AtColumn(ordinal));
                }
                return ordinals;
            }

            _file = file;
            _memoryBudget = file.ScanMemoryBudget;
            _pagePayloadCache = file.PagePayloadCache;
            var ordinals = ResolveProjection(file, options);
            ParquetScanEnumerable.ValidateTargetBatchRowCount(file, options);
            _enumerators = new ParquetScanEnumerable.ColumnCursor[ordinals.Length];
            _sourceBatches = new DecodedColumnBatch?[ordinals.Length];
            _sourceOffsets = new int[ordinals.Length];
            var selectedRowGroups = ParquetScanEnumerable.BuildRowGroups(file, options);
            ParquetScanEnumerable.PreflightSelectedChunks(file, selectedRowGroups, ordinals);
            try
            {
                (_cancellationToken, _linkedCancellation, _userLinkedCancellation) = ScanCancellation.Compose(
                    scanCancellation,
                    enumerationCancellation,
                    file.DisposalToken);
                for (var index = 0; index < ordinals.Length; index++)
                {
                    var column = file.Metadata.Schema.Columns[ordinals[index]];
                    _enumerators[index] = new ParquetScanEnumerable.ColumnCursor(
                        new ParquetScanEnumerable.ScanCursorServices(file, options),
                        column,
                        selectedRowGroups,
                        _cancellationToken,
                        CancellationToken.None,
                        ParquetScanEnumerable.ScanLifetimeOwnership.Coordinator);
                }
                file.RegisterScan(OnFileDisposed);
            }
            catch
            {
                // Best-effort rollback preserves the primary construction error.
                foreach (var enumerator in _enumerators)
                {
                    try { enumerator?.Dispose(); } catch (Exception) { }
                }

                try { _linkedCancellation?.Dispose(); } catch (Exception) { }
                try { _userLinkedCancellation?.Dispose(); } catch (Exception) { }
                throw;
            }

            // The single lane is acquired before idle caches are evicted, so a rejected
            // overlapping request leaves no cache side effects behind.
            try
            {
                file.EvictIdleColumnCaches(ordinals);
            }
            catch
            {
                // Best-effort termination preserves the primary eviction error.
                try { TerminateAndUnregister(); } catch (Exception) { }
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
                    // Mixed batches reuse the same per-column predicate as creation:
                    // offsets are untouched by CreateBatch, so the verdict agrees.
                    if (sourceBatchDisposition == SourceBatchDisposition.Mixed && CanTransferSourceBatch(index, count))
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
                var published = _current ??
                    throw new InvalidOperationException("A projected batch was not created.");
                // Pre-publication boundary: a token cancelled while copying must surface
                // instead of yielding a batch. The built batch is released best-effort so
                // the cancellation stays the primary error.
                try
                {
                    _cancellationToken.ThrowIfCancellationRequested();
                }
                catch
                {
                    try { published.Dispose(); } catch (Exception) { }
                    _current = null;
                    throw;
                }
                return true;
            }
            catch (OperationCanceledException) when (_file.IsDisposed)
            {
                // Best-effort termination preserves the primary cancellation for
                // conversion into a file-disposal error below.
                try { TerminateAndUnregister(); } catch (Exception) { }
                throw new ObjectDisposedException(nameof(ParquetFile));
            }
            catch
            {
                // Best-effort termination preserves the primary scan error.
                try { TerminateAndUnregister(); } catch (Exception) { }
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
                    if (source.Column is ParquetBinaryColumnBatch ||
                        source.Column is ParquetFixedLengthByteArrayColumnBatch)
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
                        if (source.Column is ParquetFixedLengthByteArrayColumnBatch fixedBytes)
                        {
                            payloadBytes += checked((long)middle * fixedBytes.TypeWidth);
                            continue;
                        }
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

            // One column covers the batch exactly when the batch starts at its
            // first row and the column holds no more rows. Shared by creation and
            // the post-creation ownership walk, which sees identical offsets.
            bool CanTransferSourceBatch(int index, int batchRowCount)
            {
                var source = _sourceBatches[index] ??
                    throw new InvalidOperationException("An aligned source batch is missing.");
                return _sourceOffsets[index] == 0 && source.RowCount == batchRowCount;
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
                var ownerStore = new IDisposable[_sourceBatches.Length * 3];
                var ownerCount = 0;
                var columns = new ParquetColumnBatch[_sourceBatches.Length];
                var transferredAny = false;
                try
                {
                    for (var index = 0; index < columns.Length; index++)
                    {
                        var source = _sourceBatches[index] ??
                            throw new InvalidOperationException("An aligned source batch is missing.");
                        if (CanTransferSourceBatch(index, rowCount))
                        {
                            // This column already covers the batch exactly: move its
                            // storage into the public batch instead of copying it.
                            columns[index] = source.Column;
                            ownerStore[ownerCount++] = source;
                            transferredAny = true;
                            continue;
                        }
                        columns[index] = CopyColumn(
                            source.Column,
                            _sourceOffsets[index],
                            rowCount,
                            lifetime,
                            ownerStore,
                            ref ownerCount);
                    }
                    if (transferredAny)
                        sourceBatchDisposition = SourceBatchDisposition.Mixed;
                    return new ParquetBatch(
                        rowOffset,
                        rowGroupOrdinal,
                        rowOffsetInGroup,
                        rowCount,
                        columns,
                        lifetime,
                        ownerStore.AsSpan(0, ownerCount).ToArray());
                }
                catch
                {
                    // Best-effort rollback preserves the primary copy error.
                    try { lifetime.Dispose(); } catch (Exception) { }
                    for (var ownerIndex = 0; ownerIndex < ownerCount; ownerIndex++)
                    {
                        try { ownerStore[ownerIndex].Dispose(); } catch (Exception) { }
                    }

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
            IDisposable[] ownerStore,
            ref int ownerCount)
            {
                var validity = CopyValidity(source.Validity, sourceOffset, rowCount, lifetime, ownerStore, ref ownerCount);
                return source switch
                {
                    ParquetPrimitiveColumnBatch<bool> typed => CopyPrimitive(typed, sourceOffset, rowCount, validity, lifetime, ownerStore, ref ownerCount),
                    ParquetPrimitiveColumnBatch<int> typed => CopyPrimitive(typed, sourceOffset, rowCount, validity, lifetime, ownerStore, ref ownerCount),
                    ParquetPrimitiveColumnBatch<long> typed => CopyPrimitive(typed, sourceOffset, rowCount, validity, lifetime, ownerStore, ref ownerCount),
                    ParquetPrimitiveColumnBatch<float> typed => CopyPrimitive(typed, sourceOffset, rowCount, validity, lifetime, ownerStore, ref ownerCount),
                    ParquetPrimitiveColumnBatch<double> typed => CopyPrimitive(typed, sourceOffset, rowCount, validity, lifetime, ownerStore, ref ownerCount),
                    ParquetBinaryColumnBatch binary => CopyBinary(binary, sourceOffset, rowCount, validity, lifetime, ownerStore, ref ownerCount),
                    ParquetFixedLengthByteArrayColumnBatch fixedBytes =>
                        CopyFixed(fixedBytes, sourceOffset, rowCount, validity, lifetime, ownerStore, ref ownerCount),
                    _ => throw new ParquetUnsupportedFeatureException("A projected batch representation is unsupported."),
                };
            }

            ParquetPrimitiveColumnBatch<T> CopyPrimitive<T>(
            ParquetPrimitiveColumnBatch<T> source,
            int sourceOffset,
            int rowCount,
            ParquetValidity validity,
            BatchLifetime lifetime,
            IDisposable[] ownerStore,
            ref int ownerCount)
            where T : unmanaged
            {
                _cancellationToken.ThrowIfCancellationRequested();
                var owner = PooledArrayOwner<T>.Rent(rowCount, _memoryBudget);
                source.Values.Span.Slice(sourceOffset, rowCount).CopyTo(owner.Memory.Span);
                ownerStore[ownerCount++] = owner;
                return new ParquetPrimitiveColumnBatch<T>(lifetime, source.Column, owner.Memory, validity);
            }

            ParquetBinaryColumnBatch CopyBinary(
            ParquetBinaryColumnBatch source,
            int sourceOffset,
            int rowCount,
            ParquetValidity validity,
            BatchLifetime lifetime,
            IDisposable[] ownerStore,
            ref int ownerCount)
            {
                _cancellationToken.ThrowIfCancellationRequested();
                var sourceOffsets = source.Offsets.Span;
                var baseOffset = sourceOffsets[sourceOffset];
                var payloadLength = sourceOffsets[sourceOffset + rowCount] - baseOffset;
                var offsets = PooledArrayOwner<int>.Rent(checked(rowCount + 1), _memoryBudget);
                ownerStore[ownerCount++] = offsets;
                _cancellationToken.ThrowIfCancellationRequested();
                for (var index = 0; index <= rowCount; index++)
                {
                    if ((index & 1023) == 0)
                        _cancellationToken.ThrowIfCancellationRequested();
                    offsets.Memory.Span[index] = sourceOffsets[sourceOffset + index] - baseOffset;
                }

                ReadOnlyMemory<byte> payloadMemory = ReadOnlyMemory<byte>.Empty;
                if (payloadLength != 0)
                {
                    var payload = PooledArrayOwner<byte>.Rent(payloadLength, _memoryBudget);
                    ownerStore[ownerCount++] = payload;
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
            IDisposable[] ownerStore,
            ref int ownerCount)
            {
                _cancellationToken.ThrowIfCancellationRequested();
                if (source.TypeWidth <= 0)
                    throw new InvalidOperationException("A FIXED_LEN_BYTE_ARRAY batch has an invalid width.");
                if (source.TypeWidth > _file.Options.MaximumBinaryValueBytes)
                    throw new ParquetLimitExceededException("A FIXED_LEN_BYTE_ARRAY value exceeds the configured byte limit.");
                var requestedByteCount = checked((long)rowCount * source.TypeWidth);
                if (requestedByteCount > _file.Options.MaximumBinaryBatchBytes || requestedByteCount > int.MaxValue)
                    throw new ParquetLimitExceededException("A FIXED_LEN_BYTE_ARRAY batch exceeds the configured binary batch limit.");
                var byteCount = checked(rowCount * source.TypeWidth);
                var payload = PooledArrayOwner<byte>.Rent(byteCount, _memoryBudget);
                ownerStore[ownerCount++] = payload;
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
            IDisposable[] ownerStore,
            ref int ownerCount)
            {
                if (source.IsAllValid)
                    return new ParquetValidity(lifetime, rowCount, ReadOnlyMemory<byte>.Empty, true);
                var sliced = ValidityBitmap.CopySlice(
                    source.Bits,
                    sourceOffset,
                    rowCount,
                    _memoryBudget,
                    _cancellationToken,
                    out var slicedBits,
                    out _);
                if (sliced is null)
                    return new ParquetValidity(lifetime, rowCount, ReadOnlyMemory<byte>.Empty, true);
                ownerStore[ownerCount++] = sliced;
                return new ParquetValidity(lifetime, rowCount, slicedBits, false);
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

        // Teardown exception policy. The terminating scan always releases its registration,
        // even when its cleanup throws; a late disposal must not clear a newer scan. The first
        // cleanup failure is preserved. Rollback while another error is already in flight is
        // best-effort exhaustive and preserves that primary error instead of masking it.
        private void TerminateAndUnregister()
        {
            // Unregistration belongs to the scan that terminates first; a late disposal must not clear a newer scan.
            Exception? failure = null;
            var terminates = true;
            try
            {
                terminates = Terminate();
            }
            catch (Exception exception)
            {
                failure = exception;
            }

            if (terminates)
            {
                try { _file.UnregisterScan(); } catch (Exception exception) when (failure is null) { failure = exception; } catch (Exception) { }
            }

            if (failure is not null)
                System.Runtime.ExceptionServices.ExceptionDispatchInfo.Capture(failure).Throw();
        }

        private bool Terminate()
        {
            if (Interlocked.Exchange(ref _terminationStarted, 1) != 0)
            {
                return false;
            }
            _terminated = true;
            Exception? failure = null;
            for (var index = 0; index < _sourceBatches.Length; index++)
            {
                try
                {
                    _sourceBatches[index]?.Dispose();
                }
                catch (Exception exception) when (failure is null)
                {
                    failure = exception;
                }
                catch (Exception)
                {
                }

                _sourceBatches[index] = null;
            }
            foreach (var enumerator in _enumerators)
            {
                try
                {
                    enumerator.Dispose();
                }
                catch (Exception exception) when (failure is null)
                {
                    failure = exception;
                }
                catch (Exception)
                {
                }
            }

            try { _linkedCancellation?.Dispose(); } catch (Exception exception) when (failure is null) { failure = exception; } catch (Exception) { }
            try { _userLinkedCancellation?.Dispose(); } catch (Exception exception) when (failure is null) { failure = exception; } catch (Exception) { }
            if (failure is not null)
                System.Runtime.ExceptionServices.ExceptionDispatchInfo.Capture(failure).Throw();

            return true;
        }

    }
}












