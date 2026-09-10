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
        // The single lane is acquired before idle caches are evicted, so a rejected
        // overlapping request leaves no cache side effects behind.
        // Services carry only the file and options; the budget and caches derive from
        // the file so the two can never disagree.
        var cursor = new ColumnCursor(
            new ScanCursorServices(_file, _options),
            plan.Column,
            plan.RowGroups,
            _cancellationToken,
            cancellationToken,
            ScanLifetimeOwnership.Enumerator);
        try
        {
            Span<int> keepOrdinal = stackalloc int[1];
            keepOrdinal[0] = plan.Column.Ordinal;
            _file.EvictIdleColumnCaches(keepOrdinal);
        }
        catch
        {
            // Best-effort rollback preserves the primary eviction error.
            try { cursor.Dispose(); } catch (Exception) { }
            throw;
        }
        return new Enumerator(cursor);
    }

    internal static void ValidateTargetBatchRowCount(ParquetFile file, ParquetScanOptions options)
    {
        if (options.TargetBatchRowCount <= 0 || options.TargetBatchRowCount > file.Options.MaximumRowsPerBatch)
            throw new ArgumentOutOfRangeException("options", "The target batch size is outside the reader limits.");
    }

    internal static ScanRowGroup[] BuildRowGroups(ParquetFile file, ParquetScanOptions options)
    {
        // Every supplied descriptor and the row range is validated before empty
        // selections take the fast exit below, so an invalid request cannot hide
        // behind an empty selection.
        HashSet<int>? selection = null;
        if (options.RowGroups is not null)
        {
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
        if (selection is not null && selection.Count == 0)
            return [];
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

    internal static void PreflightChunk(ParquetFile file, ParquetRowGroup rowGroup, int columnOrdinal)
    {
        var chunk = rowGroup.Columns[columnOrdinal];
        if (chunk.ExternalFilePath is not null)
            throw new ParquetUnsupportedFeatureException(
                "External column chunks are unsupported.", ParquetErrorLocation.AtRowGroupColumn(rowGroup.Ordinal, columnOrdinal));
        if (chunk.HasCryptoMetadata || chunk.HasEncryptedMetadata || file.Metadata.IsEncrypted)
            throw new ParquetUnsupportedFeatureException(
                "Encrypted column chunks are unsupported.", ParquetErrorLocation.AtRowGroupColumn(rowGroup.Ordinal, columnOrdinal));
        if (chunk.CompressionCodec is not ParquetCompressionCodec.Uncompressed and not ParquetCompressionCodec.Snappy)
            throw new ParquetUnsupportedFeatureException(
                "This scan path currently requires an uncompressed or Snappy column chunk.", ParquetErrorLocation.AtRowGroupColumn(rowGroup.Ordinal, columnOrdinal));
    }

    internal static void PreflightSelectedChunks(ParquetFile file, ScanRowGroup[] rowGroups, int[] ordinalsInProjectionOrder)
    {
        foreach (var plan in rowGroups)
        {
            if (plan.Count == 0)
                continue;
            foreach (var ordinal in ordinalsInProjectionOrder)
                PreflightChunk(file, plan.RowGroup, ordinal);
        }
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
            ParquetColumnBatch[] columns = [decoded.Column];
            _current = new ParquetBatch(
                decoded.RowOffset,
                decoded.RowGroupOrdinal,
                decoded.RowOffsetInGroup,
                decoded.RowCount,
                columns,
                decoded.Lifetime,
                decoded.Owners);
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
        // Where data pages must start: the advertised data offset, or the end
        // of an admitted leading dictionary page when none was advertised.
        private long _expectedDataPageOffset;
        private long _chunkEnd;
        private long _rowsSeenInGroup;
        private long _accumulatedUncompressedBytes;
        private int _pageOrdinal;
        // Fixed-width raw payload lease: an owned pool buffer or borrowed immutable memory. Kind distinguishes empty borrowed from none.
        private PagePayloadLease _pagePayloadLease;
        private PooledValueLease _pageValues;
        // Explicit validity: None means no page loaded, AllValid means implicitly all-valid, Explicit holds the bitmap.
        private PageValidityState _pageValidity;
        private PooledArrayOwner<int>? _pageBinaryOffsets;
        private PooledArrayOwner<byte>? _pageBinaryPayload;
        private PooledArrayOwner<byte>? _pageFixedPayload;
        private int _pageFixedWidth;
        private int _pagePayloadSliceRows;
        private readonly ScanDictionaryDecoder _dictionaryDecoder;
        private bool _seenDataPage;
        private int _pageValueCount;
        private int _pageValueIndex;
        private long _pageRowOffset;
        private DecodedColumnBatch? _current;
        private bool _terminated;
        private bool _fileDisposed;
        private bool _disposed;


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
                Exception? failure = null;
                try { DisposePage(); } catch (Exception exception) when (failure is null) { failure = exception; } catch (Exception) { }
                try { _dictionaryDecoder.Dispose(); } catch (Exception exception) when (failure is null) { failure = exception; } catch (Exception) { }
                try { _linkedCancellation?.Dispose(); } catch (Exception exception) when (failure is null) { failure = exception; } catch (Exception) { }
                try { _userLinkedCancellation?.Dispose(); } catch (Exception exception) when (failure is null) { failure = exception; } catch (Exception) { }
                if (failure is not null)
                    System.Runtime.ExceptionServices.ExceptionDispatchInfo.Capture(failure).Throw();
            }

            _file = services.File;
            _options = services.Options;
            _memoryBudget = services.File.ScanMemoryBudget;
            _pagePayloadCache = services.File.PagePayloadCache;
            _lifetimeOwnership = lifetimeOwnership;
            _column = column;
            _dictionaryDecoder = new ScanDictionaryDecoder(column, services.File.CacheProvider);
            _rowGroups = rowGroups;
            if (lifetimeOwnership == ScanLifetimeOwnership.Coordinator)
            {
                _userLinkedCancellation = null;
                _linkedCancellation = null;
                _cancellationToken = scanCancellation;
            }
            else
            {
                (_cancellationToken, _linkedCancellation, _userLinkedCancellation) = ScanCancellation.Compose(
                    scanCancellation,
                    enumerationCancellation,
                    _file.DisposalToken);
            }
            try
            {
                if (lifetimeOwnership == ScanLifetimeOwnership.Enumerator)
                    _file.RegisterScan(OnFileDisposed);
            }
            catch
            {
                // Best-effort rollback preserves the primary construction error.
                try { _linkedCancellation?.Dispose(); } catch (Exception) { }
                try { _userLinkedCancellation?.Dispose(); } catch (Exception) { }
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
                    if ((_pagePayloadLease.HasPayload || _pageValues.HasValues ||
                        _pageBinaryOffsets is not null || _pageFixedPayload is not null) &&
                        TryCreateBatchFromPage(out var batch))
                    {
                        // Pre-publication boundary: a token cancelled during decoding must
                        // surface instead of yielding a batch. The built batch is released
                        // best-effort so the cancellation stays the primary error.
                        try
                        {
                            _cancellationToken.ThrowIfCancellationRequested();
                        }
                        catch
                        {
                            try { batch.Dispose(); } catch (Exception) { }
                            throw;
                        }

                        _current = batch;
                        return true;
                    }

                    DisposePage();
                    if (_planIndex < 0 || _rowsSeenInGroup >= _plan.RowGroup.RowCount)
                    {
                        if (_planIndex >= 0 && _pageOffset != _chunkEnd)
                            throw new ParquetFormatException("A column chunk contains trailing pages or bytes after its declared rows.", ParquetErrorLocation.AtPage(_pageOffset, _plan.RowGroup.Ordinal, _column.Ordinal, _pageOrdinal));
                        if (_planIndex >= 0 && _accumulatedUncompressedBytes != _plan.RowGroup.Columns[_column.Ordinal].TotalUncompressedSize)
                            throw new ParquetFormatException("Decoded page sizes do not match the column-chunk uncompressed total.", ParquetErrorLocation.AtPage(_pageOffset, _plan.RowGroup.Ordinal, _column.Ordinal, _pageOrdinal));

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
                // Best-effort termination preserves the primary cancellation for
                // conversion into a file-disposal error below.
                try { Terminate(); } catch (Exception) { }
                throw new ObjectDisposedException(nameof(ParquetFile));
            }
            catch
            {
                // Best-effort termination preserves the primary scan error.
                try { Terminate(); } catch (Exception) { }
                throw;
            }
            finally
            {
                if (_lifetimeOwnership == ScanLifetimeOwnership.Enumerator)
                    _file.ExitScanOperation();
            }

            bool MoveToNextRowGroup()
            {
                _dictionaryDecoder.Reset();
                while (++_planIndex < _rowGroups.Length)
                {
                    _plan = _rowGroups[_planIndex];
                    if (_plan.Count == 0)
                        continue;
                    PreflightChunk(_file, _plan.RowGroup, _column.Ordinal);
                    var chunk = _plan.RowGroup.Columns[_column.Ordinal];

                    _pageOffset = chunk.DictionaryPageOffset.HasValue
                        ? Math.Min(chunk.DictionaryPageOffset.Value, chunk.DataPageOffset)
                        : chunk.DataPageOffset;
                    _expectedDataPageOffset = chunk.DataPageOffset;
                    _chunkEnd = checked(_pageOffset + chunk.TotalCompressedSize);
                    _rowsSeenInGroup = 0;
                    _accumulatedUncompressedBytes = 0;
                    _pageOrdinal = 0;
                    _seenDataPage = false;
                    return true;
                }
                return false;
            }

            void AccumulateUncompressed(ValidatedPageHeader accumulatedHeader, int accumulatedHeaderByteCount)
            {
                long pageUncompressed;
                try
                {
                    pageUncompressed = checked((long)accumulatedHeaderByteCount + accumulatedHeader.UncompressedSize);
                    _accumulatedUncompressedBytes = checked(_accumulatedUncompressedBytes + pageUncompressed);
                }
                catch (OverflowException exception)
                {
                    throw new ParquetFormatException("Column-chunk uncompressed sizes overflow.", exception, ParquetErrorLocation.AtPage(_pageOffset, _plan.RowGroup.Ordinal, _column.Ordinal, _pageOrdinal));
                }
                if (_accumulatedUncompressedBytes > _plan.RowGroup.Columns[_column.Ordinal].TotalUncompressedSize)
                    throw new ParquetFormatException("Decoded pages exceed the column-chunk uncompressed total.", ParquetErrorLocation.AtPage(_pageOffset, _plan.RowGroup.Ordinal, _column.Ordinal, _pageOrdinal));
            }

            void PreflightPageHeader(ValidatedPageHeader preflightHeader, int preflightHeaderByteCount)
            {
                if (preflightHeader.PageType is not ValidatedPageType.DataV1 and not ValidatedPageType.Dictionary and not ValidatedPageType.DataV2)
                    throw new ParquetUnsupportedFeatureException("This scan path requires Data Page V1 or V2.", ParquetErrorLocation.AtPage(_pageOffset, _plan.RowGroup.Ordinal, _column.Ordinal, _pageOrdinal));
                var preflightPhysicalType = _column.SchemaElement.PhysicalType;
                if (preflightPhysicalType is not ParquetPhysicalType.Boolean and
                    not ParquetPhysicalType.Int32 and
                    not ParquetPhysicalType.Int64 and
                    not ParquetPhysicalType.Float and
                    not ParquetPhysicalType.Double and
                    not ParquetPhysicalType.ByteArray and
                    not ParquetPhysicalType.FixedLengthByteArray ||
                    _column.SchemaElement.Repetition is not ParquetRepetition.Required and not ParquetRepetition.Optional)
                    throw new ParquetUnsupportedFeatureException("This scan path currently supports only required or optional flat Core 0.1 leaves.", ParquetErrorLocation.AtPage(_pageOffset, _plan.RowGroup.Ordinal, _column.Ordinal, _pageOrdinal));
                var preflightChunk = _plan.RowGroup.Columns[_column.Ordinal];
                var preflightDictionaryOffset = preflightChunk.DictionaryPageOffset;
                var preflightDataOffset = preflightChunk.DataPageOffset;
                if (preflightHeader.PageType == ValidatedPageType.Dictionary)
                {
                    if (_seenDataPage || _dictionaryDecoder.HasDictionary)
                        throw PageFormat("A dictionary page is duplicated or appears after data pages.");
                    if (preflightDictionaryOffset.HasValue)
                    {
                        if (_pageOffset != preflightDictionaryOffset.Value)
                            throw PageFormat("A dictionary-page offset does not match its advertised position.");
                    }
                    else if (_pageOffset != preflightDataOffset)
                        throw PageFormat("A dictionary-page offset does not match its advertised position.");
                    else
                    {
                        // Tolerated producer shape (observed in Parquet.NET output):
                        // no dictionary offset is advertised, but the leading page
                        // at the data offset is a dictionary page. Only this exact
                        // position qualifies; encoding, size, and payload checks
                        // below still apply, and data pages must follow at the
                        // computed end.
                        _expectedDataPageOffset = checked(_pageOffset + preflightHeaderByteCount + preflightHeader.CompressedSize);
                    }
                    if (preflightHeader.Dictionary.EncodingCode is not (int)ParquetEncoding.Plain and
                        not (int)ParquetEncoding.PlainDictionary)
                        throw new ParquetUnsupportedFeatureException("Dictionary values require the PLAIN or legacy PLAIN_DICTIONARY marker.", ParquetErrorLocation.AtPage(_pageOffset, _plan.RowGroup.Ordinal, _column.Ordinal, _pageOrdinal));
                    if (preflightHeader.Dictionary.ValueCount > _file.Options.MaximumDictionaryEntries ||
                        preflightHeader.UncompressedSize > _file.Options.MaximumDictionaryBytes)
                        throw new ParquetLimitExceededException("A dictionary exceeds the configured entry or byte limit.", ParquetErrorLocation.AtPage(_pageOffset, _plan.RowGroup.Ordinal, _column.Ordinal, _pageOrdinal));
                    AccumulateUncompressed(preflightHeader, preflightHeaderByteCount);
                    return;
                }
                var preflightValueCount = preflightHeader.PageType switch
                {
                    ValidatedPageType.DataV1 => preflightHeader.DataV1.ValueCount,
                    ValidatedPageType.DataV2 => preflightHeader.DataV2.ValueCount,
                    _ => throw new InvalidOperationException("A validated data page has no data header."),
                };
                var preflightEncodingCode = preflightHeader.PageType switch
                {
                    ValidatedPageType.DataV1 => preflightHeader.DataV1.EncodingCode,
                    ValidatedPageType.DataV2 => preflightHeader.DataV2.EncodingCode,
                    _ => throw new InvalidOperationException("A validated data page has no data header."),
                };
                if (preflightEncodingCode is not (int)ParquetEncoding.Plain and
                    not (int)ParquetEncoding.PlainDictionary and
                    not (int)ParquetEncoding.RunLengthDictionary)
                    throw new ParquetUnsupportedFeatureException("The data-page value encoding is unsupported.", ParquetErrorLocation.AtPage(_pageOffset, _plan.RowGroup.Ordinal, _column.Ordinal, _pageOrdinal));
                if (preflightValueCount > _file.Options.MaximumValuesPerPage)
                    throw new ParquetLimitExceededException("A data page exceeds the configured value-count limit.", ParquetErrorLocation.AtPage(_pageOffset, _plan.RowGroup.Ordinal, _column.Ordinal, _pageOrdinal));
                if (preflightValueCount > _plan.RowGroup.RowCount - _rowsSeenInGroup)
                    throw new ParquetFormatException(
                        "Data pages contain more rows than the row group declares.", ParquetErrorLocation.AtPage(_pageOffset, _plan.RowGroup.Ordinal, _column.Ordinal, _pageOrdinal));
                if (!_seenDataPage && !_dictionaryDecoder.HasDictionary && preflightDictionaryOffset.HasValue)
                {
                    if (_pageOffset == preflightDictionaryOffset.Value)
                        throw PageFormat("A data page occupies the advertised dictionary-page offset.");
                    if (preflightDictionaryOffset.Value > _pageOffset)
                        throw PageFormat("A dictionary-page offset lies after data pages.");
                }
                if (_dictionaryDecoder.HasDictionary && !_seenDataPage && _pageOffset != _expectedDataPageOffset)
                    throw PageFormat("A data-page offset does not match its advertised position.");
                var preflightDictionaryEncoded = preflightEncodingCode is (int)ParquetEncoding.PlainDictionary or
                    (int)ParquetEncoding.RunLengthDictionary;
                if (preflightDictionaryEncoded && !_dictionaryDecoder.HasDictionary)
                    throw PageFormat("A dictionary-encoded data page has no preceding dictionary page.");
                if (_column.SchemaElement.Repetition == ParquetRepetition.Optional &&
                    preflightHeader.PageType == ValidatedPageType.DataV1 &&
                    preflightHeader.DataV1.DefinitionEncodingCode != (int)ParquetEncoding.RunLength)
                {
                    if (preflightDictionaryEncoded ||
                        preflightPhysicalType == ParquetPhysicalType.ByteArray ||
                        preflightPhysicalType == ParquetPhysicalType.FixedLengthByteArray)
                        throw new ParquetUnsupportedFeatureException("Optional definition levels require V1 RLE/bit-packed hybrid encoding.", ParquetErrorLocation.AtPage(_pageOffset, _plan.RowGroup.Ordinal, _column.Ordinal, _pageOrdinal));
                    throw new ParquetUnsupportedFeatureException("Optional definition levels require RLE/bit-packed hybrid encoding.", ParquetErrorLocation.AtPage(_pageOffset, _plan.RowGroup.Ordinal, _column.Ordinal, _pageOrdinal));
                }
                if (preflightHeader.PageType == ValidatedPageType.DataV2)
                {
                    var preflightV2 = preflightHeader.DataV2;
                    var preflightCodec = _plan.RowGroup.Columns[_column.Ordinal].CompressionCodec;
                    if (_column.SchemaElement.Repetition == ParquetRepetition.Required)
                    {
                        DefinitionLevelCodec.ValidateRequiredV2(preflightV2, preflightValueCount, CurrentPageLocation());
                    }
                    else if (preflightV2.RowCount != preflightValueCount || preflightV2.RepetitionLevelsByteLength != 0)
                    {
                        if (preflightDictionaryEncoded ||
                            preflightPhysicalType == ParquetPhysicalType.ByteArray ||
                            preflightPhysicalType == ParquetPhysicalType.FixedLengthByteArray)
                            throw PageFormat("A flat optional V2 page has inconsistent row or repetition fields.");
                        if (preflightV2.RowCount != preflightValueCount)
                            throw PageFormat("A flat V2 page has different row and value counts.");
                        throw PageFormat("A flat V2 page contains repetition-level bytes.");
                    }
                    if (V2ValueSectionIsUncompressed(preflightHeader, preflightCodec, out _))
                    {
                        if (preflightHeader.CompressedSize != preflightHeader.UncompressedSize)
                            throw PageFormat("An uncompressed V2 value section has inconsistent sizes.");
                    }
                }
                else
                {
                    var preflightCodec = _plan.RowGroup.Columns[_column.Ordinal].CompressionCodec;
                    if (preflightCodec == ParquetCompressionCodec.Uncompressed &&
                        preflightHeader.UncompressedSize != preflightHeader.CompressedSize)
                        throw new ParquetFormatException(
                            "An uncompressed page declares different compressed and uncompressed sizes.", ParquetErrorLocation.AtPage(_pageOffset, _plan.RowGroup.Ordinal, _column.Ordinal, _pageOrdinal));
                }
                AccumulateUncompressed(preflightHeader, preflightHeaderByteCount);
            }

            async ValueTask LoadNextPageAsync()
            {
                if (_pageOffset >= _chunkEnd)
                    throw new ParquetFormatException(
                        "A column chunk ended before its declared row count.", ParquetErrorLocation.AtPage(_pageOffset, _plan.RowGroup.Ordinal, _column.Ordinal, _pageOrdinal));

                if (_pageOrdinal >= _file.Options.MaximumPagesPerColumnChunk)
                    throw new ParquetLimitExceededException("A column chunk exceeds the configured page-count limit.", ParquetErrorLocation.AtPage(_pageOffset, _plan.RowGroup.Ordinal, _column.Ordinal, _pageOrdinal));


                var parsed = await ScanPageReader.ReadPageHeaderAsync(_file.Source, _file.Options, _pagePayloadCache, _cancellationToken, _pageOffset, _chunkEnd, _plan.RowGroup.Ordinal, _column.Ordinal, _pageOrdinal).ConfigureAwait(false);
                _cancellationToken.ThrowIfCancellationRequested();
                var header = parsed.Header;
                var payloadOffset = checked(_pageOffset + parsed.HeaderByteCount);
                var nextPageOffset = checked(payloadOffset + header.CompressedSize);
                if (nextPageOffset > _chunkEnd)
                    throw new ParquetFormatException(
                        "A page payload exceeds its enclosing column chunk.", ParquetErrorLocation.AtPage(_pageOffset, _plan.RowGroup.Ordinal, _column.Ordinal, _pageOrdinal));
                PreflightPageHeader(header, parsed.HeaderByteCount);

                var pageValueCount = header.PageType switch
                {
                    ValidatedPageType.DataV1 => header.DataV1.ValueCount,
                    ValidatedPageType.DataV2 => header.DataV2.ValueCount,
                    _ => 0,
                };
                var selectedStart = _plan.StartInGroup;
                var selectedEnd = checked(selectedStart + _plan.Count);
                var pageStartInGroup = _rowsSeenInGroup;
                var pageEndInGroup = checked(pageStartInGroup + (long)pageValueCount);
                var overlapStart = Math.Max(pageStartInGroup, selectedStart);
                var overlapEnd = Math.Min(pageEndInGroup, selectedEnd);
                if ((header.PageType is ValidatedPageType.DataV1 or ValidatedPageType.DataV2) &&
                    _column.SchemaElement.Repetition == ParquetRepetition.Required &&
                    overlapEnd <= overlapStart)
                {
                    _seenDataPage = true;
                    _pageValueCount = 0;
                    _pageValueIndex = 0;
                    _pageRowOffset = _rowsSeenInGroup;
                    _rowsSeenInGroup = pageEndInGroup;
                    _pageOffset = nextPageOffset;
                    _pageOrdinal++;
                    return;
                }
                var sliceEncodingCode = header.PageType switch
                {
                    ValidatedPageType.DataV1 => header.DataV1.EncodingCode,
                    ValidatedPageType.DataV2 => header.DataV2.EncodingCode,
                    _ => -1,
                };
                var sliceByteWidth = _column.SchemaElement.PhysicalType switch
                {
                    ParquetPhysicalType.Int32 or ParquetPhysicalType.Float => sizeof(int),
                    ParquetPhysicalType.Int64 or ParquetPhysicalType.Double => sizeof(long),
                    ParquetPhysicalType.FixedLengthByteArray => _column.SchemaElement.TypeLength ?? 0,
                    _ => 0,
                };
                var sliceCodec = _plan.RowGroup.Columns[_column.Ordinal].CompressionCodec;
                var partialPage = overlapStart > pageStartInGroup || overlapEnd < pageEndInGroup;
                var sliceCandidate = partialPage &&
                    _column.SchemaElement.Repetition == ParquetRepetition.Required &&
                    sliceEncodingCode == (int)ParquetEncoding.Plain &&
                    sliceByteWidth > 0 &&
                    sliceCodec == ParquetCompressionCodec.Uncompressed;
                var sliceValueOffset = sliceCandidate && header.PageType == ValidatedPageType.DataV2
                    ? DefinitionLevelCodec.GetV2LevelByteCount(header.DataV2, CurrentPageLocation())
                    : 0;
                var sliceEligible = sliceCandidate &&
                    (header.PageType != ValidatedPageType.DataV2 || sliceValueOffset == 0);
                var sliceStartRow = sliceEligible ? checked((int)(overlapStart - pageStartInGroup)) : 0;
                var sliceRowCount = sliceEligible ? checked((int)(overlapEnd - overlapStart)) : 0;

                PooledArrayOwner<byte>? compressedPayload = null;
                PooledArrayOwner<byte>? decodedPayload = null;
                Exception? loadFailure = null;
                try
                {
                    ReadOnlyMemory<byte> compressedMemory;
                    var slicedPage = false;
                    var pageBorrowed = ScanPageReader.TryGetSourceMemory(_file.Source, _file.Length, payloadOffset, header.CompressedSize, out var borrowedPage);
                    if (sliceEligible && pageBorrowed)
                    {
                        if (header.Crc is int borrowedCrc && ScanPageReader.ComputeCrc32(borrowedPage.Span, _cancellationToken) != unchecked((uint)borrowedCrc))
                            throw new ParquetFormatException(
                                "A page CRC does not match its serialized payload.", ParquetErrorLocation.AtPage(_pageOffset, _plan.RowGroup.Ordinal, _column.Ordinal, _pageOrdinal));
                        _pagePayloadSliceRows = sliceStartRow;
                        compressedMemory = borrowedPage.Slice(checked(sliceStartRow * sliceByteWidth), checked(sliceRowCount * sliceByteWidth));
                        slicedPage = true;
                    }
                    else if (sliceEligible && header.Crc is null)
                    {
                        // Opaque source without a CRC: only selected bytes are read, so no
                        // CRC is owed and none is performed; chunk accounting still uses
                        // the header sizes validated above.
                        var sliceLength = checked(sliceRowCount * sliceByteWidth);
                        compressedPayload = _pagePayloadCache.Rent(sliceLength);
                        await ScanPageReader.ReadExactlyAsync(
                            _file.Source,
                            _cancellationToken,
                            checked(payloadOffset + (long)sliceStartRow * sliceByteWidth),
                            new ArraySegment<byte>(compressedPayload.Array, 0, compressedPayload.Memory.Length),
                            "The immutable input ended during a page read.",
                            ParquetErrorLocation.AtPage(_pageOffset, _plan.RowGroup.Ordinal, _column.Ordinal, _pageOrdinal))
                            .ConfigureAwait(false);
                        compressedMemory = compressedPayload.Memory;
                        _pagePayloadSliceRows = sliceStartRow;
                        slicedPage = true;
                    }
                    else if (pageBorrowed)
                    {
                        compressedMemory = borrowedPage;
                    }
                    else
                    {
                        compressedPayload = _pagePayloadCache.Rent(header.CompressedSize);
                        await ScanPageReader.ReadExactlyAsync(
                            _file.Source,
                            _cancellationToken,
                            payloadOffset,
                            new ArraySegment<byte>(compressedPayload.Array, 0, compressedPayload.Memory.Length),
                            "The immutable input ended during a page read.",
                            ParquetErrorLocation.AtPage(_pageOffset, _plan.RowGroup.Ordinal, _column.Ordinal, _pageOrdinal))
                            .ConfigureAwait(false);
                        compressedMemory = compressedPayload.Memory;
                    }
                    _cancellationToken.ThrowIfCancellationRequested();
                    if (!slicedPage && header.Crc is int expected && ScanPageReader.ComputeCrc32(compressedMemory.Span, _cancellationToken) != unchecked((uint)expected))
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
                            try
                            {
                                decodedPayload = DecodeCompressedV2Payload(header, compressedMemory.Span);
                            }
                            catch (ParquetFormatException exception) when (exception.RowGroupOrdinal is null)
                            {
                                throw AnnotatedDecompressionFailure(exception);
                            }
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
                                try
                                {
                                    SnappyBlockDecoder.Decompress(
                                        compressedMemory.Span,
                                        decodedPayload.Memory.Span,
                                        _cancellationToken);
                                }
                                catch (ParquetFormatException exception) when (exception.RowGroupOrdinal is null)
                                {
                                    throw AnnotatedDecompressionFailure(exception);
                                }
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
                        if (_seenDataPage || _dictionaryDecoder.HasDictionary)
                            throw PageFormat("A dictionary page is duplicated or appears after data pages.");
                        if (header.Dictionary.EncodingCode is not (int)ParquetEncoding.Plain and
                            not (int)ParquetEncoding.PlainDictionary)
                            throw new ParquetUnsupportedFeatureException("Dictionary values require the PLAIN or legacy PLAIN_DICTIONARY marker.", ParquetErrorLocation.AtPage(_pageOffset, _plan.RowGroup.Ordinal, _column.Ordinal, _pageOrdinal));

                        if (header.Dictionary.ValueCount > _file.Options.MaximumDictionaryEntries ||
                            header.UncompressedSize > _file.Options.MaximumDictionaryBytes)
                            throw new ParquetLimitExceededException("A dictionary exceeds the configured entry or byte limit.", ParquetErrorLocation.AtPage(_pageOffset, _plan.RowGroup.Ordinal, _column.Ordinal, _pageOrdinal));

                        _dictionaryDecoder.DecodePage(decodedMemory.Span, header.Dictionary.ValueCount, physicalType.Value, _file.Options, _memoryBudget, CurrentPageLocation(), _cancellationToken);
                        _pageOffset = nextPageOffset;
                        _pageOrdinal++;
                    }
                    else
                    {
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
                        if (valueCount > _plan.RowGroup.RowCount - _rowsSeenInGroup)
                            throw new ParquetFormatException(
                                "Data pages contain more rows than the row group declares.", ParquetErrorLocation.AtPage(_pageOffset, _plan.RowGroup.Ordinal, _column.Ordinal, _pageOrdinal));

                        var dictionaryEncoded = encodingCode is (int)ParquetEncoding.PlainDictionary or
                            (int)ParquetEncoding.RunLengthDictionary;
                        if (dictionaryEncoded && !_dictionaryDecoder.HasDictionary)
                            throw PageFormat("A dictionary-encoded data page has no preceding dictionary page.");

                        if (dictionaryEncoded)
                        {
                            _dictionaryDecoder.ExpandDataPage(header, decodedMemory.Span, valueCount, physicalType.Value, _file.Options, _memoryBudget, ref _pageValues, ref _pageValidity, ref _pageBinaryOffsets, ref _pageBinaryPayload, ref _pageFixedPayload, ref _pageFixedWidth, CurrentPageLocation(), _cancellationToken);
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
                            if (header.PageType == ValidatedPageType.DataV2)
                                DefinitionLevelCodec.ValidateRequiredV2(header.DataV2, valueCount, CurrentPageLocation());
                            var expectedBytes = PlainDecoder.GetPlainByteCount(physicalType.Value, valueCount, CurrentPageLocation());
                            var storedValuesLength = codec == ParquetCompressionCodec.Uncompressed ? header.CompressedSize : decodedMemory.Length;
                            if (storedValuesLength - valueOffset != expectedBytes)
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

                        _pageValueCount = valueCount;
                        _pageValueIndex = 0;
                        _pageRowOffset = _rowsSeenInGroup;
                        _rowsSeenInGroup += _pageValueCount;
                        _pageOffset = nextPageOffset;
                        _pageOrdinal++;
                    }
                }
                catch (Exception exception)
                {
                    loadFailure = exception;
                }
                try { decodedPayload?.Dispose(); } catch (Exception exception) when (loadFailure is null) { loadFailure = exception; } catch (Exception) { }
                try { compressedPayload?.Dispose(); } catch (Exception exception) when (loadFailure is null) { loadFailure = exception; } catch (Exception) { }
                if (loadFailure is not null)
                    System.Runtime.ExceptionServices.ExceptionDispatchInfo.Capture(loadFailure).Throw();
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
            PlainPageDecode<T>? decoder)
            where T : unmanaged
            {
                if (sourceIndex == 0 && count == _pageValueCount &&
                    _pageValues.TryTake<T>(out var pageOwner) && pageOwner is not null)
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
                    return transferredBatch;
                }

                PooledArrayOwner<T>? owner = _file.RentColumnValues<T>(_column, count);
                PooledArrayOwner<byte>? validityOwner = null;
                try
                {
                    var output = owner.Memory.Span;
                    ReadOnlyMemory<byte> validityBits;
                    bool allValid;
                    if (_pageValues.TryGetValues<T>(out var pageValues) && pageValues is not null)
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
                            input.Slice(checked((sourceIndex - _pagePayloadSliceRows) * byteWidth), checked(count * byteWidth)),
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
                catch
                {
                    // Best-effort rollback preserves the primary error.
                    try { owner?.Dispose(); } catch (Exception) { }
                    try { validityOwner?.Dispose(); } catch (Exception) { }
                    throw;
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
                catch
                {
                    // Best-effort rollback preserves the primary error.
                    try { offsets?.Dispose(); } catch (Exception) { }
                    try { payload?.Dispose(); } catch (Exception) { }
                    try { validityOwner?.Dispose(); } catch (Exception) { }
                    throw;
                }
            }

            DecodedColumnBatch CreateFixedLengthByteArrayBatch(long candidate, int sourceIndex, int count)
            {
                if (_pageFixedWidth <= 0)
                    throw new InvalidOperationException("A decoded FIXED_LEN_BYTE_ARRAY page has an invalid width.");
                if (_pageFixedWidth > _file.Options.MaximumBinaryValueBytes)
                    throw new ParquetLimitExceededException("A FIXED_LEN_BYTE_ARRAY value exceeds the configured byte limit.", ParquetErrorLocation.AtRowGroupColumnPage(_plan.RowGroup.Ordinal, _column.Ordinal, _pageOrdinal - 1));
                var requestedByteCount = checked((long)count * _pageFixedWidth);
                if (requestedByteCount > _file.Options.MaximumBinaryBatchBytes || requestedByteCount > int.MaxValue)
                    throw new ParquetLimitExceededException("A FIXED_LEN_BYTE_ARRAY batch exceeds the configured binary batch limit.", ParquetErrorLocation.AtRowGroupColumnPage(_plan.RowGroup.Ordinal, _column.Ordinal, _pageOrdinal - 1));
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
                PooledArrayOwner<byte>? payload = PooledArrayOwner<byte>.Rent(byteCount, _memoryBudget); // requestedByteCount already bounds this rent.
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
                catch
                {
                    // Best-effort rollback preserves the primary error.
                    try { payload?.Dispose(); } catch (Exception) { }
                    try { validityOwner?.Dispose(); } catch (Exception) { }
                    throw;
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
                PooledArrayOwner<int>? levels = null;
                Exception? levelsFailure = null;
                try
                {
                    var binarySection = DefinitionLevelCodec.DecodeSection(header, payload, rowCount, _column.SchemaElement.Repetition, _memoryBudget, _cancellationToken, CurrentPageLocation());
                    levels = binarySection.Levels;
                    var levelSpan = levels is null ? ReadOnlySpan<int>.Empty : levels.Memory.Span;
                    var physicalOffset = binarySection.PhysicalOffset;
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
                    catch
                    {
                        // Best-effort rollback preserves the primary page error.
                        try { offsets?.Dispose(); } catch (Exception) { }
                        try { data?.Dispose(); } catch (Exception) { }
                        try { validity?.Dispose(); } catch (Exception) { }
                        throw;
                    }
                }
                catch (Exception exception)
                {
                    levelsFailure = exception;
                }
                try { levels?.Dispose(); } catch (Exception exception) when (levelsFailure is null) { levelsFailure = exception; } catch (Exception) { }
                if (levelsFailure is not null)
                    System.Runtime.ExceptionServices.ExceptionDispatchInfo.Capture(levelsFailure).Throw();
            }

            void DecodeFixedLengthByteArrayPage(ValidatedPageHeader header, ReadOnlySpan<byte> payload, int rowCount)
            {
                var width = _column.SchemaElement.TypeLength ?? 0;
                if (width <= 0)
                    throw PageFormat("A FIXED_LEN_BYTE_ARRAY column has an invalid width.");
                if (width > _file.Options.MaximumBinaryValueBytes)
                    throw new ParquetLimitExceededException("A FIXED_LEN_BYTE_ARRAY value exceeds the configured byte limit.", ParquetErrorLocation.AtPage(_pageOffset, _plan.RowGroup.Ordinal, _column.Ordinal, _pageOrdinal));
                PooledArrayOwner<int>? levels = null;
                Exception? levelsFailure = null;
                try
                {
                    var fixedSection = DefinitionLevelCodec.DecodeSection(header, payload, rowCount, _column.SchemaElement.Repetition, _memoryBudget, _cancellationToken, CurrentPageLocation());
                    levels = fixedSection.Levels;
                    var levelSpan = levels is null ? ReadOnlySpan<int>.Empty : levels.Memory.Span;
                    var physicalOffset = fixedSection.PhysicalOffset;
                    var physicalCount = fixedSection.ValidCount;
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
                    catch
                    {
                        // Best-effort rollback preserves the primary page error.
                        try { values?.Dispose(); } catch (Exception) { }
                        try { validity?.Dispose(); } catch (Exception) { }
                        throw;
                    }
                }
                catch (Exception exception)
                {
                    levelsFailure = exception;
                }
                try { levels?.Dispose(); } catch (Exception exception) when (levelsFailure is null) { levelsFailure = exception; } catch (Exception) { }
                if (levelsFailure is not null)
                    System.Runtime.ExceptionServices.ExceptionDispatchInfo.Capture(levelsFailure).Throw();
            }

            void DecodeOptionalPrimitivePageV1(ReadOnlySpan<byte> payload, int rowCount, ParquetPhysicalType physicalType)
            {
                var section = DefinitionLevelCodec.SplitOptionalV1Section(payload, CurrentPageLocation());
                DecodeOptionalPrimitivePage(
                    payload,
                    rowCount,
                    physicalType,
                    section.LevelOffset,
                    section.LevelByteCount,
                    section.PhysicalOffset,
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

                        _pageValues.Set(rowValues);
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
                    catch
                    {
                        // Best-effort rollback preserves the primary page error.
                        try { rowValues?.Dispose(); } catch (Exception) { }
                        try { validity?.Dispose(); } catch (Exception) { }
                        throw;
                    }
                    return;
                }

                void DecodeOptionalBooleanBitmapPage(
                ReadOnlySpan<byte> payload,
                int rowCount,
                int levelOffset,
                int levelByteCount,
                int physicalOffset,
                int? expectedNullCount)
                {
                    PooledArrayOwner<bool>? rowValues = null;
                    PooledArrayOwner<byte>? validity = null;
                    try
                    {
                        rowValues = _file.RentColumnValues<bool>(_column, rowCount);
                        validity = PooledArrayOwner<byte>.Rent(
                            checked((rowCount + 7) / 8),
                            _memoryBudget);
                        var levelInput = payload.Slice(levelOffset, levelByteCount);
                        int validCount;
                        try
                        {
                            var consumed = RleBitPackedHybridDecoder.DecodeBitWidthOneToBitmap(
                                levelInput,
                                rowCount,
                                validity.Memory.Span,
                                _cancellationToken,
                                out validCount);
                            if (consumed != levelInput.Length)
                                throw PageFormat("An optional page has trailing definition-level bytes.");
                            if (expectedNullCount is int nullCount && rowCount - validCount != nullCount)
                                throw PageFormat("A V2 page null count does not match its definition levels.");
                        }
                        catch (ParquetFormatException exception) when (exception.ByteOffset is null)
                        {
                            throw new ParquetFormatException(exception.Message, exception, ParquetErrorLocation.AtPage(_pageOffset, _plan.RowGroup.Ordinal, _column.Ordinal, _pageOrdinal));

                        }
                        var booleanPhysicalByteCount = checked((validCount + 7) / 8);
                        if (booleanPhysicalByteCount != payload.Length - physicalOffset)
                            throw PageFormat("An optional fixed-width page has an inconsistent physical-value length.");
                        var booleanPhysicalPayload = payload.Slice(physicalOffset, booleanPhysicalByteCount);
                        var booleanOutput = rowValues.Memory.Span;
                        if (validCount == rowCount)
                        {
                            PlainDecoder.DecodeBoolean(booleanPhysicalPayload, booleanOutput, _cancellationToken);
                        }
                        else
                        {
                            booleanOutput.Clear();
                            var validityBytes = validity.Memory.Span;
                            var physicalBitIndex = 0;
                            for (var byteIndex = 0; byteIndex < validityBytes.Length; byteIndex++)
                            {
                                if ((byteIndex & 511) == 0)
                                    _cancellationToken.ThrowIfCancellationRequested();
                                var remainingBits = (uint)validityBytes[byteIndex];
                                while (remainingBits != 0)
                                {
                                    var bitIndex = BitOperations.TrailingZeroCount(remainingBits);
                                    booleanOutput[(byteIndex << 3) + bitIndex] =
                                        (booleanPhysicalPayload[physicalBitIndex >> 3] & (1 << (physicalBitIndex & 7))) != 0;
                                    physicalBitIndex++;
                                    remainingBits &= remainingBits - 1;
                                }
                            }
                        }

                        _pageValues.Set(rowValues);
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
                    catch
                    {
                        // Best-effort rollback preserves the primary page error.
                        try { rowValues?.Dispose(); } catch (Exception) { }
                        try { validity?.Dispose(); } catch (Exception) { }
                        throw;
                    }
                }

                void DecodeOptionalInt64BitmapPage(
                ReadOnlySpan<byte> payload,
                int rowCount,
                int levelOffset,
                int levelByteCount,
                int physicalOffset,
                int? expectedNullCount)
                {
                    PooledArrayOwner<long>? rowValues = null;
                    PooledArrayOwner<byte>? validity = null;
                    try
                    {
                        rowValues = _file.RentColumnValues<long>(_column, rowCount);
                        validity = PooledArrayOwner<byte>.Rent(
                            checked((rowCount + 7) / 8),
                            _memoryBudget);
                        var levelInput = payload.Slice(levelOffset, levelByteCount);
                        int validCount;
                        try
                        {
                            var consumed = RleBitPackedHybridDecoder.DecodeBitWidthOneToBitmap(
                                levelInput,
                                rowCount,
                                validity.Memory.Span,
                                _cancellationToken,
                                out validCount);
                            if (consumed != levelInput.Length)
                                throw PageFormat("An optional page has trailing definition-level bytes.");
                            if (expectedNullCount is int nullCount && rowCount - validCount != nullCount)
                                throw PageFormat("A V2 page null count does not match its definition levels.");
                        }
                        catch (ParquetFormatException exception) when (exception.ByteOffset is null)
                        {
                            throw new ParquetFormatException(exception.Message, exception, ParquetErrorLocation.AtPage(_pageOffset, _plan.RowGroup.Ordinal, _column.Ordinal, _pageOrdinal));

                        }
                        var int64PhysicalByteCount = checked(validCount * sizeof(long));
                        if (int64PhysicalByteCount != payload.Length - physicalOffset)
                            throw PageFormat("An optional fixed-width page has an inconsistent physical-value length.");
                        var int64PhysicalPayload = payload.Slice(physicalOffset, int64PhysicalByteCount);
                        var int64Output = rowValues.Memory.Span;
                        if (validCount == rowCount)
                        {
                            PlainDecoder.DecodeInt64(int64PhysicalPayload, int64Output, _cancellationToken);
                        }
                        else
                        {
                            int64Output.Clear();
                            var validityBytes = validity.Memory.Span;
                            var physicalByteOffset = 0;
                            for (var byteIndex = 0; byteIndex < validityBytes.Length; byteIndex++)
                            {
                                if ((byteIndex & 511) == 0)
                                    _cancellationToken.ThrowIfCancellationRequested();
                                var remainingBits = (uint)validityBytes[byteIndex];
                                while (remainingBits != 0)
                                {
                                    var bitIndex = BitOperations.TrailingZeroCount(remainingBits);
                                    int64Output[(byteIndex << 3) + bitIndex] =
                                        BinaryPrimitives.ReadInt64LittleEndian(
                                            int64PhysicalPayload.Slice(physicalByteOffset, sizeof(long)));
                                    physicalByteOffset += sizeof(long);
                                    remainingBits &= remainingBits - 1;
                                }
                            }

                            if (physicalByteOffset != int64PhysicalPayload.Length)
                                throw PageFormat("Definition levels do not match the physical-value count.");
                        }

                        _pageValues.Set(rowValues);
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
                    catch
                    {
                        // Best-effort rollback preserves the primary page error.
                        try { rowValues?.Dispose(); } catch (Exception) { }
                        try { validity?.Dispose(); } catch (Exception) { }
                        throw;
                    }
                }

                void DecodeOptionalFloatBitmapPage(
                ReadOnlySpan<byte> payload,
                int rowCount,
                int levelOffset,
                int levelByteCount,
                int physicalOffset,
                int? expectedNullCount)
                {
                    PooledArrayOwner<float>? rowValues = null;
                    PooledArrayOwner<byte>? validity = null;
                    try
                    {
                        rowValues = _file.RentColumnValues<float>(_column, rowCount);
                        validity = PooledArrayOwner<byte>.Rent(
                            checked((rowCount + 7) / 8),
                            _memoryBudget);
                        var levelInput = payload.Slice(levelOffset, levelByteCount);
                        int validCount;
                        try
                        {
                            var consumed = RleBitPackedHybridDecoder.DecodeBitWidthOneToBitmap(
                                levelInput,
                                rowCount,
                                validity.Memory.Span,
                                _cancellationToken,
                                out validCount);
                            if (consumed != levelInput.Length)
                                throw PageFormat("An optional page has trailing definition-level bytes.");
                            if (expectedNullCount is int nullCount && rowCount - validCount != nullCount)
                                throw PageFormat("A V2 page null count does not match its definition levels.");
                        }
                        catch (ParquetFormatException exception) when (exception.ByteOffset is null)
                        {
                            throw new ParquetFormatException(exception.Message, exception, ParquetErrorLocation.AtPage(_pageOffset, _plan.RowGroup.Ordinal, _column.Ordinal, _pageOrdinal));

                        }
                        var floatPhysicalByteCount = checked(validCount * sizeof(int));
                        if (floatPhysicalByteCount != payload.Length - physicalOffset)
                            throw PageFormat("An optional fixed-width page has an inconsistent physical-value length.");
                        var floatPhysicalPayload = payload.Slice(physicalOffset, floatPhysicalByteCount);
                        var floatOutput = rowValues.Memory.Span;
                        if (validCount == rowCount)
                        {
                            PlainDecoder.DecodeFloat(floatPhysicalPayload, floatOutput, _cancellationToken);
                        }
                        else
                        {
                            floatOutput.Clear();
                            var validityBytes = validity.Memory.Span;
                            var physicalByteOffset = 0;
                            for (var byteIndex = 0; byteIndex < validityBytes.Length; byteIndex++)
                            {
                                if ((byteIndex & 511) == 0)
                                    _cancellationToken.ThrowIfCancellationRequested();
                                var remainingBits = (uint)validityBytes[byteIndex];
                                while (remainingBits != 0)
                                {
                                    var bitIndex = BitOperations.TrailingZeroCount(remainingBits);
                                    floatOutput[(byteIndex << 3) + bitIndex] =
                                        BinaryPrimitives.ReadSingleLittleEndian(
                                            floatPhysicalPayload.Slice(physicalByteOffset, sizeof(int)));
                                    physicalByteOffset += sizeof(int);
                                    remainingBits &= remainingBits - 1;
                                }
                            }

                            if (physicalByteOffset != floatPhysicalPayload.Length)
                                throw PageFormat("Definition levels do not match the physical-value count.");
                        }

                        _pageValues.Set(rowValues);
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
                    catch
                    {
                        // Best-effort rollback preserves the primary page error.
                        try { rowValues?.Dispose(); } catch (Exception) { }
                        try { validity?.Dispose(); } catch (Exception) { }
                        throw;
                    }
                }

                void DecodeOptionalDoubleBitmapPage(
                ReadOnlySpan<byte> payload,
                int rowCount,
                int levelOffset,
                int levelByteCount,
                int physicalOffset,
                int? expectedNullCount)
                {
                    PooledArrayOwner<double>? rowValues = null;
                    PooledArrayOwner<byte>? validity = null;
                    try
                    {
                        rowValues = _file.RentColumnValues<double>(_column, rowCount);
                        validity = PooledArrayOwner<byte>.Rent(
                            checked((rowCount + 7) / 8),
                            _memoryBudget);
                        var levelInput = payload.Slice(levelOffset, levelByteCount);
                        int validCount;
                        try
                        {
                            var consumed = RleBitPackedHybridDecoder.DecodeBitWidthOneToBitmap(
                                levelInput,
                                rowCount,
                                validity.Memory.Span,
                                _cancellationToken,
                                out validCount);
                            if (consumed != levelInput.Length)
                                throw PageFormat("An optional page has trailing definition-level bytes.");
                            if (expectedNullCount is int nullCount && rowCount - validCount != nullCount)
                                throw PageFormat("A V2 page null count does not match its definition levels.");
                        }
                        catch (ParquetFormatException exception) when (exception.ByteOffset is null)
                        {
                            throw new ParquetFormatException(exception.Message, exception, ParquetErrorLocation.AtPage(_pageOffset, _plan.RowGroup.Ordinal, _column.Ordinal, _pageOrdinal));

                        }
                        var doublePhysicalByteCount = checked(validCount * sizeof(long));
                        if (doublePhysicalByteCount != payload.Length - physicalOffset)
                            throw PageFormat("An optional fixed-width page has an inconsistent physical-value length.");
                        var doublePhysicalPayload = payload.Slice(physicalOffset, doublePhysicalByteCount);
                        var doubleOutput = rowValues.Memory.Span;
                        if (validCount == rowCount)
                        {
                            PlainDecoder.DecodeDouble(doublePhysicalPayload, doubleOutput, _cancellationToken);
                        }
                        else
                        {
                            doubleOutput.Clear();
                            var validityBytes = validity.Memory.Span;
                            var physicalByteOffset = 0;
                            for (var byteIndex = 0; byteIndex < validityBytes.Length; byteIndex++)
                            {
                                if ((byteIndex & 511) == 0)
                                    _cancellationToken.ThrowIfCancellationRequested();
                                var remainingBits = (uint)validityBytes[byteIndex];
                                while (remainingBits != 0)
                                {
                                    var bitIndex = BitOperations.TrailingZeroCount(remainingBits);
                                    doubleOutput[(byteIndex << 3) + bitIndex] =
                                        BinaryPrimitives.ReadDoubleLittleEndian(
                                            doublePhysicalPayload.Slice(physicalByteOffset, sizeof(long)));
                                    physicalByteOffset += sizeof(long);
                                    remainingBits &= remainingBits - 1;
                                }
                            }

                            if (physicalByteOffset != doublePhysicalPayload.Length)
                                throw PageFormat("Definition levels do not match the physical-value count.");
                        }

                        _pageValues.Set(rowValues);
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
                    catch
                    {
                        // Best-effort rollback preserves the primary page error.
                        try { rowValues?.Dispose(); } catch (Exception) { }
                        try { validity?.Dispose(); } catch (Exception) { }
                        throw;
                    }
                }

                switch (physicalType)
                {
                    case ParquetPhysicalType.Boolean:
                        DecodeOptionalBooleanBitmapPage(payload, rowCount, levelOffset, levelByteCount, physicalOffset, expectedNullCount);
                        break;
                    case ParquetPhysicalType.Int64:
                        DecodeOptionalInt64BitmapPage(payload, rowCount, levelOffset, levelByteCount, physicalOffset, expectedNullCount);
                        break;
                    case ParquetPhysicalType.Float:
                        DecodeOptionalFloatBitmapPage(payload, rowCount, levelOffset, levelByteCount, physicalOffset, expectedNullCount);
                        break;
                    case ParquetPhysicalType.Double:
                        DecodeOptionalDoubleBitmapPage(payload, rowCount, levelOffset, levelByteCount, physicalOffset, expectedNullCount);
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
                catch
                {
                    // Best-effort rollback preserves the primary page error.
                    try { decoded?.Dispose(); } catch (Exception) { }
                    throw;
                }
            }

            void DecodeRequiredBooleanPage(ReadOnlySpan<byte> payload, int rowCount)
            {
                PooledArrayOwner<bool>? values = _file.RentColumnValues<bool>(_column, rowCount);
                try
                {
                    PlainDecoder.DecodeBoolean(payload, values.Memory.Span, _cancellationToken);
                    _pageValues.Set(values);
                    values = null;
                    _pageValidity.SetAllValid();
                }
                catch
                {
                    // Best-effort rollback preserves the primary page error.
                    try { values?.Dispose(); } catch (Exception) { }
                    throw;
                }
            }


            ParquetFormatException PageFormat(string message) => new(
            message,
            ParquetErrorLocation.AtPage(_pageOffset, _plan.RowGroup.Ordinal, _column.Ordinal, _pageOrdinal));

            // Attaches the page scopes to a decompression failure while keeping the
            // offending input offset when the decoder recorded one.
            ParquetFormatException AnnotatedDecompressionFailure(ParquetFormatException exception) => new(
                exception.Message,
                exception,
                ParquetErrorLocation.AtPage(exception.ByteOffset ?? _pageOffset, _plan.RowGroup.Ordinal, _column.Ordinal, _pageOrdinal));

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
            try { _pageValues.Dispose(); } catch (Exception exception) when (failure is null) { failure = exception; } catch (Exception) { }
            try { _pageValidity.Dispose(); } catch (Exception exception) when (failure is null) { failure = exception; } catch (Exception) { }
            try { _pageBinaryOffsets?.Dispose(); } catch (Exception exception) when (failure is null) { failure = exception; } catch (Exception) { }
            _pageBinaryOffsets = null;
            try { _pageBinaryPayload?.Dispose(); } catch (Exception exception) when (failure is null) { failure = exception; } catch (Exception) { }
            _pageBinaryPayload = null;
            try { _pageFixedPayload?.Dispose(); } catch (Exception exception) when (failure is null) { failure = exception; } catch (Exception) { }
            _pageFixedPayload = null;
            _pageFixedWidth = 0;
            _pagePayloadSliceRows = 0;
            _pageValueCount = 0;
            _pageValueIndex = 0;
            if (failure is not null)
                System.Runtime.ExceptionServices.ExceptionDispatchInfo.Capture(failure).Throw();
        }

        // Teardown exception policy. Standalone teardown releases every owned resource,
        // preserves the first cleanup failure, and always releases scan registration.
        // Rollback while another error is already in flight is best-effort exhaustive
        // and preserves that primary error instead of masking it with teardown diagnostics.
        private void Terminate()
        {
            if (_terminated)
                return;
            _terminated = true;
            Exception? failure = null;
            try { DisposePage(); } catch (Exception exception) when (failure is null) { failure = exception; } catch (Exception) { }
            try { _dictionaryDecoder.Dispose(); } catch (Exception exception) when (failure is null) { failure = exception; } catch (Exception) { }
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
    internal readonly record struct ScanCursorServices(ParquetFile File, ParquetScanOptions Options);
    internal enum ScanLifetimeOwnership { Enumerator, Coordinator }
    internal readonly record struct ScanRowGroup(ParquetRowGroup RowGroup, long StartInGroup, long Count);
}























