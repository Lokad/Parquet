using System.Buffers.Binary;
using System.Diagnostics;
using System.Runtime.ExceptionServices;
using Lokad.Parquet.Internal;

namespace Lokad.Parquet;

/// <summary>Represents an open, validated Parquet file.</summary>
public sealed class ParquetFile : IAsyncDisposable
{
    private static ReadOnlySpan<byte> Magic => "PAR1"u8;
    private static ReadOnlySpan<byte> EncryptedFooterMagic => "PARE"u8;

    private readonly IParquetRandomAccessSource _source;
    private readonly ParquetSourceOwnership _sourceOwnership;
    private readonly ParquetScanMemoryBudget _scanMemoryBudget;
    private readonly PooledArrayOwnerCache<byte> _pagePayloadCache;
    private readonly IDisposable?[] _columnValueCaches;
    private readonly object _lifetimeLock = new();
    private CancellationTokenSource? _disposeCancellation;
    private Action? _activeScan;
    private TaskCompletionSource? _operationsDrained;
    private Task? _disposeTask;
    private int _activeScanOperations;
    private bool _disposed;

    private ParquetFile(
        IParquetRandomAccessSource source,
        ParquetSourceOwnership sourceOwnership,
        ParquetReaderOptions options,
        ParquetFileMetadata metadata)
    {
        _source = source;
        _sourceOwnership = sourceOwnership;
        _scanMemoryBudget = new ParquetScanMemoryBudget(options.MaximumScanPooledBytes);
        _pagePayloadCache = new PooledArrayOwnerCache<byte>(_scanMemoryBudget);
        _columnValueCaches = new IDisposable?[metadata.Schema.Columns.Count];
        Options = options;
        Metadata = metadata;
        Length = source.Length;
    }

    /// <summary>Gets the immutable input length observed while opening.</summary>
    public long Length { get; }

    /// <summary>Gets the validated immutable file metadata.</summary>
    public ParquetFileMetadata Metadata { get; }

    /// <summary>Gets the reader options captured while opening.</summary>
    public ParquetReaderOptions Options { get; }

    /// <summary>Creates the file's single active asynchronous projected scan.</summary>
    /// <param name="options">Projection, row selection, and batching options.</param>
    /// <returns>An asynchronous sequence whose current batch must be disposed before advancing.</returns>
    public IAsyncEnumerable<ParquetBatch> ScanAsync(ParquetScanOptions options) =>
        ScanAsync(options, CancellationToken.None);

    /// <summary>Creates the file's single active asynchronous projected scan.</summary>
    /// <param name="options">Projection, row selection, and batching options.</param>
    /// <param name="cancellationToken">Cancellation observed throughout the scan.</param>
    /// <returns>An asynchronous sequence whose current batch must be disposed before advancing.</returns>
    public IAsyncEnumerable<ParquetBatch> ScanAsync(
        ParquetScanOptions options,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(options);
        ObjectDisposedException.ThrowIf(_disposed, this);
        var projectedCount = options.Columns.Count;
        return projectedCount > 1
            ? new ParquetProjectedScanEnumerable(this, options, cancellationToken)
            : new ParquetScanEnumerable(this, options, cancellationToken);
    }

    /// <summary>Opens a file path and takes ownership of its file handle.</summary>
    /// <param name="path">Path to an ordinary Parquet file.</param>
    /// <returns>The open Parquet file.</returns>
    public static ValueTask<ParquetFile> OpenAsync(string path) =>
        OpenAsync(path, ParquetReaderOptions.Default, CancellationToken.None);

    /// <summary>Opens a file path and takes ownership of its file handle.</summary>
    /// <param name="path">Path to an ordinary Parquet file.</param>
    /// <param name="options">Immutable safety limits.</param>
    /// <param name="cancellationToken">Cancellation for opening reads.</param>
    /// <returns>The open Parquet file.</returns>
    public static ValueTask<ParquetFile> OpenAsync(
        string path,
        ParquetReaderOptions options,
        CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(path);
        ArgumentNullException.ThrowIfNull(options);
        var source = new FileRandomAccessSource(path);
        return OpenOwnedAsync(source, options, cancellationToken);
    }

    /// <summary>Opens immutable Parquet content held in managed memory.</summary>
    /// <param name="content">The content, whose backing owner must remain alive and whose bytes must remain unchanged while the file is open.</param>
    /// <returns>The open Parquet file.</returns>
    public static ValueTask<ParquetFile> OpenAsync(ReadOnlyMemory<byte> content) =>
        OpenAsync(content, ParquetReaderOptions.Default, CancellationToken.None);

    /// <summary>Opens immutable Parquet content held in managed memory with explicit safety limits.</summary>
    /// <param name="content">The content, whose backing owner must remain alive and whose bytes must remain unchanged while the file is open.</param>
    /// <param name="options">Immutable safety limits.</param>
    /// <param name="cancellationToken">Cancellation for opening reads.</param>
    /// <returns>The open Parquet file.</returns>
    public static ValueTask<ParquetFile> OpenAsync(
        ReadOnlyMemory<byte> content,
        ParquetReaderOptions options,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(options);
        var source = new MemoryRandomAccessSource(content);
        return OpenOwnedAsync(source, options, cancellationToken);
    }

    /// <summary>Opens a readable seekable stream and leaves it caller-owned.</summary>
    /// <param name="stream">The caller-owned readable seekable stream.</param>
    /// <returns>The open Parquet file.</returns>
    public static ValueTask<ParquetFile> OpenAsync(Stream stream) =>
        OpenAsync(
            stream,
            ParquetSourceOwnership.Caller,
            ParquetReaderOptions.Default,
            CancellationToken.None);

    /// <summary>Opens a readable seekable stream with explicit ownership and safety limits.</summary>
    /// <param name="stream">The readable seekable stream.</param>
    /// <param name="ownership">Who disposes the stream.</param>
    /// <param name="options">Immutable safety limits.</param>
    /// <param name="cancellationToken">Cancellation for opening reads.</param>
    /// <returns>The open Parquet file.</returns>
    public static ValueTask<ParquetFile> OpenAsync(
        Stream stream,
        ParquetSourceOwnership ownership,
        ParquetReaderOptions options,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(stream);
        ArgumentNullException.ThrowIfNull(options);
        ValidateOwnership(ownership);
        var source = new StreamRandomAccessSource(stream, ownership);
        return OpenOwnedAsync(source, options, cancellationToken);
    }

    /// <summary>Opens a caller-provided random-access source and leaves it caller-owned.</summary>
    /// <param name="source">The caller-owned immutable random-access source.</param>
    /// <returns>The open Parquet file.</returns>
    public static ValueTask<ParquetFile> OpenAsync(IParquetRandomAccessSource source) =>
        OpenAsync(
            source,
            ParquetSourceOwnership.Caller,
            ParquetReaderOptions.Default,
            CancellationToken.None);

    /// <summary>Opens a caller-provided random-access source with explicit ownership and safety limits.</summary>
    /// <param name="source">The immutable random-access source.</param>
    /// <param name="ownership">Who disposes the source.</param>
    /// <param name="options">Immutable safety limits.</param>
    /// <param name="cancellationToken">Cancellation for opening reads.</param>
    /// <returns>The open Parquet file.</returns>
    public static ValueTask<ParquetFile> OpenAsync(
        IParquetRandomAccessSource source,
        ParquetSourceOwnership ownership,
        ParquetReaderOptions options,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(source);
        ArgumentNullException.ThrowIfNull(options);
        ValidateOwnership(ownership);
        return ownership == ParquetSourceOwnership.Caller
            ? OpenCoreAsync(source, ParquetSourceOwnership.Caller, options, cancellationToken)
            : OpenOwnedAsync(source, options, cancellationToken);
    }

    /// <summary>Ends an active scan and disposes any transferred source; an already-yielded batch remains separately disposable.</summary>
    public ValueTask DisposeAsync()
    {
        bool CanDisposeSynchronously() =>
            _sourceOwnership == ParquetSourceOwnership.Caller ||
            _source is FileRandomAccessSource or MemoryRandomAccessSource ||
            _source is StreamRandomAccessSource { CanDisposeSynchronously: true };

        void CompleteSynchronousDisposal(CancellationTokenSource? disposalCancellation)
        {
            Exception? failure = null;
            try
            {
                _pagePayloadCache.Dispose();
            }
            catch (Exception exception) when (failure is null)
            {
                failure = exception;
            }
            catch (Exception)
            {
            }

            try
            {
                DisposeColumnValueCaches();
            }
            catch (Exception exception) when (failure is null)
            {
                failure = exception;
            }
            catch (Exception)
            {
            }

            if (_sourceOwnership == ParquetSourceOwnership.ParquetFile)
            {
                try
                {
                    var sourceDisposal = _source.DisposeAsync();
                    if (!sourceDisposal.IsCompletedSuccessfully)
                        throw new InvalidOperationException("A built-in source did not dispose synchronously.");
                    sourceDisposal.GetAwaiter().GetResult();
                }
                catch (Exception exception) when (failure is null)
                {
                    failure = exception;
                }
                catch (Exception)
                {
                }
            }

            try
            {
                disposalCancellation?.Dispose();
            }
            catch (Exception exception) when (failure is null)
            {
                failure = exception;
            }
            catch (Exception)
            {
            }

            Task completedTask = failure is null ? Task.CompletedTask : Task.FromException(failure);

            lock (_lifetimeLock)
            {
                _disposeTask = completedTask;
                Monitor.PulseAll(_lifetimeLock);
            }
        }

        async Task DisposeCoreAsync(
            Task operationsDrained,
            TaskCompletionSource completion,
            CancellationTokenSource? disposalCancellation)
        {
            Exception? failure = null;
            try
            {
                disposalCancellation?.Cancel();
            }
            catch (Exception exception) when (failure is null)
            {
                failure = exception;
            }
            catch (Exception)
            {
            }

            try
            {
                await operationsDrained.ConfigureAwait(false);
            }
            catch (Exception exception) when (failure is null)
            {
                failure = exception;
            }
            catch (Exception)
            {
            }

            Action? terminateScan;
            lock (_lifetimeLock)
            {
                terminateScan = _activeScan;
                _activeScan = null;
            }

            try
            {
                terminateScan?.Invoke();
            }
            catch (Exception exception) when (failure is null)
            {
                failure = exception;
            }
            catch (Exception)
            {
            }

            try
            {
                _pagePayloadCache.Dispose();
            }
            catch (Exception exception) when (failure is null)
            {
                failure = exception;
            }
            catch (Exception)
            {
            }

            try
            {
                DisposeColumnValueCaches();
            }
            catch (Exception exception) when (failure is null)
            {
                failure = exception;
            }
            catch (Exception)
            {
            }

            if (_sourceOwnership == ParquetSourceOwnership.ParquetFile)
            {
                try
                {
                    await _source.DisposeAsync().ConfigureAwait(false);
                }
                catch (Exception exception) when (failure is null)
                {
                    failure = exception;
                }
                catch (Exception)
                {
                }
            }

            try
            {
                disposalCancellation?.Dispose();
            }
            catch (Exception exception) when (failure is null)
            {
                failure = exception;
            }
            catch (Exception)
            {
            }

            if (failure is null)
                completion.TrySetResult();
            else
                completion.TrySetException(failure);
        }

        Task? task = null;
        Task? operationsDrained = null;
        TaskCompletionSource? completion = null;
        CancellationTokenSource? disposalCancellation;
        var disposeSynchronously = false;
        lock (_lifetimeLock)
        {
            // A concurrent disposer releases this lock while the first completes eligible synchronous cleanup.
            while (_disposed && _disposeTask is null)
                Monitor.Wait(_lifetimeLock);
            if (_disposeTask is not null)
                return new ValueTask(_disposeTask);

            _disposed = true;
            disposalCancellation = _disposeCancellation;
            if (_activeScanOperations == 0 && _activeScan is null && CanDisposeSynchronously())
            {
                disposeSynchronously = true;
            }
            else
            {
                operationsDrained = _activeScanOperations == 0
                    ? Task.CompletedTask
                    : (_operationsDrained ??= new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously)).Task;
                completion = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                task = completion.Task;
                _disposeTask = task;
            }
        }

        if (disposeSynchronously)
        {
            CompleteSynchronousDisposal(disposalCancellation);
            lock (_lifetimeLock)
                return new ValueTask(_disposeTask ?? throw new InvalidOperationException("File disposal did not complete."));
        }
        _ = DisposeCoreAsync(
            operationsDrained ?? throw new InvalidOperationException("File disposal has no operation-drain task."),
            completion ?? throw new InvalidOperationException("File disposal has no completion source."),
            disposalCancellation);
        return new ValueTask(task ?? throw new InvalidOperationException("File disposal has no task."));
    }

    internal IParquetRandomAccessSource Source => _source;

    internal ParquetScanMemoryBudget ScanMemoryBudget => _scanMemoryBudget;

    internal PooledArrayOwnerCache<byte> PagePayloadCache => _pagePayloadCache;

    internal PooledArrayOwner<T> RentColumnValues<T>(ParquetColumn column, int length)
    {
        var existing = _columnValueCaches[column.Ordinal];
        if (existing is null)
        {
            var created = new PooledArrayOwnerCache<T>(_scanMemoryBudget);
            _columnValueCaches[column.Ordinal] = created;
            return created.Rent(length);
        }
        if (existing is not PooledArrayOwnerCache<T> cache)
            throw new InvalidOperationException("A column value cache has an inconsistent physical type.");
        return cache.Rent(length);
    }

    // Releases idle arrays retained by file-owned caches for columns outside the
    // new projection, before any payload I/O. Arrays checked out to live owners
    // or yielded batches are never touched. The linear keep-set scan runs once
    // per scan start; wide projections use a sorted copy for binary search.
    internal void EvictIdleColumnCaches(ReadOnlySpan<int> keepOrdinals)
    {
        int[]? sorted = null;
        if (keepOrdinals.Length > 16)
        {
            sorted = keepOrdinals.ToArray();
            Array.Sort(sorted);
        }
        for (var ordinal = 0; ordinal < _columnValueCaches.Length; ordinal++)
        {
            if (IsKept(keepOrdinals, sorted, ordinal))
                continue;
            if (_columnValueCaches[ordinal] is IEvictableArrayCache cache)
                cache.EvictIdle();
        }

        static bool IsKept(ReadOnlySpan<int> keepOrdinals, int[]? sorted, int ordinal)
        {
            if (sorted is not null)
                return Array.BinarySearch(sorted, ordinal) >= 0;
            foreach (var keep in keepOrdinals)
            {
                if (keep == ordinal)
                    return true;
            }
            return false;
        }
    }

    internal CancellationToken DisposalToken
    {
        get
        {
            lock (_lifetimeLock)
            {
                ObjectDisposedException.ThrowIf(_disposed, this);
                return (_disposeCancellation ??= new CancellationTokenSource()).Token;
            }
        }
    }

    private void DisposeColumnValueCaches()
    {
        Exception? failure = null;
        foreach (var cache in _columnValueCaches)
        {
            try
            {
                cache?.Dispose();
            }
            catch (Exception exception) when (failure is null)
            {
                failure = exception;
            }
            catch (Exception)
            {
            }
        }

        if (failure is not null)
            System.Runtime.ExceptionServices.ExceptionDispatchInfo.Capture(failure).Throw();
    }

    internal bool IsDisposed
    {
        get
        {
            lock (_lifetimeLock)
                return _disposed;
        }
    }

    internal void RegisterScan(Action onFileDisposed)
    {
        lock (_lifetimeLock)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            if (_activeScan is not null)
                throw new InvalidOperationException("Only one scan may be active on a Parquet file.");
            _activeScan = onFileDisposed;
        }
    }

    internal void UnregisterScan()
    {
        lock (_lifetimeLock)
            _activeScan = null;
    }

    internal void EnterScanOperation()
    {
        lock (_lifetimeLock)
        {
            ObjectDisposedException.ThrowIf(_disposed || _activeScan is null, this);
            _activeScanOperations++;
        }
    }

    internal void ExitScanOperation()
    {
        TaskCompletionSource? drained = null;
        lock (_lifetimeLock)
        {
            if (_activeScanOperations <= 0)
                throw new InvalidOperationException("The scan operation count is unbalanced.");
            _activeScanOperations--;
            if (_activeScanOperations == 0)
            {
                drained = _operationsDrained;
                _operationsDrained = null;
            }
        }
        drained?.TrySetResult();
    }

    private static void ValidateOwnership(ParquetSourceOwnership ownership)
    {
        if (ownership is not (ParquetSourceOwnership.Caller or ParquetSourceOwnership.ParquetFile))
            throw new ArgumentOutOfRangeException(nameof(ownership));
    }

    private static ValueTask<ParquetFile> OpenOwnedAsync(
        IParquetRandomAccessSource source,
        ParquetReaderOptions options,
        CancellationToken cancellationToken)
    {
        ValueTask<ParquetFile> opening;
        try
        {
            opening = OpenCoreAsync(
                source,
                ParquetSourceOwnership.ParquetFile,
                options,
                cancellationToken);
        }
        catch (Exception exception)
        {
            return DisposeAfterSynchronousFailureAsync(exception);
        }
        if (opening.IsCompletedSuccessfully)
            return opening;
        return AwaitOpeningAsync(opening);

        async ValueTask<ParquetFile> AwaitOpeningAsync(ValueTask<ParquetFile> pendingOpening)
        {
            try
            {
                return await pendingOpening.ConfigureAwait(false);
            }
            catch
            {
                await source.DisposeAsync().ConfigureAwait(false);
                throw;
            }
        }

        async ValueTask<ParquetFile> DisposeAfterSynchronousFailureAsync(Exception exception)
        {
            await source.DisposeAsync().ConfigureAwait(false);
            ExceptionDispatchInfo.Capture(exception).Throw();
            throw new UnreachableException();
        }
    }

    private static async ValueTask<ParquetFile> OpenCoreAsync(
        IParquetRandomAccessSource source,
        ParquetSourceOwnership sourceOwnership,
        ParquetReaderOptions options,
        CancellationToken cancellationToken)
    {
        ValidateOptions();
        if (source.Length < 0)
            throw new ArgumentException("The source length must be non-negative.", nameof(source));
        if (source.Length < 12)
            throw new ParquetFormatException("The input is too short to be an ordinary Parquet file.");

        ValueTask ReadInputExactlyAsync(
            long offset,
            ArraySegment<byte> destination)
        {
            ValueTask read;
            try
            {
                read = source.ReadExactlyAsync(offset, destination, cancellationToken);
            }
            catch (EndOfStreamException exception)
            {
                throw new ParquetFormatException("The immutable input ended during an exact read.", exception, ParquetErrorLocation.AtOffset(offset));
            }
            if (read.IsCompletedSuccessfully)
            {
                try
                {
                    read.GetAwaiter().GetResult();
                }
                catch (EndOfStreamException exception)
                {
                    throw new ParquetFormatException("The immutable input ended during an exact read.", exception, ParquetErrorLocation.AtOffset(offset));
                }

                return ValueTask.CompletedTask;
            }

            return AwaitReadAsync(read);

            async ValueTask AwaitReadAsync(ValueTask pendingRead)
            {
                try
                {
                    await pendingRead.ConfigureAwait(false);
                }
                catch (EndOfStreamException exception)
                {
                    throw new ParquetFormatException("The immutable input ended during an exact read.", exception, ParquetErrorLocation.AtOffset(offset));
                }
            }
        }

        var boundaries = new byte[12];
        var leading = new ArraySegment<byte>(boundaries, 0, 4);
        var tail = new ArraySegment<byte>(boundaries, 4, 8);
        await ReadInputExactlyAsync(0, leading).ConfigureAwait(false);
        await ReadInputExactlyAsync(source.Length - tail.Count, tail).ConfigureAwait(false);

        if (!leading.AsSpan().SequenceEqual(Magic))
            throw new ParquetFormatException("The leading Parquet magic is invalid.", ParquetErrorLocation.AtOffset(0));
        if (tail.AsSpan()[4..].SequenceEqual(EncryptedFooterMagic))
            throw new ParquetUnsupportedFeatureException("Encrypted Parquet footers are unsupported.", ParquetErrorLocation.AtOffset(source.Length - 4));
        if (!tail.AsSpan()[4..].SequenceEqual(Magic))
            throw new ParquetFormatException("The trailing Parquet magic is invalid.", ParquetErrorLocation.AtOffset(source.Length - 4));

        var footerLength = BinaryPrimitives.ReadInt32LittleEndian(tail.AsSpan());
        if (footerLength < 0)
            throw new ParquetFormatException("The footer length is negative.", ParquetErrorLocation.AtOffset(source.Length - 8));
        if (footerLength > options.MaximumFooterBytes)
            throw new ParquetLimitExceededException("The footer exceeds the configured byte limit.", ParquetErrorLocation.AtOffset(source.Length - 8));
        if (footerLength > source.Length - 12)
            throw new ParquetFormatException("The footer range lies outside the input.", ParquetErrorLocation.AtOffset(source.Length - 8));

        var footerOffset = source.Length - 8 - footerLength;
        var footer = ParquetArrayPool.Rent<byte>(Math.Max(footerLength, 1));
        try
        {
            var footerSegment = new ArraySegment<byte>(footer, 0, footerLength);
            await ReadInputExactlyAsync(footerOffset, footerSegment).ConfigureAwait(false);
            cancellationToken.ThrowIfCancellationRequested();
            var metadata = ParquetFooterParser.Parse(
                footerSegment.AsSpan(),
                footerOffset,
                source.Length,
                options,
                cancellationToken);
            return new ParquetFile(source, sourceOwnership, options, metadata);
        }
        finally
        {
            ParquetArrayPool.Return(footer);
        }

        void ValidateOptions()
        {
            static void ValidateRange(int value, int minimum, int maximum, string name)
            {
                if (value < minimum || value > maximum)
                    throw new ArgumentOutOfRangeException(name);
            }

            ValidateRange(options.MaximumFooterBytes, 1, 1024 * 1024 * 1024, nameof(options.MaximumFooterBytes));
            ValidateRange(options.MaximumThriftDepth, 1, 256, nameof(options.MaximumThriftDepth));
            ValidateRange(options.MaximumThriftContainerElements, 1, 16_777_216, nameof(options.MaximumThriftContainerElements));
            ValidateRange(options.MaximumSchemaElements, 1, 1_048_576, nameof(options.MaximumSchemaElements));
            ValidateRange(options.MaximumLeafColumns, 1, 65_536, nameof(options.MaximumLeafColumns));
            ValidateRange(options.MaximumRowGroups, 1, 1_048_576, nameof(options.MaximumRowGroups));
            ValidateRange(options.MaximumKeyValueMetadataEntries, 0, 1_048_576, nameof(options.MaximumKeyValueMetadataEntries));
            ValidateRange(options.MaximumMetadataStringBytes, 0, 256 * 1024 * 1024, nameof(options.MaximumMetadataStringBytes));
            ValidateRange(options.MaximumPageHeaderBytes, 1, 16 * 1024 * 1024, nameof(options.MaximumPageHeaderBytes));
            ValidateRange(options.MaximumCompressedPageBytes, 1, 1024 * 1024 * 1024, nameof(options.MaximumCompressedPageBytes));
            ValidateRange(options.MaximumUncompressedPageBytes, 1, 1024 * 1024 * 1024, nameof(options.MaximumUncompressedPageBytes));
            ValidateRange(options.MaximumPagesPerColumnChunk, 1, 16_777_216, nameof(options.MaximumPagesPerColumnChunk));
            ValidateRange(options.MaximumDictionaryEntries, 1, 134_217_728, nameof(options.MaximumDictionaryEntries));
            ValidateRange(options.MaximumDictionaryBytes, 1, 1024 * 1024 * 1024, nameof(options.MaximumDictionaryBytes));
            ValidateRange(options.MaximumValuesPerPage, 1, 134_217_728, nameof(options.MaximumValuesPerPage));
            ValidateRange(options.MaximumBinaryValueBytes, 1, 1024 * 1024 * 1024, nameof(options.MaximumBinaryValueBytes));
            ValidateRange(options.MaximumRowsPerBatch, 1, 16_777_216, nameof(options.MaximumRowsPerBatch));
            ValidateRange(options.MaximumBinaryBatchBytes, 1, 1024 * 1024 * 1024, nameof(options.MaximumBinaryBatchBytes));
            if (options.MaximumScanPooledBytes is < 1 or > 8L * 1024 * 1024 * 1024)
                throw new ArgumentOutOfRangeException(nameof(options.MaximumScanPooledBytes));
        }
    }

}
