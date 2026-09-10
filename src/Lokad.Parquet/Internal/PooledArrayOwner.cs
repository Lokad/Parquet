namespace Lokad.Parquet.Internal;

internal sealed class PooledArrayOwner<T> : IDisposable
{
    private T[]? _array;
    private readonly ParquetScanMemoryBudget _budget;
    private readonly int _retainedBytes;
    private readonly PooledArrayOwnerCache<T>? _cache;

    internal PooledArrayOwner(
        T[] array,
        int length,
        ParquetScanMemoryBudget budget,
        int retainedBytes,
        PooledArrayOwnerCache<T>? cache)
    {
        _array = array;
        _budget = budget;
        _retainedBytes = retainedBytes;
        _cache = cache;
        Memory = array.AsMemory(0, length);
    }

    internal static readonly int ElementByteSize;

    static PooledArrayOwner()
    {
        // Safe managed element measurement without Unsafe or Marshal: one short-lived
        // single-element array, computed once per element type. Shipped rents use
        // primitive element types; any other layout falls back to one byte, which keeps
        // the pre-rent check a sound lower bound while actual bucket capacity remains
        // enforced after the rent.
        try
        {
            ElementByteSize = Math.Max(Buffer.ByteLength(System.Array.CreateInstance(typeof(T), 1)), 1);
        }
        catch (ArgumentException)
        {
            ElementByteSize = 1;
        }
    }

    public Memory<T> Memory { get; }

    internal T[] Array => _array ?? throw new ObjectDisposedException(nameof(PooledArrayOwner<T>));

    public static PooledArrayOwner<T> Rent(int length, ParquetScanMemoryBudget budget)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(length);
        var minimumLength = Math.Max(length, 1);
        budget.ThrowIfMinimumExceedsRemaining(checked((long)minimumLength * ElementByteSize));
        var array = ParquetArrayPool.Rent<T>(minimumLength);
        var retainedBytes = Buffer.ByteLength(array);
        budget.NoteTransientAttempt(retainedBytes);
        if (retainedBytes > budget.MaximumBytes)
        {
            ParquetArrayPool.Return(array);
            throw new ParquetLimitExceededException("A scan exceeds its configured pooled-memory limit.");
        }
        var reserved = false;
        try
        {
            budget.Reserve(retainedBytes);
            reserved = true;
            return new PooledArrayOwner<T>(array, length, budget, retainedBytes, null);
        }
        catch
        {
            if (reserved)
                ReturnAndRelease(array, budget, retainedBytes);
            else
                ParquetArrayPool.Return(array);
            throw;
        }
    }

    public void Dispose()
    {
        var array = Interlocked.Exchange(ref _array, null);
        if (array is null)
            return;
        if (_cache is not null)
        {
            _cache.Return(array, _retainedBytes);
            return;
        }
        ReturnAndRelease(array, _budget, _retainedBytes);
    }

    internal static void ReturnAndRelease(
        T[] array,
        ParquetScanMemoryBudget budget,
        int retainedBytes)
    {
        // Pool return clears input-derived contents; budget release must still run if instrumentation fails.
        try
        {
            ParquetArrayPool.Return(array);
        }
        finally
        {
            budget.Release(retainedBytes);
        }
    }
}

/// <summary>Releases idle retained storage held by a file-owned array cache.</summary>
internal interface IEvictableArrayCache
{
    /// <summary>Releases the idle retained array, if any, and reports whether one was released.</summary>
    bool EvictIdle();
}

/// <summary>File-owned typed array cache: rents typed pooled buffers with disposal and idle eviction.</summary>
internal interface IColumnValueCache : IEvictableArrayCache, IDisposable
{
    /// <summary>Rents a typed pooled buffer; the requested element type must match the cache.</summary>
    PooledArrayOwner<T> Rent<T>(int length);
}

/// <summary>Rents typed pooled buffers from the file-owned column caches.</summary>
internal interface IColumnValueCacheProvider
{
    /// <summary>Rents a typed pooled buffer from the cache owned by the column.</summary>
    PooledArrayOwner<T> RentColumnValues<T>(ParquetColumn column, int length);
}
internal sealed class PooledArrayOwnerCache<T> : IColumnValueCache
{
    private readonly ParquetScanMemoryBudget _budget;
    private readonly object _lock = new();
    private T[]? _array;
    private int _retainedBytes;
    private bool _disposed;

    public PooledArrayOwnerCache(ParquetScanMemoryBudget budget) => _budget = budget;

    PooledArrayOwner<TRequested> IColumnValueCache.Rent<TRequested>(int length)
    {
        if (typeof(TRequested) != typeof(T))
            throw new InvalidOperationException("A column value cache has an inconsistent physical type.");
        return (PooledArrayOwner<TRequested>)(object)Rent(length);
    }

    public PooledArrayOwner<T> Rent(int length)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(length);
        var minimumLength = Math.Max(length, 1);
        T[]? undersized = null;
        var undersizedBytes = 0;
        lock (_lock)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            var cached = _array;
            if (cached is not null && cached.Length >= minimumLength)
            {
                _array = null;
                var retainedBytes = _retainedBytes;
                _retainedBytes = 0;
                return new PooledArrayOwner<T>(cached, length, _budget, retainedBytes, this);
            }
            if (cached is not null)
            {
                undersized = cached;
                undersizedBytes = _retainedBytes;
                _array = null;
                _retainedBytes = 0;
            }
        }
        if (undersized is not null)
            PooledArrayOwner<T>.ReturnAndRelease(undersized, _budget, undersizedBytes);

        _budget.ThrowIfMinimumExceedsRemaining(checked((long)minimumLength * PooledArrayOwner<T>.ElementByteSize));
        var array = ParquetArrayPool.Rent<T>(minimumLength);
        var bytes = Buffer.ByteLength(array);
        _budget.NoteTransientAttempt(bytes);
        if (bytes > _budget.MaximumBytes)
        {
            ParquetArrayPool.Return(array);
            throw new ParquetLimitExceededException("A scan exceeds its configured pooled-memory limit.");
        }
        var reserved = false;
        try
        {
            _budget.Reserve(bytes);
            reserved = true;
            lock (_lock)
            {
                ObjectDisposedException.ThrowIf(_disposed, this);
                return new PooledArrayOwner<T>(array, length, _budget, bytes, this);
            }
        }
        catch
        {
            if (reserved)
                PooledArrayOwner<T>.ReturnAndRelease(array, _budget, bytes);
            else
                ParquetArrayPool.Return(array);
            throw;
        }
    }

    public void Dispose()
    {
        T[]? array;
        int retainedBytes;
        // File disposal may race the final return of a yielded batch, but decoding itself remains sequential.
        lock (_lock)
        {
            if (_disposed)
                return;
            _disposed = true;
            array = _array;
            retainedBytes = _retainedBytes;
            _array = null;
            _retainedBytes = 0;
        }
        if (array is not null)
            PooledArrayOwner<T>.ReturnAndRelease(array, _budget, retainedBytes);
    }

    // Releases the idle retained array, if any, without marking the cache
    // disposed. Arrays checked out to live owners or yielded batches are never
    // touched; only idle retained storage is released.
    public bool EvictIdle()
    {
        T[]? array;
        int retainedBytes;
        lock (_lock)
        {
            array = _array;
            retainedBytes = _retainedBytes;
            _array = null;
            _retainedBytes = 0;
            if (array is null)
                return false;
        }
        PooledArrayOwner<T>.ReturnAndRelease(array, _budget, retainedBytes);
        return true;
    }

    // File-owned caches retain one array outside the shared pool; evicted or
    // disposed arrays return to the shared pool cleared via ReturnAndRelease.
    internal void Return(T[] array, int retainedBytes)
    {
        T[]? released;
        int releasedBytes;
        lock (_lock)
        {
            if (_disposed)
            {
                released = array;
                releasedBytes = retainedBytes;
            }
            else if (_array is null)
            {
                _array = array;
                _retainedBytes = retainedBytes;
                return;
            }
            else if (array.Length <= _array.Length)
            {
                released = array;
                releasedBytes = retainedBytes;
            }
            else
            {
                released = _array;
                releasedBytes = _retainedBytes;
                _array = array;
                _retainedBytes = retainedBytes;
            }
        }
        PooledArrayOwner<T>.ReturnAndRelease(released, _budget, releasedBytes);
    }
}




