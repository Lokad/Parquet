namespace Lokad.Parquet.Internal;

internal sealed class ParquetScanMemoryBudget
{
    private readonly long _maximumBytes;
    private long _retainedBytes;
    private long _peakRetainedBytes;
    private long _peakTransientBytes;

    public ParquetScanMemoryBudget(long maximumBytes) => _maximumBytes = maximumBytes;

    public long MaximumBytes => _maximumBytes;

    public long PeakRetainedBytes => Interlocked.Read(ref _peakRetainedBytes);

    public long PeakTransientBytes => Interlocked.Read(ref _peakTransientBytes);

    // Records simultaneous live bytes: already retained storage plus the array held
    // transiently before its budget reservation.
    public void NoteTransientAttempt(long byteCount)
    {
        var simultaneous = Interlocked.Read(ref _retainedBytes) + byteCount;
        while (true)
        {
            var peak = Interlocked.Read(ref _peakTransientBytes);
            if (simultaneous <= peak ||
                Interlocked.CompareExchange(ref _peakTransientBytes, simultaneous, peak) == peak)
                return;
        }
    }

    // Rejects a rent whose smallest possible footprint already exceeds the remaining
    // budget, before the shared pool is touched. Rounded bucket capacity is still
    // enforced after the rent; this check can never reject a feasible rent because
    // pooled capacity always covers the requested minimum.
    public void ThrowIfMinimumExceedsRemaining(long minimumBytes)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(minimumBytes);
        if (minimumBytes > _maximumBytes - Interlocked.Read(ref _retainedBytes))
            throw new ParquetLimitExceededException("A scan exceeds its configured pooled-memory limit.");
    }

    public void Reserve(long byteCount)
    {
        void UpdatePeak(long retainedBytes)
        {
            while (true)
            {
                var peak = Interlocked.Read(ref _peakRetainedBytes);
                if (retainedBytes <= peak ||
                    Interlocked.CompareExchange(ref _peakRetainedBytes, retainedBytes, peak) == peak)
                    return;
            }
        }

        ArgumentOutOfRangeException.ThrowIfNegative(byteCount);
        while (true)
        {
            var retained = Interlocked.Read(ref _retainedBytes);
            if (byteCount > _maximumBytes - retained)
                throw new ParquetLimitExceededException("A scan exceeds its configured pooled-memory limit.");
            var next = retained + byteCount;
            if (Interlocked.CompareExchange(ref _retainedBytes, next, retained) != retained)
                continue;
            UpdatePeak(next);
            return;
        }
    }

    public void Release(long byteCount)
    {
        var remaining = Interlocked.Add(ref _retainedBytes, -byteCount);
        if (remaining < 0)
            throw new InvalidOperationException("The scan pooled-memory budget is unbalanced.");
    }
}
