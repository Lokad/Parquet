namespace Lokad.Parquet.Internal;

internal sealed class ParquetScanMemoryBudget
{
    private readonly long _maximumBytes;
    private long _retainedBytes;
    private long _peakRetainedBytes;

    public ParquetScanMemoryBudget(long maximumBytes) => _maximumBytes = maximumBytes;

    public long PeakRetainedBytes => Interlocked.Read(ref _peakRetainedBytes);

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
