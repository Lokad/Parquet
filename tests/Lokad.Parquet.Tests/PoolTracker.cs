using System.Reflection;

namespace Lokad.Parquet.Tests;

internal sealed class PoolTracker : IDisposable
{
    private static readonly Type PoolType =
        (typeof(ParquetFile).Assembly.GetType("Lokad.Parquet.Internal.ParquetArrayPool") ??
            throw new InvalidOperationException("The internal pool facade was not found."));
    private static readonly PropertyInfo RentObserverProperty =
        PoolType.GetProperty("RentObserver", BindingFlags.Public | BindingFlags.Static) ??
            throw new InvalidOperationException("The internal pool rent observer was not found.");
    private static readonly PropertyInfo ReturnObserverProperty =
        PoolType.GetProperty("ReturnObserver", BindingFlags.Public | BindingFlags.Static) ??
            throw new InvalidOperationException("The internal pool return observer was not found.");

    internal static PropertyInfo ReturnObserver => ReturnObserverProperty;

    // Exclusive manual instrumentation with the same ownership rule as the
    // tracker scope: both observers must be free on entry, and ClearObservers
    // always runs in finally.
    internal static void SetObservers(Action<Array, int>? onRent, Action<Array, int>? onReturn)
    {
        Assert.Null(RentObserverProperty.GetValue(null));
        Assert.Null(ReturnObserverProperty.GetValue(null));
        RentObserverProperty.SetValue(null, onRent);
        ReturnObserverProperty.SetValue(null, onReturn);
    }

    internal static void ClearObservers()
    {
        RentObserverProperty.SetValue(null, null);
        ReturnObserverProperty.SetValue(null, null);
    }

    private readonly Dictionary<Array, int> _outstanding = new(ReferenceEqualityComparer.Instance);
    private bool _disposed;

    public PoolTracker()
    {
        void ObserveRent(Array array, int requestedLength)
        {
            lock (_outstanding)
            {
                Assert.True(array.Length >= requestedLength, "A pool rent was shorter than requested.");
                Assert.True(_outstanding.TryAdd(array, array.Length), "The same pooled array was rented twice concurrently.");
                RentCount++;
                OutstandingBytes += Buffer.ByteLength(array);
                PeakOutstandingBytes = Math.Max(PeakOutstandingBytes, OutstandingBytes);
            }
        }

        void ObserveReturn(Array array, int returnedLength)
        {
            lock (_outstanding)
            {
                Assert.Equal(array.Length, returnedLength);
                Assert.True(_outstanding.Remove(array, out var rentedLength), "A pooled array was returned twice or without a rent.");
                Assert.Equal(rentedLength, array.Length);
                ReturnCount++;
                OutstandingBytes -= Buffer.ByteLength(array);
            }
        }

        Assert.Null(RentObserverProperty.GetValue(null));
        Assert.Null(ReturnObserverProperty.GetValue(null));
        RentObserverProperty.SetValue(null, (Action<Array, int>)ObserveRent);
        ReturnObserverProperty.SetValue(null, (Action<Array, int>)ObserveReturn);
    }

    public int RentCount { get; private set; }
    public int ReturnCount { get; private set; }
    public long PeakOutstandingBytes { get; private set; }
    public long OutstandingBytes { get; private set; }

    public void Dispose()
    {
        if (_disposed)
            return;
        _disposed = true;
        RentObserverProperty.SetValue(null, null);
        ReturnObserverProperty.SetValue(null, null);
        Assert.True(_outstanding.Count == 0,
            $"{_outstanding.Count} pooled array(s) were not returned.");
        Assert.Equal(RentCount, ReturnCount);
        Assert.Equal(0, OutstandingBytes);
    }
}

// Live-rent set shared by failure-injection tests: records each rent, forgets
// each return, and reports whether every array went home. Locking and
// double-rent detection match the per-test doubles this replaces.
internal sealed class PoolOutstandingArrays
{
    private readonly Dictionary<Array, int> _live = new(ReferenceEqualityComparer.Instance);

    public void NoteRent(Array array)
    {
        lock (_live)
            _live.Add(array, array.Length);
    }

    public void NoteReturn(Array array)
    {
        lock (_live)
            _live.Remove(array);
    }

    public bool IsEmpty
    {
        get
        {
            lock (_live)
                return _live.Count == 0;
        }
    }
}
