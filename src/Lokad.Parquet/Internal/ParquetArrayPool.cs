using System.Buffers;

namespace Lokad.Parquet.Internal;

internal static class ParquetArrayPool
{
    private static readonly AsyncLocal<Action<Array, int>?> CurrentRentObserver = new();
    private static readonly AsyncLocal<Action<Array, int>?> CurrentReturnObserver = new();

    public static Action<Array, int>? RentObserver
    {
        get => CurrentRentObserver.Value;
        set => CurrentRentObserver.Value = value;
    }

    public static Action<Array, int>? ReturnObserver
    {
        get => CurrentReturnObserver.Value;
        set => CurrentReturnObserver.Value = value;
    }

    public static T[] Rent<T>(int minimumLength)
    {
        var array = ArrayPool<T>.Shared.Rent(minimumLength);
        try
        {
            RentObserver?.Invoke(array, minimumLength);
            return array;
        }
        catch
        {
            ArrayPool<T>.Shared.Return(array, clearArray: true);
            throw;
        }
    }

    public static void Return<T>(T[] array)
    {
        try
        {
            ReturnObserver?.Invoke(array, array.Length);
        }
        finally
        {
            ArrayPool<T>.Shared.Return(array, clearArray: true);
        }
    }
}
