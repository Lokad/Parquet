using System.Runtime.ExceptionServices;

namespace Lokad.Parquet.Internal;

internal static class OwnerDisposal
{
    internal static void Dispose(IDisposable? owner, ref Exception? failure)
    {
        try
        {
            owner?.Dispose();
        }
        catch (Exception exception)
        {
            failure ??= exception;
        }
    }

    internal static void DisposeAll(IDisposable[] owners)
    {
        Exception? failure = null;
        foreach (var owner in owners)
            Dispose(owner, ref failure);
        if (failure is not null)
            ExceptionDispatchInfo.Capture(failure).Throw();
    }
}
