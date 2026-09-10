namespace Lokad.Parquet.Internal;

// Shared scan-cancellation composition for the single and projected lanes. Links
// the caller and enumeration tokens, then the file disposal token, and hands
// ownership of both linked sources to the caller for construction rollback and
// teardown. The tuple carries no heap allocation; linked sources are created
// once per scan, never per page or batch.
internal static class ScanCancellation
{
    internal static (CancellationToken Token, CancellationTokenSource? Linked, CancellationTokenSource? UserLinked) Compose(
        CancellationToken scanCancellation,
        CancellationToken enumerationCancellation,
        CancellationToken disposalToken)
    {
        CancellationTokenSource? userLinked = null;
        if (scanCancellation.CanBeCanceled && enumerationCancellation.CanBeCanceled)
            userLinked = CancellationTokenSource.CreateLinkedTokenSource(scanCancellation, enumerationCancellation);
        var userCancellation = userLinked?.Token ??
            (scanCancellation.CanBeCanceled ? scanCancellation : enumerationCancellation);
        if (!userCancellation.CanBeCanceled)
            return (disposalToken, null, null);
        var linked = CancellationTokenSource.CreateLinkedTokenSource(userCancellation, disposalToken);
        return (linked.Token, linked, userLinked);
    }
}
