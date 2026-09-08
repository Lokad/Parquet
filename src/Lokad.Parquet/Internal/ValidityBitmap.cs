namespace Lokad.Parquet.Internal;

// Shared least-significant-bit-first validity slicing used by single-column
// batch emission and projected batch realignment. An absent returned owner
// means all-valid; callers must not rent a bitmap in that case.
internal static class ValidityBitmap
{
    internal static PooledArrayOwner<byte>? CopySlice(
        ReadOnlyMemory<byte> sourceBits,
        int sourceOffset,
        int count,
        ParquetScanMemoryBudget budget,
        CancellationToken cancellationToken,
        out ReadOnlyMemory<byte> bits,
        out bool allValid)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(sourceOffset);
        ArgumentOutOfRangeException.ThrowIfNegative(count);
        var owner = PooledArrayOwner<byte>.Rent(checked((count + 7) / 8), budget);
        owner.Memory.Span.Clear();
        allValid = true;
        var sourceSpan = sourceBits.Span;
        for (var i = 0; i < count; i++)
        {
            if ((i & 1023) == 0)
            {
                cancellationToken.ThrowIfCancellationRequested();
            }

            if ((sourceSpan[(sourceOffset + i) >> 3] & (1 << ((sourceOffset + i) & 7))) != 0)
            {
                owner.Memory.Span[i >> 3] |= (byte)(1 << (i & 7));
            }
            else
            {
                allValid = false;
            }
        }

        if (allValid)
        {
            owner.Dispose();
            bits = ReadOnlyMemory<byte>.Empty;
            return null;
        }

        bits = owner.Memory;
        return owner;
    }
}
