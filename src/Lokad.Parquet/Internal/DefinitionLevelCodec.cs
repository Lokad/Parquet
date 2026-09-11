using System.Buffers.Binary;

namespace Lokad.Parquet.Internal;

// Cohesive definition-level section component with narrow explicit inputs.
// It splits optional page payloads into validated levels plus a physical
// section, builds LSB-first validity bitmaps, and measures V2 level sections.
// Level storage is rented from the scan budget and returned by the caller on
// both success and failure paths; this component retains no page state. Level
// counting observes cancellation at bounded intervals. Every malformed input is
// reported with the caller-supplied page location, preserving the scan error
// taxonomy without capturing cursor state.

// The checked outcome of decoding an optional bitmap section straight into a
// validity bitmap: rented levels as a bitmap (null when every row is valid),
// the physical payload byte offset, and the validated non-null value count.
internal readonly record struct DecodedBitmapSection(PooledArrayOwner<byte>? Validity, int PhysicalOffset, int ValidCount);
internal static class DefinitionLevelCodec
{
    // Decodes an optional definition-level section straight into a validity bitmap,
    // shared by the primitive page paths and dictionary expansion so all enforce
    // identical rent, consumed-bytes and null-count checks with the same all-valid
    // transfer. Decoder errors propagate unannotated so each consumer keeps its own
    // error policy; only boundary, trailing-bytes and null-count failures carry the
    // supplied page location. Ownership transfers to the caller on success.
    internal static DecodedBitmapSection DecodeBitmapSection(
        ReadOnlySpan<byte> payload,
        int rowCount,
        int levelOffset,
        int levelByteCount,
        int physicalOffset,
        int? expectedNullCount,
        ParquetScanMemoryBudget budget,
        CancellationToken cancellationToken,
        ParquetErrorLocation location)
    {
        if (levelOffset < 0 || levelByteCount < 0 || physicalOffset < 0 ||
            levelByteCount > payload.Length - levelOffset ||
            physicalOffset > payload.Length)
            throw new ParquetFormatException("An optional page has invalid level or value boundaries.", location);
        PooledArrayOwner<byte>? validity = PooledArrayOwner<byte>.Rent(checked((rowCount + 7) / 8), budget);
        try
        {
            var levelInput = payload.Slice(levelOffset, levelByteCount);
            var consumed = RleBitPackedHybridDecoder.DecodeBitWidthOneToBitmap(
                levelInput,
                rowCount,
                validity.Memory.Span,
                cancellationToken,
                out var validCount);
            if (consumed != levelInput.Length)
                throw new ParquetFormatException("An optional page has trailing definition-level bytes.", location);
            if (expectedNullCount is int nullCount && rowCount - validCount != nullCount)
                throw new ParquetFormatException("A V2 page null count does not match its definition levels.", location);
            if (validCount == rowCount)
            {
                // All-valid pages carry no bitmap; consumers overwrite every output slot.
                validity.Dispose();
                validity = null;
            }

            var result = validity;
            validity = null;
            return new DecodedBitmapSection(result, physicalOffset, validCount);
        }
        catch
        {
            // Best-effort rollback preserves the primary page error.
            try { validity?.Dispose(); } catch (Exception) { }
            throw;
        }
    }

    // Splits an optional V1 page payload into its definition-level section and
    // physical section, validating all boundaries. Shared by section decoding and
    // the specialized primitive path so both enforce identical boundaries.
    internal static (int LevelOffset, int LevelByteCount, int PhysicalOffset) SplitOptionalV1Section(ReadOnlySpan<byte> payload, ParquetErrorLocation location)
    {
        if (payload.Length < sizeof(int))
            throw new ParquetFormatException("An optional V1 page is missing its definition-level length.", location);
        var levelByteCount = BinaryPrimitives.ReadInt32LittleEndian(payload);
        if (levelByteCount < 0 || levelByteCount > payload.Length - sizeof(int))
            throw new ParquetFormatException("An optional V1 page has an invalid definition-level length.", location);
        var levelOffset = sizeof(int);
        return (levelOffset, levelByteCount, checked(levelOffset + levelByteCount));
    }

    // Validates a required flat V2 page against its row count: no nulls and no
    // level bytes. Shared by section decoding and page loading so both enforce
    // identical boundaries.
    internal static void ValidateRequiredV2(ValidatedDataPageV2 v2, int rowCount, ParquetErrorLocation location)
    {
        if (v2.RowCount != rowCount || v2.NullCount != 0 || GetV2LevelByteCount(v2, location) != 0)
            throw new ParquetFormatException("A required flat V2 page has inconsistent row, null, or level fields.", location);
    }

    internal static int GetV2LevelByteCount(ValidatedDataPageV2 v2, ParquetErrorLocation location)
    {
        try
        {
            return checked(v2.RepetitionLevelsByteLength + v2.DefinitionLevelsByteLength);
        }
        catch (OverflowException)
        {
            throw new ParquetFormatException("A V2 level-section length overflows.", location);
        }
    }
}
