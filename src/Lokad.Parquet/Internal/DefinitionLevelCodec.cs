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

// The checked outcome of splitting and validating an optional page section:
// rented definition levels (null for required pages), the physical payload
// offset, and the validated non-null value count.
internal readonly record struct DecodedSection(PooledArrayOwner<int>? Levels, int PhysicalOffset, int ValidCount);
internal static class DefinitionLevelCodec
{
    internal static DecodedSection DecodeSection(
        ValidatedPageHeader header,
        ReadOnlySpan<byte> payload,
        int rowCount,
        ParquetRepetition? repetition,
        ParquetScanMemoryBudget budget,
        CancellationToken cancellationToken,
        ParquetErrorLocation location)
    {
        if (repetition == ParquetRepetition.Required)
        {
            if (header.PageType == ValidatedPageType.DataV2)
                ValidateRequiredV2(header.DataV2, rowCount, location);
            return new DecodedSection(null, 0, rowCount);
        }

        int levelOffset;
        int levelByteCount;
        int? expectedNullCount;
        int physicalOffset;
        if (header.PageType == ValidatedPageType.DataV1)
        {
            if (header.DataV1.DefinitionEncodingCode != (int)ParquetEncoding.RunLength)
                throw new ParquetUnsupportedFeatureException("Optional definition levels require V1 RLE/bit-packed hybrid encoding.", location);
            var section = SplitOptionalV1Section(payload, location);
            levelOffset = section.LevelOffset;
            levelByteCount = section.LevelByteCount;
            physicalOffset = section.PhysicalOffset;
            expectedNullCount = null;
        }
        else
        {
            if (header.PageType != ValidatedPageType.DataV2)
                throw new InvalidOperationException("A validated V2 page has no V2 header.");
            var v2 = header.DataV2;
            if (v2.RowCount != rowCount || v2.RepetitionLevelsByteLength != 0)
                throw new ParquetFormatException("A flat optional V2 page has inconsistent row or repetition fields.", location);
            levelOffset = 0;
            levelByteCount = v2.DefinitionLevelsByteLength;
            physicalOffset = levelByteCount;
            expectedNullCount = v2.NullCount;
        }

        PooledArrayOwner<int>? levels = PooledArrayOwner<int>.Rent(rowCount, budget);
        try
        {
            var input = payload.Slice(levelOffset, levelByteCount);
            var validCount = DecodeValidated(input, levels.Memory.Span, expectedNullCount, cancellationToken, location);
            var result = levels;
            levels = null;
            return new DecodedSection(result, physicalOffset, validCount);
        }
        catch
        {
            // Best-effort rollback preserves the primary page error.
            try { levels?.Dispose(); } catch (Exception) { }
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

    internal static PooledArrayOwner<byte>? CreateBitmap(
        ReadOnlySpan<int> levels,
        ParquetScanMemoryBudget budget,
        CancellationToken cancellationToken)
    {
        if (levels.IsEmpty)
            return null;
        cancellationToken.ThrowIfCancellationRequested();
        PooledArrayOwner<byte>? validity = PooledArrayOwner<byte>.Rent(checked((levels.Length + 7) / 8), budget);
        try
        {
            var destination = validity.Memory.Span;
            destination.Clear();
            for (var row = 0; row < levels.Length; row++)
            {
                if ((row & 1023) == 0)
                    cancellationToken.ThrowIfCancellationRequested();
                if (levels[row] != 0)
                    destination[row >> 3] |= (byte)(1 << (row & 7));
            }

            var result = validity;
            validity = null;
            return result;
        }
        catch
        {
            validity?.Dispose();
            throw;
        }
    }

    internal static int DecodeValidated(
        ReadOnlySpan<byte> levelInput,
        Span<int> levelOutput,
        int? expectedNullCount,
        CancellationToken cancellationToken,
        ParquetErrorLocation location)
    {
        int consumed;
        try
        {
            consumed = RleBitPackedHybridDecoder.Decode(
                levelInput,
                1,
                levelOutput,
                cancellationToken);
        }
        catch (ParquetFormatException exception) when (exception.ByteOffset is null)
        {
            throw new ParquetFormatException(exception.Message, exception, location);
        }

        if (consumed != levelInput.Length)
            throw new ParquetFormatException("An optional page has trailing definition-level bytes.", location);
        var decodedNullCount = 0;
        for (var index = 0; index < levelOutput.Length; index++)
        {
            if ((index & 1023) == 0)
                cancellationToken.ThrowIfCancellationRequested();
            if (levelOutput[index] == 0)
                decodedNullCount++;
        }

        if (expectedNullCount is int nullCount && decodedNullCount != nullCount)
            throw new ParquetFormatException("A V2 null count does not match its definition levels.", location);
        return levelOutput.Length - decodedNullCount;
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
