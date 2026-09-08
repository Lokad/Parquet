using System.Buffers.Binary;

namespace Lokad.Parquet.Internal;

// Cohesive definition-level section component with narrow explicit inputs.
// It splits optional page payloads into validated levels plus a physical
// section, builds LSB-first validity bitmaps, and measures V2 level sections.
// Level storage is rented from the scan budget and returned by the caller on
// both success and failure paths; this component retains no page state. Pure
// level counting needs no budget or cancellation. Every malformed input is
// reported with the caller-supplied page location, preserving the scan error
// taxonomy without capturing cursor state.
internal static class DefinitionLevelCodec
{
    internal static PooledArrayOwner<int>? DecodeSection(
        ValidatedPageHeader header,
        ReadOnlySpan<byte> payload,
        int rowCount,
        ParquetRepetition? repetition,
        ParquetScanMemoryBudget budget,
        CancellationToken cancellationToken,
        ParquetErrorLocation location,
        out int physicalOffset)
    {
        if (repetition == ParquetRepetition.Required)
        {
            physicalOffset = header.PageType == ValidatedPageType.DataV2 ? GetV2LevelByteCount(header.DataV2, location) : 0;
            if (header.PageType == ValidatedPageType.DataV2 &&
                (header.DataV2.RowCount != rowCount || header.DataV2.NullCount != 0 || physicalOffset != 0))
                throw new ParquetFormatException("A required flat V2 page has inconsistent level fields.", location);
            return null;
        }

        int levelOffset;
        int levelByteCount;
        int? expectedNullCount;
        if (header.PageType == ValidatedPageType.DataV1)
        {
            if (header.DataV1.DefinitionEncodingCode != (int)ParquetEncoding.RunLength)
                throw new ParquetUnsupportedFeatureException("Optional definition levels require V1 RLE/bit-packed hybrid encoding.", location);
            if (payload.Length < sizeof(int))
                throw new ParquetFormatException("An optional V1 page is missing its definition-level length.", location);
            levelByteCount = BinaryPrimitives.ReadInt32LittleEndian(payload);
            if (levelByteCount < 0 || levelByteCount > payload.Length - sizeof(int))
                throw new ParquetFormatException("An optional V1 page has an invalid definition-level length.", location);
            levelOffset = sizeof(int);
            physicalOffset = checked(levelOffset + levelByteCount);
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
            _ = DecodeValidated(input, levels.Memory.Span, expectedNullCount, cancellationToken, location);
            var result = levels;
            levels = null;
            return result;
        }
        finally
        {
            levels?.Dispose();
        }
    }

    internal static PooledArrayOwner<byte>? CreateBitmap(
        ReadOnlySpan<int> levels,
        ParquetScanMemoryBudget budget,
        CancellationToken cancellationToken)
    {
        if (levels.IsEmpty)
            return null;
        cancellationToken.ThrowIfCancellationRequested();
        var validity = PooledArrayOwner<byte>.Rent(checked((levels.Length + 7) / 8), budget);
        validity.Memory.Span.Clear();
        for (var row = 0; row < levels.Length; row++)
        {
            if ((row & 1023) == 0)
                cancellationToken.ThrowIfCancellationRequested();
            if (levels[row] != 0)
                validity.Memory.Span[row >> 3] |= (byte)(1 << (row & 7));
        }
        return validity;
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
        if (expectedNullCount is int nullCount && CountLevel(levelOutput, 0) != nullCount)
            throw new ParquetFormatException("A V2 null count does not match its definition levels.", location);
        return levelOutput.Length - CountLevel(levelOutput, 0);
    }

    internal static int CountLevel(ReadOnlySpan<int> levels, int expected)
    {
        var count = 0;
        foreach (var level in levels)
        {
            if (level == expected)
                count++;
        }
        return count;
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
