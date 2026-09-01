using System.Numerics;

namespace Lokad.Parquet.Internal;

internal static class RleBitPackedHybridDecoder
{
    public static int Decode(
        ReadOnlySpan<byte> source,
        int bitWidth,
        Span<int> destination,
        CancellationToken cancellationToken)
    {
        static void DecodeBitPacked(
            ReadOnlySpan<byte> source,
            int bitWidth,
            Span<int> destination,
            CancellationToken cancellationToken)
        {
            if (bitWidth == 0)
            {
                cancellationToken.ThrowIfCancellationRequested();
                destination.Clear();
                return;
            }

            var sourceOffset = 0;
            ulong buffer = 0;
            var bufferedBits = 0;
            var mask = bitWidth == 32 ? uint.MaxValue : (1u << bitWidth) - 1;
            for (var i = 0; i < destination.Length; i++)
            {
                if ((i & 4095) == 0)
                    cancellationToken.ThrowIfCancellationRequested();
                while (bufferedBits < bitWidth)
                {
                    buffer |= (ulong)source[sourceOffset++] << bufferedBits;
                    bufferedBits += 8;
                }
                destination[i] = unchecked((int)((uint)buffer & mask));
                buffer >>= bitWidth;
                bufferedBits -= bitWidth;
            }
        }

        if ((uint)bitWidth > 32)
            throw new ParquetFormatException("An RLE/bit-packed stream has an invalid bit width.");

        var sourceOffset = 0;
        var outputOffset = 0;
        while (outputOffset < destination.Length)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var header = ReadVarUInt32(source, ref sourceOffset);
            if (header == 0)
                throw new ParquetFormatException("An RLE/bit-packed stream contains an empty run.");

            if ((header & 1) == 0)
            {
                var runLength = checked((int)(header >> 1));
                if (runLength > destination.Length - outputOffset)
                    throw new ParquetFormatException("An RLE run exceeds the declared value count.");
                var byteWidth = (bitWidth + 7) >> 3;
                if (byteWidth > source.Length - sourceOffset)
                    throw new ParquetFormatException("An RLE value is truncated.");
                uint value = 0;
                for (var i = 0; i < byteWidth; i++)
                    value |= (uint)source[sourceOffset++] << (i * 8);
                if (bitWidth < 32 && value >= (1u << bitWidth))
                    throw new ParquetFormatException("An RLE value exceeds its declared bit width.");
                var remaining = runLength;
                while (remaining != 0)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    var sliceLength = Math.Min(remaining, 4096);
                    destination.Slice(outputOffset, sliceLength).Fill(unchecked((int)value));
                    outputOffset += sliceLength;
                    remaining -= sliceLength;
                }
            }
            else
            {
                var groupCount = checked((int)(header >> 1));
                if (groupCount > int.MaxValue / 8 || (bitWidth != 0 && groupCount > int.MaxValue / bitWidth))
                    throw new ParquetFormatException("A bit-packed run length overflows the managed input range.");
                var runValueCount = groupCount * 8;
                var runByteCount = groupCount * bitWidth;
                if (runByteCount > source.Length - sourceOffset)
                    throw new ParquetFormatException("A bit-packed run is truncated.");

                var valuesToWrite = Math.Min(runValueCount, destination.Length - outputOffset);
                DecodeBitPacked(
                    source.Slice(sourceOffset, runByteCount),
                    bitWidth,
                    destination.Slice(outputOffset, valuesToWrite),
                    cancellationToken);
                sourceOffset += runByteCount;
                outputOffset += valuesToWrite;
            }
        }
        return sourceOffset;
    }

    // This specialization must consume exactly the same wire bytes as Decode; equivalence tests cover both lanes.
    public static int DecodeBitWidthOneToBitmap(
        ReadOnlySpan<byte> source,
        int valueCount,
        Span<byte> destination,
        CancellationToken cancellationToken,
        out int setBitCount)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(valueCount);
        if (destination.Length != checked((valueCount + 7) / 8))
            throw new ArgumentException("The bitmap destination length does not match the value count.", nameof(destination));
        cancellationToken.ThrowIfCancellationRequested();
        destination.Clear();
        var sourceOffset = 0;
        var outputOffset = 0;
        setBitCount = 0;
        while (outputOffset < valueCount)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var header = ReadVarUInt32(source, ref sourceOffset);
            if (header == 0)
                throw new ParquetFormatException("An RLE/bit-packed stream contains an empty run.");
            if ((header & 1) == 0)
            {
                var runLength = checked((int)(header >> 1));
                if (runLength > valueCount - outputOffset)
                    throw new ParquetFormatException("An RLE run exceeds the declared value count.");
                if ((uint)sourceOffset >= (uint)source.Length)
                    throw new ParquetFormatException("An RLE value is truncated.");
                var value = source[sourceOffset++];
                if (value > 1)
                    throw new ParquetFormatException("An RLE value exceeds its declared bit width.");
                if (value != 0)
                {
                    SetBits(destination, outputOffset, runLength);
                    setBitCount = checked(setBitCount + runLength);
                }
                outputOffset += runLength;
                continue;
            }

            var groupCount = checked((int)(header >> 1));
            if (groupCount > int.MaxValue / 8)
                throw new ParquetFormatException("A bit-packed run length overflows the managed input range.");
            var runValueCount = groupCount * 8;
            if (groupCount > source.Length - sourceOffset)
                throw new ParquetFormatException("A bit-packed run is truncated.");
            var valuesToWrite = Math.Min(runValueCount, valueCount - outputOffset);
            if ((outputOffset & 7) == 0)
            {
                var sourceBytes = source.Slice(sourceOffset, checked((valuesToWrite + 7) / 8));
                var destinationBytes = destination[(outputOffset >> 3)..];
                var fullByteCount = valuesToWrite >> 3;
                var byteOffset = 0;
                while (byteOffset < fullByteCount)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    var chunkLength = Math.Min(512, fullByteCount - byteOffset);
                    var chunk = sourceBytes.Slice(byteOffset, chunkLength);
                    chunk.CopyTo(destinationBytes[byteOffset..]);
                    foreach (var value in chunk)
                        setBitCount = checked(setBitCount + BitOperations.PopCount((uint)value));
                    byteOffset += chunkLength;
                }
                var trailingBitCount = valuesToWrite & 7;
                if (trailingBitCount != 0)
                {
                    var mask = (byte)((1 << trailingBitCount) - 1);
                    var trailing = (byte)(sourceBytes[fullByteCount] & mask);
                    destinationBytes[fullByteCount] = trailing;
                    setBitCount = checked(setBitCount + BitOperations.PopCount((uint)trailing));
                }
                sourceOffset += groupCount;
                outputOffset += valuesToWrite;
                continue;
            }
            for (var valueIndex = 0; valueIndex < valuesToWrite; valueIndex++)
            {
                if ((valueIndex & 4095) == 0)
                    cancellationToken.ThrowIfCancellationRequested();
                if ((source[sourceOffset + (valueIndex >> 3)] & (1 << (valueIndex & 7))) == 0)
                    continue;
                destination[(outputOffset + valueIndex) >> 3] |=
                    (byte)(1 << ((outputOffset + valueIndex) & 7));
                setBitCount++;
            }
            sourceOffset += groupCount;
            outputOffset += valuesToWrite;
        }
        return sourceOffset;

        static void SetBits(Span<byte> bits, int start, int count)
        {
            var end = checked(start + count);
            while (start < end && (start & 7) != 0)
            {
                bits[start >> 3] |= (byte)(1 << (start & 7));
                start++;
            }
            var fullByteCount = (end - start) >> 3;
            if (fullByteCount != 0)
            {
                bits.Slice(start >> 3, fullByteCount).Fill(byte.MaxValue);
                start += fullByteCount << 3;
            }
            while (start < end)
            {
                bits[start >> 3] |= (byte)(1 << (start & 7));
                start++;
            }
        }
    }

    private static uint ReadVarUInt32(ReadOnlySpan<byte> source, ref int offset)
    {
        uint value = 0;
        for (var shift = 0; shift < 35; shift += 7)
        {
            if ((uint)offset >= (uint)source.Length)
                throw new ParquetFormatException("An RLE/bit-packed run header is truncated.");
            var next = source[offset++];
            if (shift == 28 && (next & 0xF0) != 0)
                throw new ParquetFormatException("An RLE/bit-packed run header overflows.");
            value |= (uint)(next & 0x7F) << shift;
            if ((next & 0x80) == 0)
                return value;
        }
        throw new ParquetFormatException("An RLE/bit-packed run header overflows.");
    }
}
