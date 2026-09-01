namespace Lokad.Parquet.Internal;

internal static class SnappyBlockDecoder
{
    public static void Decompress(
        ReadOnlySpan<byte> source,
        Span<byte> destination,
        CancellationToken cancellationToken)
    {
        static void CopyLiteral(
            ReadOnlySpan<byte> source,
            ref int sourceOffset,
            Span<byte> destination,
            ref int outputOffset,
            byte tag,
            CancellationToken cancellationToken)
        {
            var lengthCode = tag >> 2;
            int length;
            if (lengthCode < 60)
            {
                length = lengthCode + 1;
            }
            else
            {
                var byteCount = lengthCode - 59;
                RequireSource(source, sourceOffset, byteCount, "A Snappy literal length is truncated.");
                uint lengthMinusOne = 0;
                for (var i = 0; i < byteCount; i++)
                    lengthMinusOne |= (uint)source[sourceOffset++] << (8 * i);
                if (lengthMinusOne >= int.MaxValue)
                    throw new ParquetFormatException("A Snappy literal length is outside the managed output range.");
                length = checked((int)lengthMinusOne + 1);
            }

            RequireSource(source, sourceOffset, length, "A Snappy literal is truncated.");
            RequireOutput(destination, outputOffset, length, "A Snappy literal exceeds the declared output length.");
            cancellationToken.ThrowIfCancellationRequested();
            source.Slice(sourceOffset, length).CopyTo(destination.Slice(outputOffset, length));
            sourceOffset += length;
            outputOffset += length;
        }

        static uint ReadVarUInt32(ReadOnlySpan<byte> source, ref int offset)
        {
            uint value = 0;
            for (var shift = 0; shift < 35; shift += 7)
            {
                RequireSource(source, offset, 1, "A Snappy length prefix is truncated.");
                var next = source[offset++];
                if (shift == 28 && (next & 0xF0) != 0)
                    throw new ParquetFormatException("A Snappy length prefix overflows.");
                value |= (uint)(next & 0x7F) << shift;
                if ((next & 0x80) == 0)
                    return value;
            }
            throw new ParquetFormatException("A Snappy length prefix overflows.");
        }

        static void CopyBackReference(
            Span<byte> destination,
            ref int outputOffset,
            int distance,
            int length,
            CancellationToken cancellationToken)
        {
            if (distance <= 0 || distance > outputOffset)
                throw new ParquetFormatException("A Snappy copy has an invalid backward distance.");
            RequireOutput(destination, outputOffset, length, "A Snappy copy exceeds the declared output length.");
            for (var i = 0; i < length; i++)
            {
                if ((i & 4095) == 0)
                    cancellationToken.ThrowIfCancellationRequested();
                destination[outputOffset + i] = destination[outputOffset - distance + i];
            }
            outputOffset += length;
        }

        var sourceOffset = 0;
        var declaredLength = ReadVarUInt32(source, ref sourceOffset);
        if (declaredLength != (uint)destination.Length)
            throw new ParquetFormatException("A Snappy block declares an unexpected output length.");

        var outputOffset = 0;
        while (sourceOffset < source.Length)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var tag = source[sourceOffset++];
            switch (tag & 3)
            {
                case 0:
                    CopyLiteral(source, ref sourceOffset, destination, ref outputOffset, tag, cancellationToken);
                    break;
                case 1:
                    {
                        RequireSource(source, sourceOffset, 1, "A Snappy one-byte copy is truncated.");
                        var length = 4 + ((tag >> 2) & 7);
                        var distance = ((tag & 0xE0) << 3) | source[sourceOffset++];
                        CopyBackReference(destination, ref outputOffset, distance, length, cancellationToken);
                        break;
                    }
                case 2:
                    {
                        RequireSource(source, sourceOffset, 2, "A Snappy two-byte copy is truncated.");
                        var length = 1 + (tag >> 2);
                        var distance = source[sourceOffset] | (source[sourceOffset + 1] << 8);
                        sourceOffset += 2;
                        CopyBackReference(destination, ref outputOffset, distance, length, cancellationToken);
                        break;
                    }
                default:
                    {
                        RequireSource(source, sourceOffset, 4, "A Snappy four-byte copy is truncated.");
                        var length = 1 + (tag >> 2);
                        var distance = (uint)source[sourceOffset] |
                            ((uint)source[sourceOffset + 1] << 8) |
                            ((uint)source[sourceOffset + 2] << 16) |
                            ((uint)source[sourceOffset + 3] << 24);
                        sourceOffset += 4;
                        if (distance > int.MaxValue)
                            throw new ParquetFormatException("A Snappy copy distance is outside the managed output range.");
                        CopyBackReference(destination, ref outputOffset, (int)distance, length, cancellationToken);
                        break;
                    }
            }
        }

        if (outputOffset != destination.Length)
            throw new ParquetFormatException("A Snappy block did not produce its declared output length.");
    }

    private static void RequireSource(ReadOnlySpan<byte> source, int offset, int length, string message)
    {
        if (length < 0 || offset < 0 || offset > source.Length || length > source.Length - offset)
            throw new ParquetFormatException(message);
    }

    private static void RequireOutput(Span<byte> destination, int offset, int length, string message)
    {
        if (length < 0 || offset < 0 || offset > destination.Length || length > destination.Length - offset)
            throw new ParquetFormatException(message);
    }
}
