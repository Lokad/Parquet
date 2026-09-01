using System.Buffers.Binary;
using System.Numerics;

namespace Lokad.Parquet.Internal;

internal static class PlainDecoder
{
    public static void DecodeInt32(ReadOnlySpan<byte> source, Span<int> destination, CancellationToken cancellationToken)
    {
        // The scalar kernel is the semantic oracle for the portable vector lane and the forced-scalar gate.
        static void DecodeScalar(
            ReadOnlySpan<byte> source,
            Span<int> destination,
            CancellationToken cancellationToken)
        {
            const int CancellationChunkLength = 4096;
            var outputOffset = 0;
            while (outputOffset < destination.Length)
            {
                cancellationToken.ThrowIfCancellationRequested();
                var chunkLength = Math.Min(CancellationChunkLength, destination.Length - outputOffset);
                var chunkSource = source.Slice(outputOffset * sizeof(int), chunkLength * sizeof(int));
                var chunkDestination = destination.Slice(outputOffset, chunkLength);
                for (var i = 0; i < chunkDestination.Length; i++)
                {
                    chunkDestination[i] = BinaryPrimitives.ReadInt32LittleEndian(
                        chunkSource.Slice(i * sizeof(int), sizeof(int)));
                }
                outputOffset += chunkLength;
            }
        }

        static void DecodeVector(
            ReadOnlySpan<byte> source,
            Span<int> destination,
            CancellationToken cancellationToken)
        {
            const int CancellationChunkLength = 4096;
            var vectorValueCount = Vector<int>.Count;
            var vectorByteCount = Vector<byte>.Count;
            var outputOffset = 0;
            while (outputOffset < destination.Length)
            {
                cancellationToken.ThrowIfCancellationRequested();
                var chunkLength = Math.Min(CancellationChunkLength, destination.Length - outputOffset);
                var chunkSource = source.Slice(outputOffset * sizeof(int), chunkLength * sizeof(int));
                var chunkDestination = destination.Slice(outputOffset, chunkLength);
                var i = 0;
                for (; i <= chunkLength - vectorValueCount; i += vectorValueCount)
                {
                    var bytes = new Vector<byte>(chunkSource.Slice(i * sizeof(int), vectorByteCount));
                    Vector.AsVectorInt32(bytes).CopyTo(chunkDestination[i..]);
                }
                for (; i < chunkLength; i++)
                {
                    chunkDestination[i] = BinaryPrimitives.ReadInt32LittleEndian(
                        chunkSource.Slice(i * sizeof(int), sizeof(int)));
                }
                outputOffset += chunkLength;
            }
        }

        if (source.Length != checked(destination.Length * sizeof(int)))
            throw new ParquetFormatException("A PLAIN INT32 payload length does not match its destination.");

        if (ParquetRuntime.ForceScalar || !Vector.IsHardwareAccelerated || !BitConverter.IsLittleEndian)
            DecodeScalar(source, destination, cancellationToken);
        else
            DecodeVector(source, destination, cancellationToken);
    }

    public static void DecodeInt64(ReadOnlySpan<byte> source, Span<long> destination, CancellationToken cancellationToken)
    {
        if (source.Length != checked(destination.Length * sizeof(long)))
            throw new ParquetFormatException("A PLAIN INT64 payload length does not match its destination.");
        for (var i = 0; i < destination.Length; i++)
        {
            ObserveCancellation(i, cancellationToken);
            destination[i] = BinaryPrimitives.ReadInt64LittleEndian(source.Slice(i * sizeof(long), sizeof(long)));
        }
    }

    public static void DecodeFloat(ReadOnlySpan<byte> source, Span<float> destination, CancellationToken cancellationToken)
    {
        if (source.Length != checked(destination.Length * sizeof(float)))
            throw new ParquetFormatException("A PLAIN FLOAT payload length does not match its destination.");
        for (var i = 0; i < destination.Length; i++)
        {
            ObserveCancellation(i, cancellationToken);
            destination[i] = BitConverter.Int32BitsToSingle(
                BinaryPrimitives.ReadInt32LittleEndian(source.Slice(i * sizeof(float), sizeof(float))));
        }
    }

    public static void DecodeDouble(ReadOnlySpan<byte> source, Span<double> destination, CancellationToken cancellationToken)
    {
        if (source.Length != checked(destination.Length * sizeof(double)))
            throw new ParquetFormatException("A PLAIN DOUBLE payload length does not match its destination.");
        for (var i = 0; i < destination.Length; i++)
        {
            ObserveCancellation(i, cancellationToken);
            destination[i] = BitConverter.Int64BitsToDouble(
                BinaryPrimitives.ReadInt64LittleEndian(source.Slice(i * sizeof(double), sizeof(double))));
        }
    }

    public static void DecodeBoolean(ReadOnlySpan<byte> source, Span<bool> destination, CancellationToken cancellationToken)
    {
        var expectedLength = checked((destination.Length + 7) / 8);
        if (source.Length != expectedLength)
            throw new ParquetFormatException("A PLAIN BOOLEAN payload length does not match its destination.");
        for (var i = 0; i < destination.Length; i++)
        {
            ObserveCancellation(i, cancellationToken);
            destination[i] = (source[i >> 3] & (1 << (i & 7))) != 0;
        }
    }

    private static void ObserveCancellation(int index, CancellationToken cancellationToken)
    {
        if ((index & 4095) == 0)
            cancellationToken.ThrowIfCancellationRequested();
    }
}
