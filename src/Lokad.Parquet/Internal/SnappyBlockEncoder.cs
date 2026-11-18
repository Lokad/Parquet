using System.Buffers.Binary;
using System.Numerics;

namespace Lokad.Parquet.Internal;

internal static class SnappyBlockEncoder
{
    private const int HashBits = 14;
    private const int SearchShift = 5;
    private const int InitialSearch = 1 << SearchShift;
    private const int MaximumSearch = 2_048 << SearchShift;
    public const int HashEntries = 1 << HashBits;

    // Each copy saves at least one byte, paying for the next literal's tag.
    // Extended literal lengths consume at most one extra byte per six inputs.
    public static int MaximumCompressedLength(int length) => checked(32 + length + length / 6);

    public static int Compress(
        ReadOnlySpan<byte> source,
        Span<byte> destination,
        Span<int> positions,
        CancellationToken cancellationToken)
    {
        positions = positions[..HashEntries];
        positions.Clear();
        var output = 0;
        var length = (uint)source.Length;
        while (length >= 128)
        {
            destination[output++] = (byte)(length | 128);
            length >>= 7;
        }
        destination[output++] = (byte)length;

        var literalStart = 0;
        var position = 0;
        var nextCancellationCheck = 0;
        var search = InitialSearch;
        var lastWord = source.Length - sizeof(uint);
        while (position <= lastWord)
        {
            if (position >= nextCancellationCheck)
            {
                cancellationToken.ThrowIfCancellationRequested();
                nextCancellationCheck = position + 4096;
            }
            var word = BinaryPrimitives.ReadUInt32LittleEndian(source[position..]);
            var hash = Hash(word);
            var candidate = positions[hash] - 1;
            positions[hash] = position + 1;
            var distance = position - candidate;
            if (candidate < 0 || distance > ushort.MaxValue ||
                BinaryPrimitives.ReadUInt32LittleEndian(source[candidate..]) != word)
            {
                // Probe less often during a long run of misses. The stride cap
                // lets large pages still pick up changes in compressibility.
                var stride = search >> SearchShift;
                position += stride;
                search = Math.Min(search + stride, MaximumSearch);
                continue;
            }

            WriteLiteral(source[literalStart..position], destination, ref output);
            var matchLength = sizeof(uint) +
                source[(position + sizeof(uint))..].CommonPrefixLength(source[(candidate + sizeof(uint))..]);
            WriteCopy(distance, matchLength, destination, ref output);
            position += matchLength;
            // Include the window starting at the last byte of the match. A
            // later match can start there and reuse the bytes that follow it.
            if (position - 1 <= lastWord)
                positions[Hash(BinaryPrimitives.ReadUInt32LittleEndian(source[(position - 1)..]))] = position;
            literalStart = position;
            search = InitialSearch;
        }
        cancellationToken.ThrowIfCancellationRequested();
        WriteLiteral(source[literalStart..], destination, ref output);
        return output;

        static int Hash(uint word) => (int)(unchecked(word * 0x1e35a7bdu) >> (32 - HashBits));

        static void WriteLiteral(ReadOnlySpan<byte> literal, Span<byte> destination, ref int output)
        {
            if (literal.IsEmpty)
                return;
            var encodedLength = literal.Length - 1;
            if (encodedLength < 60)
            {
                destination[output++] = (byte)(encodedLength << 2);
            }
            else
            {
                var bytes = (BitOperations.Log2((uint)encodedLength) >> 3) + 1;
                destination[output++] = (byte)((59 + bytes) << 2);
                for (var i = 0; i < bytes; i++)
                {
                    destination[output++] = (byte)encodedLength;
                    encodedLength >>= 8;
                }
            }
            literal.CopyTo(destination[output..]);
            output += literal.Length;
        }

        static void WriteCopy(int distance, int length, Span<byte> destination, ref int output)
        {
            while (length != 0)
            {
                var count = Math.Min(length, 64);
                if (count is >= 4 and <= 11 && distance < 2048)
                {
                    destination[output++] = (byte)(1 | (count - 4) << 2 | (distance >> 8) << 5);
                    destination[output++] = (byte)distance;
                }
                else
                {
                    destination[output++] = (byte)(2 | (count - 1) << 2);
                    BinaryPrimitives.WriteUInt16LittleEndian(destination[output..], (ushort)distance);
                    output += sizeof(ushort);
                }
                length -= count;
            }
        }
    }
}
