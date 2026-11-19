using System.Buffers.Binary;
using System.Numerics;

namespace Lokad.Parquet.Internal;

internal static class SnappyBlockEncoder
{
    private const int HashBits = 14;
    private const int SearchShift = 5;
    private const int InitialSearch = 1 << SearchShift;
    private const int MaximumSearch = 2_048 << SearchShift;
    private const int MaximumShortCopyLength = 11;
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
        var recentDistance = 0;
        var repeatedShortMatches = 0;
        var lastWord = source.Length - sizeof(uint);
        var lastWideWord = source.Length - sizeof(ulong);
        while (position <= lastWord)
        {
            if (position >= nextCancellationCheck)
            {
                cancellationToken.ThrowIfCancellationRequested();
                nextCancellationCheck = position + 4096;
            }
            int candidate;
            var recentOffset = -1;
            if (search == InitialSearch && repeatedShortMatches >= 2 && recentDistance != 0 &&
                position >= recentDistance && position <= lastWideWord)
            {
                // Confirm that short matches keep using this distance before
                // bypassing the hash search, which can find longer matches.
                // Two reads check five overlapping windows in order.
                var difference = BinaryPrimitives.ReadUInt64LittleEndian(source[position..]) ^
                    BinaryPrimitives.ReadUInt64LittleEndian(source[(position - recentDistance)..]);
                for (var offset = 0; offset <= 4; offset++, difference >>= 8)
                {
                    if ((uint)difference == 0)
                    {
                        recentOffset = offset;
                        break;
                    }
                }
            }
            if (recentOffset >= 0)
            {
                position += recentOffset;
                candidate = position - recentDistance;
                positions[Hash(BinaryPrimitives.ReadUInt32LittleEndian(source[position..]))] = position + 1;
            }
            else if (search <= InitialSearch * 2 - 4 && position <= lastWideWord)
            {
                // The next four probes all have stride one. Load their
                // overlapping windows together and update the table in order.
                var words = BinaryPrimitives.ReadUInt64LittleEndian(source[position..]);
                var batchEnd = position + 4;
                do
                {
                    candidate = FindCandidate((uint)words, source, positions, position);
                    if (candidate >= 0)
                        break;
                    position++;
                    words >>= 8;
                }
                while (position < batchEnd);

                if (candidate < 0)
                {
                    search += 4;
                    continue;
                }
            }
            else
            {
                var word = BinaryPrimitives.ReadUInt32LittleEndian(source[position..]);
                candidate = FindCandidate(word, source, positions, position);
                if (candidate < 0)
                {
                    // Cap the stride so large pages can pick up changes in compressibility.
                    var stride = search >> SearchShift;
                    position += stride;
                    search = Math.Min(search + stride, MaximumSearch);
                    continue;
                }
            }

            var distance = position - candidate;
            var matchLength = sizeof(uint) +
                source[(position + sizeof(uint))..].CommonPrefixLength(source[(candidate + sizeof(uint))..]);
            var literalLength = position - literalStart;
            if (literalLength is 3 or 4 && matchLength <= MaximumShortCopyLength && distance < 2048 &&
                output <= destination.Length - sizeof(ulong))
            {
                // The following match makes four bytes readable even for a three-byte literal.
                var literal = BinaryPrimitives.ReadUInt32LittleEndian(source[literalStart..]);
                var literalMask = (1UL << (literalLength * 8)) - 1;
                var copyTag = 1 | ((matchLength - 4) << 2) | ((distance >> 8) << 5) | ((distance & 255) << 8);
                var packed = (uint)((literalLength - 1) << 2) |
                    (((ulong)literal & literalMask) << 8) | ((ulong)copyTag << ((literalLength + 1) * 8));
                BinaryPrimitives.WriteUInt64LittleEndian(destination[output..], packed);
                // Only the emitted bytes count toward the block.
                output += literalLength + 3;
            }
            else
            {
                WriteLiteral(source[literalStart..position], destination, ref output);
                WriteCopy(distance, matchLength, destination, ref output);
            }
            repeatedShortMatches = matchLength <= MaximumShortCopyLength && distance == recentDistance
                ? repeatedShortMatches + 1
                : 0;
            recentDistance = distance;
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

        static int FindCandidate(uint word, ReadOnlySpan<byte> source, Span<int> positions, int position)
        {
            var hash = Hash(word);
            var candidate = positions[hash] - 1;
            positions[hash] = position + 1;
            if (candidate < 0 || position - candidate > ushort.MaxValue ||
                BinaryPrimitives.ReadUInt32LittleEndian(source[candidate..]) != word)
                return -1;
            return candidate;
        }

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
                if (count is >= 4 and <= MaximumShortCopyLength && distance < 2048)
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
