using System.Buffers.Binary;
using System.Diagnostics;
using System.Reflection;

namespace Lokad.Parquet.Tests;

public sealed class DecoderKernelTests
{
    private delegate void PlainDecode<T>(ReadOnlySpan<byte> source, Span<T> destination, CancellationToken cancellationToken);
    private delegate int HybridDecode(
        ReadOnlySpan<byte> source,
        int bitWidth,
        Span<int> destination,
        CancellationToken cancellationToken);
    private delegate int HybridBitmapDecode(
        ReadOnlySpan<byte> source,
        int valueCount,
        Span<byte> destination,
        CancellationToken cancellationToken,
        out int setBitCount);
    private delegate void SnappyDecode(
        ReadOnlySpan<byte> source,
        Span<byte> destination,
        CancellationToken cancellationToken);

    private static readonly PlainDecode<int> DecodeInt32 = GetMethod<PlainDecode<int>>("PlainDecoder", "DecodeInt32");
    private static readonly PlainDecode<long> DecodeInt64 = GetMethod<PlainDecode<long>>("PlainDecoder", "DecodeInt64");
    private static readonly PlainDecode<float> DecodeFloat = GetMethod<PlainDecode<float>>("PlainDecoder", "DecodeFloat");
    private static readonly PlainDecode<double> DecodeDouble = GetMethod<PlainDecode<double>>("PlainDecoder", "DecodeDouble");
    private static readonly PlainDecode<bool> DecodeBoolean = GetMethod<PlainDecode<bool>>("PlainDecoder", "DecodeBoolean");
    private static readonly HybridDecode DecodeHybrid = GetMethod<HybridDecode>("RleBitPackedHybridDecoder", "Decode");
    private static readonly HybridBitmapDecode DecodeHybridBitmap =
        GetMethod<HybridBitmapDecode>("RleBitPackedHybridDecoder", "DecodeBitWidthOneToBitmap");
    private static readonly SnappyDecode DecodeSnappy = GetMethod<SnappyDecode>("SnappyBlockDecoder", "Decompress");

    [Fact]
    public void PlainFixedWidthDecodersPreserveValuesAndGuardRegions()
    {
        var int32Source = new byte[3 * sizeof(int)];
        BinaryPrimitives.WriteInt32LittleEndian(int32Source.AsSpan(0), int.MinValue);
        BinaryPrimitives.WriteInt32LittleEndian(int32Source.AsSpan(4), 0);
        BinaryPrimitives.WriteInt32LittleEndian(int32Source.AsSpan(8), int.MaxValue);
        var guarded = Enumerable.Repeat(123456, 5).ToArray();

        DecodeInt32(int32Source, guarded.AsSpan(1, 3), CancellationToken.None);

        Assert.Equal(123456, guarded[0]);
        Assert.Equal([int.MinValue, 0, int.MaxValue], guarded[1..4]);
        Assert.Equal(123456, guarded[4]);

        var int64Source = new byte[sizeof(long)];
        BinaryPrimitives.WriteInt64LittleEndian(int64Source, long.MinValue);
        var int64 = new long[1];
        DecodeInt64(int64Source, int64, CancellationToken.None);
        Assert.Equal(long.MinValue, int64[0]);

        var floatSource = new byte[sizeof(float)];
        BinaryPrimitives.WriteInt32LittleEndian(floatSource, unchecked((int)0x7FC01234));
        var floats = new float[1];
        DecodeFloat(floatSource, floats, CancellationToken.None);
        Assert.Equal(unchecked((int)0x7FC01234), BitConverter.SingleToInt32Bits(floats[0]));

        var doubleSource = new byte[sizeof(double)];
        BinaryPrimitives.WriteInt64LittleEndian(doubleSource, unchecked((long)0x7FF8000012345678));
        var doubles = new double[1];
        DecodeDouble(doubleSource, doubles, CancellationToken.None);
        Assert.Equal(unchecked((long)0x7FF8000012345678), BitConverter.DoubleToInt64Bits(doubles[0]));
    }

    [Fact]
    public void PlainBooleanUsesLeastSignificantBitFirst()
    {
        var values = new bool[9];
        DecodeBoolean([0b1000_0101, 0b0000_0001], values, CancellationToken.None);

        Assert.Equal([true, false, true, false, false, false, false, true, true], values);
    }

    [Fact]
    public void ForcedScalarModeMatchesDefaultMode()
    {
        const int Guard = unchecked((int)0xC0DEC0DE);
        var cases = new List<(byte[] Source, int[] Expected)>();
        for (var length = 0; length <= 65; length++)
        {
            var source = new byte[checked(length * sizeof(int))];
            for (var index = 0; index < length; index++)
                BinaryPrimitives.WriteInt32LittleEndian(source.AsSpan(index * sizeof(int)), index * 1_000_003);
            var expected = Enumerable.Repeat(Guard, length + 2).ToArray();
            DecodeInt32(source, expected.AsSpan(1, length), CancellationToken.None);
            cases.Add((source, expected));
        }

        var ambientScalar = ScalarTestMode.BeginForcedScalar();
        try
        {
            foreach (var testCase in cases)
            {
                var actual = Enumerable.Repeat(Guard, testCase.Expected.Length).ToArray();
                DecodeInt32(testCase.Source, actual.AsSpan(1, actual.Length - 2), CancellationToken.None);
                Assert.Equal(testCase.Expected, actual);
            }
        }
        finally
        {
            ScalarTestMode.EndForcedScalar(ambientScalar);
        }
    }

    [Fact]
    public void PlainDecoderChecksExactLengthAndCancellation()
    {
        Assert.Throws<ParquetFormatException>(() =>
            DecodeInt32([0, 0, 0], new int[1], CancellationToken.None));
        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();
        Assert.ThrowsAny<OperationCanceledException>(() =>
            DecodeInt32([0, 0, 0, 0], new int[1], cancellation.Token));
    }

    [Fact]
    public void HybridDecoderHandlesRleAndBitPackedRuns()
    {
        static byte[] PackBits(IReadOnlyList<int> values, int bitWidth)
        {
            var result = new byte[checked((values.Count * bitWidth + 7) / 8)];
            var bitOffset = 0;
            foreach (var value in values)
            {
                for (var bit = 0; bit < bitWidth; bit++, bitOffset++)
                {
                    if ((value & (1 << bit)) != 0)
                        result[bitOffset >> 3] |= (byte)(1 << (bitOffset & 7));
                }
            }
            return result;
        }

        var packed = PackBits([0, 1, 2, 3, 4, 5, 6, 7], 3);
        var source = new byte[2 + 1 + packed.Length];
        source[0] = 10;
        source[1] = 6;
        source[2] = 3;
        packed.CopyTo(source, 3);
        var values = new int[13];

        var consumed = DecodeHybrid(source, 3, values, CancellationToken.None);

        Assert.Equal(source.Length, consumed);
        Assert.Equal([6, 6, 6, 6, 6, 0, 1, 2, 3, 4, 5, 6, 7], values);
    }

    [Fact]
    public void HybridDecoderPermitsOnlyMinimalFinalBitPackedPadding()
    {
        // Ordinary final-group padding: 3 values in one group of eight (five padding values).
        var source = new byte[] { 3, 0b0011_1001 };
        var values = new int[3];

        Assert.Equal(2, DecodeHybrid(source, 1, values, CancellationToken.None));
        Assert.Equal([1, 0, 0], values);
        Assert.Throws<ParquetFormatException>(() =>
            DecodeHybrid([8, 1], 1, new int[3], CancellationToken.None));
        // Wider allowance is rejected: two groups (sixteen values) for one destination slot.
        Assert.Throws<ParquetFormatException>(() =>
            DecodeHybrid([5, 0, 0], 1, new int[1], CancellationToken.None));
        // Boundaries: eight values need exactly one group; nine values need exactly two groups.
        Assert.Equal(2, DecodeHybrid([3, 0xFF], 1, new int[8], CancellationToken.None));
        Assert.Equal(3, DecodeHybrid([5, 0xFF, 0xFF], 1, new int[9], CancellationToken.None));
        Assert.Throws<ParquetFormatException>(() =>
            DecodeHybrid([7, 0, 0, 0], 1, new int[9], CancellationToken.None));
        // Bitmap lane mirrors the oracle: minimal final padding passes, wider fails.
        Assert.Equal(2, DecodeHybridBitmap([3, 0b0011_1001], 3, new byte[1], CancellationToken.None, out _));
        Assert.Throws<ParquetFormatException>(() =>
            DecodeHybridBitmap([5, 0, 0], 1, new byte[1], CancellationToken.None, out _));
    }

    [Theory]
    [InlineData(new byte[] { 0 })]
    [InlineData(new byte[] { 0x80 })]
    [InlineData(new byte[] { 2 })]
    [InlineData(new byte[] { 0x80, 0x80, 0x80, 0x80, 0x10 })]
    public void HybridDecoderRejectsMalformedRuns(byte[] source) =>
        Assert.Throws<ParquetFormatException>(() =>
            DecodeHybrid(source, 1, new int[1], CancellationToken.None));

    [Fact]
    public void HybridDecoderObservesCancellationBeforeWritingOutput()
    {
        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();

        Assert.ThrowsAny<OperationCanceledException>(() =>
            DecodeHybrid([2, 1], 1, new int[1], cancellation.Token));
    }

    [Fact]
    public void HybridBitmapDecoderHandlesRleBitPackedPaddingAndGuardRegions()
    {
        var guarded = new byte[] { 0xCC, 0xCC, 0xCC };
        var consumed = DecodeHybridBitmap(
            [6, 1, 3, 0b1010_0101],
            11,
            guarded.AsSpan(1, 2),
            CancellationToken.None,
            out var setBitCount);

        Assert.Equal(4, consumed);
        Assert.Equal(7, setBitCount);
        Assert.Equal(0xCC, guarded[0]);
        Assert.Equal(0b0010_1111, guarded[1]);
        Assert.Equal(0b0000_0101, guarded[2]);
    }

    [Fact]
    public void HybridBitmapDecoderRejectsMalformedInputAndObservesCancellation()
    {
        Assert.Throws<ParquetFormatException>(() =>
            DecodeHybridBitmap([4, 2], 2, new byte[1], CancellationToken.None, out _));
        Assert.Throws<ParquetFormatException>(() =>
            DecodeHybridBitmap([8, 1], 3, new byte[1], CancellationToken.None, out _));
        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();
        Assert.ThrowsAny<OperationCanceledException>(() =>
            DecodeHybridBitmap([2, 1], 1, new byte[1], cancellation.Token, out _));
    }

    [Fact]
    public void SnappyDecodesLiteralsAndOverlappingCopies()
    {
        var literal = new byte[] { 5, 16, (byte)'h', (byte)'e', (byte)'l', (byte)'l', (byte)'o' };
        var hello = new byte[5];
        DecodeSnappy(literal, hello, CancellationToken.None);
        Assert.Equal("hello"u8.ToArray(), hello);

        var repeated = new byte[] { 6, 0, (byte)'a', 5, 1 };
        var output = new byte[6];
        DecodeSnappy(repeated, output, CancellationToken.None);
        Assert.Equal("aaaaaa"u8.ToArray(), output);
    }

    [Fact]
    public void SnappyDecodesExtendedLiteralLength()
    {
        var source = new byte[3 + 60];
        source[0] = 60;
        source[1] = 240;
        source[2] = 59;
        for (var i = 0; i < 60; i++)
            source[3 + i] = (byte)i;
        var output = new byte[60];

        DecodeSnappy(source, output, CancellationToken.None);

        Assert.Equal(Enumerable.Range(0, 60).Select(static value => (byte)value), output);
    }

    [Fact]
    public void SnappyObservesCancellationBeforeCopyingOutput()
    {
        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();

        Assert.ThrowsAny<OperationCanceledException>(() =>
            DecodeSnappy([1, 0, 42], new byte[1], cancellation.Token));
    }

    [Theory]
    [InlineData(new byte[] { 1, 1, 1 })]
    [InlineData(new byte[] { 2, 0, 1 })]
    [InlineData(new byte[] { 1, 0, 0, 0 })]
    [InlineData(new byte[] { 0x80 })]
    public void SnappyRejectsInvalidDistanceLengthAndTruncation(byte[] source) =>
        Assert.Throws<ParquetFormatException>(() =>
            DecodeSnappy(source, new byte[1], CancellationToken.None));

    [Fact]
    public void DeterministicBoundedDecoderMutationsHaveOnlyClassifiedOutcomes()
    {
        const int seed = 0xDEC0DE;
        var random = new Random(seed);
        var stopwatch = Stopwatch.StartNew();

        for (var iteration = 0; iteration < 2048; iteration++)
        {
            var input = new byte[random.Next(0, 65)];
            random.NextBytes(input);
            var valueCount = random.Next(0, 65);
            try
            {
                DecodeHybrid(input, random.Next(0, 34), new int[valueCount], CancellationToken.None);
            }
            catch (ParquetException)
            {
            }
            catch (Exception exception)
            {
                Assert.Fail(
                    $"Hybrid seed {seed}, iteration {iteration} produced " +
                    $"{exception.GetType().FullName}: {exception.Message}");
            }

            try
            {
                DecodeSnappy(input, new byte[random.Next(0, 129)], CancellationToken.None);
            }
            catch (ParquetException)
            {
            }
            catch (Exception exception)
            {
                Assert.Fail(
                    $"Snappy seed {seed}, iteration {iteration} produced " +
                    $"{exception.GetType().FullName}: {exception.Message}");
            }

            try
            {
                DecodeInt32(input, new int[valueCount], CancellationToken.None);
            }
            catch (ParquetException)
            {
            }
            catch (Exception exception)
            {
                Assert.Fail(
                    $"PLAIN seed {seed}, iteration {iteration} produced " +
                    $"{exception.GetType().FullName}: {exception.Message}");
            }
        }

        Assert.True(stopwatch.Elapsed < TimeSpan.FromSeconds(5),
            $"Bounded decoder mutation run exceeded its 5-second cap: {stopwatch.Elapsed}.");
    }

    [Fact]
    public void SnappyOneByteCopiesCoverLengthsAndDistances()
    {
        // Eight seed bytes, an overlapping maximal-length copy at distance 8,
        // then a minimal copy at distance one.
        var source = new List<byte> { 23, (8 - 1) << 2 };
        for (var value = 10; value < 18; value++)
            source.Add((byte)value);
        source.Add((byte)(((11 - 4) << 2) | 1));
        source.Add(8);
        source.Add(1);
        source.Add(1);
        var output = new byte[23];
        DecodeSnappy(source.ToArray(), output, CancellationToken.None);
        Assert.Equal(
            [10, 11, 12, 13, 14, 15, 16, 17, 10, 11, 12, 13, 14, 15, 16, 17, 10, 11, 12, 12, 12, 12, 12],
            output);
    }

    [Fact]
    public void SnappyTwoByteCopiesCoverLengthAndDistanceBounds()
    {
        // A 64-byte copy at the maximal two-byte distance, then a minimal copy.
        const int seedLength = 65535;
        var source = new List<byte> { 0xC0, 0x80, 0x04, 244, 0xFE, 0xFF };
        for (var index = 0; index < seedLength; index++)
            source.Add((byte)(index % 251));
        source.Add(254);
        source.Add(0xFF);
        source.Add(0xFF);
        source.Add(2);
        source.Add(1);
        source.Add(0);
        var output = new byte[checked(seedLength + 65)];
        DecodeSnappy(source.ToArray(), output, CancellationToken.None);
        var expected = new byte[checked(seedLength + 65)];
        for (var index = 0; index < seedLength; index++)
            expected[index] = (byte)(index % 251);
        for (var index = 0; index < 64; index++)
            expected[seedLength + index] = expected[index];
        expected[^1] = expected[^2];
        Assert.Equal(expected, output);
    }

    [Fact]
    public void SnappyFourByteCopiesCoverLargeDistances()
    {
        // Copies beyond the two-byte distance range, then a minimal copy.
        const int seedLength = 70000;
        var distance = 70000;
        var source = new List<byte> { 0xB1, 0xA3, 0x04, 248, 0x6F, 0x11, 0x01 };
        for (var index = 0; index < seedLength; index++)
            source.Add((byte)((index * 31 + 7) % 251));
        source.Add(255);
        source.Add((byte)distance);
        source.Add((byte)(distance >> 8));
        source.Add((byte)(distance >> 16));
        source.Add((byte)(distance >> 24));
        source.Add(3);
        source.Add((byte)distance);
        source.Add((byte)(distance >> 8));
        source.Add((byte)(distance >> 16));
        source.Add((byte)(distance >> 24));
        var output = new byte[checked(seedLength + 65)];
        DecodeSnappy(source.ToArray(), output, CancellationToken.None);
        var expected = new byte[checked(seedLength + 65)];
        for (var index = 0; index < seedLength; index++)
            expected[index] = (byte)((index * 31 + 7) % 251);
        for (var index = 0; index < 64; index++)
            expected[seedLength + index] = expected[index];
        expected[seedLength + 64] = expected[64];
        Assert.Equal(expected, output);
    }

    [Fact]
    public void SnappyDistanceOneFillMatchesAcrossLengths()
    {
        foreach (var length in new[] { 1, 4, 11, 63, 64, 65, 4095, 4096, 4097, 10000 })
        {
            var source = new List<byte>();
            var declared = (uint)(length + 1);
            while (declared >= 0x80)
            {
                source.Add((byte)(declared | 0x80));
                declared >>= 7;
            }
            source.Add((byte)declared);
            source.Add(0);
            source.Add(0x2A);
            var remaining = length;
            while (remaining > 0)
            {
                var chunk = Math.Min(64, remaining);
                source.Add((byte)(((chunk - 1) << 2) | 2));
                source.Add(1);
                source.Add(0);
                remaining -= chunk;
            }
            var output = new byte[length + 1];
            DecodeSnappy(source.ToArray(), output, CancellationToken.None);
            Assert.Equal(Enumerable.Repeat((byte)0x2A, length + 1).ToArray(), output);
        }
    }

    [Fact]
    public void SnappyRandomCopiesMatchScalarOracle()
    {
        var random = new Random(0x5EED20);
        for (var iteration = 0; iteration < 48; iteration++)
        {
            var target = 500 + iteration * 400;
            var expected = new List<byte>();
            var body = new List<byte>();
            var seedLength = random.Next(1, 25);
            for (var index = 0; index < seedLength; index++)
                expected.Add((byte)random.Next(256));
            EmitLiterals(body, expected, 0, seedLength);
            while (expected.Count < target)
            {
                if (random.Next(100) < 45)
                {
                    var start = expected.Count;
                    var count = random.Next(1, 71);
                    for (var index = 0; index < count; index++)
                        expected.Add((byte)random.Next(256));
                    EmitLiterals(body, expected, start, expected.Count);
                }
                else
                {
                    var distance = PickDistance(random, expected.Count);
                    var length = PickLength(random);
                    var position = expected.Count;
                    for (var index = 0; index < length; index++)
                        expected.Add(expected[position - distance + index]);
                    EmitCopy(body, distance, length, random);
                }
            }
            var stream = new List<byte>();
            EmitVarUInt32(stream, (uint)expected.Count);
            stream.AddRange(body);
            var guarded = new byte[checked(7 + expected.Count + 11)];
            for (var index = 0; index < guarded.Length; index++)
                guarded[index] = 0xA5;
            DecodeSnappy(stream.ToArray(), guarded.AsSpan(7, expected.Count), CancellationToken.None);
            Assert.Equal(expected.ToArray(), guarded.AsSpan(7, expected.Count).ToArray());
            for (var index = 0; index < 7; index++)
                Assert.Equal(0xA5, guarded[index]);
            for (var index = 7 + expected.Count; index < guarded.Length; index++)
                Assert.Equal(0xA5, guarded[index]);
        }

        static int PickDistance(Random random, int available)
        {
            var roll = random.Next(100);
            var bound = roll < 20 ? 1 : roll < 60 ? Math.Min(16, available) : roll < 85 ? Math.Min(2047, available) : Math.Min(50000, available);
            return random.Next(1, bound + 1);
        }

        static int PickLength(Random random)
        {
            var roll = random.Next(100);
            if (roll < 55)
                return random.Next(1, 17);
            if (roll < 75)
                return random.Next(17, 65);
            if (roll < 90)
                return random.Next(65, 301);
            return random.Next(4097, 12001);
        }

        static void EmitVarUInt32(List<byte> output, uint value)
        {
            while (value >= 0x80)
            {
                output.Add((byte)(value | 0x80));
                value >>= 7;
            }
            output.Add((byte)value);
        }

        static void EmitLiterals(List<byte> output, List<byte> expected, int start, int end)
        {
            var position = start;
            while (position < end)
            {
                var chunk = Math.Min(60, end - position);
                output.Add((byte)((chunk - 1) << 2));
                for (var index = 0; index < chunk; index++)
                    output.Add(expected[position + index]);
                position += chunk;
            }
        }

        static void EmitCopy(List<byte> output, int distance, int length, Random random)
        {
            var remaining = length;
            while (remaining > 0)
            {
                var chunk = Math.Min(64, remaining);
                if (remaining - chunk is > 0 and < 4)
                    chunk = remaining - 4;
                if (chunk >= 4 && chunk <= 11 && distance <= 2047)
                {
                    output.Add((byte)(((chunk - 4) << 2) | ((distance >> 8) << 5) | 1));
                    output.Add((byte)distance);
                }
                else if (random.Next(4) == 0)
                {
                    output.Add((byte)(((chunk - 1) << 2) | 3));
                    output.Add((byte)distance);
                    output.Add((byte)(distance >> 8));
                    output.Add((byte)(distance >> 16));
                    output.Add((byte)(distance >> 24));
                }
                else
                {
                    output.Add((byte)(((chunk - 1) << 2) | 2));
                    output.Add((byte)distance);
                    output.Add((byte)(distance >> 8));
                }
                remaining -= chunk;
            }
        }
    }

    [Theory]
    [InlineData(new byte[] { 4, 1, 0 }, 4, "A Snappy copy has an invalid backward distance.")]
    [InlineData(new byte[] { 4, 2, 1, 0 }, 4, "A Snappy copy has an invalid backward distance.")]
    [InlineData(new byte[] { 4, 4, 7, 7, 2, 3, 0 }, 4, "A Snappy copy has an invalid backward distance.")]
    [InlineData(new byte[] { 8, 0, 65, 29, 1 }, 8, "A Snappy copy exceeds the declared output length.")]
    [InlineData(new byte[] { 1, 3, 255, 255, 255, 255 }, 1, "A Snappy copy distance is outside the managed output range.")]
    [InlineData(new byte[] { 2, 3, 0, 0, 0, 0 }, 2, "A Snappy copy has an invalid backward distance.")]
    [InlineData(new byte[] { 4, 1 }, 4, "A Snappy one-byte copy is truncated.")]
    [InlineData(new byte[] { 4, 2, 9 }, 4, "A Snappy two-byte copy is truncated.")]
    [InlineData(new byte[] { 4, 3, 1, 2 }, 4, "A Snappy four-byte copy is truncated.")]
    [InlineData(new byte[] { 5, 0, 65 }, 5, "A Snappy block did not produce its declared output length.")]
    public void SnappyRejectsMalformedCopies(byte[] source, int outputLength, string message)
    {
        var exception = Assert.Throws<ParquetFormatException>(() =>
            DecodeSnappy(source, new byte[outputLength], CancellationToken.None));
        Assert.Equal(message, exception.Message);
    }

    [Fact]
    public void SnappyCancelledCopyThrowsBeforeWriting()
    {
        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();
        var source = new byte[] { 65, 0, 7, 2, 1, 0 };
        Assert.ThrowsAny<OperationCanceledException>(() =>
            DecodeSnappy(source, new byte[65], cancellation.Token));
    }

    [Fact]
    public void SnappyCompressibleAndIncompressibleStreamsRoundTrip()
    {
        var zeros = new List<byte>();
        var zeroLength = 4096u;
        while (zeroLength >= 0x80)
        {
            zeros.Add((byte)(zeroLength | 0x80));
            zeroLength >>= 7;
        }
        zeros.Add((byte)zeroLength);
        zeros.Add(0);
        zeros.Add(0);
        var remaining = 4095;
        while (remaining > 0)
        {
            var chunk = Math.Min(64, remaining);
            zeros.Add((byte)(((chunk - 1) << 2) | 2));
            zeros.Add(1);
            zeros.Add(0);
            remaining -= chunk;
        }
        var zeroOutput = new byte[4096];
        DecodeSnappy(zeros.ToArray(), zeroOutput, CancellationToken.None);
        Assert.Equal(new byte[4096], zeroOutput);

        var random = new Random(1234);
        var payload = new byte[1024];
        random.NextBytes(payload);
        var literals = new List<byte> { 0x80, 0x08 };
        var restStart = 0;
        while (restStart < payload.Length)
        {
            var chunk = Math.Min(60, payload.Length - restStart);
            literals.Add((byte)((chunk - 1) << 2));
            literals.AddRange(payload[restStart..(restStart + chunk)]);
            restStart += chunk;
        }
        var literalOutput = new byte[1024];
        DecodeSnappy(literals.ToArray(), literalOutput, CancellationToken.None);
        Assert.Equal(payload, literalOutput);
    }
    [Fact]
    public void WidePlainDecodersMatchScalarWithForcedScalar()
    {
        var random = new Random(0x51DE);
        var int64Source = new byte[513 * sizeof(long)];
        random.NextBytes(int64Source);
        var floatSource = new byte[517 * sizeof(float)];
        random.NextBytes(floatSource);
        var doubleSource = new byte[519 * sizeof(double)];
        random.NextBytes(doubleSource);
        var defaultInt64 = new long[513];
        var defaultFloat = new float[517];
        var defaultDouble = new double[519];
        DecodeInt64(int64Source, defaultInt64, CancellationToken.None);
        DecodeFloat(floatSource, defaultFloat, CancellationToken.None);
        DecodeDouble(doubleSource, defaultDouble, CancellationToken.None);

        var ambientScalar = ScalarTestMode.BeginForcedScalar();
        try
        {
            var scalarInt64 = new long[513];
            var scalarFloat = new float[517];
            var scalarDouble = new double[519];
            DecodeInt64(int64Source, scalarInt64, CancellationToken.None);
            DecodeFloat(floatSource, scalarFloat, CancellationToken.None);
            DecodeDouble(doubleSource, scalarDouble, CancellationToken.None);
            Assert.Equal(defaultInt64, scalarInt64);
            Assert.Equal(
                defaultFloat.Select(BitConverter.SingleToInt32Bits),
                scalarFloat.Select(BitConverter.SingleToInt32Bits));
            Assert.Equal(
                defaultDouble.Select(BitConverter.DoubleToInt64Bits),
                scalarDouble.Select(BitConverter.DoubleToInt64Bits));
        }
        finally
        {
            ScalarTestMode.EndForcedScalar(ambientScalar);
        }
    }

    [Fact]
    public void WidePlainDecodersHandleTailsAndMisalignment()
    {
        var random = new Random(0x7A11);
        foreach (var length in new[] { 0, 1, 2, 3, 5, 7, 8, 9, 15, 16, 17, 31, 33, 63, 65, 100 })
        {
            foreach (var offset in new[] { 0, 1, 3, 7 })
            {
                var int64Source = new byte[offset + length * sizeof(long) + 8];
                var floatSource = new byte[offset + length * sizeof(float) + 8];
                var doubleSource = new byte[offset + length * sizeof(double) + 8];
                random.NextBytes(int64Source);
                random.NextBytes(floatSource);
                random.NextBytes(doubleSource);
                var int64Actual = new long[offset + length + 2];
                var floatActual = new float[offset + length + 2];
                var doubleActual = new double[offset + length + 2];
                DecodeInt64(
                    int64Source.AsSpan(offset, length * sizeof(long)),
                    int64Actual.AsSpan(offset, length),
                    CancellationToken.None);
                DecodeFloat(
                    floatSource.AsSpan(offset, length * sizeof(float)),
                    floatActual.AsSpan(offset, length),
                    CancellationToken.None);
                DecodeDouble(
                    doubleSource.AsSpan(offset, length * sizeof(double)),
                    doubleActual.AsSpan(offset, length),
                    CancellationToken.None);

                var ambientScalar = ScalarTestMode.BeginForcedScalar();
                try
                {
                    var int64Expected = new long[offset + length + 2];
                    var floatExpected = new float[offset + length + 2];
                    var doubleExpected = new double[offset + length + 2];
                    DecodeInt64(
                        int64Source.AsSpan(offset, length * sizeof(long)),
                        int64Expected.AsSpan(offset, length),
                        CancellationToken.None);
                    DecodeFloat(
                        floatSource.AsSpan(offset, length * sizeof(float)),
                        floatExpected.AsSpan(offset, length),
                        CancellationToken.None);
                    DecodeDouble(
                        doubleSource.AsSpan(offset, length * sizeof(double)),
                        doubleExpected.AsSpan(offset, length),
                        CancellationToken.None);
                    Assert.Equal(int64Expected, int64Actual);
                    Assert.Equal(
                        floatExpected.Select(BitConverter.SingleToInt32Bits),
                        floatActual.Select(BitConverter.SingleToInt32Bits));
                    Assert.Equal(
                        doubleExpected.Select(BitConverter.DoubleToInt64Bits),
                        doubleActual.Select(BitConverter.DoubleToInt64Bits));
                }
                finally
                {
                    ScalarTestMode.EndForcedScalar(ambientScalar);
                }
            }
        }
    }
    [Fact]
    public void WidePlainDecodersPreserveSpecialFloatBits()
    {
        var floatBits = new int[]
        {
            0x7F800000, unchecked((int)0xFF800000), 0x00000000, unchecked((int)0x80000000),
            0x007FFFFF, 0x00000001, 0x7FC00000, 0x7F800001, unchecked((int)0x7FFFFFFF),
            0x7F7FFFFF, 0x00800000, unchecked((int)0x7FC01234),
        };
        var floatSource = new byte[floatBits.Length * sizeof(float)];
        for (var index = 0; index < floatBits.Length; index++)
            BinaryPrimitives.WriteInt32LittleEndian(floatSource.AsSpan(index * sizeof(float)), floatBits[index]);
        var floats = new float[floatBits.Length];
        DecodeFloat(floatSource, floats, CancellationToken.None);
        Assert.Equal(floatBits, floats.Select(BitConverter.SingleToInt32Bits));
        Assert.True(float.IsPositiveInfinity(floats[0]));
        Assert.True(float.IsNegativeInfinity(floats[1]));
        Assert.True(float.IsNegative(floats[3]));
        Assert.True(float.IsNaN(floats[6]));

        var doubleBits = new long[]
        {
            0x7FF0000000000000L, unchecked((long)0xFFF0000000000000UL), 0L, unchecked((long)0x8000000000000000UL),
            0x000FFFFFFFFFFFFFL, 0x0000000000000001L, 0x7FF8000000000000L, 0x7FF0000000000001L,
            0x7FFFFFFFFFFFFFFFL, 0x7FF0000000000000L | 0x0008000000000000L, 0x7FEFFFFFFFFFFFFFL, 0x0010000000000000L,
        };
        var doubleSource = new byte[doubleBits.Length * sizeof(double)];
        for (var index = 0; index < doubleBits.Length; index++)
            BinaryPrimitives.WriteInt64LittleEndian(doubleSource.AsSpan(index * sizeof(double)), doubleBits[index]);
        var doubles = new double[doubleBits.Length];
        DecodeDouble(doubleSource, doubles, CancellationToken.None);
        Assert.Equal(doubleBits, doubles.Select(BitConverter.DoubleToInt64Bits));
        Assert.True(double.IsPositiveInfinity(doubles[0]));
        Assert.True(double.IsNegativeInfinity(doubles[1]));
        Assert.True(double.IsNegative(doubles[3]));
        Assert.True(double.IsNaN(doubles[6]));

        var int64Values = new long[] { long.MinValue, long.MaxValue, -1L, 0L, 1L, 0x0102030405060708L };
        var int64Source = new byte[int64Values.Length * sizeof(long)];
        for (var index = 0; index < int64Values.Length; index++)
            BinaryPrimitives.WriteInt64LittleEndian(int64Source.AsSpan(index * sizeof(long)), int64Values[index]);
        var int64Actual = new long[int64Values.Length];
        DecodeInt64(int64Source, int64Actual, CancellationToken.None);
        Assert.Equal(int64Values, int64Actual);
    }

    [Theory]
    [InlineData(3, 1)]
    [InlineData(5, 1)]
    [InlineData(7, 2)]
    public void WidePlainDecodersRejectMalformedByteCounts(int byteLength, int valueLength)
    {
        Assert.Throws<ParquetFormatException>(() =>
            DecodeInt64(new byte[byteLength], new long[valueLength], CancellationToken.None));
        Assert.Throws<ParquetFormatException>(() =>
            DecodeFloat(new byte[byteLength], new float[valueLength], CancellationToken.None));
        Assert.Throws<ParquetFormatException>(() =>
            DecodeDouble(new byte[byteLength], new double[valueLength], CancellationToken.None));
    }

    [Fact]
    public void WidePlainDecodersObserveCancellation()
    {
        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();
        Assert.ThrowsAny<OperationCanceledException>(() =>
            DecodeInt64(new byte[sizeof(long)], new long[1], cancellation.Token));
        Assert.ThrowsAny<OperationCanceledException>(() =>
            DecodeFloat(new byte[sizeof(float)], new float[1], cancellation.Token));
        Assert.ThrowsAny<OperationCanceledException>(() =>
            DecodeDouble(new byte[sizeof(double)], new double[1], cancellation.Token));

        var ambientScalar = ScalarTestMode.BeginForcedScalar();
        try
        {
            Assert.ThrowsAny<OperationCanceledException>(() =>
                DecodeInt64(new byte[sizeof(long)], new long[1], cancellation.Token));
            Assert.ThrowsAny<OperationCanceledException>(() =>
                DecodeFloat(new byte[sizeof(float)], new float[1], cancellation.Token));
            Assert.ThrowsAny<OperationCanceledException>(() =>
                DecodeDouble(new byte[sizeof(double)], new double[1], cancellation.Token));
        }
        finally
        {
            ScalarTestMode.EndForcedScalar(ambientScalar);
        }
    }
    private static TDelegate GetMethod<TDelegate>(string typeName, string methodName)
        where TDelegate : Delegate
    {
        var type = typeof(ParquetFile).Assembly.GetType($"Lokad.Parquet.Internal.{typeName}") ??
            throw new InvalidOperationException($"Internal type {typeName} was not found.");
        var method = type.GetMethod(methodName, BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Static) ??
            throw new InvalidOperationException($"Internal method {typeName}.{methodName} was not found.");
        return method.CreateDelegate<TDelegate>();
    }

}
