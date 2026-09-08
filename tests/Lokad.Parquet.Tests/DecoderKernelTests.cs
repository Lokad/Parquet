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

        AppContext.SetSwitch("Lokad.Parquet.ForceScalar", true);
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
            AppContext.SetSwitch("Lokad.Parquet.ForceScalar", false);
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
