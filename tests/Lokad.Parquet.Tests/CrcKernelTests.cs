namespace Lokad.Parquet.Tests;

// Page-CRC kernel coverage: the shipped slicing-by-8 kernel must agree with
// independent IEEE check vectors and with the retired bit-at-a-time kernel,
// which is kept here as the test-side oracle, across empty/tail/block and
// cancellation boundaries plus end-to-end CRC-bearing pages.
public sealed class CrcKernelTests
{
    [Theory]
    [InlineData(new byte[0], 0x00000000u)]
    [InlineData(new byte[] { 0 }, 0xD202EF8Du)]
    [InlineData(new byte[] { 97, 98, 99 }, 0x352441C2u)]
    public void Crc32MatchesIndependentCheckVectors(byte[] input, uint expected) =>
        Assert.Equal(expected, ComputeCrc32(input, CancellationToken.None));

    [Fact]
    public void Crc32MatchesCheckValueForStandardString()
    {
        var input = System.Text.Encoding.ASCII.GetBytes("123456789");
        Assert.Equal(0xCBF43926u, ComputeCrc32(input, CancellationToken.None));
        Assert.Equal(0xCBF43926u, ScalarCrc32Oracle(input));
    }

    [Fact]
    public void Crc32MatchesCheckValueForFoxSentence()
    {
        var input = System.Text.Encoding.ASCII.GetBytes("The quick brown fox jumps over the lazy dog");
        Assert.Equal(0x414FA339u, ComputeCrc32(input, CancellationToken.None));
        Assert.Equal(0x414FA339u, ScalarCrc32Oracle(input));
    }

    [Fact]
    public void Crc32MatchesOracleAcrossBlockAndTailLengths()
    {
        var random = new Random(0xC2C21);
        foreach (var length in new[] { 0, 1, 2, 7, 8, 9, 15, 16, 17, 63, 64, 65, 4095, 4096, 4097, 8191, 8192, 8193 })
        {
            var input = new byte[length];
            random.NextBytes(input);
            Assert.Equal(ScalarCrc32Oracle(input), ComputeCrc32(input, CancellationToken.None));
        }
    }

    [Fact]
    public void Crc32MatchesOracleOnRandomPayloads()
    {
        var random = new Random(0xC2C22);
        for (var iteration = 0; iteration < 32; iteration++)
        {
            var input = new byte[random.Next(0, 70000)];
            random.NextBytes(input);
            Assert.Equal(ScalarCrc32Oracle(input), ComputeCrc32(input, CancellationToken.None));
        }
    }

    [Fact]
    public void Crc32MatchesOracleOnLargePayload()
    {
        var input = new byte[1024 * 1024];
        new Random(0xC2C23).NextBytes(input);
        Assert.Equal(ScalarCrc32Oracle(input), ComputeCrc32(input, CancellationToken.None));
    }

    [Fact]
    public void Crc32DetectsSingleByteCorruption()
    {
        var input = new byte[1000];
        new Random(0xC2C24).NextBytes(input);
        var baseline = ComputeCrc32(input, CancellationToken.None);
        Assert.Equal(ScalarCrc32Oracle(input), baseline);
        foreach (var position in new[] { 0, 1, 7, 8, 511, 512, 998, 999 })
        {
            var corrupted = (byte[])input.Clone();
            corrupted[position] ^= 0x01;
            Assert.NotEqual(baseline, ComputeCrc32(corrupted, CancellationToken.None));
        }
    }

    [Fact]
    public void CancelledCrc32ThrowsBeforeWriting()
    {
        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();
        Assert.ThrowsAny<OperationCanceledException>(() =>
            ComputeCrc32(new byte[16], cancellation.Token));
        Assert.ThrowsAny<OperationCanceledException>(() =>
            ComputeCrc32(new byte[1024 * 1024], cancellation.Token));
    }

    [Fact]
    public async Task LargeCrcBearingPageScansExactly()
    {
        const int rows = 65536;
        var values = new int[rows];
        for (var row = 0; row < rows; row++)
            values[row] = row * 3 + 1;
        var bytes = ParquetFixtureBuilder.CreateInt32(new()
        {
            Values = values,
            CrcMode = FixtureCrcMode.Valid,
        });
        await using var file = await ParquetFile.OpenAsync(new MemoryStream(bytes, writable: false));
        await using var enumerator = file.ScanAsync(new([file.Metadata.Schema.Columns[0]])).GetAsyncEnumerator();
        Assert.True(await enumerator.MoveNextAsync());
        using (var batch = enumerator.Current)
            Assert.Equal(values, Assert.IsType<ParquetPrimitiveColumnBatch<int>>(batch.Columns[0]).Values.ToArray());
        Assert.False(await enumerator.MoveNextAsync());
    }

    // Reflection bridge for the internal kernel; every fact below calls it,
    // so no single-caller exception is needed.
    private static uint ComputeCrc32(byte[] input, CancellationToken cancellationToken)
    {
        var method = typeof(ParquetFile).Assembly.GetType("Lokad.Parquet.Internal.ScanPageReader")?.GetMethod(
            "ComputeCrc32",
            System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static) ??
            throw new InvalidOperationException("Internal method ScanPageReader.ComputeCrc32 was not found.");
        return method.CreateDelegate<Func<ReadOnlySpan<byte>, CancellationToken, uint>>()(input, cancellationToken);
    }

    // Retired bit-at-a-time kernel kept as the test-side oracle.
    private static uint ScalarCrc32Oracle(ReadOnlySpan<byte> input)
    {
        var crc = uint.MaxValue;
        for (var index = 0; index < input.Length; index++)
        {
            crc ^= input[index];
            for (var bit = 0; bit < 8; bit++)
                crc = (crc >> 1) ^ (0xEDB88320u & (uint)-(int)(crc & 1));
        }
        return ~crc;
    }
}
