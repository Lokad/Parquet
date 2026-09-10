namespace Lokad.Parquet.Tests;

// End-to-end coverage for copy-bearing Snappy pages: the fixture builder encodes
// the same payloads with literal-only and copy-emitting streams, and the scans
// must agree with each other and with locally built oracles.
public sealed class SnappyCopyDecodingTests
{
    [Fact]
    public async Task CopyHeavyZerosScan()
    {
        const int rows = 4096;
        var bytes = ParquetFixtureBuilder.CreateInt32(new()
        {
            Values = new int[rows],
            CompressionCodec = ParquetCompressionCodec.Snappy,
            SnappyCopyEncoding = true,
            CrcMode = FixtureCrcMode.Valid,
        });
        await using var file = await ParquetFile.OpenAsync(new MemoryStream(bytes, writable: false));
        await using var enumerator = file.ScanAsync(new([file.Metadata.Schema.Columns[0]])).GetAsyncEnumerator();
        Assert.True(await enumerator.MoveNextAsync());
        using (var batch = enumerator.Current)
            Assert.Equal(new int[rows], Assert.IsType<ParquetPrimitiveColumnBatch<int>>(batch.Columns[0]).Values.ToArray());
        Assert.False(await enumerator.MoveNextAsync());
    }

    [Fact]
    public async Task MixedCopyEncodingMatchesLiteralEncoding()
    {
        // Fifteen values (60 payload bytes) fit the literal-only encoder, so
        // both encodings cover the same payload: a zero run, a short period,
        // and unique tail values.
        var values = new int[] { 0, 0, 0, 0, 0, 1000, 2000, 1000, 2000, 5, 999983, 42, -7, 123456, 0 };
        var literalBytes = ParquetFixtureBuilder.CreateInt32(new()
        {
            Values = values,
            CompressionCodec = ParquetCompressionCodec.Snappy,
        });
        var copyBytes = ParquetFixtureBuilder.CreateInt32(new()
        {
            Values = values,
            CompressionCodec = ParquetCompressionCodec.Snappy,
            SnappyCopyEncoding = true,
        });
        Assert.Equal(values, await ScanRequiredInt32Async(literalBytes));
        Assert.Equal(values, await ScanRequiredInt32Async(copyBytes));
    }

    [Fact]
    public async Task CopyHeavyV2Scan()
    {
        const int rows = 1024;
        var values = new int[rows];
        for (var row = 0; row < rows; row++)
            values[row] = (row % 16) - 8;
        var bytes = ParquetFixtureBuilder.CreateInt32(new()
        {
            Values = values,
            PageVersion = FixturePageVersion.DataPageV2,
            CompressionCodec = ParquetCompressionCodec.Snappy,
            SnappyCopyEncoding = true,
            CrcMode = FixtureCrcMode.Valid,
        });
        Assert.Equal(values, await ScanRequiredInt32Async(bytes));
    }

    [Fact]
    public async Task CopyHeavyOptionalScan()
    {
        const int rows = 1024;
        var values = new int[rows];
        var validity = new bool[rows];
        for (var row = 0; row < rows; row++)
        {
            values[row] = (row % 5) * 11;
            validity[row] = (row % 8) != 0;
        }
        var bytes = ParquetFixtureBuilder.CreateInt32(new()
        {
            Values = values,
            Repetition = ParquetRepetition.Optional,
            Validity = validity,
            CompressionCodec = ParquetCompressionCodec.Snappy,
            SnappyCopyEncoding = true,
        });
        await using var file = await ParquetFile.OpenAsync(new MemoryStream(bytes, writable: false));
        await using var enumerator = file.ScanAsync(new([file.Metadata.Schema.Columns[0]])).GetAsyncEnumerator();
        Assert.True(await enumerator.MoveNextAsync());
        using (var batch = enumerator.Current)
        {
            var column = Assert.IsType<ParquetPrimitiveColumnBatch<int>>(batch.Columns[0]);
            Assert.Equal(Enumerable.Range(0, rows).Select(static row => (row % 8) != 0), Enumerable.Range(0, rows).Select(column.Validity.IsValid));
            var slots = column.Values.Span;
            for (var row = 0; row < rows; row++)
            {
                if (validity[row])
                    Assert.Equal(values[row], slots[row]);
            }
        }
        Assert.False(await enumerator.MoveNextAsync());
    }

    [Fact]
    public async Task CopyHeavyDictionaryScan()
    {
        var dictionary = new int[] { 10, 20, 30 };
        var indices = new int[1500];
        for (var row = 0; row < indices.Length; row++)
            indices[row] = (row / 500) % dictionary.Length;
        var expected = indices.Select(index => dictionary[index]).ToArray();
        var bytes = ParquetFixtureBuilder.CreateInt32(new()
        {
            Values = expected,
            DictionaryValues = dictionary,
            DictionaryIndices = indices,
            CoalesceIndexRuns = true,
            CompressionCodec = ParquetCompressionCodec.Snappy,
            SnappyCopyEncoding = true,
        });
        Assert.Equal(expected, await ScanRequiredInt32Async(bytes));
    }

    [Fact]
    public async Task CopyHeavyBinaryScan()
    {
        var pool = new byte[][] { [1], [2, 3], [4, 5, 6], [7] };
        const int rows = 512;
        var values = new byte[rows][];
        for (var row = 0; row < rows; row++)
            values[row] = pool[row % pool.Length];
        var bytes = ParquetFixtureBuilder.CreateInt32(new()
        {
            PhysicalTypeCode = (int)ParquetPhysicalType.ByteArray,
            PhysicalValues = values,
            CompressionCodec = ParquetCompressionCodec.Snappy,
            SnappyCopyEncoding = true,
        });
        await using var file = await ParquetFile.OpenAsync(new MemoryStream(bytes, writable: false));
        await using var enumerator = file.ScanAsync(new([file.Metadata.Schema.Columns[0]])).GetAsyncEnumerator();
        Assert.True(await enumerator.MoveNextAsync());
        using (var batch = enumerator.Current)
        {
            var column = Assert.IsType<ParquetBinaryColumnBatch>(batch.Columns[0]);
            var offsets = column.Offsets.Span;
            var payload = column.Payload.Span;
            Assert.Equal(rows, offsets.Length - 1);
            for (var row = 0; row < rows; row++)
                Assert.Equal(values[row], payload[offsets[row]..offsets[row + 1]].ToArray());
        }
        Assert.False(await enumerator.MoveNextAsync());
    }

    private static async Task<int[]> ScanRequiredInt32Async(byte[] fixture)
    {
        var collected = new List<int>();
        await using var file = await ParquetFile.OpenAsync(new MemoryStream(fixture, writable: false));
        await foreach (var batch in file.ScanAsync(new([file.Metadata.Schema.Columns[0]])))
        {
            using (batch)
                collected.AddRange(Assert.IsType<ParquetPrimitiveColumnBatch<int>>(batch.Columns[0]).Values.ToArray());
        }
        return collected.ToArray();
    }
}
