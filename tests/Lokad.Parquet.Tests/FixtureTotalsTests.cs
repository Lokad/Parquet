namespace Lokad.Parquet.Tests;

public sealed class FixtureTotalsTests
{
    [Fact]
    public async Task UncompressedPlainTotalsMatchChunkLength()
    {
        var bytes = ParquetFixtureBuilder.CreateInt32(new() { Values = [10, -2, 42] });
        await using var file = await ParquetFile.OpenAsync(new MemoryStream(bytes, writable: false));
        var chunk = file.Metadata.RowGroups[0].Columns[0];
        Assert.Equal(chunk.TotalCompressedSize, chunk.TotalUncompressedSize);
        Assert.Equal(FooterStart(bytes) - 4, chunk.TotalCompressedSize);
        await using var enumerator = file.ScanAsync(new([file.Metadata.Schema.Columns[0]])).GetAsyncEnumerator();
        Assert.True(await enumerator.MoveNextAsync());
        using (var batch = enumerator.Current)
            Assert.Equal([10, -2, 42], Assert.IsType<ParquetPrimitiveColumnBatch<int>>(batch.Columns[0]).Values.ToArray());
        Assert.False(await enumerator.MoveNextAsync());
    }

    [Fact]
    public async Task UncompressedDictionaryTotalsMatchChunkLength()
    {
        var bytes = ParquetFixtureBuilder.CreateInt32(new()
        {
            PhysicalTypeCode = (int)ParquetPhysicalType.FixedLengthByteArray,
            TypeLength = 2,
            PhysicalValues = new byte[][] { [3, 4], [1, 2] },
            DictionaryValues = new byte[][] { [1, 2], [3, 4] },
            DictionaryIndices = [1, 0],
        });
        await using var file = await ParquetFile.OpenAsync(new MemoryStream(bytes, writable: false));
        var chunk = file.Metadata.RowGroups[0].Columns[0];
        Assert.Equal(chunk.TotalCompressedSize, chunk.TotalUncompressedSize);
        Assert.Equal(FooterStart(bytes) - 4, chunk.TotalCompressedSize);
        await using var enumerator = file.ScanAsync(new([file.Metadata.Schema.Columns[0]])).GetAsyncEnumerator();
        Assert.True(await enumerator.MoveNextAsync());
        using (var batch = enumerator.Current)
        {
            var column = Assert.IsType<ParquetFixedLengthByteArrayColumnBatch>(batch.Columns[0]);
            Assert.Equal([[3, 4], [1, 2]], ToRows(column));
        }

        Assert.False(await enumerator.MoveNextAsync());
    }

    [Fact]
    public async Task SnappyPlainTotalsSeparateCompressedFromUncompressed()
    {
        var bytes = ParquetFixtureBuilder.CreateInt32(new()
        {
            Values = [10, -2, 42],
            CompressionCodec = ParquetCompressionCodec.Snappy,
            CrcMode = FixtureCrcMode.Absent,
        });
        await using var file = await ParquetFile.OpenAsync(new MemoryStream(bytes, writable: false));
        var chunk = file.Metadata.RowGroups[0].Columns[0];
        Assert.Equal(FooterStart(bytes) - 4, chunk.TotalCompressedSize);
        Assert.Equal(chunk.TotalUncompressedSize + 2, chunk.TotalCompressedSize);
        await using var enumerator = file.ScanAsync(new([file.Metadata.Schema.Columns[0]])).GetAsyncEnumerator();
        Assert.True(await enumerator.MoveNextAsync());
        using (var batch = enumerator.Current)
            Assert.Equal([10, -2, 42], Assert.IsType<ParquetPrimitiveColumnBatch<int>>(batch.Columns[0]).Values.ToArray());
        Assert.False(await enumerator.MoveNextAsync());
    }

    [Fact]
    public async Task SnappyDictionaryTotalsIncludeBothPages()
    {
        var bytes = ParquetFixtureBuilder.CreateInt32(new()
        {
            PhysicalTypeCode = (int)ParquetPhysicalType.FixedLengthByteArray,
            TypeLength = 2,
            PhysicalValues = new byte[][] { [3, 4], [1, 2] },
            DictionaryValues = new byte[][] { [1, 2], [3, 4] },
            DictionaryIndices = [1, 0],
            CompressionCodec = ParquetCompressionCodec.Snappy,
            CrcMode = FixtureCrcMode.Absent,
        });
        await using var file = await ParquetFile.OpenAsync(new MemoryStream(bytes, writable: false));
        var chunk = file.Metadata.RowGroups[0].Columns[0];
        Assert.Equal(FooterStart(bytes) - 4, chunk.TotalCompressedSize);
        Assert.Equal(chunk.TotalUncompressedSize + 4, chunk.TotalCompressedSize);
        await using var enumerator = file.ScanAsync(new([file.Metadata.Schema.Columns[0]])).GetAsyncEnumerator();
        Assert.True(await enumerator.MoveNextAsync());
        using (var batch = enumerator.Current)
        {
            var column = Assert.IsType<ParquetFixedLengthByteArrayColumnBatch>(batch.Columns[0]);
            Assert.Equal([[3, 4], [1, 2]], ToRows(column));
        }

        Assert.False(await enumerator.MoveNextAsync());
    }

    [Fact]
    public async Task SnappyV2TotalsSeparate()
    {
        var bytes = ParquetFixtureBuilder.CreateInt32(new()
        {
            PhysicalTypeCode = (int)ParquetPhysicalType.Int64,
            PhysicalValues = new long[] { 10, -20, 30 },
            PageVersion = FixturePageVersion.DataPageV2,
            CompressionCodec = ParquetCompressionCodec.Snappy,
            CrcMode = FixtureCrcMode.Absent,
        });
        await using var file = await ParquetFile.OpenAsync(new MemoryStream(bytes, writable: false));
        var chunk = file.Metadata.RowGroups[0].Columns[0];
        Assert.Equal(FooterStart(bytes) - 4, chunk.TotalCompressedSize);
        Assert.Equal(chunk.TotalUncompressedSize + 2, chunk.TotalCompressedSize);
        await using var enumerator = file.ScanAsync(new([file.Metadata.Schema.Columns[0]])).GetAsyncEnumerator();
        Assert.True(await enumerator.MoveNextAsync());
        using (var batch = enumerator.Current)
            Assert.Equal([10L, -20L, 30L], Assert.IsType<ParquetPrimitiveColumnBatch<long>>(batch.Columns[0]).Values.ToArray());
        Assert.False(await enumerator.MoveNextAsync());
    }

    [Fact]
    public async Task TrailingSnappyPagesSumBothTotals()
    {
        var bytes = ParquetFixtureBuilder.CreateInt32(new()
        {
            Values = [1],
            TrailingPlainValues = new int[] { 30 },
            CompressionCodec = ParquetCompressionCodec.Snappy,
            CrcMode = FixtureCrcMode.Absent,
        });
        await using var file = await ParquetFile.OpenAsync(new MemoryStream(bytes, writable: false));
        var chunk = file.Metadata.RowGroups[0].Columns[0];
        Assert.Equal(2, file.Metadata.RowCount);
        Assert.Equal(FooterStart(bytes) - 4, chunk.TotalCompressedSize);
        Assert.Equal(chunk.TotalUncompressedSize + 4, chunk.TotalCompressedSize);
        var values = new List<int>();
        await foreach (var batch in file.ScanAsync(new([file.Metadata.Schema.Columns[0]])))
        {
            using (batch)
                values.AddRange(Assert.IsType<ParquetPrimitiveColumnBatch<int>>(batch.Columns[0]).Values.ToArray());
        }

        Assert.Equal([1, 30], values);
    }

    [Fact]
    public async Task ChunkTotalOverridesProduceAdvertisedTotals()
    {
        var bytes = ParquetFixtureBuilder.CreateInt32(new()
        {
            Values = [10, -2, 42],
            CompressionCodec = ParquetCompressionCodec.Snappy,
            CrcMode = FixtureCrcMode.Absent,
            ChunkTotalUncompressedSize = 0,
        });
        await using var file = await ParquetFile.OpenAsync(new MemoryStream(bytes, writable: false));
        var chunk = file.Metadata.RowGroups[0].Columns[0];
        Assert.Equal(0, chunk.TotalUncompressedSize);
        Assert.Equal(FooterStart(bytes) - 4, chunk.TotalCompressedSize);
    }

    private static long FooterStart(byte[] bytes)
    {
        var footerLength = System.Buffers.Binary.BinaryPrimitives.ReadInt32LittleEndian(bytes.AsSpan(bytes.Length - 8));
        return bytes.Length - 8 - footerLength;
    }

    private static byte[][] ToRows(ParquetFixedLengthByteArrayColumnBatch column)
    {
        var rows = new byte[column.RowCount][];
        for (var row = 0; row < rows.Length; row++)
            rows[row] = column.Payload.Span.Slice(row * column.TypeWidth, column.TypeWidth).ToArray();
        return rows;
    }
}
