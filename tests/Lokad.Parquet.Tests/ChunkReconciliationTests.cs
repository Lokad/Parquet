namespace Lokad.Parquet.Tests;

public sealed class ChunkReconciliationTests
{
    [Fact]
    public async Task LiedUncompressedTotalZeroFailsBeforePayloadRead()
    {
        using var tracker = new PoolTracker();
        var bytes = ParquetFixtureBuilder.CreateInt32(new() { Values = [10, -2, 42], ChunkTotalUncompressedSize = 0 });
        var source = new SizedCountingSource(bytes);
        await using var file = await ParquetFile.OpenAsync(source);
        Assert.Equal(0, file.Metadata.RowGroups[0].Columns[0].TotalUncompressedSize);
        source.Sizes.Clear();
        var exception = await Assert.ThrowsAsync<ParquetFormatException>(async () =>
        {
            await foreach (var batch in file.ScanAsync(new([file.Metadata.Schema.Columns[0]])))
                batch.Dispose();
        });
        Assert.Equal(0, exception.RowGroupOrdinal);
        Assert.Equal(0, exception.ColumnOrdinal);
        Assert.Equal(0, exception.PageOrdinal);
        Assert.Single(source.Sizes);
    }

    [Fact]
    public async Task UncompressedTotalOneShortFailsOnFinalPage()
    {
        using var tracker = new PoolTracker();
        var truthful = ParquetFixtureBuilder.CreateInt32(new() { Values = [1], TrailingPlainValues = new int[] { 30 } });
        long actual;
        await using (var truth = await ParquetFile.OpenAsync(new MemoryStream(truthful, writable: false)))
            actual = truth.Metadata.RowGroups[0].Columns[0].TotalUncompressedSize;
        var bytes = ParquetFixtureBuilder.CreateInt32(new()
        {
            Values = [1],
            TrailingPlainValues = new int[] { 30 },
            ChunkTotalUncompressedSize = actual - 1,
        });
        await using var file = await ParquetFile.OpenAsync(new MemoryStream(bytes, writable: false));
        var exception = await Assert.ThrowsAsync<ParquetFormatException>(async () =>
        {
            await foreach (var batch in file.ScanAsync(new([file.Metadata.Schema.Columns[0]])))
                batch.Dispose();
        });
        Assert.Equal(0, exception.RowGroupOrdinal);
        Assert.Equal(0, exception.ColumnOrdinal);
        Assert.Equal(1, exception.PageOrdinal);
    }

    [Fact]
    public async Task HugeUncompressedTotalFailsAtChunkEnd()
    {
        using var tracker = new PoolTracker();
        var bytes = ParquetFixtureBuilder.CreateInt32(new() { Values = [10, -2, 42], ChunkTotalUncompressedSize = long.MaxValue });
        await using var file = await ParquetFile.OpenAsync(new MemoryStream(bytes, writable: false));
        var exception = await Assert.ThrowsAsync<ParquetFormatException>(async () =>
        {
            await foreach (var batch in file.ScanAsync(new([file.Metadata.Schema.Columns[0]])))
                batch.Dispose();
        });
        Assert.Equal(0, exception.RowGroupOrdinal);
        Assert.Equal(0, exception.ColumnOrdinal);
    }

    [Fact]
    public async Task ValidSnappyDictionaryMixedChunkScans()
    {
        using var tracker = new PoolTracker();
        var bytes = ParquetFixtureBuilder.CreateInt32(new()
        {
            Values = [20, 10],
            DictionaryValues = new int[] { 10, 20 },
            DictionaryIndices = [1, 0],
            TrailingPlainValues = new int[] { 30, 40 },
            CompressionCodec = ParquetCompressionCodec.Snappy,
            CrcMode = FixtureCrcMode.Absent,
        });
        var observed = new List<int>();
        await using var file = await ParquetFile.OpenAsync(new MemoryStream(bytes, writable: false));
        await foreach (var batch in file.ScanAsync(new([file.Metadata.Schema.Columns[0]])))
        {
            using (batch)
                observed.AddRange(Assert.IsType<ParquetPrimitiveColumnBatch<int>>(batch.Columns[0]).Values.ToArray());
        }

        Assert.Equal([20, 10, 30, 40], observed);
    }

    [Fact]
    public async Task ValidV2SnappyChunkScans()
    {
        using var tracker = new PoolTracker();
        var bytes = ParquetFixtureBuilder.CreateInt32(new()
        {
            PhysicalTypeCode = (int)ParquetPhysicalType.Int64,
            PhysicalValues = new long[] { 10, -20, 30 },
            PageVersion = FixturePageVersion.DataPageV2,
            CompressionCodec = ParquetCompressionCodec.Snappy,
            CrcMode = FixtureCrcMode.Absent,
        });
        await using var file = await ParquetFile.OpenAsync(new MemoryStream(bytes, writable: false));
        await using var enumerator = file.ScanAsync(new([file.Metadata.Schema.Columns[0]])).GetAsyncEnumerator();
        Assert.True(await enumerator.MoveNextAsync());
        using (var batch = enumerator.Current)
            Assert.Equal([10L, -20L, 30L], Assert.IsType<ParquetPrimitiveColumnBatch<long>>(batch.Columns[0]).Values.ToArray());
        Assert.False(await enumerator.MoveNextAsync());
    }

    [Fact]
    public async Task DictionaryAfterDataRejectedBeforePayloadRead()
    {
        using var tracker = new PoolTracker();
        var bytes = ParquetFixtureBuilder.CreateInt32(new()
        {
            Values = [10],
            DictionaryValues = new int[] { 10 },
            DictionaryIndices = [0],
            DictionaryMode = FixtureDictionaryMode.AfterDataPage,
        });
        var source = new SizedCountingSource(bytes);
        await using var file = await ParquetFile.OpenAsync(source);
        source.Sizes.Clear();
        var exception = await Assert.ThrowsAsync<ParquetFormatException>(async () =>
        {
            await foreach (var batch in file.ScanAsync(new([file.Metadata.Schema.Columns[0]])))
                batch.Dispose();
        });
        Assert.Equal(0, exception.RowGroupOrdinal);
        Assert.Equal(0, exception.ColumnOrdinal);
        Assert.Equal(0, exception.PageOrdinal);
        Assert.Single(source.Sizes);
    }

    [Fact]
    public async Task SwappedDictionaryAndDataPagesRejectedAtFirstPage()
    {
        using var tracker = new PoolTracker();
        var valid = ParquetFixtureBuilder.CreateInt32(new()
        {
            Values = [10, 20],
            DictionaryValues = new int[] { 10, 20 },
            DictionaryIndices = [0, 1],
        });
        long dictionaryOffset;
        long dataOffset;
        await using (var probe = await ParquetFile.OpenAsync(new MemoryStream(valid, writable: false)))
        {
            var chunk = probe.Metadata.RowGroups[0].Columns[0];
            dictionaryOffset = chunk.DictionaryPageOffset ?? throw new InvalidOperationException("The generated fixture has no dictionary offset.");
            dataOffset = chunk.DataPageOffset;
        }

        Assert.True(dictionaryOffset < dataOffset);
        var footerLength = System.Buffers.Binary.BinaryPrimitives.ReadInt32LittleEndian(valid.AsSpan(valid.Length - 8));
        var footerStart = valid.Length - 8 - footerLength;
        var dictionaryLength = checked((int)(dataOffset - dictionaryOffset));
        var dataLength = checked((int)(footerStart - dataOffset));
        var swapped = new byte[valid.Length];
        valid.AsSpan(0, 4).CopyTo(swapped);
        valid.AsSpan((int)dataOffset, dataLength).CopyTo(swapped.AsSpan(4));
        valid.AsSpan((int)dictionaryOffset, dictionaryLength).CopyTo(swapped.AsSpan(4 + dataLength));
        valid.AsSpan(footerStart).CopyTo(swapped.AsSpan(footerStart));
        var source = new SizedCountingSource(swapped);
        await using var file = await ParquetFile.OpenAsync(source);
        source.Sizes.Clear();
        var exception = await Assert.ThrowsAsync<ParquetFormatException>(async () =>
        {
            await foreach (var batch in file.ScanAsync(new([file.Metadata.Schema.Columns[0]])))
                batch.Dispose();
        });
        Assert.Equal(0, exception.RowGroupOrdinal);
        Assert.Equal(0, exception.ColumnOrdinal);
        Assert.Equal(0, exception.PageOrdinal);
        Assert.Single(source.Sizes);
    }

    [Fact]
    public async Task AdvisoryRowGroupTotalDoesNotAffectScan()
    {
        using var tracker = new PoolTracker();
        var bytes = ParquetFixtureBuilder.CreateInt32(new() { Values = [10, -2, 42], RowGroupTotalByteSize = 1 });
        var observed = new List<int>();
        await using var file = await ParquetFile.OpenAsync(new MemoryStream(bytes, writable: false));
        Assert.Equal(1, file.Metadata.RowGroups[0].TotalByteSize);
        await foreach (var batch in file.ScanAsync(new([file.Metadata.Schema.Columns[0]])))
        {
            using (batch)
                observed.AddRange(Assert.IsType<ParquetPrimitiveColumnBatch<int>>(batch.Columns[0]).Values.ToArray());
        }

        Assert.Equal([10, -2, 42], observed);
    }

    [Fact]
    public async Task EmptyTrailingRowGroupScansWithoutTotalsFailure()
    {
        using var tracker = new PoolTracker();
        var bytes = ParquetFixtureBuilder.CreateRequiredInt32RowGroups([[1, 2], []]);
        var observed = new List<int>();
        await using var file = await ParquetFile.OpenAsync(new MemoryStream(bytes, writable: false));
        await foreach (var batch in file.ScanAsync(new([file.Metadata.Schema.Columns[0]])))
        {
            using (batch)
                observed.AddRange(Assert.IsType<ParquetPrimitiveColumnBatch<int>>(batch.Columns[0]).Values.ToArray());
        }

        Assert.Equal([1, 2], observed);
    }

    [Fact]
    public async Task EarlyDisposalSkipsTotalsEnforcement()
    {
        using var tracker = new PoolTracker();
        var bytes = ParquetFixtureBuilder.CreateInt32(new() { Values = [1], TrailingPlainValues = new int[] { 30 } });
        await using var file = await ParquetFile.OpenAsync(new MemoryStream(bytes, writable: false));
        await using var enumerator = file.ScanAsync(new([file.Metadata.Schema.Columns[0]], null, null, 1)).GetAsyncEnumerator();
        Assert.True(await enumerator.MoveNextAsync());
        using (var batch = enumerator.Current)
            Assert.Equal([1], Assert.IsType<ParquetPrimitiveColumnBatch<int>>(batch.Columns[0]).Values.ToArray());
    }

    [Fact]
    public async Task RowRangeSelectionStillReconcilesFullChunkTotal()
    {
        using var tracker = new PoolTracker();
        var bytes = ParquetFixtureBuilder.CreateInt32(new() { Values = [10, -2, 42], ChunkTotalUncompressedSize = 0 });
        await using var file = await ParquetFile.OpenAsync(new MemoryStream(bytes, writable: false));
        await Assert.ThrowsAsync<ParquetFormatException>(async () =>
        {
            await foreach (var batch in file.ScanAsync(new([file.Metadata.Schema.Columns[0]], null, new ParquetRowRange(1, 1), 64)))
                batch.Dispose();
        });
    }


    private sealed class SizedCountingSource(byte[] bytes) : IParquetRandomAccessSource
    {
        public readonly List<int> Sizes = [];
        public long Length => bytes.Length;
        public ValueTask ReadExactlyAsync(long offset, ArraySegment<byte> destination, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            Sizes.Add(destination.Count);
            bytes.AsMemory(checked((int)offset), destination.Count).CopyTo(destination.AsMemory());
            return ValueTask.CompletedTask;
        }

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }
}


