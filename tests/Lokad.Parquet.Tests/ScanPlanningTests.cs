namespace Lokad.Parquet.Tests;

public sealed class ScanPlanningTests
{
    [Fact]
    public async Task EmptyRowGroupSelectionYieldsNothingWithoutPayloadReads()
    {
        var bytes = ParquetFixtureBuilder.CreateInt32(new() { Values = [1, 2, 3] });
        var source = new CountingSource(bytes);
        await using var file = await ParquetFile.OpenAsync(source);
        var readsAfterOpen = source.Reads;
        var batches = 0;
        await foreach (var batch in file.ScanAsync(new([file.Metadata.Schema.Columns[0]], Array.Empty<ParquetRowGroup>(), null, 64)))
        {
            batch.Dispose();
            batches++;
        }

        Assert.Equal(0, batches);
        Assert.Equal(readsAfterOpen, source.Reads);
    }

    [Fact]
    public async Task EmptyRowRangeYieldsNothingWithoutPayloadReads()
    {
        var bytes = ParquetFixtureBuilder.CreateInt32(new() { Values = [1, 2, 3] });
        var source = new CountingSource(bytes);
        await using var file = await ParquetFile.OpenAsync(source);
        var readsAfterOpen = source.Reads;
        var batches = 0;
        await foreach (var batch in file.ScanAsync(new([file.Metadata.Schema.Columns[0]], null, new ParquetRowRange(1, 0), 64)))
        {
            batch.Dispose();
            batches++;
        }

        Assert.Equal(0, batches);
        Assert.Equal(readsAfterOpen, source.Reads);
    }

    [Fact]
    public async Task TinyRangeOverManyRowGroupsSelectsOnlyIntersectingGroups()
    {
        var bytes = ParquetFixtureBuilder.CreateRequiredInt32RowGroups([[10, 11], [20, 21], [30, 31], [40, 41], [50, 51], [60, 61], [70, 71], [80, 81]]);
        await using var file = await ParquetFile.OpenAsync(new MemoryStream(bytes, writable: false));
        var offsets = new List<long>();
        var groups = new List<int>();
        var values = new List<int>();
        await foreach (var batch in file.ScanAsync(new([file.Metadata.Schema.Columns[0]], null, new ParquetRowRange(5, 2), 64)))
        {
            using (batch)
            {
                offsets.Add(batch.RowOffset);
                groups.Add(batch.RowGroupOrdinal);
                values.AddRange(Assert.IsType<ParquetPrimitiveColumnBatch<int>>(batch.Columns[0]).Values.ToArray());
            }
        }

        Assert.Equal([5L, 6L], offsets);
        Assert.Equal([2, 3], groups);
        Assert.Equal([31, 40], values);
    }

    [Fact]
    public async Task WideProjectionDuplicateIsRejectedBeforePayloadReads()
    {
        var columns = new RequiredInt32FixtureColumn[2000];
        for (var i = 0; i < columns.Length; i++)
        {
            columns[i] = new RequiredInt32FixtureColumn { Name = "c" + i, Pages = [[i]] };
        }

        var bytes = ParquetFixtureBuilder.CreateRequiredInt32Columns(columns);
        var source = new CountingSource(bytes);
        await using var file = await ParquetFile.OpenAsync(source);
        var readsAfterOpen = source.Reads;
        var projection = new List<ParquetColumn>(file.Metadata.Schema.Columns);
        projection.Add(file.Metadata.Schema.Columns[0]);
        Assert.Throws<ArgumentException>(() => file.ScanAsync(new ParquetScanOptions(projection)).GetAsyncEnumerator());
        Assert.Equal(readsAfterOpen, source.Reads);
    }

    [Fact]
    public async Task WideFileNarrowProjectionPreservesOrderAndIdentity()
    {
        var columns = new RequiredInt32FixtureColumn[2000];
        for (var i = 0; i < columns.Length; i++)
        {
            columns[i] = new RequiredInt32FixtureColumn { Name = "c" + i, Pages = [[i]] };
        }

        var bytes = ParquetFixtureBuilder.CreateRequiredInt32Columns(columns);
        await using var file = await ParquetFile.OpenAsync(new MemoryStream(bytes, writable: false));
        var selected = new[] { file.Metadata.Schema.Columns[1999], file.Metadata.Schema.Columns[0], file.Metadata.Schema.Columns[1000] };
        await using var enumerator = file.ScanAsync(new ParquetScanOptions(selected)).GetAsyncEnumerator();
        Assert.True(await enumerator.MoveNextAsync());
        using (var batch = enumerator.Current)
        {
            Assert.Equal(1, batch.RowCount);
            Assert.Same(selected[0], batch.Columns[0].Column);
            Assert.Same(selected[1], batch.Columns[1].Column);
            Assert.Same(selected[2], batch.Columns[2].Column);
            Assert.Equal([1999], Assert.IsType<ParquetPrimitiveColumnBatch<int>>(batch.Columns[0]).Values.ToArray());
            Assert.Equal([0], Assert.IsType<ParquetPrimitiveColumnBatch<int>>(batch.Columns[1]).Values.ToArray());
            Assert.Equal([1000], Assert.IsType<ParquetPrimitiveColumnBatch<int>>(batch.Columns[2]).Values.ToArray());
        }

        Assert.False(await enumerator.MoveNextAsync());
    }

    private sealed class CountingSource : IParquetRandomAccessSource
    {
        private readonly byte[] _bytes;
        public CountingSource(byte[] bytes) => _bytes = bytes;
        public int Reads { get; private set; }
        public long Length => _bytes.Length;
        public ValueTask ReadExactlyAsync(long offset, ArraySegment<byte> destination, CancellationToken cancellationToken)
        {
            Reads++;
            _bytes.AsMemory((int)offset, destination.Count).CopyTo(destination.AsMemory());
            return ValueTask.CompletedTask;
        }

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }
}
