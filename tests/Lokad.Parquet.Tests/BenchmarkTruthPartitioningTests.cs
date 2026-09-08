namespace Lokad.Parquet.Tests;

// Guards batch-partition independence through decoded values rather than a
// checksum replica: identical rows must surface no matter how pages split
// across batches. Checksum combination itself belongs to the benchmark
// project and is qualified there.
public sealed class BenchmarkTruthPartitioningTests
{
    [Fact]
    public async Task MultiColumnValuesDoNotDependOnBatchPartitioning()
    {
        var left = new[] { 1, 2, 3, 4, 5 };
        var right = new[] { 10, 20, 30, 40, 50 };
        var bytes = ParquetFixtureBuilder.CreateRequiredInt32Columns(
        [
            new RequiredInt32FixtureColumn { Name = "left", Pages = [[1, 2], [3, 4, 5]] },
            new RequiredInt32FixtureColumn { Name = "right", Pages = [[10], [20, 30, 40, 50]] },
        ]);
        var whole = await ScanColumnValues(bytes, 5, 2);
        Assert.Equal([left, right], whole.Columns);
        foreach (var target in new[] { 1, 2 })
        {
            var partitioned = await ScanColumnValues(bytes, target, 2);
            Assert.True(partitioned.BatchCount > 1);
            Assert.Equal(whole.Columns, partitioned.Columns);
        }
    }

    [Fact]
    public async Task SingleColumnValuesDoNotDependOnBatchPartitioning()
    {
        var bytes = ParquetFixtureBuilder.CreateRequiredInt32Columns(
        [
            new RequiredInt32FixtureColumn { Name = "only", Pages = [[7, 8], [9, 10]] },
        ]);
        // A single projected column uses the single-column scan path.
        var whole = await ScanColumnValues(bytes, 4, 1);
        Assert.Equal([[7, 8, 9, 10]], whole.Columns);
        var partitioned = await ScanColumnValues(bytes, 1, 1);
        Assert.True(partitioned.BatchCount > 1);
        Assert.Equal(whole.Columns, partitioned.Columns);
    }

    private static async Task<(int[][] Columns, int BatchCount)> ScanColumnValues(byte[] bytes, int target, int columnCount)
    {
        using var tracker = new PoolTracker();
        var columns = new List<int>[columnCount];
        for (var i = 0; i < columns.Length; i++)
            columns[i] = [];
        var batches = 0;
        await using (var file = await ParquetFile.OpenAsync(new MemoryStream(bytes, writable: false)))
        {
            var projection = file.Metadata.Schema.Columns.Take(columnCount).ToArray();
            await foreach (var batch in file.ScanAsync(new(projection, null, null, target)))
            {
                using (batch)
                {
                    batches++;
                    Assert.Equal(columnCount, batch.Columns.Count);
                    for (var column = 0; column < batch.Columns.Count; column++)
                        columns[column].AddRange(Assert.IsType<ParquetPrimitiveColumnBatch<int>>(batch.Columns[column]).Values.ToArray());
                }
            }
        }

        return (columns.Select(static column => column.ToArray()).ToArray(), batches);
    }
}
