namespace Lokad.Parquet.Tests;

public sealed class ParserAllocationTests
{
    [Fact]
    public async Task SixteenColumnSchemaOpenStaysBounded()
    {
        var columns = new RequiredInt32FixtureColumn[16];
        for (var i = 0; i < columns.Length; i++)
            columns[i] = new RequiredInt32FixtureColumn { Name = $"c{i:00}", Pages = [[1, 2, 3]] };
        var bytes = ParquetFixtureBuilder.CreateRequiredInt32Columns(columns);

        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();
        var before = GC.GetAllocatedBytesForCurrentThread();
        await using var file = await ParquetFile.OpenAsync(new MemoryStream(bytes, writable: false));
        var allocated = GC.GetAllocatedBytesForCurrentThread() - before;

        Assert.Equal(16, file.Metadata.Schema.Columns.Count);
        Assert.True(allocated < 65536, $"Sixteen-column schema open allocated {allocated} bytes.");
    }

    [Fact]
    public async Task ManySmallPagesScanStaysBounded()
    {
        var pages = new int[16][];
        for (var i = 0; i < pages.Length; i++)
            pages[i] = [i + 1];
        var bytes = ParquetFixtureBuilder.CreateRequiredInt32Columns(
        [
            new RequiredInt32FixtureColumn { Name = "value", Pages = pages },
        ]);
        await using var file = await ParquetFile.OpenAsync(new MemoryStream(bytes, writable: false));
        var values = new List<int>();
        await foreach (var batch in file.ScanAsync(new([file.Metadata.Schema.Columns[0]], null, null, 1)))
        {
            using (batch)
                values.AddRange(Assert.IsType<ParquetPrimitiveColumnBatch<int>>(batch.Columns[0]).Values.ToArray());
        }

        Assert.Equal(Enumerable.Range(1, 16), values);

        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();
        var before = GC.GetAllocatedBytesForCurrentThread();
        var batchCount = 0;
        await foreach (var batch in file.ScanAsync(new([file.Metadata.Schema.Columns[0]], null, null, 1)))
        {
            batchCount++;
            batch.Dispose();
        }

        var allocated = GC.GetAllocatedBytesForCurrentThread() - before;
        Assert.Equal(16, batchCount);
        var perBatch = (double)allocated / batchCount;
        Assert.True(perBatch < 2048, $"Small-page scan allocated {allocated} bytes for {batchCount} batches ({perBatch:F1} B/batch).");
    }
}
