namespace Lokad.Parquet.Tests;

// Guards batch-partition independence through decoded values: identical rows must
// surface no matter how pages split across batches. Checksum combination itself
// belongs to the benchmark project and is qualified there.
public sealed class MultiColumnPartitioningTests
{
    [Fact]
    public async Task EightBy65kElementWise()
    {
        const int cols = 8;
        const int rows = 65536;
        var bytes = BuildFixture(cols, rows);
        await using var file = await ParquetFile.OpenAsync(new MemoryStream(bytes, writable: false));
        var counts = new int[cols];
        var batches = 0;
        await foreach (var batch in file.ScanAsync(new(file.Metadata.Schema.Columns)))
        {
            batches++;
            using (batch)
            {
                Assert.Equal(cols, batch.Columns.Count);
                for (var c = 0; c < cols; c++)
                {
                    var column = Assert.IsType<ParquetPrimitiveColumnBatch<int>>(batch.Columns[c]);
                    Assert.True(column.Validity.IsAllValid);
                    var span = column.Values.Span;
                    Assert.Equal(batch.RowCount, span.Length);
                    var baseRow = (int)batch.RowOffset;
                    for (var i = 0; i < span.Length; i++)
                    {
                        var expected = checked(c * 1000000 + baseRow + i);
                        if (span[i] != expected)
                            Assert.Fail($"Mismatch col {c} global row {baseRow + i}: expected {expected}, got {span[i]} (batch {batches}, batchOffset {i}).");
                    }
                    counts[c] += span.Length;
                }
            }
        }
        for (var c = 0; c < cols; c++)
            Assert.Equal(rows, counts[c]);
    }

    [Theory]
    [InlineData(2048)]
    [InlineData(512)]
    [InlineData(7)]
    public async Task ValuesDoNotDependOnBatchPartitioning(int target)
    {
        const int cols = 8;
        const int rows = 2048;
        var bytes = BuildFixture(cols, rows);
        await using var file = await ParquetFile.OpenAsync(new MemoryStream(bytes, writable: false));
        var perColumn = new List<int>[cols];
        for (var c = 0; c < cols; c++)
            perColumn[c] = [];
        var totalRows = 0;
        var batches = 0;
        await foreach (var batch in file.ScanAsync(new(file.Metadata.Schema.Columns, null, null, target)))
        {
            using (batch)
            {
                batches++;
                Assert.Equal(cols, batch.Columns.Count);
                totalRows += batch.RowCount;
                for (var c = 0; c < cols; c++)
                {
                    var column = Assert.IsType<ParquetPrimitiveColumnBatch<int>>(batch.Columns[c]);
                    perColumn[c].AddRange(column.Values.ToArray());
                }
            }
        }

        Assert.Equal(rows, totalRows);
        if (target == 7)
        {
            Assert.True(batches > 1);
        }

        for (var c = 0; c < cols; c++)
        {
            var expected = new int[rows];
            for (var r = 0; r < rows; r++)
                expected[r] = checked(c * 1000000 + r);
            Assert.Equal(expected, perColumn[c]);
        }
    }

    private static byte[] BuildFixture(int cols, int rows)
    {
        var columns = new RequiredInt32FixtureColumn[cols];
        for (var c = 0; c < cols; c++)
        {
            var values = new int[rows];
            for (var r = 0; r < rows; r++)
                values[r] = checked(c * 1000000 + r);
            columns[c] = new RequiredInt32FixtureColumn { Name = $"c{c}", Pages = [values] };
        }
        return ParquetFixtureBuilder.CreateRequiredInt32Columns(columns);
    }

}
