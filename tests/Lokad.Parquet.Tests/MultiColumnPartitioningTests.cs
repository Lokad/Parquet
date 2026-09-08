namespace Lokad.Parquet.Tests;

// Guards the canonical multi-column truth contract: per-column value order
// plus a commutative combination must not depend on how pages batch across
// columns. The checksum below reimplements that contract independently
// (per-column Mix chains combined commutatively); it shares no code with the
// benchmark consumers it protects.
public sealed class MultiColumnPartitioningTests
{
    private const long Seed = 1_469_598_103_934_665_603L;

    private static long Mix(long checksum, int value) =>
        unchecked((checksum * 1_099_511_628_211L) ^ value);

    private static long CombineColumns(long[] columnChecksums)
    {
        if (columnChecksums.Length == 1)
            return columnChecksums[0];
        var combined = Seed;
        for (var column = 0; column < columnChecksums.Length; column++)
            combined ^= Mix(columnChecksums[column], column);
        return combined;
    }

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
    public async Task CanonicalChecksumIsBatchPartitionIndependent(int target)
    {
        const int cols = 8;
        const int rows = 2048;
        var bytes = BuildFixture(cols, rows);
        await using var file = await ParquetFile.OpenAsync(new MemoryStream(bytes, writable: false));
        var perColumn = new long[cols];
        Array.Fill(perColumn, Seed);
        var totalRows = 0;
        await foreach (var batch in file.ScanAsync(new(file.Metadata.Schema.Columns, null, null, target)))
        {
            using (batch)
            {
                Assert.Equal(cols, batch.Columns.Count);
                totalRows += batch.RowCount;
                for (var c = 0; c < cols; c++)
                {
                    var column = Assert.IsType<ParquetPrimitiveColumnBatch<int>>(batch.Columns[c]);
                    foreach (var value in column.Values.Span)
                        perColumn[c] = Mix(perColumn[c], value);
                }
            }
        }
        Assert.Equal(rows, totalRows);
        var combined = CombineColumns(perColumn);
        Assert.Equal(ExpectedCombined(), combined);

        long ExpectedCombined()
        {
            var expected = new long[cols];
            for (var c = 0; c < cols; c++)
            {
                var checksum = Seed;
                for (var r = 0; r < rows; r++)
                    checksum = Mix(checksum, checked(c * 1000000 + r));
                expected[c] = checksum;
            }
            return CombineColumns(expected);
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
