namespace Lokad.Parquet.Tests;

public sealed class BenchmarkTruthPartitioningTests
{
    [Fact]
    public async Task MultiColumnTruthDoesNotDependOnBatchPartitioning()
    {
        var left = new[] { 1, 2, 3, 4, 5 };
        var right = new[] { 10, 20, 30, 40, 50 };
        var bytes = ParquetFixtureBuilder.CreateRequiredInt32Columns(
        [
            new RequiredInt32FixtureColumn { Name = "left", Pages = [[1, 2], [3, 4, 5]] },
            new RequiredInt32FixtureColumn { Name = "right", Pages = [[10], [20, 30, 40, 50]] },
        ]);
        var expected = CombineColumns([ChecksumColumn(left), ChecksumColumn(right)]);
        foreach (var target in new[] { 1, 2, 5 })
        {
            var (checksums, batches) = await ScanColumnChecksums(bytes, target, 2);
            if (target == 1)
            {
                Assert.True(batches > 1);
            }

            Assert.Equal(expected, CombineColumns(checksums));
        }
    }

    [Fact]
    public async Task SingleColumnTruthDoesNotDependOnBatchPartitioning()
    {
        var values = new[] { 7, 8, 9, 10 };
        var bytes = ParquetFixtureBuilder.CreateRequiredInt32Columns(
        [
            new RequiredInt32FixtureColumn { Name = "only", Pages = [[7, 8], [9, 10]] },
        ]);
        // A single projected column uses the single-column scan path.
        var expected = ChecksumColumn(values);
        foreach (var target in new[] { 1, 4 })
        {
            var (checksums, batches) = await ScanColumnChecksums(bytes, target, 1);
            if (target == 1)
            {
                Assert.True(batches > 1);
            }

            Assert.Equal(expected, CombineColumns(checksums));
        }
    }

    // Mirrors the ScanChecksum endpoint definition owned by the benchmark project;
    // the per-column chain plus commutative combination is the contract under test.
    private static long ChecksumColumn(int[] values)
    {
        const long seed = 1_469_598_103_934_665_603L;
        var checksum = seed;
        foreach (var value in values)
            checksum = unchecked((checksum * 1_099_511_628_211L) ^ value);
        return checksum;
    }

    private static long CombineColumns(long[] columnChecksums)
    {
        const long seed = 1_469_598_103_934_665_603L;
        if (columnChecksums.Length == 1)
            return columnChecksums[0];
        var combined = seed;
        for (var column = 0; column < columnChecksums.Length; column++)
            combined ^= unchecked((columnChecksums[column] * 1_099_511_628_211L) ^ column);
        return combined;
    }

    private static async Task<(long[] Checksums, int Batches)> ScanColumnChecksums(byte[] bytes, int target, int columnCount)
    {
        using var tracker = new PoolTracker();
        const long seed = 1_469_598_103_934_665_603L;
        var checksums = new long[columnCount];
        Array.Fill(checksums, seed);
        var batches = 0;
        await using (var file = await ParquetFile.OpenAsync(new MemoryStream(bytes, writable: false)))
        {
            var projection = file.Metadata.Schema.Columns.Take(columnCount).ToArray();
            await foreach (var batch in file.ScanAsync(new(projection, null, null, target)))
            {
                using (batch)
                {
                    batches++;
                    for (var column = 0; column < batch.Columns.Count; column++)
                    {
                        var values = Assert.IsType<ParquetPrimitiveColumnBatch<int>>(batch.Columns[column]).Values.Span;
                        foreach (var value in values)
                            checksums[column] = unchecked((checksums[column] * 1_099_511_628_211L) ^ value);
                    }
                }
            }
        }

        return (checksums, batches);
    }
}
