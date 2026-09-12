using Xunit.Abstractions;

namespace Lokad.Parquet.Tests;

// Bookkeeping-allocation instrument for small and misaligned batches: each fact
// warms the shared pool, then measures process-wide allocated bytes over
// sequential scans and reports exact bytes per batch for before/after
// comparison. The ceiling guards regressions in the full suite; exact figures
// come from isolated runs of this class.
[CollectionDefinition("Scan batch allocation", DisableParallelization = true)]
public sealed class ScanBatchAllocationCollection { }

[Collection("Scan batch allocation")]
public sealed class ScanBatchAllocationTests
{
    private readonly ITestOutputHelper _output;

    public ScanBatchAllocationTests(ITestOutputHelper output) => _output = output;

    [Fact]
    public async Task SmallTargetInt32BatchesStayBounded()
    {
        var values = new int[65536];
        for (var row = 0; row < values.Length; row++)
            values[row] = row * 3 + 1;
        var bytes = ParquetFixtureBuilder.CreateInt32(new() { Values = values });
        await MeasureBatches(bytes, new[] { 0 }, 128, 1_000, "small-target-int32");
    }

    [Fact]
    public async Task MisalignedTwoColumnBatchesStayBounded()
    {
        var leftPages = new List<int[]>();
        var rightPages = new List<int[]>();
        for (var group = 0; group < 10; group++)
        {
            leftPages.Add(Enumerable.Range(group * 2000, 1000).Select(static row => row * 3 + 1).ToArray());
            leftPages.Add(Enumerable.Range(group * 2000 + 1000, 1000).Select(static row => row * 3 + 1).ToArray());
            rightPages.Add(Enumerable.Range(group * 2000, 1500).Select(static row => 100000 + row).ToArray());
            rightPages.Add(Enumerable.Range(group * 2000 + 1500, 500).Select(static row => 100000 + row).ToArray());
        }
        var bytes = ParquetFixtureBuilder.CreateRequiredInt32Columns(
        [
            new RequiredInt32FixtureColumn { Name = "left", Pages = leftPages.ToArray() },
            new RequiredInt32FixtureColumn { Name = "right", Pages = rightPages.ToArray() },
        ]);
        await MeasureBatches(bytes, new[] { 0, 1 }, 128, 1_200, "misaligned-two-int32");
    }

    [Fact]
    public async Task SingleRowInt32BatchesStayBounded()
    {
        var values = new int[4096];
        for (var row = 0; row < values.Length; row++)
            values[row] = row * 3 + 1;
        var bytes = ParquetFixtureBuilder.CreateInt32(new() { Values = values });
        await MeasureBatches(bytes, new[] { 0 }, 1, 1_000, "single-row-int32");
    }

    private async Task MeasureBatches(byte[] fixtureBytes, int[] ordinals, int target, long ceilingBytesPerBatch, string label)
    {
        for (var warmup = 0; warmup < 3; warmup++)
            await ScanBatches();
        var best = long.MaxValue;
        for (var repetition = 0; repetition < 6; repetition++)
        {
            var before = GC.GetTotalAllocatedBytes(true);
            var batches = await ScanBatches();
            var bytesPerBatch = (GC.GetTotalAllocatedBytes(true) - before) / Math.Max(batches, 1L);
            _output.WriteLine($"{label}: pass {repetition} allocated {bytesPerBatch} bytes per batch over {batches} batches.");
            Assert.True(batches > 0);
            Assert.True(bytesPerBatch <= ceilingBytesPerBatch);
            best = Math.Min(best, bytesPerBatch);
        }

        _output.WriteLine($"{label}: best {best} bytes per batch.");

        async Task<long> ScanBatches()
        {
            await using var file = await ParquetFile.OpenAsync(fixtureBytes, new ParquetReaderOptions(), CancellationToken.None);
            var projection = new ParquetColumn[ordinals.Length];
            for (var index = 0; index < ordinals.Length; index++)
                projection[index] = file.Metadata.Schema.Columns[ordinals[index]];
            var batches = 0L;
            await foreach (var batch in file.ScanAsync(new(projection, null, null, target)))
            {
                batches++;
                batch.Dispose();
            }

            return batches;
        }
    }
}





