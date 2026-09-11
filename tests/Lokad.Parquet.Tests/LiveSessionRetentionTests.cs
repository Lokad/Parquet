using System.Reflection;

namespace Lokad.Parquet.Tests;

public sealed class LiveSessionRetentionTests
{
    [Fact]
    public async Task LokadLiveProbeMatchesFullScanTruth()
    {
        var fixture = await CreateInt32FixtureAsync(64);
        var measurement = await MeasureLokadAsync(
            fixture, "RequiredInt32Plain", null, null, fixture.RowCount, fixture.RowCount, 2, fixture.Checksum, "FullScan", [0]);
        Assert.Equal(fixture.Checksum, measurement.Checksum);
    }

    [Fact]
    public async Task LokadLiveProbeHonorsRowRange()
    {
        var fixture = await CreateInt32FixtureAsync(64);
        var expected = RangeOracle(10, 20);
        var measurement = await MeasureLokadAsync(
            fixture, "RequiredInt32Plain", 10L, 20L, 20, fixture.RowCount, 2, expected, "RowRange", [0]);
        Assert.Equal(expected, measurement.Checksum);
        Assert.NotEqual(fixture.Checksum, measurement.Checksum);
    }

    [Fact]
    public async Task BaselineLiveProbeMatchesFullScanTruth()
    {
        var fixture = await CreateInt32FixtureAsync(64);
        var measurement = await MeasureBaselineAsync(
            fixture, "RequiredInt32Plain", null, null, fixture.RowCount, fixture.RowCount, 2, fixture.Checksum, "FullScan", [0]);
        Assert.Equal(fixture.Checksum, measurement.Checksum);
    }

    [Fact]
    public async Task BaselineLiveProbeHonorsRowRange()
    {
        var fixture = await CreateInt32FixtureAsync(64);
        var expected = RangeOracle(10, 20);
        var measurement = await MeasureBaselineAsync(
            fixture, "RequiredInt32Plain", 10L, 20L, 20, fixture.RowCount, 2, expected, "RowRange", [0]);
        Assert.Equal(expected, measurement.Checksum);
        Assert.NotEqual(fixture.Checksum, measurement.Checksum);
    }

    [Fact]
    public async Task LokadLiveProbeMatchesNarrowProjectionTruth()
    {
        // B01: the probes scan the first-pass projection, so a narrow projection
        // over a wide fixture checks the projected combination, not the full checksum.
        var fixture = await CreateFixtureAsync("EightRequiredInt32Plain", 64);
        var projection = new int[] { 0 };
        var expected = await ProjectedChecksumAsync(fixture, projection);
        Assert.NotEqual(fixture.Checksum, expected);
        var measurement = await MeasureLokadAsync(
            fixture, "EightRequiredInt32Plain", null, null, fixture.RowCount, fixture.RowCount, 2, expected, "NarrowProjection", projection);
        Assert.Equal(expected, measurement.Checksum);
    }

    [Fact]
    public async Task BaselineLiveProbeMatchesNarrowProjectionTruth()
    {
        var fixture = await CreateFixtureAsync("EightRequiredInt32Plain", 64);
        var projection = new int[] { 0 };
        var expected = await ProjectedChecksumAsync(fixture, projection);
        Assert.NotEqual(fixture.Checksum, expected);
        var measurement = await MeasureBaselineAsync(
            fixture, "EightRequiredInt32Plain", null, null, fixture.RowCount, fixture.RowCount, 2, expected, "NarrowProjection", projection);
        Assert.Equal(expected, measurement.Checksum);
    }

    [Fact]
    public async Task LokadLiveProbeMatchesReorderedProjectionTruth()
    {
        // Column combination is order-sensitive, so a reordered projection checks
        // the combination in scan order rather than the stored full checksum.
        var fixture = await CreateFixtureAsync("TwoRequiredInt32Plain", 64);
        var projection = new int[] { 1, 0 };
        var expected = await ProjectedChecksumAsync(fixture, projection);
        Assert.NotEqual(fixture.Checksum, expected);
        var measurement = await MeasureLokadAsync(
            fixture, "TwoRequiredInt32Plain", null, null, fixture.RowCount, fixture.RowCount, 2, expected, "ReorderedProjection", projection);
        Assert.Equal(expected, measurement.Checksum);
    }

    [Fact]
    public async Task BaselineLiveProbeMatchesReorderedProjectionTruth()
    {
        var fixture = await CreateFixtureAsync("TwoRequiredInt32Plain", 64);
        var projection = new int[] { 1, 0 };
        var expected = await ProjectedChecksumAsync(fixture, projection);
        Assert.NotEqual(fixture.Checksum, expected);
        var measurement = await MeasureBaselineAsync(
            fixture, "TwoRequiredInt32Plain", null, null, fixture.RowCount, fixture.RowCount, 2, expected, "ReorderedProjection", projection);
        Assert.Equal(expected, measurement.Checksum);
    }

    [Fact]
    public async Task LokadProbeFailureNamesCensusCase()
    {
        var fixture = await CreateInt32FixtureAsync(64);
        // Probing an async method through reflection surfaces the original failure,
        // so the census case name is asserted on the probe error itself.
        var failure = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await MeasureLokadAsync(
                fixture, "RequiredInt32Plain", null, null, fixture.RowCount, fixture.RowCount, 1, fixture.Checksum + 1, "NarrowInt32Plain", [0]));
        Assert.Contains("NarrowInt32Plain", failure.Message, StringComparison.Ordinal);
        Assert.NotNull(failure.InnerException);
    }

    [Fact]
    public async Task BaselineProbeFailureNamesCensusCase()
    {
        var fixture = await CreateInt32FixtureAsync(64);
        var failure = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await MeasureBaselineAsync(
                fixture, "RequiredInt32Plain", null, null, fixture.RowCount, fixture.RowCount, 1, fixture.Checksum + 1, "NarrowInt32Plain", [0]));
        Assert.Contains("NarrowInt32Plain", failure.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void SnapshotSeparatesLiveFromPostDisposal()
    {
        var assembly = BenchmarkAssembly();
        var snapshotCase = assembly.GetType("Lokad.Parquet.Benchmarks.WorkCensusCase") ??
            throw new InvalidOperationException("The benchmark census case is unavailable.");
        Assert.NotNull(snapshotCase.GetProperty("LokadLiveOwnedBytes"));
        Assert.NotNull(snapshotCase.GetProperty("BaselineLiveOwnedBytes"));
        Assert.NotNull(snapshotCase.GetProperty("LokadRetainedManagedBytes"));
        var measurement = assembly.GetType("Lokad.Parquet.Benchmarks.LiveSessionMeasurement") ??
            throw new InvalidOperationException("The benchmark live-session measurement is unavailable.");
        foreach (var property in new[] { "LiveOwnedBytes", "LiveOwnedProcessPrivateBytes", "PostDisposalBytes", "PostDisposalProcessPrivateBytes", "Checksum" })
            Assert.NotNull(measurement.GetProperty(property));
    }

    [Fact]
    public void PassesCarryInstrumentedRoles()
    {
        var assembly = BenchmarkAssembly();
        var pass = assembly.GetType("Lokad.Parquet.Benchmarks.CensusPassMeasurement") ??
            throw new InvalidOperationException("The benchmark pass measurement is unavailable.");
        Assert.NotNull(pass.GetProperty("Role"));
    }


    private static Task<FixtureSnapshot> CreateInt32FixtureAsync(int rowCount) =>
        CreateFixtureAsync("RequiredInt32Plain", rowCount);

    private static async Task<FixtureSnapshot> CreateFixtureAsync(string workload, int rowCount)
    {
        var assembly = BenchmarkAssembly();
        var fixtureType = assembly.GetType("Lokad.Parquet.Benchmarks.ScanFixture") ??
            throw new InvalidOperationException("The benchmark fixture writer is unavailable.");
        var workloadType = assembly.GetType("Lokad.Parquet.Benchmarks.ScanWorkload") ??
            throw new InvalidOperationException("The benchmark workload token is unavailable.");
        var create = fixtureType.GetMethod("CreateAsync", BindingFlags.NonPublic | BindingFlags.Static) ??
            throw new InvalidOperationException("The benchmark fixture writer has no creation method.");
        var task = (Task)(create.Invoke(null, [Enum.Parse(workloadType, workload), rowCount]) ?? throw new InvalidOperationException("The benchmark fixture creation returned nothing."));
        await task.ConfigureAwait(false);
        var fixture = task.GetType().GetProperty("Result")?.GetValue(task) ??
            throw new InvalidOperationException("The benchmark fixture creation returned nothing.");
        byte[] bytes = Assert.IsType<byte[]>(fixtureType.GetProperty("Bytes")?.GetValue(fixture));
        return new FixtureSnapshot(
            bytes,
            Assert.IsType<long>(fixtureType.GetProperty("Checksum")?.GetValue(fixture)),
            Assert.IsType<int>(fixtureType.GetProperty("RowCount")?.GetValue(fixture)),
            Assert.IsType<long[]>(fixtureType.GetProperty("ColumnChecksums")?.GetValue(fixture)),
            Assert.IsType<int[]>(fixtureType.GetProperty("NullCounts")?.GetValue(fixture)));
    }

    private static long RangeOracle(long start, long count)
    {
        var assembly = BenchmarkAssembly();
        var checksumType = assembly.GetType("Lokad.Parquet.Benchmarks.ScanChecksum") ??
            throw new InvalidOperationException("The benchmark checksum helper is unavailable.");
        var create = checksumType.GetMethod("CreateInt32", BindingFlags.NonPublic | BindingFlags.Static) ??
            throw new InvalidOperationException("The benchmark value mapper is unavailable.");
        var mix = checksumType.GetMethod("Mix", BindingFlags.NonPublic | BindingFlags.Static) ??
            throw new InvalidOperationException("The benchmark mixer is unavailable.");
        var combine = checksumType.GetMethod("CombineColumn", BindingFlags.NonPublic | BindingFlags.Static) ??
            throw new InvalidOperationException("The benchmark column combiner is unavailable.");
        var seed = Assert.IsType<long>(checksumType.GetField("Seed", BindingFlags.NonPublic | BindingFlags.Static)?.GetValue(null));
        var chain = seed;
        for (var row = start; row < start + count; row++)
        {
            var value = Assert.IsType<int>(create.Invoke(null, [(int)row]));
            chain = Assert.IsType<long>(mix.Invoke(null, [chain, value]));
        }

        return Assert.IsType<long>(combine.Invoke(null, [chain, seed]));
    }

    private static async Task<MeasuredSession> MeasureLokadAsync(
        FixtureSnapshot fixture, string workload, long? rangeStart, long? rangeCount, int emittedRows, int rowCount, int repetitions, long expectedChecksum, string caseName, int[] projection)
    {
        var assembly = BenchmarkAssembly();
        var retention = assembly.GetType("Lokad.Parquet.Benchmarks.LiveSessionRetention") ??
            throw new InvalidOperationException("The benchmark live-session probe is unavailable.");
        var workloadType = assembly.GetType("Lokad.Parquet.Benchmarks.ScanWorkload") ??
            throw new InvalidOperationException("The benchmark workload token is unavailable.");
        var measure = retention.GetMethod("MeasureLokadAsync", BindingFlags.Public | BindingFlags.Static) ??
            throw new InvalidOperationException("The benchmark Lokad live probe is unavailable.");
        var hashes = rangeStart.HasValue ? new long[] { expectedChecksum } : fixture.ColumnChecksums;
        var nulls = rangeStart.HasValue ? new int[] { 0 } : fixture.NullCounts;
        var task = (Task)(measure.Invoke(null,
        [
            fixture.Bytes, projection, rowCount, rangeStart, rangeCount, emittedRows, 0,
            Enum.Parse(workloadType, workload), expectedChecksum,
            hashes, nulls, repetitions, caseName,
        ]) ?? throw new InvalidOperationException("The benchmark live probe returned nothing."));
        await task.ConfigureAwait(false);
        var measurement = task.GetType().GetProperty("Result")?.GetValue(task) ??
            throw new InvalidOperationException("The benchmark live probe returned nothing.");
        return new MeasuredSession(measurement);
    }

    private static async Task<MeasuredSession> MeasureBaselineAsync(
        FixtureSnapshot fixture, string workload, long? rangeStart, long? rangeCount, int emittedRows, int rowCount, int repetitions, long expectedChecksum, string caseName, int[] projection)
    {
        var assembly = BenchmarkAssembly();
        var retention = assembly.GetType("Lokad.Parquet.Benchmarks.LiveSessionRetention") ??
            throw new InvalidOperationException("The benchmark live-session probe is unavailable.");
        var workloadType = assembly.GetType("Lokad.Parquet.Benchmarks.ScanWorkload") ??
            throw new InvalidOperationException("The benchmark workload token is unavailable.");
        var measure = retention.GetMethod("MeasureBaselineAsync", BindingFlags.Public | BindingFlags.Static) ??
            throw new InvalidOperationException("The benchmark baseline live probe is unavailable.");
        var task = (Task)(measure.Invoke(null,
        [
            fixture.Bytes, projection, rangeStart, rangeCount, emittedRows, 0,
            Enum.Parse(workloadType, workload), expectedChecksum, repetitions, caseName,
        ]) ?? throw new InvalidOperationException("The benchmark live probe returned nothing."));
        await task.ConfigureAwait(false);
        var measurement = task.GetType().GetProperty("Result")?.GetValue(task) ??
            throw new InvalidOperationException("The benchmark live probe returned nothing.");
        return new MeasuredSession(measurement);
    }

    private static async Task<long> ProjectedChecksumAsync(FixtureSnapshot fixture, int[] projection)
    {
        // The production projection oracle resolves the expected checksum for an
        // arbitrary projection, mirroring how the census derives retention truth.
        await using var file = await ParquetFile.OpenAsync(fixture.Bytes, new ParquetReaderOptions(), CancellationToken.None);
        var columns = file.Metadata.Schema.Columns;
        var assembly = BenchmarkAssembly();
        var runner = assembly.GetType("Lokad.Parquet.Benchmarks.WorkCensusRunner") ??
            throw new InvalidOperationException("The benchmark census runner is unavailable.");
        var oracle = runner.GetMethod("CensusExpectedChecksum", BindingFlags.NonPublic | BindingFlags.Static) ??
            throw new InvalidOperationException("The benchmark projection oracle is unavailable.");
        return Assert.IsType<long>(oracle.Invoke(null,
            [fixture.Checksum, fixture.ColumnChecksums, columns.Count, projection.Select(ordinal => columns[ordinal]).ToArray()]));
    }

    private static Assembly BenchmarkAssembly()
    {
        var testOutput = Path.GetDirectoryName(typeof(LiveSessionRetentionTests).Assembly.Location) ??
            throw new InvalidOperationException("The test assembly has no output directory.");
        var framework = Path.GetFileName(testOutput);
        var configuration = Directory.GetParent(testOutput)?.Name ??
            throw new InvalidOperationException("The test assembly has no configuration directory.");
        return Assembly.LoadFrom(Path.Combine(
            RepositoryTestPaths.Root,
            "bench",
            "Lokad.Parquet.Benchmarks",
            "bin",
            configuration,
            framework,
            "Lokad.Parquet.Benchmarks.dll"));
    }

    private sealed record FixtureSnapshot(byte[] Bytes, long Checksum, int RowCount, long[] ColumnChecksums, int[] NullCounts);

    private sealed class MeasuredSession(object measurement)
    {
        public long Checksum => Assert.IsType<long>(measurement.GetType().GetProperty("Checksum")?.GetValue(measurement));
    }
}



