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

    [Theory]
    [InlineData("RequiredInt32Plain", typeof(int[]))]
    [InlineData("NullableInt32Plain", typeof(int?[]))]
    [InlineData("RequiredStringPlain", typeof(string?[]))]
    [InlineData("TwoRequiredInt32Plain", typeof(int[]))]
    public async Task BaselineDestinationsSelectBenchFixtureLayout(string workload, Type layout)
    {
        // B02: one retained buffer per projected column, chosen from the field.
        var fixture = await CreateFixtureAsync(workload, 64);
        var fields = await BaselineFieldsForFixtureAsync(fixture.Bytes);
        var destinations = CreateDestinations(fields, workload, 128);
        for (var column = 0; column < fields.Length; column++)
        {
            var values = DestinationValues(destinations, column);
            Assert.Equal(layout, values.GetType());
            Assert.Equal(128, values.Length);
        }
    }

    [Fact]
    public async Task BaselineDestinationsSelectRequiredInt64Layout()
    {
        var values = new long[64];
        for (var row = 0; row < values.Length; row++)
            values[row] = row * 1_000_003L + 7;
        var bytes = ParquetFixtureBuilder.CreateInt32(new()
        {
            PhysicalTypeCode = (int)ParquetPhysicalType.Int64,
            PhysicalValues = values,
        });
        var fields = await BaselineFieldsForFixtureAsync(bytes);
        var destinations = CreateDestinations(fields, "RequiredInt64Plain", 128);
        var buffer = DestinationValues(destinations, 0);
        Assert.Equal(typeof(long[]), buffer.GetType());
        Assert.Equal(128, buffer.Length);
    }

    [Fact]
    public async Task BaselineDestinationsSelectNullableFloatLayout()
    {
        const int rows = 64;
        var values = new float[rows];
        var validity = new bool[rows];
        for (var row = 0; row < rows; row++)
        {
            values[row] = row * 1.5f + 1;
            validity[row] = (row & 7) != 0;
        }
        var bytes = ParquetFixtureBuilder.CreateInt32(new()
        {
            PhysicalTypeCode = (int)ParquetPhysicalType.Float,
            PhysicalValues = values,
            Repetition = ParquetRepetition.Optional,
            Validity = validity,
        });
        var fields = await BaselineFieldsForFixtureAsync(bytes);
        var destinations = CreateDestinations(fields, "RequiredFloatPlain", 128);
        var buffer = DestinationValues(destinations, 0);
        Assert.Equal(typeof(float?[]), buffer.GetType());
        Assert.Equal(128, buffer.Length);
    }

    [Fact]
    public async Task BaselineDestinationsSelectNullableBooleanLayout()
    {
        const int rows = 64;
        var values = new bool[rows];
        var validity = new bool[rows];
        for (var row = 0; row < rows; row++)
        {
            values[row] = (row & 3) != 0;
            validity[row] = (row & 7) != 0;
        }
        var bytes = ParquetFixtureBuilder.CreateInt32(new()
        {
            PhysicalTypeCode = (int)ParquetPhysicalType.Boolean,
            PhysicalValues = values,
            Repetition = ParquetRepetition.Optional,
            Validity = validity,
        });
        var fields = await BaselineFieldsForFixtureAsync(bytes);
        var destinations = CreateDestinations(fields, "NullableBooleanPlain", 128);
        var buffer = DestinationValues(destinations, 0);
        Assert.Equal(typeof(bool?[]), buffer.GetType());
        Assert.Equal(128, buffer.Length);
    }

    [Fact]
    public async Task BaselineDestinationsSelectNullableBinaryLayout()
    {
        const int rows = 48;
        var payloads = new byte[rows][];
        var validity = new bool[rows];
        for (var row = 0; row < rows; row++)
        {
            payloads[row] = [(byte)(row & 255), (byte)((row >> 8) & 255)];
            validity[row] = row % 3 != 2;
        }
        var bytes = ParquetFixtureBuilder.CreateInt32(new()
        {
            PhysicalTypeCode = (int)ParquetPhysicalType.ByteArray,
            PhysicalValues = payloads,
            Repetition = ParquetRepetition.Optional,
            Validity = validity,
        });
        var fields = await BaselineFieldsForFixtureAsync(bytes);
        var destinations = CreateDestinations(fields, "NullableBinaryPlain", 128);
        var buffer = DestinationValues(destinations, 0);
        Assert.Equal(typeof(ReadOnlyMemory<byte>?[]), buffer.GetType());
        Assert.Equal(128, buffer.Length);
    }

    [Fact]
    public async Task BaselineDestinationsRetainSingleLayoutBuffer()
    {
        // B02: one required INT32 column retains a single row-group-sized int
        // buffer instead of twelve parallel layouts.
        const int rows = 65536;
        var fixture = await CreateFixtureAsync("RequiredInt32Plain", 64);
        var fields = await BaselineFieldsForFixtureAsync(fixture.Bytes);
        CreateDestinations(fields, "RequiredInt32Plain", rows);
        var before = GC.GetAllocatedBytesForCurrentThread();
        var destinations = CreateDestinations(fields, "RequiredInt32Plain", rows);
        var allocated = GC.GetAllocatedBytesForCurrentThread() - before;
        GC.KeepAlive(destinations);
        Assert.True(allocated < rows * sizeof(int) + 8192, "Baseline destinations allocated " + allocated + " bytes for one int buffer.");
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
        var assembly = BenchmarkReflection.BenchmarkAssembly();
        var snapshotCase = BenchmarkReflection.RequireType(assembly, "Lokad.Parquet.Benchmarks.WorkCensusCase");
        Assert.NotNull(snapshotCase.GetProperty("LokadLiveOwnedBytes"));
        Assert.NotNull(snapshotCase.GetProperty("BaselineLiveOwnedBytes"));
        Assert.NotNull(snapshotCase.GetProperty("LokadRetainedManagedBytes"));
        var measurement = BenchmarkReflection.RequireType(assembly, "Lokad.Parquet.Benchmarks.LiveSessionMeasurement");
        foreach (var property in new[] { "LiveOwnedBytes", "LiveOwnedProcessPrivateBytes", "PostDisposalBytes", "PostDisposalProcessPrivateBytes", "Checksum" })
            Assert.NotNull(measurement.GetProperty(property));
    }

    [Fact]
    public void PassesCarryInstrumentedRoles()
    {
        var assembly = BenchmarkReflection.BenchmarkAssembly();
        var pass = BenchmarkReflection.RequireType(assembly, "Lokad.Parquet.Benchmarks.CensusPassMeasurement");
        Assert.NotNull(pass.GetProperty("Role"));
    }


    private static Task<FixtureSnapshot> CreateInt32FixtureAsync(int rowCount) =>
        CreateFixtureAsync("RequiredInt32Plain", rowCount);

    private static async Task<FixtureSnapshot> CreateFixtureAsync(string workload, int rowCount)
    {
        var assembly = BenchmarkReflection.BenchmarkAssembly();
        var fixtureType = BenchmarkReflection.RequireType(assembly, "Lokad.Parquet.Benchmarks.ScanFixture");
        var workloadType = BenchmarkReflection.RequireType(assembly, "Lokad.Parquet.Benchmarks.ScanWorkload");
        var fixture = await BenchmarkReflection.InvokeAsync(assembly, "Lokad.Parquet.Benchmarks.ScanFixture", "CreateAsync", [Enum.Parse(workloadType, workload), rowCount]) ??
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
        var assembly = BenchmarkReflection.BenchmarkAssembly();
        var checksumType = BenchmarkReflection.RequireType(assembly, "Lokad.Parquet.Benchmarks.ScanChecksum");
        var create = BenchmarkReflection.RequireStaticMethod(checksumType, "CreateInt32", null);
        var mix = BenchmarkReflection.RequireStaticMethod(checksumType, "Mix", null);
        var combine = BenchmarkReflection.RequireStaticMethod(checksumType, "CombineColumn", null);
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
        var assembly = BenchmarkReflection.BenchmarkAssembly();
        var workloadType = BenchmarkReflection.RequireType(assembly, "Lokad.Parquet.Benchmarks.ScanWorkload");
        var hashes = rangeStart.HasValue ? new long[] { expectedChecksum } : fixture.ColumnChecksums;
        var nulls = rangeStart.HasValue ? new int[] { 0 } : fixture.NullCounts;
        var measurement = await BenchmarkReflection.InvokeAsync(assembly, "Lokad.Parquet.Benchmarks.LiveSessionRetention", "MeasureLokadAsync",
        [
            fixture.Bytes, projection, rowCount, rangeStart, rangeCount, emittedRows, 0,
            Enum.Parse(workloadType, workload), expectedChecksum,
            hashes, nulls, repetitions, caseName,
        ]) ??
            throw new InvalidOperationException("The benchmark live probe returned nothing.");
        return new MeasuredSession(measurement);
    }

    private static async Task<MeasuredSession> MeasureBaselineAsync(
        FixtureSnapshot fixture, string workload, long? rangeStart, long? rangeCount, int emittedRows, int rowCount, int repetitions, long expectedChecksum, string caseName, int[] projection)
    {
        var assembly = BenchmarkReflection.BenchmarkAssembly();
        var workloadType = BenchmarkReflection.RequireType(assembly, "Lokad.Parquet.Benchmarks.ScanWorkload");
        var measurement = await BenchmarkReflection.InvokeAsync(assembly, "Lokad.Parquet.Benchmarks.LiveSessionRetention", "MeasureBaselineAsync",
        [
            fixture.Bytes, projection, rangeStart, rangeCount, emittedRows, 0,
            Enum.Parse(workloadType, workload), expectedChecksum, repetitions, caseName,
        ]) ??
            throw new InvalidOperationException("The benchmark live probe returned nothing.");
        return new MeasuredSession(measurement);
    }

    private static async Task<long> ProjectedChecksumAsync(FixtureSnapshot fixture, int[] projection)
    {
        // The production projection oracle resolves the expected checksum for an
        // arbitrary projection, mirroring how the census derives retention truth.
        await using var file = await ParquetFile.OpenAsync(fixture.Bytes, new ParquetReaderOptions(), CancellationToken.None);
        var columns = file.Metadata.Schema.Columns;
        var assembly = BenchmarkReflection.BenchmarkAssembly();
        var runner = BenchmarkReflection.RequireType(assembly, "Lokad.Parquet.Benchmarks.WorkCensusRunner");
        var oracle = BenchmarkReflection.RequireStaticMethod(runner, "CensusExpectedChecksum", null);
        return Assert.IsType<long>(oracle.Invoke(null,
            [fixture.Checksum, fixture.ColumnChecksums, columns.Count, projection.Select(ordinal => columns[ordinal]).ToArray()]));
    }

    private static async Task<Array> BaselineFieldsForFixtureAsync(byte[] bytes)
    {
        // Baseline schema fields for one fixture, so destination selection is
        // pinned against the fields the baseline session actually reads.
        var assembly = BenchmarkReflection.BenchmarkAssembly();
        var binDirectory = Path.GetDirectoryName(assembly.Location) ?? throw new InvalidOperationException("The benchmark output has no directory.");
        var baseline = Assembly.LoadFrom(Path.Combine(binDirectory, "Parquet.dll"));
        var readerType = baseline.GetType("Parquet.ParquetReader") ?? throw new InvalidOperationException("The baseline reader is unavailable.");
        MethodInfo? create = null;
        foreach (var candidate in readerType.GetMethods(BindingFlags.Public | BindingFlags.Static))
        {
            var parameters = candidate.GetParameters();
            if (candidate.Name == "CreateAsync" && parameters.Length == 4 && parameters[0].ParameterType == typeof(Stream))
                create = candidate;
        }
        if (create is null)
            throw new InvalidOperationException("The baseline reader has no stream creation method.");
        using var stream = new MemoryStream(bytes, writable: false);
        var readerTask = (Task)(create.Invoke(null, new object?[] { stream, null, true, CancellationToken.None }) ?? throw new InvalidOperationException("The baseline reader creation returned nothing."));
        await readerTask.ConfigureAwait(false);
        var reader = readerTask.GetType().GetProperty("Result")?.GetValue(readerTask) ?? throw new InvalidOperationException("The baseline reader creation returned nothing.");
        try
        {
            var schema = reader.GetType().GetProperty("Schema")?.GetValue(reader) ?? throw new InvalidOperationException("The baseline reader has no schema.");
            var dataFields = schema.GetType().GetProperty("DataFields")?.GetValue(schema) ?? throw new InvalidOperationException("The baseline schema has no fields.");
            if (dataFields is Array fields)
                return fields;
            var items = ((System.Collections.IEnumerable)dataFields).Cast<object>().ToArray();
            if (items.Length == 0)
                throw new InvalidOperationException("The baseline schema has no fields.");
            var element = items[0]?.GetType() ?? throw new InvalidOperationException("A baseline field is null.");
            var typed = Array.CreateInstance(element, items.Length);
            for (var index = 0; index < items.Length; index++)
                typed.SetValue(items[index], index);
            return typed;
        }
        finally
        {
            if (reader is IAsyncDisposable asyncReader)
                await asyncReader.DisposeAsync().ConfigureAwait(false);
            else if (reader is IDisposable disposable)
                disposable.Dispose();
        }
    }

    private static object CreateDestinations(Array fields, string workload, int maximumGroupRows)
    {
        var assembly = BenchmarkReflection.BenchmarkAssembly();
        var destinationsType = BenchmarkReflection.RequireType(assembly, "Lokad.Parquet.Benchmarks.LiveSessionRetention+BaselineDestinations");
        var workloadType = BenchmarkReflection.RequireType(assembly, "Lokad.Parquet.Benchmarks.ScanWorkload");
        var create = BenchmarkReflection.RequireStaticMethod(destinationsType, "Create", null);
        return create.Invoke(null, [fields, Enum.Parse(workloadType, workload), maximumGroupRows]) ?? throw new InvalidOperationException("The benchmark destination allocation returned nothing.");
    }

    private static Array DestinationValues(object destinations, int column)
    {
        var table = destinations.GetType().GetProperty("Columns")?.GetValue(destinations) ?? throw new InvalidOperationException("The benchmark destinations have no columns.");
        var holder = ((Array)table).GetValue(column) ?? throw new InvalidOperationException("A benchmark destination column is missing.");
        var values = holder.GetType().GetProperty("Values")?.GetValue(holder) ?? throw new InvalidOperationException("A benchmark destination column has no buffer.");
        return Assert.IsAssignableFrom<Array>(values);
    }

    private sealed record FixtureSnapshot(byte[] Bytes, long Checksum, int RowCount, long[] ColumnChecksums, int[] NullCounts);

    private sealed class MeasuredSession(object measurement)
    {
        public long Checksum => Assert.IsType<long>(measurement.GetType().GetProperty("Checksum")?.GetValue(measurement));
    }
}



