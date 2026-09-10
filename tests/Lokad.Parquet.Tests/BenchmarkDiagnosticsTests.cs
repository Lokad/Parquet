using System.Reflection;

namespace Lokad.Parquet.Tests;

// Diagnostic census-lane coverage: the new RunCensusPassAsync consumer branches
// (Data Page V2, nullable variable-width binary, wider required primitives) and
// the committed-fixture truth oracle with its pinned hashes. Expected checksums
// fold through the same bench ScanChecksum entry points the runner uses, so these
// facts guard lane wiring and fail-closed truth checks; decoded-value truth stays
// with the reader tests over the same fixture shapes.
public sealed class BenchmarkDiagnosticsTests
{
    [Fact]
    public async Task NullableV2PassFoldsValuesAndNulls()
    {
        const int rows = 64;
        var values = new int[rows];
        var validity = new bool[rows];
        for (var row = 0; row < rows; row++)
        {
            values[row] = row * 3 + 1;
            validity[row] = (row & 7) != 0;
        }

        var bytes = ParquetFixtureBuilder.CreateInt32(new()
        {
            Values = values,
            Repetition = ParquetRepetition.Optional,
            Validity = validity,
            PageVersion = FixturePageVersion.DataPageV2,
        });
        var assembly = BenchmarkAssembly();
        var checksumType = ChecksumType(assembly);
        var mix = ChecksumFoldMethod(checksumType, "Mix", typeof(int));
        var seed = Assert.IsType<long>(checksumType.GetField("Seed", BindingFlags.NonPublic | BindingFlags.Static)?.GetValue(null));
        var valueChain = seed;
        var nullChain = seed;
        var nullCount = 0;
        for (var row = 0; row < rows; row++)
        {
            if (validity[row])
                valueChain = Assert.IsType<long>(mix.Invoke(null, [valueChain, values[row]]));
            else
            {
                nullChain = Assert.IsType<long>(mix.Invoke(null, [nullChain, row]));
                nullCount++;
            }
        }

        var folded = CombineColumn(checksumType, valueChain, nullChain);
        await using var file = await ParquetFile.OpenAsync(new MemoryStream(bytes, writable: false));
        var outcome = await RunCensusPassAsync(assembly, file, "NullableInt32V2", rows, 0, [file.Metadata.Schema.Columns[0]], rows, folded, [folded], [nullCount]);
        Assert.Equal(folded, outcome.Checksum);
        Assert.True(outcome.PassBatches > 0);
    }

    [Fact]
    public async Task NullableBinaryPassFoldsPayloadBytes()
    {
        const int rows = 48;
        var payloads = new byte[rows][];
        var validity = new bool[rows];
        for (var row = 0; row < rows; row++)
        {
            if (row % 3 == 2)
            {
                payloads[row] = [];
                validity[row] = false;
            }
            else
            {
                payloads[row] = [(byte)(row & 255), (byte)((row >> 8) & 255)];
                validity[row] = true;
            }
        }

        var bytes = ParquetFixtureBuilder.CreateInt32(new()
        {
            PhysicalTypeCode = (int)ParquetPhysicalType.ByteArray,
            PhysicalValues = payloads,
            Repetition = ParquetRepetition.Optional,
            Validity = validity,
        });
        var assembly = BenchmarkAssembly();
        var checksumType = ChecksumType(assembly);
        var mixBytes = ChecksumFoldMethod(checksumType, "MixBytes", typeof(byte[]));
        var mix = ChecksumFoldMethod(checksumType, "Mix", typeof(int));
        var seed = Assert.IsType<long>(checksumType.GetField("Seed", BindingFlags.NonPublic | BindingFlags.Static)?.GetValue(null));
        var valueChain = seed;
        var nullChain = seed;
        var nullCount = 0;
        for (var row = 0; row < rows; row++)
        {
            if (validity[row])
                valueChain = Assert.IsType<long>(mixBytes.Invoke(null, [valueChain, payloads[row]]));
            else
            {
                nullChain = Assert.IsType<long>(mix.Invoke(null, [nullChain, row]));
                nullCount++;
            }
        }

        var folded = CombineColumn(checksumType, valueChain, nullChain);
        await using var file = await ParquetFile.OpenAsync(new MemoryStream(bytes, writable: false));
        var outcome = await RunCensusPassAsync(assembly, file, "NullableBinaryPlain", rows, 0, [file.Metadata.Schema.Columns[0]], rows, folded, [folded], [nullCount]);
        Assert.Equal(folded, outcome.Checksum);
        Assert.True(outcome.PassBatches > 0);
    }

    [Fact]
    public async Task RequiredInt64PassFoldsWiderValues()
    {
        const int rows = 64;
        var values = new long[rows];
        for (var row = 0; row < rows; row++)
            values[row] = row * 1_000_003L + 7;
        var bytes = ParquetFixtureBuilder.CreateInt32(new()
        {
            PhysicalTypeCode = (int)ParquetPhysicalType.Int64,
            PhysicalValues = values,
        });
        var assembly = BenchmarkAssembly();
        var checksumType = ChecksumType(assembly);
        var mixInt64 = ChecksumFoldMethod(checksumType, "MixInt64", typeof(long));
        var seed = Assert.IsType<long>(checksumType.GetField("Seed", BindingFlags.NonPublic | BindingFlags.Static)?.GetValue(null));
        var valueChain = seed;
        for (var row = 0; row < rows; row++)
            valueChain = Assert.IsType<long>(mixInt64.Invoke(null, [valueChain, values[row]]));
        var folded = CombineColumn(checksumType, valueChain, seed);
        await using var file = await ParquetFile.OpenAsync(new MemoryStream(bytes, writable: false));
        var outcome = await RunCensusPassAsync(assembly, file, "RequiredInt64Plain", rows, 0, [file.Metadata.Schema.Columns[0]], rows, folded, [folded], [0]);
        Assert.Equal(folded, outcome.Checksum);
        Assert.True(outcome.PassBatches > 0);
    }

    [Fact]
    public async Task CensusPassRejectsMismatchedChecksum()
    {
        var bytes = ParquetFixtureBuilder.CreateInt32(new() { Values = [1, 2, 3] });
        var assembly = BenchmarkAssembly();
        await using var file = await ParquetFile.OpenAsync(new MemoryStream(bytes, writable: false));
        var exception = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await RunCensusPassAsync(assembly, file, "RequiredInt32Plain", 3, 0, [file.Metadata.Schema.Columns[0]], 3, 424242L, [424242L], [0]));
        Assert.Contains("truth check", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task CommittedCrcLaneEstablishesPinnedTruth()
    {
        var bytes = await File.ReadAllBytesAsync(CommittedFixturePath("plain-dict-uncompressed-checksum.parquet"));
        var truth = await EstablishCommittedTruthAsync(bytes, 0, "b4e1c8ce8ea209fb64ee37db3c5b356b0952a756d919b5b31f69e1dc49087cf2");
        Assert.Equal(1000, truth.RowCount);
        Assert.Equal(truth.Checksum, truth.ColumnHashes[0]);
        Assert.Equal(0, truth.NullCounts[0]);
        Assert.Equal(0, truth.BinaryPayloadBytes);
    }

    [Fact]
    public async Task CommittedLaneRejectsWrongPinnedHash()
    {
        var bytes = await File.ReadAllBytesAsync(CommittedFixturePath("plain-dict-uncompressed-checksum.parquet"));
        var exception = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await EstablishCommittedTruthAsync(bytes, 0, "0000000000000000000000000000000000000000000000000000000000000000"));
        Assert.Contains("pinned", exception.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task CommittedLaneRejectsOutOfRangeOrdinal()
    {
        var bytes = await File.ReadAllBytesAsync(CommittedFixturePath("plain-dict-uncompressed-checksum.parquet"));
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(async () =>
            await EstablishCommittedTruthAsync(bytes, 99, "b4e1c8ce8ea209fb64ee37db3c5b356b0952a756d919b5b31f69e1dc49087cf2"));
    }

    private static async Task<CensusOutcome> RunCensusPassAsync(
        Assembly assembly,
        ParquetFile file,
        string workload,
        int rowCount,
        int utf8PayloadBytes,
        ParquetColumn[] projection,
        int target,
        long expectedChecksum,
        long[] columnHashes,
        int[] nullCounts)
    {
        var runnerType = assembly.GetType("Lokad.Parquet.Benchmarks.WorkCensusRunner") ??
            throw new InvalidOperationException("The benchmark census runner is unavailable.");
        var workloadType = assembly.GetType("Lokad.Parquet.Benchmarks.ScanWorkload") ??
            throw new InvalidOperationException("The benchmark workload token is unavailable.");
        var run = runnerType.GetMethod("RunCensusPassAsync", BindingFlags.NonPublic | BindingFlags.Static) ??
            throw new InvalidOperationException("The benchmark census pass is unavailable.");
        var task = (Task)(run.Invoke(null,
        [
            file, Enum.Parse(workloadType, workload), rowCount, utf8PayloadBytes,
            projection, target, expectedChecksum, columnHashes, nullCounts, null,
            new long[projection.Length], new long[projection.Length], null,
        ]) ?? throw new InvalidOperationException("The census pass returned nothing."));
        await task.ConfigureAwait(false);
        var outcome = task.GetType().GetProperty("Result")?.GetValue(task) ??
            throw new InvalidOperationException("The census pass produced nothing.");
        var outcomeType = outcome.GetType();
        return new CensusOutcome(
            Assert.IsType<long>(outcomeType.GetProperty("Checksum")?.GetValue(outcome)),
            Assert.IsType<int>(outcomeType.GetProperty("PassBatches")?.GetValue(outcome)));
    }

    private static async Task<CommittedTruth> EstablishCommittedTruthAsync(byte[] fixtureBytes, int projectedOrdinal, string pinnedColumnHash)
    {
        var assembly = BenchmarkAssembly();
        var runnerType = assembly.GetType("Lokad.Parquet.Benchmarks.WorkCensusRunner") ??
            throw new InvalidOperationException("The benchmark census runner is unavailable.");
        var establish = runnerType.GetMethod("EstablishCommittedTruthAsync", BindingFlags.NonPublic | BindingFlags.Static) ??
            throw new InvalidOperationException("The committed truth oracle is unavailable.");
        var task = (Task)(establish.Invoke(null, [fixtureBytes, projectedOrdinal, pinnedColumnHash]) ??
            throw new InvalidOperationException("The truth oracle returned nothing."));
        await task.ConfigureAwait(false);
        var truth = task.GetType().GetProperty("Result")?.GetValue(task) ??
            throw new InvalidOperationException("The truth oracle produced nothing.");
        var truthType = truth.GetType();
        return new CommittedTruth(
            Assert.IsType<long>(truthType.GetProperty("Checksum")?.GetValue(truth)),
            Assert.IsType<long[]>(truthType.GetProperty("ColumnHashes")?.GetValue(truth)),
            Assert.IsType<int[]>(truthType.GetProperty("NullCounts")?.GetValue(truth)),
            Assert.IsType<int>(truthType.GetProperty("RowCount")?.GetValue(truth)),
            Assert.IsType<int>(truthType.GetProperty("BinaryPayloadBytes")?.GetValue(truth)));
    }

    private static Type ChecksumType(Assembly assembly) =>
        assembly.GetType("Lokad.Parquet.Benchmarks.ScanChecksum") ??
            throw new InvalidOperationException("The benchmark checksum helper is unavailable.");

    private static MethodInfo ChecksumFoldMethod(Type checksumType, string name, Type valueType) =>
        checksumType.GetMethod(name, BindingFlags.NonPublic | BindingFlags.Static, [typeof(long), valueType]) ??
            throw new InvalidOperationException("The benchmark checksum fold is unavailable.");

    private static long CombineColumn(Type checksumType, long valueChain, long nullChain)
    {
        var combine = ChecksumFoldMethod(checksumType, "CombineColumn", typeof(long));
        return Assert.IsType<long>(combine.Invoke(null, [valueChain, nullChain]));
    }

    private static string CommittedFixturePath(string fileName) =>
        Path.Combine(RepositoryTestPaths.Root, "tests", "fixtures", "apache-parquet-testing", fileName);

    private static Assembly BenchmarkAssembly()
    {
        var testOutput = Path.GetDirectoryName(typeof(BenchmarkDiagnosticsTests).Assembly.Location) ??
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

    private sealed record CensusOutcome(long Checksum, int PassBatches);

    private sealed record CommittedTruth(long Checksum, long[] ColumnHashes, int[] NullCounts, int RowCount, int BinaryPayloadBytes);
}
