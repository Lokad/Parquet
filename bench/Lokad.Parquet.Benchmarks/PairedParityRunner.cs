using System.Diagnostics;
using System.Numerics;
using System.Reflection;
using System.Runtime.InteropServices;
using System.Security.Cryptography;
using System.Text.Json;
using Parquet;
using BaselineParquetReader = Parquet.ParquetReader;

namespace Lokad.Parquet.Benchmarks;

internal static class PairedParityRunner
{
    private const int RowCount = 65_536;
    private const int SampleCount = 400;
    internal const int SnapshotSchemaVersion = 7;
    private const int WarmupCount = 64;
    private const int MinimumStabilizationOperations = 16_384;
    private const int PreliminaryStabilizationBlockCount = 40;
    private const int FinalStabilizationBlockCount = 10;
    private const int RandomSeed = 24_081_993;
    private static readonly TimeSpan TargetBlockTime = TimeSpan.FromMilliseconds(100);

    public static async Task<int> RunAsync(string[] arguments)
    {
        var enforce = arguments.Contains("--paired-enforce", StringComparer.Ordinal);
        var outputPath = ResolveOutputPath();
        using var process = Process.GetCurrentProcess();
        long appliedAffinity;
        if (OperatingSystem.IsWindows() || OperatingSystem.IsLinux())
            appliedAffinity = process.ProcessorAffinity.ToInt64();
        else
            throw new PlatformNotSupportedException("Paired benchmarks require Windows or Linux processor affinity.");
        if (appliedAffinity == 0 || (appliedAffinity & (appliedAffinity - 1)) != 0)
            throw new InvalidOperationException("The paired benchmark process is not bound to one logical processor.");
        var processorAffinity = $"0x{appliedAffinity:x}";
        if (OperatingSystem.IsWindows())
            process.PriorityClass = ProcessPriorityClass.High;
        string? selectedCase = null;
        for (var index = 0; index < arguments.Length; index++)
        {
            if (!string.Equals(arguments[index], "--paired-case", StringComparison.Ordinal))
                continue;
            if (index + 1 >= arguments.Length)
                throw new ArgumentException("--paired-case requires a workload name.", nameof(arguments));
            selectedCase = arguments[index + 1];
        }
        var caseResults = new List<PairedCaseResult>();
        var forceScalar = AppContext.TryGetSwitch("Lokad.Parquet.ForceScalar", out var scalarEnabled) &&
            scalarEnabled;
        foreach (var workload in ScanWorkloadCatalog.ParityWorkloads)
        {
            if (selectedCase is not null && !string.Equals(selectedCase, workload.ToString(), StringComparison.Ordinal))
                continue;
            await using var benchmarkCase = await CreateScanCaseAsync(workload);
            caseResults.Add(await MeasureAsync(benchmarkCase));
        }
        if (selectedCase is null || string.Equals(selectedCase, "WarmMetadataOpen", StringComparison.Ordinal))
        {
            await using var metadataCase = await CreateMetadataCaseAsync();
            caseResults.Add(await MeasureAsync(metadataCase));
        }
        if (caseResults.Count == 0)
            throw new ArgumentException($"Unknown paired case '{selectedCase}'.", nameof(arguments));

        var snapshot = new PairedRunSnapshot(
            SchemaVersion: SnapshotSchemaVersion,
            SessionId: Guid.NewGuid(),
            RecordedAtUtc: DateTimeOffset.UtcNow,
            SourceRevision: Environment.GetEnvironmentVariable("LOKAD_PARQUET_SOURCE_REVISION") ?? "unrecorded",
            RunnerFingerprint: GetRunnerFingerprint(),
            PackageLockHash: Environment.GetEnvironmentVariable("LOKAD_PARQUET_PACKAGE_LOCK_HASH") ?? "unrecorded",
            Runtime: RuntimeInformation.FrameworkDescription,
            OperatingSystem: OperatingSystem.IsLinux() && !RuntimeInformation.OSDescription.Contains("Linux", StringComparison.Ordinal) ? "Linux " + RuntimeInformation.OSDescription : RuntimeInformation.OSDescription,
            Architecture: RuntimeInformation.ProcessArchitecture.ToString(),
            Processor: GetProcessorName(),
            ProcessPriority: process.PriorityClass.ToString(),
            ProcessorAffinity: processorAffinity,
            PowerMode: Environment.GetEnvironmentVariable("LOKAD_PARQUET_POWER_MODE") ?? "unrecorded",
            InstructionMode: forceScalar
                ? "forced scalar"
                : $"portable Vector<T>; {Vector<byte>.Count * 8}-bit; " +
                    $"hardwareAccelerated={Vector.IsHardwareAccelerated}",
            StopwatchFrequency: Stopwatch.Frequency,
            RandomSeed: RandomSeed,
            SampleCount: SampleCount,
            InitialWarmupOperations: WarmupCount,
            MinimumStabilizationOperations: MinimumStabilizationOperations,
            PreliminaryStabilizationBlocks: PreliminaryStabilizationBlockCount,
            FinalStabilizationBlocks: FinalStabilizationBlockCount,
            TargetBlockMilliseconds: TargetBlockTime.TotalMilliseconds,
            Estimator: "exp(mean(paired log ratio)); one-sided 95% Student-t upper bound",
            OutlierRule: "none; retain every balanced AB/BA observation",
            Cases: caseResults);

        var directory = Path.GetDirectoryName(outputPath);
        if (!string.IsNullOrEmpty(directory))
            Directory.CreateDirectory(directory);
        await using (var stream = File.Create(outputPath))
        {
            await JsonSerializer.SerializeAsync(stream, snapshot, new JsonSerializerOptions
            {
                PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
                WriteIndented = true,
            });
            await stream.FlushAsync();
        }

        Console.WriteLine($"Paired snapshot: {Path.GetFullPath(outputPath)}");
        foreach (var result in caseResults)
        {
            Console.WriteLine(
                $"{result.Name}: ratio {result.PointRatio:F4}, upper 95% {result.Upper95Ratio:F4}, " +
                $"{(result.Passed ? "PASS" : "FAIL")} ({result.OperationsPerBlock} operations/block).");
        }
        return enforce && caseResults.Exists(static result => !result.Passed) ? 2 : 0;

        async Task<PairedOperationCase> CreateScanCaseAsync(ScanWorkload workload)
        {
            var scanCase = await ParityScanCase.CreateAsync(workload, RowCount);
            var expected = await scanCase.ReadLokadAsync();
            return new PairedOperationCase(
                $"PreopenedScan/{workload}",
                scanCase.FixtureHash,
                RowCount,
                ScanWorkloadCatalog.GetColumnCount(workload),
                ScanWorkloadCatalog.GetUtf8PayloadByteCount(workload, RowCount),
                expected,
                scanCase.ReadLokadAsync,
                scanCase.ReadParquetNetAsync,
                scanCase.DisposeAsync);
        }

        static async Task<PairedOperationCase> CreateMetadataCaseAsync()
        {
            var fixture = await ScanFixture.CreateAsync(ScanWorkload.RequiredInt32Plain, 262_144);
            var hash = Convert.ToHexStringLower(SHA256.HashData(fixture.Bytes));

            async Task<long> OpenLokadAsync()
            {
                using var stream = new MemoryStream(fixture.Bytes, writable: false);
                await using var file = await ParquetFile.OpenAsync(stream);
                return file.Metadata.RowCount;
            }

            async Task<long> OpenParquetNetAsync()
            {
                using var stream = new MemoryStream(fixture.Bytes, writable: false);
                await using var reader = await BaselineParquetReader.CreateAsync(stream);
                return reader.Metadata?.NumRows ?? 0;
            }

            return new PairedOperationCase(
                "WarmMetadataOpen",
                hash,
                262_144,
                1,
                0,
                262_144,
                OpenLokadAsync,
                OpenParquetNetAsync,
                static () => ValueTask.CompletedTask);
        }

        string ResolveOutputPath()
        {
            for (var index = 0; index < arguments.Length; index++)
            {
                if (!string.Equals(arguments[index], "--paired-output", StringComparison.Ordinal))
                    continue;
                if (index + 1 >= arguments.Length)
                    throw new ArgumentException("--paired-output requires a path.", nameof(arguments));
                return arguments[index + 1];
            }
            var platform = OperatingSystem.IsWindows() ? "windows" : "linux";
            return Path.Combine(
                "artifacts",
                "benchmarks",
                $"paired-{platform}-{DateTimeOffset.UtcNow:yyyyMMdd-HHmmss}.json");
        }

        static string GetRunnerFingerprint()
        {
            var path = Assembly.GetExecutingAssembly().Location;
            return Convert.ToHexStringLower(SHA256.HashData(File.ReadAllBytes(path)));
        }

        // PROCESSOR_IDENTIFIER exists only on Windows; collect the Linux CPU
        // model directly so qualification evidence names concrete hardware.
        static string GetProcessorName()
        {
            var identifier = Environment.GetEnvironmentVariable("PROCESSOR_IDENTIFIER");
            if (!string.IsNullOrEmpty(identifier))
                return identifier;
            if (OperatingSystem.IsLinux())
            {
                try
                {
                    foreach (var line in File.ReadLines("/proc/cpuinfo"))
                    {
                        const string prefix = "model name\t: ";
                        if (line.StartsWith(prefix, StringComparison.Ordinal))
                            return line[prefix.Length..].Trim();
                    }
                }
                catch (IOException)
                {
                }
                catch (UnauthorizedAccessException)
                {
                }
            }
            return "unrecorded";
        }
    }

    private static async Task<PairedCaseResult> MeasureAsync(PairedOperationCase benchmarkCase)
    {
        static async Task<TimedResult> TimeAsync(Func<Task<long>> operation, int operationCount)
        {
            var sentinel = 0L;
            var allocatedBefore = GC.GetTotalAllocatedBytes(false);
            var gen0Before = GC.CollectionCount(0);
            var gen1Before = GC.CollectionCount(1);
            var gen2Before = GC.CollectionCount(2);
            var started = Stopwatch.GetTimestamp();
            for (var operationIndex = 0; operationIndex < operationCount; operationIndex++)
                sentinel = await operation();
            var elapsed = Stopwatch.GetTimestamp() - started;
            return new TimedResult(
                elapsed,
                sentinel,
                GC.GetTotalAllocatedBytes(false) - allocatedBefore,
                GC.CollectionCount(0) - gen0Before,
                GC.CollectionCount(1) - gen1Before,
                GC.CollectionCount(2) - gen2Before);
        }

        static double TicksToNanoseconds(long ticks) =>
            ticks * 1_000_000_000d / Stopwatch.Frequency;

        for (var warmup = 0; warmup < WarmupCount; warmup++)
        {
            _ = await benchmarkCase.RunLokad();
            _ = await benchmarkCase.RunParquetNet();
        }

        var targetTicks = TargetBlockTime.TotalSeconds * Stopwatch.Frequency;
        var initialLokadCalibration = await TimeAsync(benchmarkCase.RunLokad, 1);
        var initialParquetNetCalibration = await TimeAsync(benchmarkCase.RunParquetNet, 1);
        ValidateSentinel(initialLokadCalibration);
        ValidateSentinel(initialParquetNetCalibration);
        var initialSlowerTicks = Math.Max(
            initialLokadCalibration.ElapsedTicks,
            initialParquetNetCalibration.ElapsedTicks);
        var preliminaryOperationsPerBlock = Math.Clamp(
            (int)Math.Ceiling(targetTicks / initialSlowerTicks),
            1,
            131_072);
        preliminaryOperationsPerBlock = Math.Max(
            preliminaryOperationsPerBlock,
            (int)Math.Ceiling(
                (double)(MinimumStabilizationOperations - WarmupCount) /
                PreliminaryStabilizationBlockCount));
        for (var block = 0; block < PreliminaryStabilizationBlockCount; block++)
        {
            TimedResult first;
            TimedResult second;
            if ((block & 1) == 0)
            {
                first = await TimeAsync(benchmarkCase.RunLokad, preliminaryOperationsPerBlock);
                second = await TimeAsync(benchmarkCase.RunParquetNet, preliminaryOperationsPerBlock);
            }
            else
            {
                first = await TimeAsync(benchmarkCase.RunParquetNet, preliminaryOperationsPerBlock);
                second = await TimeAsync(benchmarkCase.RunLokad, preliminaryOperationsPerBlock);
            }
            ValidateSentinel(first);
            ValidateSentinel(second);
        }

        var stableLokadCalibration = await TimeAsync(
            benchmarkCase.RunLokad,
            preliminaryOperationsPerBlock);
        var stableParquetNetCalibration = await TimeAsync(
            benchmarkCase.RunParquetNet,
            preliminaryOperationsPerBlock);
        ValidateSentinel(stableLokadCalibration);
        ValidateSentinel(stableParquetNetCalibration);
        var stableSlowerTicksPerOperation = Math.Max(
            (double)stableLokadCalibration.ElapsedTicks / preliminaryOperationsPerBlock,
            (double)stableParquetNetCalibration.ElapsedTicks / preliminaryOperationsPerBlock);
        var operationsPerBlock = Math.Clamp(
            (int)Math.Ceiling(targetTicks / stableSlowerTicksPerOperation),
            1,
            131_072);
        for (var block = 0; block < FinalStabilizationBlockCount; block++)
        {
            var lokad = await TimeAsync(benchmarkCase.RunLokad, operationsPerBlock);
            var parquetNet = await TimeAsync(benchmarkCase.RunParquetNet, operationsPerBlock);
            ValidateSentinel(lokad);
            ValidateSentinel(parquetNet);
        }

        var orders = CreateBalancedOrders(SampleCount, RandomSeed ^ GetStableHashCode(benchmarkCase.Name));
        var observations = new List<PairedObservation>(SampleCount);

        GC.Collect(2, GCCollectionMode.Forced, blocking: true, compacting: true);
        GC.WaitForPendingFinalizers();
        GC.Collect(2, GCCollectionMode.Forced, blocking: true, compacting: true);
        foreach (var lokadFirst in orders)
        {
            var timestamp = DateTimeOffset.UtcNow;
            TimedResult lokad;
            TimedResult parquetNet;
            if (lokadFirst)
            {
                lokad = await TimeAsync(benchmarkCase.RunLokad, operationsPerBlock);
                parquetNet = await TimeAsync(benchmarkCase.RunParquetNet, operationsPerBlock);
            }
            else
            {
                parquetNet = await TimeAsync(benchmarkCase.RunParquetNet, operationsPerBlock);
                lokad = await TimeAsync(benchmarkCase.RunLokad, operationsPerBlock);
            }
            ValidateSentinel(lokad);
            ValidateSentinel(parquetNet);

            var lokadNanoseconds = TicksToNanoseconds(lokad.ElapsedTicks) / operationsPerBlock;
            var parquetNetNanoseconds = TicksToNanoseconds(parquetNet.ElapsedTicks) / operationsPerBlock;
            observations.Add(new PairedObservation(
                timestamp,
                lokadFirst ? "AB" : "BA",
                lokadNanoseconds,
                parquetNetNanoseconds,
                lokad.AllocatedBytes,
                parquetNet.AllocatedBytes,
                lokad.Gen0Collections,
                parquetNet.Gen0Collections,
                lokad.Gen1Collections,
                parquetNet.Gen1Collections,
                lokad.Gen2Collections,
                parquetNet.Gen2Collections,
                Math.Log(lokadNanoseconds / parquetNetNanoseconds)));
        }

        var logs = observations.Select(static observation => observation.LogRatio).ToArray();
        var meanLog = logs.Average();
        var sumSquaredDeviation = logs.Sum(value => Math.Pow(value - meanLog, 2));
        var standardDeviation = Math.Sqrt(sumSquaredDeviation / (logs.Length - 1));
        var standardError = standardDeviation / Math.Sqrt(logs.Length);
        var upperLog = meanLog + OneSided95StudentT(logs.Length - 1) * standardError;
        var pointRatio = Math.Exp(meanLog);
        var upperRatio = Math.Exp(upperLog);
        return new PairedCaseResult(
            benchmarkCase.Name,
            benchmarkCase.FixtureHash,
            benchmarkCase.RowCount,
            benchmarkCase.ColumnCount,
            benchmarkCase.Utf8PayloadBytes,
            operationsPerBlock,
            pointRatio,
            upperRatio,
            upperRatio <= 1.05,
            observations);

        void ValidateSentinel(TimedResult result)
        {
            if (result.Sentinel != benchmarkCase.ExpectedSentinel)
                throw new InvalidOperationException($"The paired {benchmarkCase.Name} truth check failed.");
        }

        static bool[] CreateBalancedOrders(int count, int seed)
        {
            if ((count & 1) != 0)
                throw new ArgumentOutOfRangeException(nameof(count), "A balanced schedule requires an even count.");
            var random = new Random(seed);
            var result = new bool[count];
            for (var pair = 0; pair < count; pair += 2)
            {
                var lokadFirst = random.Next(2) == 0;
                result[pair] = lokadFirst;
                result[pair + 1] = !lokadFirst;
            }
            return result;
        }

        static int GetStableHashCode(string value)
        {
            var hash = 17;
            foreach (var character in value)
                hash = unchecked(hash * 31 + character);
            return hash;
        }

        static double OneSided95StudentT(int degreesOfFreedom)
        {
            ReadOnlySpan<double> values =
            [
                6.314, 2.920, 2.353, 2.132, 2.015, 1.943, 1.895, 1.860, 1.833, 1.812,
                1.796, 1.782, 1.771, 1.761, 1.753, 1.746, 1.740, 1.734, 1.729, 1.725,
                1.721, 1.717, 1.714, 1.711, 1.708, 1.706, 1.703, 1.701, 1.699, 1.697,
            ];
            if (degreesOfFreedom <= 0)
                throw new ArgumentOutOfRangeException(nameof(degreesOfFreedom));
            return degreesOfFreedom <= values.Length ? values[degreesOfFreedom - 1] : 1.645;
        }
    }

}

internal sealed class PairedOperationCase : IAsyncDisposable
{
    private readonly Func<ValueTask> _dispose;

    public PairedOperationCase(
        string name,
        string fixtureHash,
        int rowCount,
        int columnCount,
        int utf8PayloadBytes,
        long expectedSentinel,
        Func<Task<long>> runLokad,
        Func<Task<long>> runParquetNet,
        Func<ValueTask> dispose)
    {
        Name = name;
        FixtureHash = fixtureHash;
        RowCount = rowCount;
        ColumnCount = columnCount;
        Utf8PayloadBytes = utf8PayloadBytes;
        ExpectedSentinel = expectedSentinel;
        RunLokad = runLokad;
        RunParquetNet = runParquetNet;
        _dispose = dispose;
    }

    public string Name { get; }
    public string FixtureHash { get; }
    public int RowCount { get; }
    public int ColumnCount { get; }
    public int Utf8PayloadBytes { get; }
    public long ExpectedSentinel { get; }
    public Func<Task<long>> RunLokad { get; }
    public Func<Task<long>> RunParquetNet { get; }
    public ValueTask DisposeAsync() => _dispose();
}

internal readonly record struct TimedResult(
    long ElapsedTicks,
    long Sentinel,
    long AllocatedBytes,
    int Gen0Collections,
    int Gen1Collections,
    int Gen2Collections);

internal sealed record PairedObservation(
    DateTimeOffset RecordedAtUtc,
    string Order,
    double LokadNanoseconds,
    double ParquetNetNanoseconds,
    long LokadAllocatedBytes,
    long ParquetNetAllocatedBytes,
    int LokadGen0Collections,
    int ParquetNetGen0Collections,
    int LokadGen1Collections,
    int ParquetNetGen1Collections,
    int LokadGen2Collections,
    int ParquetNetGen2Collections,
    double LogRatio);

internal sealed record PairedCaseResult(
    string Name,
    string FixtureHash,
    int RowCount,
    int ColumnCount,
    int Utf8PayloadBytes,
    int OperationsPerBlock,
    double PointRatio,
    double Upper95Ratio,
    bool Passed,
    IReadOnlyList<PairedObservation> Observations);

internal sealed record PairedRunSnapshot(
    int SchemaVersion,
    Guid SessionId,
    DateTimeOffset RecordedAtUtc,
    string SourceRevision,
    string RunnerFingerprint,
    string PackageLockHash,
    string Runtime,
    string OperatingSystem,
    string Architecture,
    string Processor,
    string ProcessPriority,
    string ProcessorAffinity,
    string PowerMode,
    string InstructionMode,
    long StopwatchFrequency,
    int RandomSeed,
    int SampleCount,
    int InitialWarmupOperations,
    int MinimumStabilizationOperations,
    int PreliminaryStabilizationBlocks,
    int FinalStabilizationBlocks,
    double TargetBlockMilliseconds,
    string Estimator,
    string OutlierRule,
    IReadOnlyList<PairedCaseResult> Cases);
