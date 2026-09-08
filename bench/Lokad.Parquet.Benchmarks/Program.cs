using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Engines;
using BenchmarkDotNet.Jobs;
using BenchmarkDotNet.Running;
using System.Diagnostics;
using Lokad.Parquet.Benchmarks;
using Perfolizer.Horology;
using System.Text.Json;

#if DEBUG
Console.Error.WriteLine("Lokad.Parquet benchmarks require a Release build.");
return 1;
#else
using var benchmarkProcess = Process.GetCurrentProcess();
string BindToOneLogicalProcessor()
{
    if (!OperatingSystem.IsWindows() && !OperatingSystem.IsLinux())
        return "unsupported";

    var available = benchmarkProcess.ProcessorAffinity.ToInt64();
    if (available == 0)
        throw new InvalidOperationException("The benchmark process has no available logical processor.");
    var selected = available & -available;
    benchmarkProcess.ProcessorAffinity = (nint)selected;
    var applied = benchmarkProcess.ProcessorAffinity.ToInt64();
    if (applied != selected)
        throw new InvalidOperationException("The benchmark process could not be bound to one logical processor.");
    return $"0x{applied:x}";
}

var repositoryRoot = Environment.GetEnvironmentVariable("LOKAD_PARQUET_REPOSITORY_ROOT");
if (string.IsNullOrEmpty(repositoryRoot))
{
    repositoryRoot = Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, "..", "..", "..", "..", ".."));
    if (!File.Exists(Path.Combine(repositoryRoot, "Lokad.Parquet.slnx")))
        throw new InvalidOperationException("The benchmark repository root could not be resolved.");
    Environment.SetEnvironmentVariable("LOKAD_PARQUET_REPOSITORY_ROOT", repositoryRoot);
}

if (args.Contains("--verify-truth", StringComparer.Ordinal))
    return await ScanTruthVerification.RunAsync();
BenchmarkEnvironment.EnsureNativeWorkspace(repositoryRoot, AppContext.BaseDirectory, Path.GetFullPath(Path.Combine(repositoryRoot, "artifacts", "benchmarks")));
if (args.Contains("--force-scalar", StringComparer.Ordinal))
    AppContext.SetSwitch("Lokad.Parquet.ForceScalar", true);
Console.WriteLine($"Processor affinity: {BindToOneLogicalProcessor()}");
Console.WriteLine($"Source revision: {Environment.GetEnvironmentVariable("LOKAD_PARQUET_SOURCE_REVISION") ?? "unrecorded"}");
if (args.Contains("--catalog", StringComparer.Ordinal))
{
    var catalogCases = ScanWorkloadCatalog.ParityWorkloads
        .Select(static workload => new { name = "PreopenedScan/" + workload.ToString(), label = ScanWorkloadCatalog.Labels[workload] })
        .Append(new { name = "WarmMetadataOpen", label = "Warm metadata open" })
        .ToArray();
    var catalog = new { pairedSchemaVersion = PairedParityRunner.SnapshotSchemaVersion, cases = catalogCases };
    Directory.CreateDirectory(Path.Combine("artifacts", "benchmarks"));
    var catalogPath = Path.Combine("artifacts", "benchmarks", "parity-catalog.json");
    await using (var catalogOutput = File.Create(catalogPath))
    {
        await JsonSerializer.SerializeAsync(catalogOutput, catalog, new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
            WriteIndented = true,
        });
    }
    Console.WriteLine($"Parity catalog: {Path.GetFullPath(catalogPath)}");
    return 0;
}
if (args.Contains("--paired", StringComparer.Ordinal))
    return await PairedParityRunner.RunAsync(args);
if (args.Contains("--census", StringComparer.Ordinal))
    return await WorkCensusRunner.RunAsync();
var hasExplicitJob = args.Any(static argument =>
    string.Equals(argument, "--job", StringComparison.Ordinal) ||
    argument.StartsWith("--job=", StringComparison.Ordinal));
var config = DefaultConfig.Instance
    .AddColumn(new NormalizedScanColumn(NormalizedScanMetric.NanosecondsPerCell))
    .AddColumn(new NormalizedScanColumn(NormalizedScanMetric.MillionCellsPerSecond))
    .AddColumn(new NormalizedScanColumn(NormalizedScanMetric.NanosecondsPerUtf8Byte))
    .AddColumn(new NormalizedScanColumn(NormalizedScanMetric.Utf8GigabytesPerSecond));
if (!hasExplicitJob)
{
    var mode = Environment.GetEnvironmentVariable("LOKAD_PARQUET_BENCHMARK_MODE");
    config = config.AddJob(string.Equals(mode, "ColdOpen", StringComparison.Ordinal)
        ? Job.Default
            .WithId("ColdOpen")
            .WithStrategy(RunStrategy.ColdStart)
            .WithLaunchCount(5)
            .WithWarmupCount(0)
            .WithIterationCount(1)
        : Job.Default
            .WithId("Qualification")
            .WithLaunchCount(1)
            .WithWarmupCount(5)
            .WithIterationCount(15)
            .WithMinIterationTime(TimeInterval.FromMilliseconds(250)));
}
BenchmarkSwitcher.FromAssembly(typeof(Program).Assembly).Run(args, config);
return 0;
#endif
