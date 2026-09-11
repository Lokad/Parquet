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
int CheckPath(string[] arguments)
{
    string? target = null;
    var role = "benchmark path";
    for (var index = 0; index < arguments.Length; index++)
    {
        if (string.Equals(arguments[index], "--check-path", StringComparison.Ordinal))
        {
            if (index + 1 >= arguments.Length)
                throw new ArgumentException("--check-path requires a path.", nameof(arguments));
            target = arguments[index + 1];
        }
        if (string.Equals(arguments[index], "--check-role", StringComparison.Ordinal))
        {
            if (index + 1 >= arguments.Length)
                throw new ArgumentException("--check-role requires a role.", nameof(arguments));
            role = arguments[index + 1];
        }
    }
    if (target is null)
        throw new ArgumentException("--check-path requires a path.", nameof(arguments));
    var evidence = BenchmarkHostPolicy.CheckNativeWorkspacePath(target, role);
    Console.WriteLine("Workspace (" + evidence.Role + "): " + evidence.ResolvedPath + "; mount: " + evidence.MountPoint + " " + evidence.FileSystem + "; windows-backed: " + evidence.WindowsBacked + ".");
    return 0;
}

var repositoryRoot = Environment.GetEnvironmentVariable("LOKAD_PARQUET_REPOSITORY_ROOT");
if (string.IsNullOrEmpty(repositoryRoot))
{
    repositoryRoot = Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, "..", "..", "..", "..", ".."));
    if (!File.Exists(Path.Combine(repositoryRoot, "Lokad.Parquet.slnx")))
        throw new InvalidOperationException("The benchmark repository root could not be resolved.");
    Environment.SetEnvironmentVariable("LOKAD_PARQUET_REPOSITORY_ROOT", repositoryRoot);
}

// The lane switch precedes every early return so truth verification and all
// measured lanes observe the requested mode. Benchmark workers establish their
// own lane in-process; the paired and census lanes run here and read this switch.
if (args.Contains("--force-scalar", StringComparer.Ordinal))
    AppContext.SetSwitch("Lokad.Parquet.ForceScalar", true);
if (args.Contains("--check-path", StringComparer.Ordinal))
    return CheckPath(args);
if (args.Contains("--verify-truth", StringComparer.Ordinal))
    return await ScanTruthVerification.RunAsync();
BenchmarkEnvironment.EnsureNativeWorkspace(repositoryRoot, AppContext.BaseDirectory, Path.GetFullPath(Path.Combine(repositoryRoot, "artifacts", "benchmarks")));
var launcherAffinity = BindToOneLogicalProcessor();
Console.WriteLine($"Processor affinity: {launcherAffinity}");
if (OperatingSystem.IsWindows() || OperatingSystem.IsLinux())
    Environment.SetEnvironmentVariable(BenchmarkHostPolicy.AffinityEnvironmentVariable, launcherAffinity);
Console.WriteLine($"Source revision: {Environment.GetEnvironmentVariable("LOKAD_PARQUET_SOURCE_REVISION") ?? "unrecorded"}");
if (args.Contains("--catalog", StringComparer.Ordinal))
    return await PairedParityRunner.WriteCatalogAsync(Path.Combine("artifacts", "benchmarks", "parity-catalog.json"));
if (args.Contains("--paired", StringComparer.Ordinal))
    return await PairedParityRunner.RunAsync(args);
if (args.Contains("--census", StringComparer.Ordinal))
    return await WorkCensusRunner.RunAsync();
if (args.Contains("--census-smoke", StringComparer.Ordinal))
    return await WorkCensusRunner.RunSmokeAsync();
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
