using System.Reflection;
using System.Runtime.InteropServices;
using System.Text.Json;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Exporters.Json;
using BenchmarkDotNet.Filters;
using BenchmarkDotNet.Jobs;
using BenchmarkDotNet.Reports;
using BenchmarkDotNet.Running;
using Perfolizer.Horology;

namespace Lokad.Parquet.Benchmarks;

internal static class PublicationRunner
{
    private static readonly Type[] Benchmarks =
    [
        typeof(PreopenedScanBenchmarks), typeof(PreopenedUtf8ScanBenchmarks), typeof(WriteBenchmarks),
    ];

    public static async Task VerifyWritesAsync()
    {
        foreach (var workload in Enum.GetValues<WriteWorkload>())
        {
            var write = new WriteBenchmarks { Workload = workload };
            try { await write.PrepareAsync(); }
            finally { write.Cleanup(); }
        }
    }

    public static async Task<int> RunAsync(string repositoryRoot, string affinity)
    {
        var source = Environment.GetEnvironmentVariable("LOKAD_PARQUET_SOURCE_REVISION") ??
            throw new InvalidOperationException("Use bench.ps1 to record the benchmark source revision.");
        var packageLock = Environment.GetEnvironmentVariable("LOKAD_PARQUET_PACKAGE_LOCK_HASH") ??
            throw new InvalidOperationException("Use bench.ps1 to record the package lock fingerprint.");
        var fingerprint = Environment.GetEnvironmentVariable("LOKAD_PARQUET_SOURCE_FINGERPRINT") ??
            throw new InvalidOperationException("Use bench.ps1 to record the source fingerprint.");
        using var catalogStream = typeof(PublicationRunner).Assembly.GetManifestResourceStream("PublicationCatalog.json") ??
            throw new InvalidOperationException("The publication catalog is missing.");
        var catalog = JsonSerializer.Deserialize<PublicationCase[]>(catalogStream) ??
            throw new InvalidOperationException("The publication catalog is empty.");
        var artifacts = Path.Combine(repositoryRoot, "artifacts", "benchmarks", "publication");
        Directory.CreateDirectory(artifacts);
        var forcedScalar = AppContext.TryGetSwitch("Lokad.Parquet.ForceScalar", out var enabled) && enabled;

        var config = DefaultConfig.Instance
            .WithArtifactsPath(artifacts)
            .AddExporter(JsonExporter.Full)
            .AddFilter(new SimpleFilter(benchmark =>
                catalog.Any(item => item.Matches(benchmark))))
            .AddJob(Job.Default.WithId("Publication")
                .WithLaunchCount(1)
                .WithWarmupCount(4)
                .WithIterationCount(8)
                .WithEnvironmentVariable("LOKAD_PARQUET_BENCHMARK_FORCE_SCALAR", forcedScalar ? "1" : "0")
                .WithMinIterationTime(TimeInterval.FromMilliseconds(250)));
        var reports = BenchmarkSwitcher.FromTypes(Benchmarks)
            .Run(["--filter", "*"], config)
            .SelectMany(summary => summary.Reports)
            .ToArray();

        if (reports.Length != catalog.Length * 2)
            throw new InvalidOperationException("The publication benchmark did not produce the expected comparison catalog.");

        var measurements = new List<PublicationMeasurement>(catalog.Length);
        foreach (var item in catalog)
        {
            var (lokad, evidence) = ReadResult(reports, item, "Lokad");
            var (baseline, baselineEvidence) = ReadResult(reports, item, "ParquetNet");
            if (evidence != baselineEvidence || evidence.Workload != item.Workload ||
                evidence.Rows != item.Rows || evidence.Columns != item.Columns)
                throw new InvalidOperationException($"{item.Id} has inconsistent setup evidence.");

            List<WriterMemorySample>? memory = null;
            if (item.Benchmark == nameof(WriteBenchmarks))
            {
                var write = new WriteBenchmarks { Workload = Enum.Parse<WriteWorkload>(item.Workload) };
                try
                {
                    write.PrepareInput();
                    memory = new List<WriterMemorySample>();
                    foreach (var groups in new[] { 1, WriteBenchmarks.Groups, 64 })
                        memory.Add(await write.MeasureWorkingBuffersAsync(groups));
                    if (memory.Any(sample => sample.PageBuffersPeakBytes != memory[0].PageBuffersPeakBytes))
                        throw new InvalidOperationException($"{item.Id} writer page buffers grew with row-group count.");
                }
                finally
                {
                    write.Cleanup();
                }
            }
            measurements.Add(new PublicationMeasurement(
                item.Id, item.Case, item.Rows, item.Columns,
                evidence.InputBytes, evidence.LokadFileBytes, evidence.BaselineFileBytes,
                evidence.FixtureSha256, evidence.LokadFileSha256, evidence.BaselineFileSha256, memory, lokad, baseline));
        }
        var baselineVersion = typeof(global::Parquet.ParquetReader).Assembly
            .GetCustomAttribute<AssemblyInformationalVersionAttribute>()?.InformationalVersion.Split('+')[0]
            ?? throw new InvalidOperationException("The loaded Parquet.NET assembly has no informational version.");

        var snapshot = new PublicationSnapshot(
            2, DateTimeOffset.UtcNow.ToString("yyyy-MM-dd"), source, fingerprint, packageLock, baselineVersion,
            "Linux " + RuntimeInformation.OSDescription, Environment.OSVersion.VersionString,
            RuntimeInformation.FrameworkDescription, BenchmarkHostPolicy.GetProcessorName(), affinity,
            forcedScalar ? "Forced scalar" : "Auto", measurements);
        var output = Path.Combine(artifacts, "snapshot.json");
        PairedSessionRecorder.WriteSessionFileAtomic(output,
            JsonSerializer.Serialize(snapshot, new JsonSerializerOptions { WriteIndented = true }));
        Console.WriteLine($"Publication snapshot: {output}");
        return 0;

        static (PublicationTiming Timing, PublicationSetupEvidence Evidence) ReadResult(
            BenchmarkReport[] reports, PublicationCase item, string method)
        {
            var report = reports.Single(candidate =>
                item.Matches(candidate.BenchmarkCase) &&
                candidate.BenchmarkCase.Descriptor.WorkloadMethod.Name.StartsWith(method, StringComparison.Ordinal));
            var statistics = report.ResultStatistics;
            if (!report.Success || statistics is null || !double.IsFinite(statistics.Median) || statistics.Median <= 0)
                throw new InvalidOperationException($"BenchmarkDotNet produced no usable timing for {item.Id} / {method}.");
            var timing = new PublicationTiming(
                statistics.Median,
                report.GcStats.GetBytesAllocatedPerOperation(report.BenchmarkCase),
                report.GcStats.Gen0Collections, report.GcStats.Gen1Collections, report.GcStats.Gen2Collections);
            var evidenceLine = report.ExecuteResults.SelectMany(result => result.StandardOutput).Single(line =>
                line.StartsWith(PublicationSetupEvidence.Prefix, StringComparison.Ordinal));
            var evidence = JsonSerializer.Deserialize<PublicationSetupEvidence>(evidenceLine[PublicationSetupEvidence.Prefix.Length..])
                ?? throw new InvalidOperationException($"{item.Id} / {method} did not record setup evidence.");
            return (timing, evidence);
        }
    }

    private sealed record PublicationCase(string Id, string Case, string Benchmark, string Workload, int Rows, int Columns)
    {
        public bool Matches(BenchmarkCase benchmark) =>
            benchmark.Descriptor.Type.Name == Benchmark &&
            benchmark.Parameters[nameof(WriteBenchmarks.Workload)]?.ToString() == Workload &&
            (Benchmark == nameof(WriteBenchmarks)
                ? WriteBenchmarks.Rows == Rows
                : benchmark.Parameters[nameof(PreopenedScanBenchmarks.RowCount)] is int count && count == Rows);
    }

    private sealed record PublicationSnapshot(
        int Schema, string Date, string Source, string SourceFingerprint, string PackageLock, string BaselineVersion, string OperatingSystem,
        string Kernel, string Runtime, string Processor, string Affinity, string InstructionSet,
        List<PublicationMeasurement> Measurements);

    private sealed record PublicationMeasurement(
        string Id, string Case, int Rows, int Columns, int? InputBytes, int? LokadFileBytes, int? BaselineFileBytes,
        string? FixtureSha256, string? LokadFileSha256, string? BaselineFileSha256,
        List<WriterMemorySample>? WriterMemory, PublicationTiming Lokad, PublicationTiming Baseline)
    {
        public double LokadRowsPerSecond => Rows * 1e9 / Lokad.MedianNanoseconds;
        public double BaselineRowsPerSecond => Rows * 1e9 / Baseline.MedianNanoseconds;
    }

    private sealed record PublicationTiming(double MedianNanoseconds, long? AllocatedBytes, int Gen0, int Gen1, int Gen2);
}

internal sealed record PublicationSetupEvidence(string Workload, int Rows, int Columns)
{
    internal const string Prefix = "Lokad publication setup: ";
    public int? InputBytes { get; init; }
    public string? FixtureSha256 { get; init; }
    public int? LokadFileBytes { get; init; }
    public int? BaselineFileBytes { get; init; }
    public string? LokadFileSha256 { get; init; }
    public string? BaselineFileSha256 { get; init; }

    internal void Log() => Console.WriteLine(Prefix + JsonSerializer.Serialize(this));
}
