using System.Runtime.InteropServices;
using System.Security.Cryptography;
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
    private const int Rows = 65_536;
    private static readonly (ScanWorkload Workload, string Label, Type Benchmark)[] ReadCases =
    [
        (ScanWorkload.RequiredInt32Plain, "Read INT32, PLAIN", typeof(PreopenedScanBenchmarks)),
        (ScanWorkload.RequiredInt32Snappy, "Read INT32, PLAIN + Snappy", typeof(PreopenedScanBenchmarks)),
        (ScanWorkload.RequiredStringSnappy, "Read UTF-8, PLAIN + Snappy", typeof(PreopenedUtf8ScanBenchmarks)),
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
        var artifacts = Path.Combine(repositoryRoot, "artifacts", "benchmarks", "publication");
        Directory.CreateDirectory(artifacts);
        var forcedScalar = AppContext.TryGetSwitch("Lokad.Parquet.ForceScalar", out var enabled) && enabled;

        var config = DefaultConfig.Instance
            .WithArtifactsPath(artifacts)
            .AddExporter(JsonExporter.Full)
            .AddFilter(new SimpleFilter(benchmark =>
                benchmark.Descriptor.Type == typeof(WriteBenchmarks) ||
                ReadCases.Any(item => item.Benchmark == benchmark.Descriptor.Type &&
                    item.Workload.Equals(GetWorkload(benchmark)))))
            .AddJob(Job.Default.WithId("Publication")
                .WithLaunchCount(1)
                .WithWarmupCount(4)
                .WithIterationCount(8)
                .WithEnvironmentVariable("LOKAD_PARQUET_BENCHMARK_FORCE_SCALAR", forcedScalar ? "1" : "0")
                .WithMinIterationTime(TimeInterval.FromMilliseconds(250)));
        var reports = BenchmarkSwitcher.FromTypes(
            [typeof(PreopenedScanBenchmarks), typeof(PreopenedUtf8ScanBenchmarks), typeof(WriteBenchmarks)])
            .Run(["--filter", "*"], config)
            .SelectMany(summary => summary.Reports)
            .ToArray();

        var measurements = new List<PublicationMeasurement>();
        foreach (var (workload, label, benchmark) in ReadCases)
        {
            var fixture = await ScanFixture.CreateAsync(workload, Rows);
            measurements.Add(new PublicationMeasurement(
                label, Rows, fixture.ColumnCount, fixture.Bytes.Length, null, null,
                Convert.ToHexStringLower(SHA256.HashData(fixture.Bytes)), null,
                Timing(reports, benchmark, workload, "Lokad"),
                Timing(reports, benchmark, workload, "ParquetNet")));
        }
        foreach (var workload in Enum.GetValues<WriteWorkload>())
        {
            var write = new WriteBenchmarks { Workload = workload };
            try
            {
                await write.PrepareAsync();
                var memory = new List<WriterMemorySample>();
                foreach (var groups in new[] { 1, WriteBenchmarks.Groups, 64 })
                    memory.Add(await write.MeasureWorkingBuffersAsync(groups));
                if (memory.Any(sample => sample.PageBuffersPeakBytes != memory[0].PageBuffersPeakBytes))
                    throw new InvalidOperationException($"{workload} writer page buffers grew with row-group count.");
                var label = workload switch
                {
                    WriteWorkload.FixedWidth => "Write BOOLEAN, FLOAT, INT64",
                    WriteWorkload.FixedWidthRuns => "Write fixed-width runs",
                    WriteWorkload.RepeatedUtf8 => "Write repeated UTF-8",
                    WriteWorkload.HighCardinalityUtf8 => "Write high-cardinality UTF-8",
                    _ => throw new InvalidOperationException("The writer workload needs a publication label."),
                };
                measurements.Add(new PublicationMeasurement(
                    label, WriteBenchmarks.Rows, write.ColumnCount, null, write.LokadBytes, write.BaselineBytes,
                    null, memory, Timing(reports, typeof(WriteBenchmarks), workload, nameof(WriteBenchmarks.LokadWrite)),
                    Timing(reports, typeof(WriteBenchmarks), workload, nameof(WriteBenchmarks.ParquetNetWrite))));
            }
            finally
            {
                write.Cleanup();
            }
        }
        if (reports.Length != measurements.Count * 2)
            throw new InvalidOperationException("The publication benchmark did not produce the expected comparison catalog.");

        var snapshot = new PublicationSnapshot(
            1, DateTimeOffset.UtcNow.ToString("yyyy-MM-dd"), source, packageLock, "6.1.0",
            "Linux " + RuntimeInformation.OSDescription, Environment.OSVersion.VersionString,
            RuntimeInformation.FrameworkDescription, BenchmarkHostPolicy.GetProcessorName(), affinity,
            forcedScalar ? "Forced scalar" : "Auto", measurements);
        var output = Path.Combine(artifacts, "snapshot.json");
        PairedSessionRecorder.WriteSessionFileAtomic(output,
            JsonSerializer.Serialize(snapshot, new JsonSerializerOptions { WriteIndented = true }));
        Console.WriteLine($"Publication snapshot: {output}");
        return 0;

        static PublicationTiming Timing(
            BenchmarkReport[] reports, Type benchmark, object workload, string method)
        {
            var report = reports.Single(item =>
                item.BenchmarkCase.Descriptor.Type == benchmark &&
                GetWorkload(item.BenchmarkCase).Equals(workload) &&
                item.BenchmarkCase.Descriptor.WorkloadMethod.Name.StartsWith(method, StringComparison.Ordinal));
            var statistics = report.ResultStatistics;
            if (!report.Success || statistics is null || !double.IsFinite(statistics.Median) || statistics.Median <= 0)
                throw new InvalidOperationException($"BenchmarkDotNet produced no usable timing for {workload} / {method}.");
            return new PublicationTiming(
                statistics.Median,
                report.GcStats.GetBytesAllocatedPerOperation(report.BenchmarkCase),
                report.GcStats.Gen0Collections, report.GcStats.Gen1Collections, report.GcStats.Gen2Collections);
        }
    }

    private static object GetWorkload(BenchmarkCase benchmark) =>
        benchmark.Parameters.Items.Single(item => item.Name == "Workload").Value;

    private sealed record PublicationSnapshot(
        int Schema, string Date, string Source, string PackageLock, string BaselineVersion, string OperatingSystem,
        string Kernel, string Runtime, string Processor, string Affinity, string InstructionSet,
        List<PublicationMeasurement> Measurements);

    private sealed record PublicationMeasurement(
        string Case, int Rows, int Columns, int? InputBytes, int? LokadFileBytes, int? BaselineFileBytes,
        string? FixtureSha256, List<WriterMemorySample>? WriterMemory, PublicationTiming Lokad, PublicationTiming Baseline)
    {
        public double LokadRowsPerSecond => Rows * 1e9 / Lokad.MedianNanoseconds;
        public double BaselineRowsPerSecond => Rows * 1e9 / Baseline.MedianNanoseconds;
    }

    private sealed record PublicationTiming(double MedianNanoseconds, long? AllocatedBytes, int Gen0, int Gen1, int Gen2);
}
