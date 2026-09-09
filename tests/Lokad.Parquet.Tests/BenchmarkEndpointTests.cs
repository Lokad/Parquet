using System.Reflection;
using System.Runtime.CompilerServices;

namespace Lokad.Parquet.Tests;

// Pins the frozen benchmark-endpoint catalog: every BenchmarkDotNet benchmark
// method in the benchmark assembly belongs to exactly one cataloged endpoint,
// every catalog entry resolves to a real benchmark, the endpoint id set is
// frozen, and checksum-only, sentinel, kernel, open-only, and diagnostic
// probes stay flagged as non-pipeline evidence.
public sealed class BenchmarkEndpointTests
{
    [Fact]
    public void EveryBenchmarkBelongsToExactlyOneEndpoint()
    {
        var assembly = LoadBenchmarkAssembly();
        var benchmarks = new List<string>();
        foreach (var type in assembly.GetTypes())
        {
            if (type.IsDefined(typeof(CompilerGeneratedAttribute), false))
                continue;
            foreach (var method in type.GetMethods(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance | BindingFlags.Static | BindingFlags.DeclaredOnly))
            {
                var benchmarked = false;
                foreach (var attribute in method.GetCustomAttributesData())
                {
                    if (attribute.AttributeType.FullName == "BenchmarkDotNet.Attributes.BenchmarkAttribute")
                        benchmarked = true;
                }
                if (benchmarked)
                    benchmarks.Add(type.Name + "." + method.Name);
            }
        }
        var endpointsType = assembly.GetType("Lokad.Parquet.Benchmarks.BenchmarkEndpoints") ??
            throw new InvalidOperationException("The benchmark endpoint catalog is unavailable.");
        var entries = Assert.IsAssignableFrom<System.Collections.IList>(
            endpointsType.GetField("All")?.GetValue(null) ??
            throw new InvalidOperationException("The benchmark endpoint catalog entries are unavailable."));
        var claimed = new List<string>();
        var identifiers = new HashSet<string>(StringComparer.Ordinal);
        foreach (var entry in entries)
        {
            if (entry is null)
                throw new InvalidOperationException("The benchmark endpoint catalog contains a missing entry.");
            var entryType = entry.GetType();
            var benchmarkType = Assert.IsType<string>(entryType.GetProperty("BenchmarkType")?.GetValue(entry));
            var benchmarkMethod = Assert.IsType<string>(entryType.GetProperty("BenchmarkMethod")?.GetValue(entry));
            var identifier = Assert.IsType<string>(entryType.GetProperty("Id")?.GetValue(entry));
            identifiers.Add(identifier);
            claimed.Add(benchmarkType + "." + benchmarkMethod);
        }
        var frozen = new HashSet<string>(
            ["open", "preopened-scan", "open-scan-required", "open-scan-workloads", "consumer-only", "materialization", "source-io", "decoder-codec"],
            StringComparer.Ordinal);
        Assert.True(frozen.SetEquals(identifiers), "Benchmark endpoint ids changed: " + string.Join(",", identifiers.OrderBy(static identifier => identifier)));
        Assert.Empty(benchmarks.Except(claimed, StringComparer.Ordinal));
        Assert.Empty(claimed.Except(benchmarks, StringComparer.Ordinal));
        Assert.Equal(benchmarks.Count, claimed.Count);
    }

    [Fact]
    public void NonPipelineProbesStayLabeled()
    {
        var assembly = LoadBenchmarkAssembly();
        var endpointsType = assembly.GetType("Lokad.Parquet.Benchmarks.BenchmarkEndpoints") ??
            throw new InvalidOperationException("The benchmark endpoint catalog is unavailable.");
        var entries = Assert.IsAssignableFrom<System.Collections.IList>(
            endpointsType.GetField("All")?.GetValue(null) ??
            throw new InvalidOperationException("The benchmark endpoint catalog entries are unavailable."));
        var expectedNonPipeline = new HashSet<string>(
            [
                "MetadataOpenBenchmarks.LokadOpen",
                "MetadataOpenBenchmarks.ParquetNetOpen",
                "RequiredInt32Benchmarks.LokadPublicAccessorDiagnostic",
                "SteadyStateScanBenchmarks.Scan",
                "PlainInt32KernelBenchmarks.EightColumnScan",
                "PlainInt32KernelBenchmarks.ArrayChecksum",
                "PlainInt32KernelBenchmarks.MemorySpanChecksum",
                "PreopenedMaterializationBenchmarks.LokadMaterialize",
                "PreopenedMaterializationBenchmarks.ParquetNetMaterialize",
                "PlainInt32KernelBenchmarks.Decode",
                "SnappyCodecBenchmarks.Decode",
            ],
            StringComparer.Ordinal);
        var actualNonPipeline = new HashSet<string>(StringComparer.Ordinal);
        var customSourcePipeline = false;
        foreach (var entry in entries)
        {
            if (entry is null)
                throw new InvalidOperationException("The benchmark endpoint catalog contains a missing entry.");
            var entryType = entry.GetType();
            var key = Assert.IsType<string>(entryType.GetProperty("BenchmarkType")?.GetValue(entry)) + "." +
                Assert.IsType<string>(entryType.GetProperty("BenchmarkMethod")?.GetValue(entry));
            if (!Assert.IsType<bool>(entryType.GetProperty("PipelineEvidence")?.GetValue(entry)))
                actualNonPipeline.Add(key);
            if (key == "SourceScanBenchmarks.CustomSource" && Assert.IsType<bool>(entryType.GetProperty("PipelineEvidence")?.GetValue(entry)))
                customSourcePipeline = true;
        }
        Assert.True(expectedNonPipeline.SetEquals(actualNonPipeline), "Non-pipeline probes changed: " + string.Join(",", actualNonPipeline.OrderBy(static key => key)));
        Assert.True(customSourcePipeline, "The custom-source endpoint must stay pipeline evidence.");
    }

    private static Assembly LoadBenchmarkAssembly()
    {
        var testAssembly = typeof(BenchmarkEndpointTests).Assembly;
        var testOutput = Path.GetDirectoryName(testAssembly.Location) ??
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
}
