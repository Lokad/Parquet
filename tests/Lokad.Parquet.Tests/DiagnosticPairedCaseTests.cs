using System.Reflection;
using System.Text.Json.Nodes;

namespace Lokad.Parquet.Tests;

// B06: reusable optional diagnostic paired cases stay outside the frozen claim
// while pairing one Lokad read path with a Parquet.NET counterpart each.
public sealed class DiagnosticPairedCaseTests
{
    [Fact]
    public void DiagnosticCasesAreListed()
    {
        var names = DiagnosticCaseNames();
        Assert.Contains("Diagnostic/SourceMemory", names);
        Assert.Contains("Diagnostic/SourceStream", names);
        Assert.Contains("Diagnostic/SourceFile", names);
        Assert.Contains("Diagnostic/SourceCustom", names);
    }

    [Fact]
    public async Task UnknownDiagnosticCaseIsRejected()
    {
        var assembly = BenchmarkAssembly();
        var factory = assembly.GetType("Lokad.Parquet.Benchmarks.DiagnosticPairedCases") ??
            throw new InvalidOperationException("The benchmark diagnostic factory is unavailable.");
        var create = factory.GetMethod("CreateAsync", BindingFlags.NonPublic | BindingFlags.Static) ??
            throw new InvalidOperationException("The benchmark diagnostic creation is unavailable.");
        var failure = await Assert.ThrowsAsync<ArgumentException>(async () =>
            await (Task)(create.Invoke(null, ["Diagnostic/Nope"]) ?? throw new InvalidOperationException("The benchmark diagnostic creation returned nothing.")));
        Assert.Contains("Diagnostic/Nope", failure.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task EveryDiagnosticCaseBuildsWithTruth()
    {
        // Creation runs both readers once with full truth checks, so equal
        // emitted values, nulls and ranges hold before any timing runs.
        var assembly = BenchmarkAssembly();
        var factory = assembly.GetType("Lokad.Parquet.Benchmarks.DiagnosticPairedCases") ??
            throw new InvalidOperationException("The benchmark diagnostic factory is unavailable.");
        var create = factory.GetMethod("CreateAsync", BindingFlags.NonPublic | BindingFlags.Static) ??
            throw new InvalidOperationException("The benchmark diagnostic creation is unavailable.");
        foreach (var name in DiagnosticCaseNames())
        {
            var built = create.Invoke(null, [name]) ??
                throw new InvalidOperationException($"The {name} diagnostic creation returned nothing.");
            var pending = (Task)built;
            await pending;
            await using var paired = (IAsyncDisposable)(pending.GetType().GetProperty("Result")?.GetValue(pending) ?? throw new InvalidOperationException($"The {name} diagnostic creation returned nothing."));
        }
    }

    [System.Runtime.Versioning.SupportedOSPlatform("windows")]
    [System.Runtime.Versioning.SupportedOSPlatform("linux")]
    [Fact]
    public async Task DiagnosticSourceCaseMeasuresPairedEvidence()
    {
        // One end-to-end paired run: 400 balanced observations with allocation
        // and GC evidence for a diagnostic source lane. The paired runner
        // requires single-processor affinity, so pin and restore the host.
        var assembly = BenchmarkAssembly();
        using var host = System.Diagnostics.Process.GetCurrentProcess();
        var previousAffinity = host.ProcessorAffinity;
        var previousPriority = OperatingSystem.IsWindows() ? host.PriorityClass : System.Diagnostics.ProcessPriorityClass.Normal;
        host.ProcessorAffinity = (nint)1;
        var working = Path.Combine(Path.GetTempPath(), "lokad-diagnostic-paired-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(working);
        try
        {
            var runner = assembly.GetType("Lokad.Parquet.Benchmarks.PairedParityRunner") ??
                throw new InvalidOperationException("The benchmark parity runner is unavailable.");
            var run = runner.GetMethod("RunAsync", BindingFlags.Public | BindingFlags.Static, [typeof(string[])]) ??
                throw new InvalidOperationException("The benchmark paired run is unavailable.");
            var output = Path.Combine(working, "paired.json");
            var exit = await (Task<int>)(run.Invoke(null, [(object)new string[] { "--paired-case", "Diagnostic/SourceMemory", "--paired-output", output }]) ?? throw new InvalidOperationException("The benchmark paired run returned nothing."));
            Assert.Equal(0, exit);
            var snapshot = Assert.IsType<JsonObject>(JsonNode.Parse(File.ReadAllText(output)));
            var cases = Assert.IsType<JsonArray>(snapshot["cases"]);
            Assert.Single(cases);
            var entry = Assert.IsType<JsonObject>(cases[0]);
            Assert.Equal("Diagnostic/SourceMemory", Assert.IsAssignableFrom<JsonNode>(entry["name"]).GetValue<string>());
            var observations = Assert.IsType<JsonArray>(entry["observations"]);
            Assert.Equal(400, observations.Count);
            foreach (var node in observations)
            {
                var observation = Assert.IsType<JsonObject>(node);
                Assert.True(Assert.IsAssignableFrom<JsonNode>(observation["lokadAllocatedBytes"]).GetValue<long>() >= 0);
                Assert.True(Assert.IsAssignableFrom<JsonNode>(observation["parquetNetAllocatedBytes"]).GetValue<long>() >= 0);
            }

            Assert.True(double.IsFinite(Assert.IsAssignableFrom<JsonNode>(entry["pointRatio"]).GetValue<double>()));
            Assert.True(double.IsFinite(Assert.IsAssignableFrom<JsonNode>(entry["upper95Ratio"]).GetValue<double>()));
        }
        finally
        {
            if (OperatingSystem.IsWindows())
                host.PriorityClass = previousPriority;
            host.ProcessorAffinity = previousAffinity;
            Directory.Delete(working, true);
        }
    }
    private static string[] DiagnosticCaseNames()
    {
        var assembly = BenchmarkAssembly();
        var factory = assembly.GetType("Lokad.Parquet.Benchmarks.DiagnosticPairedCases") ??
            throw new InvalidOperationException("The benchmark diagnostic factory is unavailable.");
        var names = factory.GetProperty("Names", BindingFlags.NonPublic | BindingFlags.Static)?.GetValue(null) as System.Collections.IEnumerable ??
            throw new InvalidOperationException("The benchmark diagnostic list is unavailable.");
        var result = new List<string>();
        foreach (var name in names)
            result.Add(Assert.IsType<string>(name));
        return result.ToArray();
    }

    private static Assembly BenchmarkAssembly()
    {
        var testOutput = Path.GetDirectoryName(typeof(DiagnosticPairedCaseTests).Assembly.Location) ??
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
