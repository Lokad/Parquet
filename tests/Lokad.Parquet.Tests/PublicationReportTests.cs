using System.Text.Json.Nodes;

namespace Lokad.Parquet.Tests;

[Collection("Benchmark integration")]
public sealed class PublicationReportTests : IDisposable
{
    private readonly DirectoryInfo _directory = Directory.CreateTempSubdirectory("lokad-publication-");
    private string Report => Path.Combine(_directory.FullName, "BENCHMARKS.md");
    private static string Renderer => Path.Combine(RepositoryTestPaths.Root, "benchmark-report.ps1");

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    public async Task RendersAndVerifiesCompleteSnapshots(int schema)
    {
        File.WriteAllText(Path.Combine(_directory.FullName, "snapshot.json"), CreateSnapshot(schema).ToJsonString());
        var result = await RunDriverAsync("""
            param([string] $Renderer, [string] $Document)
            $ErrorActionPreference = "Stop"
            $snapshot = Join-Path $PSScriptRoot "snapshot.json"
            & $Renderer -PublicationSnapshot $snapshot -Document $Document
            & $Renderer -PublicationSnapshot $snapshot -Document $Document -Verify
            [IO.File]::AppendAllText($Document, "Changed")
            try {
                & $Renderer -PublicationSnapshot $snapshot -Document $Document -Verify
                throw "Unexpectedly verified a changed document."
            } catch {
                $_.Exception.Message
            }
            """);
        Assert.True(result.ExitCode == 0, result.Output);
        Assert.Contains("report differs", result.Output);
        var document = File.ReadAllText(Report);
        Assert.Equal(7, document.Split('\n').Count(line => line.StartsWith("| Read ") || line.StartsWith("| Write ")));
        Assert.Contains("| Read INT32, PLAIN | 65,536 | 10.00 |", document);
        Assert.Contains("| Write repeated UTF-8 | 65,536 | 4.00 / 8.00 |", document);
    }

    [Fact]
    public async Task RejectsMalformedPublications()
    {
        var problems = new (string Message, Action<JsonObject, JsonArray, JsonObject, JsonObject> Mutate)[]
        {
            ("must contain all", (_, cases, _, _) => cases.RemoveAt(cases.Count - 1)),
            ("missing case", (_, _, read, _) => read["Id"] = "unknown"),
            ("duplicate case", (_, cases, read, _) =>
                Assert.IsType<JsonObject>(cases[1])["Id"] = Assert.IsAssignableFrom<JsonNode>(read["Id"]).DeepClone()),
            ("invalid measurement", (_, _, read, _) => read["Rows"] = 1),
            ("invalid measurement", (_, _, read, _) => read["Columns"] = 2),
            ("requires both output sizes", (_, _, _, write) => write.Remove("LokadFileBytes")),
            ("requires both output digests", (_, _, _, write) => write.Remove("LokadFileSha256")),
            ("requires an input size and fixture digest", (_, _, read, _) => read.Remove("FixtureSha256")),
            ("invalid measurement", (_, _, read, _) => Assert.IsType<JsonObject>(read["Lokad"])["MedianNanoseconds"] = -1),
            ("invalid measurement", (_, _, read, _) => Assert.IsType<JsonObject>(read["Baseline"]).Remove("AllocatedBytes")),
            ("Invalid package lock hash", (snapshot, _, _, _) => snapshot.Remove("PackageLock")),
            ("Invalid instruction-set mode", (snapshot, _, _, _) => snapshot["InstructionSet"] = "Unknown"),
        };
        for (var index = 0; index < problems.Length; index++)
        {
            var snapshot = CreateSnapshot(2);
            var cases = Assert.IsType<JsonArray>(snapshot["Measurements"]);
            problems[index].Mutate(snapshot, cases, Assert.IsType<JsonObject>(cases[0]), Assert.IsType<JsonObject>(cases[^1]));
            File.WriteAllText(Path.Combine(_directory.FullName, $"{index:D2}.json"), snapshot.ToJsonString());
        }

        var result = await RunDriverAsync("""
            param([string] $Renderer, [string] $Document)
            $ErrorActionPreference = "Stop"
            $messages = foreach ($snapshot in Get-ChildItem -LiteralPath $PSScriptRoot -Filter "*.json" | Sort-Object Name) {
                try {
                    & $Renderer -PublicationSnapshot $snapshot.FullName -Document $Document
                    "Unexpectedly accepted $($snapshot.Name)"
                } catch {
                    $_.Exception.Message
                }
            }
            ConvertTo-Json -InputObject @($messages) -Compress
            """);
        Assert.True(result.ExitCode == 0, result.Output);
        var messages = Assert.IsType<JsonArray>(JsonNode.Parse(result.Output));
        Assert.Equal(problems.Length, messages.Count);
        for (var index = 0; index < problems.Length; index++)
            Assert.Contains(problems[index].Message, (string?)messages[index]);
        Assert.False(File.Exists(Report));
    }

    [Fact]
    public async Task CheckingCurrentSourceRejectsHistoricalFingerprints()
    {
        var snapshot = Path.Combine(_directory.FullName, "snapshot.json");
        File.WriteAllText(snapshot, CreateSnapshot(2).ToJsonString());
        var result = await BenchmarkProcess.RunAsync("pwsh",
            ["-NoProfile", "-NonInteractive", "-File", Renderer,
                "-PublicationSnapshot", snapshot, "-Document", Report, "-VerifyCurrentSource"]);
        Assert.NotEqual(0, result.ExitCode);
        Assert.Contains("source fingerprint does not match", result.Output);
    }

    private static JsonObject CreateSnapshot(int schema)
    {
        var catalogPath = Path.Combine(RepositoryTestPaths.Root, "bench", "Lokad.Parquet.Benchmarks", "PublicationCatalog.json");
        var catalog = Assert.IsType<JsonArray>(JsonNode.Parse(File.ReadAllText(catalogPath)));
        var measurements = new JsonArray();
        foreach (var entry in catalog)
        {
            var definition = Assert.IsType<JsonObject>(entry);
            var writing = (string?)definition["Benchmark"] == "WriteBenchmarks";
            var measurement = new JsonObject
            {
                ["Case"] = Copy(definition, "Case"),
                ["Rows"] = Copy(definition, "Rows"),
                ["Columns"] = Copy(definition, "Columns"),
                ["Lokad"] = Timing(),
                ["Baseline"] = Timing(),
            };
            if (schema == 2)
                measurement["Id"] = Copy(definition, "Id");
            if (writing)
            {
                measurement["LokadFileBytes"] = 4096;
                measurement["BaselineFileBytes"] = 8192;
                if (schema == 2)
                {
                    measurement["LokadFileSha256"] = new string('d', 64);
                    measurement["BaselineFileSha256"] = new string('e', 64);
                }
            }
            else
            {
                measurement["InputBytes"] = 10240;
                measurement["FixtureSha256"] = new string('e', 64);
            }
            measurements.Add(measurement);
        }

        var snapshot = new JsonObject
        {
            ["Schema"] = schema,
            ["Date"] = "2026-11-18",
            ["Source"] = new string('a', 40),
            ["PackageLock"] = new string('b', 64),
            ["BaselineVersion"] = "6.1.0",
            ["OperatingSystem"] = "Linux fixture",
            ["Runtime"] = ".NET fixture",
            ["Processor"] = "Fixture CPU",
            ["Affinity"] = "0x1",
            ["InstructionSet"] = "Auto",
            ["Measurements"] = measurements,
        };
        if (schema == 2)
            snapshot["SourceFingerprint"] = new string('c', 64);
        return snapshot;

        static JsonNode Copy(JsonObject definition, string key) => Assert.IsAssignableFrom<JsonNode>(definition[key]).DeepClone();
        static JsonObject Timing() => new() { ["MedianNanoseconds"] = 1234.5, ["AllocatedBytes"] = 0 };
    }

    private Task<(int ExitCode, string Output)> RunDriverAsync(string script)
    {
        var driver = Path.Combine(_directory.FullName, "check-report.ps1");
        File.WriteAllText(driver, script);
        return BenchmarkProcess.RunAsync("pwsh",
            ["-NoProfile", "-NonInteractive", "-File", driver, "-Renderer", Renderer, "-Document", Report]);
    }

    public void Dispose() => _directory.Delete(recursive: true);
}
