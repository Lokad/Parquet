namespace Lokad.Parquet.Tests;

[Collection("Benchmark integration")]
public sealed class BenchmarkIdentityTests
{
    [Fact]
    public async Task IdentityPathsIncludeLinkedBuilderAndCommittedFixtures()
    {
        var output = await InvokeIdentityLibraryAsync("Get-BenchmarkIdentityPaths", RepositoryTestPaths.Root);
        var paths = output.Split(["\r\n", "\n"], StringSplitOptions.RemoveEmptyEntries);
        Assert.Contains("benchmark-report.ps1", paths);
        Assert.Contains("tests/Lokad.Parquet.Tests/ParquetFixtureBuilder.cs", paths);
        Assert.Contains("tests/fixtures/manifest.json", paths);
        Assert.Contains("tests/fixtures/apache-parquet-testing/binary.parquet", paths);
    }

    [Fact]
    public async Task FingerprintCoversTrackedAndUntrackedFixtureContent()
    {
        var repository = Directory.CreateTempSubdirectory("lokad-identity-");
        try
        {
            var root = repository.FullName;
            var fixtures = Directory.CreateDirectory(Path.Combine(root, "tests", "fixtures"));
            var tracked = Path.Combine(fixtures.FullName, "tracked.txt");
            File.WriteAllText(tracked, "tracked input");
            await GitAsync(root, "init", "--quiet", "--template=");
            await GitAsync(root, "add", "--", "tests/fixtures/tracked.txt");
            var expected = await InvokeIdentityLibraryAsync("Get-BenchmarkSourceFingerprint", root);
            Assert.Matches("^[0-9a-f]{64}$", expected);

            var probe = Path.Combine(fixtures.FullName, "untracked.txt");
            File.WriteAllText(probe, "untracked input");
            var paths = await InvokeIdentityLibraryAsync("Get-BenchmarkIdentityPaths", root);
            Assert.Contains("tests/fixtures/untracked.txt", paths);
            Assert.NotEqual(expected, await InvokeIdentityLibraryAsync("Get-BenchmarkSourceFingerprint", root));

            File.Delete(probe);
            File.WriteAllText(Path.Combine(root, "unrelated.txt"), "unrelated");
            Assert.Equal(expected, await InvokeIdentityLibraryAsync("Get-BenchmarkSourceFingerprint", root));

            File.WriteAllText(tracked, "changed tracked input");
            Assert.NotEqual(expected, await InvokeIdentityLibraryAsync("Get-BenchmarkSourceFingerprint", root));
        }
        finally
        {
            if (OperatingSystem.IsWindows())
            {
                foreach (var file in repository.EnumerateFiles("*", SearchOption.AllDirectories))
                    file.Attributes &= ~FileAttributes.ReadOnly;
            }
            repository.Delete(recursive: true);
        }

        static async Task GitAsync(string root, params string[] arguments)
        {
            var result = await BenchmarkProcess.RunAsync("git", ["-C", root, .. arguments]);
            Assert.True(result.ExitCode == 0, result.Output);
        }
    }

    private static async Task<string> InvokeIdentityLibraryAsync(string function, string repository)
    {
        var library = Path.Combine(RepositoryTestPaths.Root, "bench", "benchmark-identity.ps1");
        var result = await BenchmarkProcess.RunAsync("pwsh",
        [
            "-NoProfile", "-NonInteractive", "-Command",
            "$ErrorActionPreference = 'Stop'; . " + Quote(library) + "; " + function + " -RepositoryRoot " + Quote(repository),
        ]);
        Assert.True(result.ExitCode == 0, result.Output);
        return result.Output.Trim();

        static string Quote(string value) => "'" + value.Replace("'", "''") + "'";
    }
}
