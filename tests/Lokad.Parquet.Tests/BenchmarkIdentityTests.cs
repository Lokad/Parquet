using System.Diagnostics;

namespace Lokad.Parquet.Tests;

// B05: the benchmark input fingerprint covers linked sources and fixtures,
// so a changed input always changes the recorded revision.
public sealed class BenchmarkIdentityTests
{
    [Fact]
    public void IdentityPathsIncludeLinkedBuilder()
    {
        Assert.Contains("tests/Lokad.Parquet.Tests/ParquetFixtureBuilder.cs", IdentityPaths());
    }

    [Fact]
    public void IdentityPathsIncludeCommittedFixtures()
    {
        var paths = IdentityPaths();
        Assert.Contains("tests/fixtures/manifest.json", paths);
        Assert.Contains("tests/fixtures/apache-parquet-testing/binary.parquet", paths);
    }

    [Fact]
    public void IdentityPathsIncludeUntrackedFixtures()
    {
        var probe = Path.Combine(RepositoryTestPaths.Root, "tests", "fixtures", "b05-fingerprint-probe.txt");
        File.WriteAllText(probe, "probe");
        try
        {
            Assert.Contains("tests/fixtures/b05-fingerprint-probe.txt", IdentityPaths());
        }
        finally
        {
            File.Delete(probe);
        }
    }

    [Fact]
    public void SourceFingerprintChangesWithUntrackedFixture()
    {
        var before = Fingerprint();
        var probe = Path.Combine(RepositoryTestPaths.Root, "tests", "fixtures", "b05-fingerprint-probe.txt");
        File.WriteAllText(probe, "probe");
        try
        {
            Assert.NotEqual(before, Fingerprint());
        }
        finally
        {
            File.Delete(probe);
        }

        Assert.Equal(before, Fingerprint());

        static string Fingerprint() => InvokeIdentityLibrary("Get-BenchmarkSourceFingerprint -RepositoryRoot " + Quote(RepositoryTestPaths.Root)).Trim();
    }

    private static string[] IdentityPaths()
    {
        return InvokeIdentityLibrary("Get-BenchmarkIdentityPaths -RepositoryRoot " + Quote(RepositoryTestPaths.Root))
            .Split(["\r\n", "\n"], StringSplitOptions.RemoveEmptyEntries);
    }

    private static string InvokeIdentityLibrary(string command)
    {
        var library = Path.Combine(RepositoryTestPaths.Root, "bench", "benchmark-identity.ps1");
        var candidates = new[] { "pwsh", "powershell" };
        foreach (var candidate in candidates)
        {
            var startInfo = new ProcessStartInfo
            {
                FileName = candidate,
                UseShellExecute = false,
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                CreateNoWindow = true,
            };
            startInfo.ArgumentList.Add("-NoProfile");
            startInfo.ArgumentList.Add("-Command");
            startInfo.ArgumentList.Add(". '" + library.Replace("'", "''") + "'; " + command);
            Process? process = null;
            try
            {
                process = Process.Start(startInfo);
            }
            catch (System.ComponentModel.Win32Exception)
            {
            }

            if (process is null)
                continue;
            using (process)
            {
                var completed = process.WaitForExit(60000);
                var output = process.StandardOutput.ReadToEnd() + "\n" + process.StandardError.ReadToEnd();
                if (!completed)
                {
                    try
                    {
                        process.Kill();
                    }
                    catch (InvalidOperationException)
                    {
                    }
                    catch (System.ComponentModel.Win32Exception)
                    {
                    }

                    throw new InvalidOperationException("The benchmark identity library timed out.");
                }

                if (process.ExitCode != 0)
                    throw new InvalidOperationException("The benchmark identity library failed:\n" + output);
                return output;
            }
        }

        throw new InvalidOperationException("Neither pwsh nor powershell is available to verify the benchmark identity.");
    }

    private static string Quote(string value)
    {
        return "'" + value.Replace("'", "''") + "'";
    }
}
