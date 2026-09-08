using System.Runtime.InteropServices;

namespace Lokad.Parquet.Benchmarks;

// Benchmark-only native-workspace qualification. This stays outside the
// shipped reader. On Linux it rejects `/mnt/`-prefixed workspace paths for every
// path that affects qualification (source checkout, build output, result output)
// and logs concrete runtime/CPU/filesystem/path evidence. The check is a path
// prefix only; it does not resolve symlinks or mount types. On Windows it logs
// the same paths and preserves power-mode reporting via the
// LOKAD_PARQUET_POWER_MODE environment value set by bench.ps1.
internal static class BenchmarkEnvironment
{
    internal static void EnsureNativeWorkspace(string repositoryRoot, string buildOutput, string artifactsOutput)
    {
        ArgumentException.ThrowIfNullOrEmpty(repositoryRoot);
        ArgumentException.ThrowIfNullOrEmpty(buildOutput);
        ArgumentException.ThrowIfNullOrEmpty(artifactsOutput);
        var repositoryFull = Path.GetFullPath(repositoryRoot);
        var buildFull = Path.GetFullPath(buildOutput);
        var artifactsFull = Path.GetFullPath(artifactsOutput);
        Console.WriteLine($"Repository root: {repositoryFull}");
        Console.WriteLine($"Build output: {buildFull}");
        Console.WriteLine($"Artifacts output: {artifactsFull}");
        Console.WriteLine($"Runtime: {RuntimeInformation.FrameworkDescription}");
        Console.WriteLine($"OS: {RuntimeInformation.OSDescription}");
        Console.WriteLine($"Architecture: {RuntimeInformation.ProcessArchitecture}");
        Console.WriteLine($"Processor: {GetProcessorName()}");
        Console.WriteLine($"Power mode: {Environment.GetEnvironmentVariable("LOKAD_PARQUET_POWER_MODE") ?? "unrecorded"}");
        if (OperatingSystem.IsLinux())
        {
            LogLinuxFileSystem("repository", repositoryFull);
            LogLinuxFileSystem("build", buildFull);
            LogLinuxFileSystem("artifacts", artifactsFull);
            LogLinuxSystemEvidence();
            EnsureNativeLinuxPath(repositoryFull, "repository");
            EnsureNativeLinuxPath(buildFull, "build output");
            EnsureNativeLinuxPath(artifactsFull, "artifacts output");
        }

        static bool IsWindowsBackedMount(string fullPath) =>
            fullPath.StartsWith("/mnt/", StringComparison.Ordinal);

        static void EnsureNativeLinuxPath(string fullPath, string role)
        {
            if (IsWindowsBackedMount(fullPath))
                throw new InvalidOperationException($"Benchmark {role} must run from a native Linux filesystem workspace, never a Windows-backed mount such as /mnt/c: {fullPath}");
        }

        static void LogLinuxFileSystem(string role, string fullPath)
        {
            try
            {
                var match = DriveInfo.GetDrives()
                    .Where(drive =>
                    {
                        try
                        {
                            return fullPath.StartsWith(drive.Name, StringComparison.Ordinal);
                        }
                        catch (IOException)
                        {
                            return false;
                        }
                        catch (UnauthorizedAccessException)
                        {
                            return false;
                        }
                    })
                    .OrderByDescending(drive => drive.Name.Length)
                    .FirstOrDefault();
                if (match is null)
                {
                    Console.WriteLine($"Filesystem ({role}): unknown for {fullPath}");
                    return;
                }
                Console.WriteLine($"Filesystem ({role}): {match.Name} format={match.DriveFormat} type={match.DriveType} path={fullPath}");
            }
            catch (IOException exception)
            {
                Console.WriteLine($"Filesystem ({role}): unavailable for {fullPath}: {exception.Message}");
            }
            catch (UnauthorizedAccessException exception)
            {
                Console.WriteLine($"Filesystem ({role}): unavailable for {fullPath}: {exception.Message}");
            }
        }

        static void LogLinuxSystemEvidence()
        {
            Console.WriteLine($"Kernel: {Environment.OSVersion}");
            foreach (var path in new[] { "/proc/version", "/etc/os-release" })
            {
                try
                {
                    if (!File.Exists(path))
                        continue;
                    var first = File.ReadLines(path).FirstOrDefault();
                    if (!string.IsNullOrWhiteSpace(first))
                        Console.WriteLine($"{path}: {first.Trim()}");
                }
                catch (IOException)
                {
                }
                catch (UnauthorizedAccessException)
                {
                }
            }
            try
            {
                const string governorPath = "/sys/devices/system/cpu/cpu0/cpufreq/scaling_governor";
                if (File.Exists(governorPath))
                    Console.WriteLine($"CPU governor: {File.ReadAllText(governorPath).Trim()}");
            }
            catch (IOException)
            {
            }
            catch (UnauthorizedAccessException)
            {
            }
        }

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
}
