using System.Runtime.InteropServices;

namespace Lokad.Parquet.Benchmarks;

// Benchmark-only native-workspace qualification. This stays outside the
// shipped reader. Path verdicts come from BenchmarkHostPolicy alone: links
// resolve to a final path and the Linux mount table decides whether the
// storage is Windows-backed, so symlinks cannot smuggle foreign storage past
// the check and native mounts under /mnt stay accepted. On other operating
// systems the same evidence resolves and prints without rejecting.
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
        Console.WriteLine($"Processor: {BenchmarkHostPolicy.GetProcessorName()}");
        Console.WriteLine($"Power mode: {Environment.GetEnvironmentVariable("LOKAD_PARQUET_POWER_MODE") ?? "unrecorded"}");
        if (OperatingSystem.IsLinux())
            LogLinuxSystemEvidence();
        foreach (var (path, role) in new[] { (repositoryFull, "repository"), (buildFull, "build output"), (artifactsFull, "artifacts output") })
        {
            var evidence = BenchmarkHostPolicy.CheckNativeWorkspacePath(path, role);
            Console.WriteLine($"Workspace ({evidence.Role}): {evidence.ResolvedPath}; mount: {evidence.MountPoint} {evidence.FileSystem}.");
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
    }
}
