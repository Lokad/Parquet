using System.Diagnostics;
using System.Runtime.InteropServices;

namespace Lokad.Parquet.Benchmarks;

// Single home for benchmark-host qualification: which logical processor the
// process runs on, what the runtime looks like, and whether a workspace path
// lives on a native filesystem. The paired/census runners, the BenchmarkDotNet
// worker setups, bench.ps1 (through --check-path), and benchmark-report.ps1
// share these answers so snapshots, logs, and the report gate the same
// evidence. Decision-relevant helpers are either pure over explicit inputs
// (mount tables, masks, cpuinfo text) or read the live machine through
// managed APIs only.
public static class BenchmarkHostPolicy
{
    // Explicit per-job affinity travels from the launcher to BenchmarkDotNet
    // workers through the environment because worker processes start from
    // their own entry point and must not rely on affinity inheritance alone.
    public const string AffinityEnvironmentVariable = "LOKAD_PARQUET_AFFINITY";

    public sealed record MountEntry(string MountPoint, string FileSystem, string Source);

    public sealed record WorkspacePathEvidence(string Role, string ResolvedPath, string MountPoint, string FileSystem, bool WindowsBacked);

    // Consolidates the former per-runner CPU collectors: PROCESSOR_IDENTIFIER
    // exists only on Windows, so Linux evidence names the model from cpuinfo.
    public static string GetProcessorName()
    {
        var identifier = Environment.GetEnvironmentVariable("PROCESSOR_IDENTIFIER");
        if (!string.IsNullOrEmpty(identifier))
            return identifier;
        if (OperatingSystem.IsLinux())
        {
            try
            {
                return FindProcessorModel(File.ReadAllText("/proc/cpuinfo"), -1);
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

    // Names the exact core a bound worker runs on. Hybrid x86 parts report a
    // different model per core, so the selected logical processor picks its own
    // block; a negative index keeps the historic first-model behavior.
    public static string GetProcessorModelForCpu(int logicalProcessor)
    {
        if (!OperatingSystem.IsLinux())
            return GetProcessorName();
        try
        {
            return FindProcessorModel(File.ReadAllText("/proc/cpuinfo"), logicalProcessor);
        }
        catch (IOException)
        {
            return GetProcessorName();
        }
        catch (UnauthorizedAccessException)
        {
            return GetProcessorName();
        }
    }

    public static string FindProcessorModel(string cpuinfoContent, int logicalProcessor)
    {
        var current = -1;
        var fallback = "unrecorded";
        var recordedFallback = false;
        foreach (var rawLine in cpuinfoContent.Split('\n'))
        {
            var line = rawLine.TrimEnd();
            if (line.StartsWith("processor", StringComparison.Ordinal))
            {
                var colon = line.IndexOf(':');
                if (colon >= 0 && int.TryParse(line.Substring(colon + 1).Trim(), out var identifier))
                    current = identifier;
            }
            else if (line.StartsWith("model name", StringComparison.Ordinal))
            {
                var colon = line.IndexOf(':');
                if (colon < 0)
                    continue;
                var model = line.Substring(colon + 1).Trim();
                if (model.Length == 0)
                    continue;
                if (!recordedFallback)
                {
                    fallback = model;
                    recordedFallback = true;
                }
                if (current == logicalProcessor)
                    return model;
            }
        }
        return fallback;
    }

    // Bit index of a single-bit mask, or -1 when the mask names zero or
    // several logical processors.
    public static int GetSelectedLogicalProcessor(long affinityMask)
    {
        if (affinityMask <= 0 || (affinityMask & (affinityMask - 1)) != 0)
            return -1;
        var index = 0;
        while ((affinityMask & 1L) == 0)
        {
            affinityMask >>= 1;
            index++;
        }
        return index;
    }

    public static string FormatAffinity(long affinityMask) =>
        "0x" + affinityMask.ToString("x");

    public static long ParseAffinityMask(string text)
    {
        ArgumentException.ThrowIfNullOrEmpty(text);
        var digits = text.StartsWith("0x", StringComparison.OrdinalIgnoreCase) ? text.Substring(2) : text;
        long mask;
        try
        {
            mask = Convert.ToInt64(digits, 16);
        }
        catch (Exception exception) when (exception is FormatException || exception is OverflowException || exception is ArgumentException)
        {
            throw new InvalidOperationException("Invalid processor affinity mask: " + text + ".", exception);
        }
        if (mask <= 0)
            throw new InvalidOperationException("Invalid processor affinity mask: " + text + ".");
        return mask;
    }

    // Applies the launcher-provided mask when present, otherwise keeps the
    // inherited mask, then proves the process runs on exactly one logical
    // processor. Workers call this from their own setup because inheritance
    // alone is never asserted where timing happens.
    public static long ApplySingleProcessorAffinity()
    {
        using var process = Process.GetCurrentProcess();
        long mask;
        if (OperatingSystem.IsWindows() || OperatingSystem.IsLinux())
        {
            mask = process.ProcessorAffinity.ToInt64();
            var overrideText = Environment.GetEnvironmentVariable(AffinityEnvironmentVariable);
            if (!string.IsNullOrEmpty(overrideText))
            {
                mask = ParseAffinityMask(overrideText.Trim());
                process.ProcessorAffinity = (nint)mask;
                mask = process.ProcessorAffinity.ToInt64();
            }
        }
        else
            throw new PlatformNotSupportedException("Benchmark processor affinity requires Windows or Linux.");
        if (mask == 0 || (mask & (mask - 1)) != 0)
            throw new InvalidOperationException("The benchmark process is not bound to exactly one logical processor: " + FormatAffinity(mask) + ".");
        return mask;
    }

    public static bool GetServerGarbageCollection() =>
        System.Runtime.GCSettings.IsServerGC;

    public static string GetGcLatencyMode() =>
        System.Runtime.GCSettings.LatencyMode.ToString();

    public static string GetTieredCompilation() =>
        DescribeSwitch("System.Runtime.TieredCompilation");

    public static string GetTieredPgo() =>
        DescribeSwitch("System.Runtime.TieredPGO");

    static string DescribeSwitch(string name) =>
        AppContext.TryGetSwitch(name, out var enabled) ? (enabled ? "enabled" : "disabled") : "unrecorded";

    // Reports the evidence every BenchmarkDotNet worker setup prints into the
    // job log and proves the worker did not gain CPU capacity: one logical
    // processor, named core, runtime tuning, and native BDN artifact storage.
    public static void AssertWorkerEnvironment()
    {
        var mask = ApplySingleProcessorAffinity();
        var selected = GetSelectedLogicalProcessor(mask);
        Console.WriteLine($"Worker affinity: {FormatAffinity(mask)}; logical processor: {selected}; processors: {Environment.ProcessorCount}.");
        Console.WriteLine($"Worker processor: {GetProcessorModelForCpu(selected)}.");
        Console.WriteLine($"Worker runtime: {RuntimeInformation.FrameworkDescription} {RuntimeInformation.ProcessArchitecture}; server GC: {GetServerGarbageCollection()}; latency: {GetGcLatencyMode()}; tiered: {GetTieredCompilation()}; pgo: {GetTieredPgo()}.");
        var artifacts = Path.GetFullPath(Path.Combine(Directory.GetCurrentDirectory(), "BenchmarkDotNet.Artifacts"));
        var evidence = CheckNativeWorkspacePath(artifacts, "BDN artifacts");
        Console.WriteLine($"Worker artifacts: {evidence.ResolvedPath}; mount: {evidence.MountPoint} {evidence.FileSystem}.");
    }    // Resolves one link level without following anything: the leaf first,
    // then the deepest linked ancestor, so a symlinked parent directory
    // cannot smuggle a Windows-backed target past the mount check below.
    // Single-level reads never chase the target, which keeps dangling links
    // resolvable; the fixpoint loop in ResolveFinalPath follows chains while
    // a missing path simply is not a link. Other I/O failures stay loud so
    // an unreadable link can never qualify silently.
    public static string ResolveLinkStep(string absolutePath)
    {
        var full = Path.GetFullPath(absolutePath);
        var tail = string.Empty;
        var current = full;
        while (true)
        {
            if (string.Equals(current, Path.GetPathRoot(current), StringComparison.OrdinalIgnoreCase))
                return full;
            var target = ReadSingleLinkTarget(current);
            if (target is not null)
                return tail.Length == 0 ? target.FullName : Path.Combine(target.FullName, tail);
            var name = Path.GetFileName(current);
            var parent = Path.GetDirectoryName(current);
            if (string.IsNullOrEmpty(name) || parent is null || string.Equals(parent, current, StringComparison.Ordinal))
                return full;
            tail = tail.Length == 0 ? name : Path.Combine(name, tail);
            current = parent;
        }
    }

    public static FileSystemInfo? ReadSingleLinkTarget(string linkPath)
    {
        FileSystemInfo info;
        if (Directory.Exists(linkPath))
            info = new DirectoryInfo(linkPath);
        else
            info = new FileInfo(linkPath);
        try
        {
            return info.ResolveLinkTarget(returnFinalTarget: false);
        }
        catch (FileNotFoundException)
        {
            return null;
        }
        catch (DirectoryNotFoundException)
        {
            return null;
        }
    }

    public static string ResolveFinalPath(string path)
    {
        var current = Path.GetFullPath(path);
        for (var depth = 0; depth < 40; depth++)
        {
            var stepped = ResolveLinkStep(current);
            if (string.Equals(stepped, current, StringComparison.Ordinal))
                return current;
            current = stepped;
        }
        throw new InvalidOperationException("Too many symbolic-link levels while resolving " + path + ".");
    }

    // Parses /proc/self/mounts content: source, mount point, and file system
    // are the first three blank-separated fields with spaces escaped as \040
    // (plus \011, \012, \134).
    public static IReadOnlyList<MountEntry> ParseMountTable(string content)
    {
        var entries = new List<MountEntry>();
        foreach (var rawLine in content.Split('\n'))
        {
            var line = rawLine.TrimEnd();
            if (line.Length == 0 || line.StartsWith('#'))
                continue;
            var fields = line.Split(' ', StringSplitOptions.RemoveEmptyEntries);
            if (fields.Length < 3)
                continue;
            entries.Add(new MountEntry(Unescape(fields[1]), fields[2], Unescape(fields[0])));
        }
        return entries;

        static string Unescape(string value) =>
            value.Replace("\\040", " ").Replace("\\011", "\t").Replace("\\012", "\n").Replace("\\134", "\\");
    }

    // Longest-prefix mount wins with segment boundaries, so /mnt/c2 never
    // matches a /mnt/c mount. A null result means no identifiable mount.
    // Parses a mount table and returns the winning entry for a resolved path,
    // or null when no mount covers it. Diagnostics share this with
    // CheckNativeWorkspacePath instead of reimplementing table handling.
    public static MountEntry? FindMountEntry(string resolvedPath, string mountTableContent) =>
        FindMount(resolvedPath, ParseMountTable(mountTableContent));

    public static MountEntry? FindMount(string resolvedPath, IReadOnlyList<MountEntry> mounts)
    {
        MountEntry? best = null;
        foreach (var mount in mounts)
        {
            var point = mount.MountPoint;
            var within = point.Length == 1
                ? resolvedPath.StartsWith("/", StringComparison.Ordinal)
                : resolvedPath.Equals(point, StringComparison.Ordinal) ||
                    resolvedPath.StartsWith(point + "/", StringComparison.Ordinal);
            if (within && (best is null || point.Length > best.MountPoint.Length))
                best = mount;
        }
        return best;
    }

    // WSL2 exposes Windows drives as 9p (plan9 protocol family); WSL1 used
    // drvfs. Native Linux disks report ext4, xfs, btrfs, overlay, or tmpfs,
    // including bind mounts placed anywhere under /mnt, so the mount type
    // decides instead of the path prefix.
    public static bool IsWindowsBackedFileSystem(string fileSystem) =>
        string.Equals(fileSystem, "9p", StringComparison.OrdinalIgnoreCase) ||
        string.Equals(fileSystem, "plan9", StringComparison.OrdinalIgnoreCase) ||
        string.Equals(fileSystem, "drvfs", StringComparison.OrdinalIgnoreCase);

    public static void RejectWindowsBackedMount(bool isLinux, MountEntry? mount, string role, string resolvedPath)
    {
        if (!isLinux)
            return;
        if (mount is null)
            throw new InvalidOperationException("Benchmark " + role + " has no identifiable Linux mount; refusing to qualify " + resolvedPath + " as native.");
        if (IsWindowsBackedFileSystem(mount.FileSystem))
            throw new InvalidOperationException("Benchmark " + role + " must run from a native Linux filesystem workspace, never a Windows-backed mount (" + mount.FileSystem + " on " + mount.MountPoint + "): " + resolvedPath);
    }

    public static string ReadMountTable()
    {
        const string tablePath = "/proc/self/mounts";
        try
        {
            return File.ReadAllText(tablePath);
        }
        catch (IOException exception)
        {
            throw new InvalidOperationException("Linux mount table " + tablePath + " is unreadable; refusing to qualify the workspace as native.", exception);
        }
        catch (UnauthorizedAccessException exception)
        {
            throw new InvalidOperationException("Linux mount table " + tablePath + " is unreadable; refusing to qualify the workspace as native.", exception);
        }
    }

    // Resolves links, then enforces native storage on Linux and reports the
    // evidence for snapshots and logs. Off Linux the evidence still resolves
    // so reports name concrete paths, but nothing is rejected.
    public static WorkspacePathEvidence CheckNativeWorkspacePath(string path, string role)
    {
        ArgumentException.ThrowIfNullOrEmpty(path);
        ArgumentException.ThrowIfNullOrEmpty(role);
        var resolved = ResolveFinalPath(path);
        MountEntry? mount = null;
        if (OperatingSystem.IsLinux())
            mount = FindMount(resolved, ParseMountTable(ReadMountTable()));
        RejectWindowsBackedMount(OperatingSystem.IsLinux(), mount, role, resolved);
        return new WorkspacePathEvidence(
            role,
            resolved,
            mount?.MountPoint ?? "unrecorded",
            mount?.FileSystem ?? "unrecorded",
            OperatingSystem.IsLinux() && mount is not null && IsWindowsBackedFileSystem(mount.FileSystem));
    }
}
