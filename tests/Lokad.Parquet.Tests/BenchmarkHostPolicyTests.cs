using System.Reflection;

namespace Lokad.Parquet.Tests;

// Pins the benchmark-host policy without referencing the benchmark project:
// pure mount-table, mask, and cpuinfo decisions run anywhere, while live
// machine reads only assert shape. Linux mount verdicts additionally run on
// native Linux in the B08 campaign; the synthetic tables below already cover
// a Windows-backed 9p mount, a symlinked resolution target, and a native
// mount placed under /mnt.
public sealed class BenchmarkHostPolicyTests
{
    [Fact]
    public void ParseMountTableEntries()
    {
        var content = "# synthetic mounts\n" +
            "C:\\ /mnt/c 9p ro,dirsync,aname=drvfs 0 0\n" +
            "/dev/sda1 / ext4 rw,relatime 0 1\n" +
            "/dev/sdb1 /mnt/data\\040set ext4 rw 0 0\n" +
            "malformed\n";
        var entries = Assert.IsAssignableFrom<System.Collections.IList>(CallPolicy("ParseMountTable", content));
        Assert.Equal(3, entries.Count);
        Assert.Equal("/mnt/c", ReadValue<string>(entries[0], "MountPoint"));
        Assert.Equal("9p", ReadValue<string>(entries[0], "FileSystem"));
        Assert.Equal("/mnt/data set", ReadValue<string>(entries[2], "MountPoint"));
        Assert.Equal("ext4", ReadValue<string>(entries[2], "FileSystem"));
    }

    [Fact]
    public void WindowsBackedClassification()
    {
        Assert.True(CallValue<bool>("IsWindowsBackedFileSystem", "9p"));
        Assert.True(CallValue<bool>("IsWindowsBackedFileSystem", "9P"));
        Assert.True(CallValue<bool>("IsWindowsBackedFileSystem", "plan9"));
        Assert.True(CallValue<bool>("IsWindowsBackedFileSystem", "drvfs"));
        Assert.False(CallValue<bool>("IsWindowsBackedFileSystem", "ext4"));
        Assert.False(CallValue<bool>("IsWindowsBackedFileSystem", "overlay"));
        Assert.False(CallValue<bool>("IsWindowsBackedFileSystem", "xfs"));
        Assert.False(CallValue<bool>("IsWindowsBackedFileSystem", "btrfs"));
        Assert.False(CallValue<bool>("IsWindowsBackedFileSystem", "tmpfs"));
        Assert.False(CallValue<bool>("IsWindowsBackedFileSystem", "fuseblk"));
    }

    [Fact]
    public void LongestPrefixMountMatching()
    {
        const string table = "/dev/sda1 / ext4 rw 0 1\n" +
            "C:\\ /mnt/c 9p ro 0 0\n" +
            "/dev/sdb1 /mnt/c/work ext4 rw 0 0\n";
        Assert.Equal("/mnt/c/work", ReadValue<string>(CallPolicy("FindMountEntry", "/mnt/c/work/file", table), "MountPoint"));
        Assert.Equal("/mnt/c", ReadValue<string>(CallPolicy("FindMountEntry", "/mnt/c/other", table), "MountPoint"));
        Assert.Equal("/", ReadValue<string>(CallPolicy("FindMountEntry", "/home/user", table), "MountPoint"));
        Assert.Equal("/mnt/c", ReadValue<string>(CallPolicy("FindMountEntry", "/mnt/c", table), "MountPoint"));
        Assert.Null(CallPolicy("FindMountEntry", "/mnt/c2/file", "C:\\ /mnt/c 9p ro 0 0\n"));
    }

    [Fact]
    public void NativeMountUnderMntAccepted()
    {
        const string table = "/dev/sda1 / ext4 rw 0 1\n/dev/sdb1 /mnt/data ext4 rw 0 0\n";
        var mount = CallPolicy("FindMountEntry", "/mnt/data/file", table);
        Assert.Equal("ext4", ReadValue<string>(mount, "FileSystem"));
        CallPolicy("RejectWindowsBackedMount", true, mount, "role", "/mnt/data/file");
    }

    [Fact]
    public void WindowsBackedMountRejected()
    {
        const string table = "/dev/sda1 / ext4 rw 0 1\nC:\\ /mnt/c 9p ro 0 0\n";
        var mount = CallPolicy("FindMountEntry", "/mnt/c/file", table);
        var thrown = Assert.Throws<TargetInvocationException>(() => CallPolicy("RejectWindowsBackedMount", true, mount, "snapshot output", "/mnt/c/file"));
        var inner = Assert.IsType<InvalidOperationException>(thrown.InnerException);
        Assert.Contains("snapshot output", inner.Message);
    }

    [Fact]
    public void MissingMountRejectedOnlyOnLinux()
    {
        var linux = Assert.Throws<TargetInvocationException>(() => CallPolicy("RejectWindowsBackedMount", true, null, "role", "/some/path"));
        Assert.IsType<InvalidOperationException>(linux.InnerException);
        CallPolicy("RejectWindowsBackedMount", false, null, "role", "/some/path");
        const string table = "/dev/sda1 / ext4 rw 0 1\nC:\\ /mnt/c 9p ro 0 0\n";
        var backed = CallPolicy("FindMountEntry", "/mnt/c/file", table);
        CallPolicy("RejectWindowsBackedMount", false, backed, "role", "/mnt/c/file");
    }

    [Fact]
    public void SelectedProcessorIndex()
    {
        Assert.Equal(0, CallValue<int>("GetSelectedLogicalProcessor", 1L));
        Assert.Equal(1, CallValue<int>("GetSelectedLogicalProcessor", 2L));
        Assert.Equal(31, CallValue<int>("GetSelectedLogicalProcessor", 0x80000000L));
        Assert.Equal(-1, CallValue<int>("GetSelectedLogicalProcessor", 3L));
        Assert.Equal(-1, CallValue<int>("GetSelectedLogicalProcessor", 0L));
        Assert.Equal(-1, CallValue<int>("GetSelectedLogicalProcessor", -1L));
        Assert.Equal("0x1", CallValue<string>("FormatAffinity", 1L));
        Assert.Equal("0xff", CallValue<string>("FormatAffinity", 255L));
        Assert.Equal(2L, CallValue<long>("ParseAffinityMask", "0x2"));
        Assert.Equal(16L, CallValue<long>("ParseAffinityMask", "10"));
        var malformed = Assert.Throws<TargetInvocationException>(() => CallPolicy("ParseAffinityMask", "zz"));
        Assert.IsType<InvalidOperationException>(malformed.InnerException);
        var empty = Assert.Throws<TargetInvocationException>(() => CallPolicy("ParseAffinityMask", string.Empty));
        Assert.IsType<ArgumentException>(empty.InnerException);
    }

    [Fact]
    public void ProcessorModelParsing()
    {
        const string cpuinfo = "processor\t: 0\nmodel name\t: First Model\n\nprocessor\t: 3\nmodel name\t: Third Model\n";
        Assert.Equal("First Model", CallValue<string>("FindProcessorModel", cpuinfo, 0));
        Assert.Equal("Third Model", CallValue<string>("FindProcessorModel", cpuinfo, 3));
        Assert.Equal("First Model", CallValue<string>("FindProcessorModel", cpuinfo, 1));
        Assert.Equal("First Model", CallValue<string>("FindProcessorModel", cpuinfo, -1));
        Assert.Equal("unrecorded", CallValue<string>("FindProcessorModel", string.Empty, 0));
    }

    [Fact]
    public void ResolveFinalPathIdentity()
    {
        var file = Path.GetTempFileName();
        try
        {
            Assert.Equal(Path.GetFullPath(file), CallValue<string>("ResolveFinalPath", file));
            var missing = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString("N"), "output.json");
            Assert.Equal(Path.GetFullPath(missing), CallValue<string>("ResolveFinalPath", missing));
        }
        finally
        {
            File.Delete(file);
        }
    }

    [Fact]
    public void WorkspaceEvidenceResolves()
    {
        var directory = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(directory);
        try
        {
            var evidence = CallPolicy("CheckNativeWorkspacePath", directory, "probe");
            Assert.Equal(Path.GetFullPath(directory), ReadValue<string>(evidence, "ResolvedPath"));
            Assert.Equal("probe", ReadValue<string>(evidence, "Role"));
            if (!OperatingSystem.IsLinux())
                Assert.False(ReadValue<bool>(evidence, "WindowsBacked"));
        }
        finally
        {
            Directory.Delete(directory, true);
        }
    }

    [Fact]
    public void RuntimeTuningEvidenceShape()
    {
        _ = CallValue<bool>("GetServerGarbageCollection");
        Assert.NotEmpty(CallValue<string>("GetGcLatencyMode"));
        Assert.NotEmpty(CallValue<string>("GetTieredCompilation"));
        Assert.NotEmpty(CallValue<string>("GetTieredPgo"));
        Assert.NotEmpty(CallValue<string>("GetProcessorName"));
    }

    private static object? CallPolicy(string name, params object?[] arguments)
    {
        var assembly = BenchmarkReflection.BenchmarkAssembly();
        var policy = BenchmarkReflection.RequireType(assembly, "Lokad.Parquet.Benchmarks.BenchmarkHostPolicy");
        var method = BenchmarkReflection.RequireStaticMethod(policy, name, null);
        return method.Invoke(null, arguments);
    }

    private static T CallValue<T>(string name, params object?[] arguments) => Assert.IsType<T>(CallPolicy(name, arguments));

    private static T ReadValue<T>(object? record, string property)
    {
        if (record is null)
            throw new InvalidOperationException("The evidence record is missing.");
        var target = record;
        var info = target.GetType().GetProperty(property) ??
            throw new InvalidOperationException("The evidence record has no property " + property + ".");
        return Assert.IsType<T>(info.GetValue(target));
    }
}
