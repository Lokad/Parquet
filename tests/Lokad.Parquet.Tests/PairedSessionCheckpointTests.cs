using System.Reflection;
using System.Text.Json;

namespace Lokad.Parquet.Tests;

// Session-checkpoint coverage for paired parity campaigns: atomic session-file
// writes, the completeness-marker contract, and incomplete-session discovery.
// An interrupted session (metadata plus checkpoints but no marker) must stay
// visibly incomplete next to a fresh session; torn temporary files must never
// surface as checkpoints; sibling sessions must never be merged.
public sealed class PairedSessionCheckpointTests
{
    [Fact]
    public void AtomicWriteLeavesExactContentWithoutTempFiles()
    {
        var root = NewTempRoot();
        try
        {
            var path = Path.Combine(root, "session.json");
            BenchmarkReflection.InvokeStatic(BenchmarkReflection.BenchmarkAssembly(), "Lokad.Parquet.Benchmarks.PairedSessionRecorder", "WriteSessionFileAtomic", [path, "{\"sessionId\":\"abc\"}"]);
            Assert.Equal("{\"sessionId\":\"abc\"}", File.ReadAllText(path));
            Assert.Single(Directory.GetFiles(root));
            BenchmarkReflection.InvokeStatic(BenchmarkReflection.BenchmarkAssembly(), "Lokad.Parquet.Benchmarks.PairedSessionRecorder", "WriteSessionFileAtomic", [path, "{\"sessionId\":\"def\"}"]);
            Assert.Equal("{\"sessionId\":\"def\"}", File.ReadAllText(path));
            Assert.Single(Directory.GetFiles(root));
            var nested = Path.Combine(root, "fresh", "session.json");
            BenchmarkReflection.InvokeStatic(BenchmarkReflection.BenchmarkAssembly(), "Lokad.Parquet.Benchmarks.PairedSessionRecorder", "WriteSessionFileAtomic", [nested, "{}"]);
            Assert.Equal("{}", File.ReadAllText(nested));
        }
        finally
        {
            Directory.Delete(root, recursive: true);
        }
    }

    [Fact]
    public void InterruptedSessionStaysVisiblyIncompleteNextToFreshSession()
    {
        var root = NewTempRoot();
        try
        {
            var assembly = BenchmarkReflection.BenchmarkAssembly();
            var interrupted = Path.Combine(root, "paired-windows-20260909-120000.aaaabbbb.session");
            BenchmarkReflection.InvokeStatic(assembly, "Lokad.Parquet.Benchmarks.PairedSessionRecorder", "WriteSessionFileAtomic", [Path.Combine(interrupted, "session.json"), "{\"sessionId\":\"aaaabbbb\",\"status\":\"running\"}"]);
            BenchmarkReflection.InvokeStatic(assembly, "Lokad.Parquet.Benchmarks.PairedSessionRecorder", "WriteSessionFileAtomic", [Path.Combine(interrupted, "checkpoint-00.json"), "{\"sessionId\":\"aaaabbbb\",\"order\":0,\"caseName\":\"PreopenedScan/RequiredInt32Plain\"}"]);
            var fresh = Path.Combine(root, "paired-windows-20260909-130000.ccccdddd.session");
            BenchmarkReflection.InvokeStatic(assembly, "Lokad.Parquet.Benchmarks.PairedSessionRecorder", "WriteSessionFileAtomic", [Path.Combine(fresh, "session.json"), "{\"sessionId\":\"ccccdddd\",\"status\":\"running\"}"]);
            BenchmarkReflection.InvokeStatic(assembly, "Lokad.Parquet.Benchmarks.PairedSessionRecorder", "WriteSessionFileAtomic", [Path.Combine(fresh, "checkpoint-00.json"), "{\"sessionId\":\"ccccdddd\",\"order\":0,\"caseName\":\"PreopenedScan/RequiredInt32Plain\"}"]);
            BenchmarkReflection.InvokeStatic(assembly, "Lokad.Parquet.Benchmarks.PairedSessionRecorder", "WriteSessionFileAtomic", [Path.Combine(fresh, "checkpoint-01.json"), "{\"sessionId\":\"ccccdddd\",\"order\":1,\"caseName\":\"WarmMetadataOpen\"}"]);
            BenchmarkReflection.InvokeStatic(assembly, "Lokad.Parquet.Benchmarks.PairedSessionRecorder", "WriteSessionFileAtomic", [Path.Combine(fresh, "completed.json"), "{\"sessionId\":\"ccccdddd\",\"caseCount\":2,\"exitStatus\":0}"]);
            var incomplete = ListIncomplete(assembly, root);
            Assert.Single(incomplete);
            var entry = DescribeIncomplete(incomplete[0]);
            Assert.Equal(interrupted, entry.Directory);
            Assert.Equal("aaaabbbb", entry.SessionId);
            Assert.Equal("running", entry.Status);
            Assert.Equal(1, entry.Checkpoints);
            Assert.Equal(2, Directory.GetFiles(interrupted).Length);
            Assert.Equal(4, Directory.GetFiles(fresh).Length);
        }
        finally
        {
            Directory.Delete(root, recursive: true);
        }
    }

    [Fact]
    public void TornTempFilesAndMetadataLossNeverSurfaceAsCheckpoints()
    {
        var root = NewTempRoot();
        try
        {
            var assembly = BenchmarkReflection.BenchmarkAssembly();
            var torn = Path.Combine(root, "paired-linux-20260909-120000.eeeeffff.session");
            Directory.CreateDirectory(torn);
            File.WriteAllText(Path.Combine(torn, "session.json"), "{\"sessionId\":\"eeeeffff\",\"status\":\"running\"}");
            File.WriteAllBytes(Path.Combine(torn, "checkpoint-00.json.tmp-ab12"), [1, 2, 3, 4]);
            var earlyDeath = Path.Combine(root, "paired-linux-20260909-121000.gggghhhh.session");
            Directory.CreateDirectory(earlyDeath);
            var incomplete = ListIncomplete(assembly, root);
            Assert.Equal(2, incomplete.Count);
            var tornEntry = DescribeIncomplete(incomplete[0]);
            Assert.Equal(torn, tornEntry.Directory);
            Assert.Equal(0, tornEntry.Checkpoints);
            var earlyEntry = DescribeIncomplete(incomplete[1]);
            Assert.Equal(earlyDeath, earlyEntry.Directory);
            Assert.Null(earlyEntry.SessionId);
            Assert.Null(earlyEntry.Status);
            Assert.Equal(0, earlyEntry.Checkpoints);
            Assert.Empty(ListIncomplete(assembly, Path.Combine(root, "missing")));
        }
        finally
        {
            Directory.Delete(root, recursive: true);
        }
    }

    private static List<object?> ListIncomplete(Assembly assembly, string directory)
    {
        var found = BenchmarkReflection.InvokeStatic(assembly, "Lokad.Parquet.Benchmarks.PairedSessionRecorder", "FindIncompleteSessionDirectories", [directory]);
        var entries = Assert.IsAssignableFrom<System.Collections.IEnumerable>(found);
        var result = new List<object?>();
        foreach (var entry in entries)
            result.Add(entry);
        return result;
    }

    private static (string Directory, string? SessionId, string? Status, int Checkpoints) DescribeIncomplete(object? entry)
    {
        var type = entry?.GetType() ??
            throw new InvalidOperationException("The incomplete session entry is unavailable.");
        return (
            Assert.IsType<string>(type.GetProperty("SessionDirectory")?.GetValue(entry)),
            type.GetProperty("SessionId")?.GetValue(entry) as string,
            type.GetProperty("Status")?.GetValue(entry) as string,
            Assert.IsType<int>(type.GetProperty("CheckpointCount")?.GetValue(entry)));
    }

    private static string NewTempRoot()
    {
        var root = Path.Combine(Path.GetTempPath(), "lokad-paired-session-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(root);
        return root;
    }

}
