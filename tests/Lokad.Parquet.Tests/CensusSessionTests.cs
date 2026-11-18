using System.Diagnostics;
using System.Reflection;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;

namespace Lokad.Parquet.Tests;

// Session-lifecycle coverage for the work census: an aborted session keeps its
// per-case checkpoints with a failure marker and no snapshot, a completed
// session binds its snapshot hash, session directories are unique, and a
// killed census stays visibly incomplete without a completion marker.
[Collection("Benchmark integration")]
public sealed class CensusSessionTests
{
    [Fact]
    public void AbortedSessionKeepsCheckpointsWithFailureMarkerAndNoSnapshot()
    {
        var root = NewTempRoot();
        try
        {
            var assembly = BenchmarkReflection.BenchmarkAssembly();
            var snapshotPath = Path.Combine(root, "work-census-windows-20260911-120000.json");
            var session = BeginSession(assembly, snapshotPath);
            var directory = SessionDirectory(session);
            var sessionId = SessionId(session);
            var probe = ProbeCase(assembly);
            var started = new DateTimeOffset(2026, 9, 11, 12, 0, 0, TimeSpan.Zero);
            RecordCheckpoint(assembly, session, 0, "Probe/First", started, started.AddSeconds(1), probe);
            RecordCheckpoint(assembly, session, 1, "Probe/Second", started.AddSeconds(1), started.AddSeconds(2), probe);
            AbortSession(assembly, session, "probe failure");
            AssertCheckpoint(directory, sessionId, 0, "Probe/First");
            AssertCheckpoint(directory, sessionId, 1, "Probe/Second");
            using (var metadata = ReadJson(Path.Combine(directory, "session.json")))
            {
                Assert.Equal("aborted", Assert.IsType<string>(Property(metadata, "status").GetString()));
                Assert.Equal("probe failure", Assert.IsType<string>(Property(metadata, "failure").GetString()));
                Assert.Equal(sessionId, Guid.Parse(Assert.IsType<string>(Property(metadata, "sessionId").GetString())));
            }
            Assert.False(File.Exists(Path.Combine(directory, "completed.json")));
            Assert.False(File.Exists(snapshotPath));
            var incomplete = ListIncomplete(assembly, root);
            var entry = Assert.Single(incomplete);
            Assert.Equal(directory, DescribeIncomplete(entry).Directory);
            Assert.Equal(2, DescribeIncomplete(entry).Checkpoints);
            Assert.Equal("aborted", DescribeIncomplete(entry).Status);
        }
        finally
        {
            Directory.Delete(root, recursive: true);
        }

        static void AbortSession(Assembly assembly, object session, string failure)
        {
            BenchmarkReflection.InvokeStatic(assembly, "Lokad.Parquet.Benchmarks.WorkCensusRunner", "AbortCensusSession", [session, failure]);
        }

        static void AssertCheckpoint(string directory, Guid sessionId, int order, string caseName)
        {
            using var checkpoint = ReadJson(Path.Combine(directory, $"checkpoint-{order:D2}.json"));
            Assert.Equal(sessionId, Guid.Parse(Assert.IsType<string>(Property(checkpoint, "sessionId").GetString())));
            Assert.Equal(order, Property(checkpoint, "order").GetInt32());
            Assert.Equal(caseName, Assert.IsType<string>(Property(checkpoint, "caseName").GetString()));
        }
    }

    [Fact]
    public void CompletedSessionBindsSnapshotHashAndClearsDiscovery()
    {
        var root = NewTempRoot();
        try
        {
            var assembly = BenchmarkReflection.BenchmarkAssembly();
            var snapshotPath = Path.Combine(root, "work-census-windows-20260911-120000.json");
            var session = BeginSession(assembly, snapshotPath);
            var directory = SessionDirectory(session);
            var sessionId = SessionId(session);
            var probe = ProbeCase(assembly);
            var started = new DateTimeOffset(2026, 9, 11, 12, 0, 0, TimeSpan.Zero);
            RecordCheckpoint(assembly, session, 0, "Probe/First", started, started.AddSeconds(1), probe);
            var payload = "{census:probe}";
            BenchmarkReflection.InvokeStatic(assembly, "Lokad.Parquet.Benchmarks.PairedSessionRecorder", "WriteSessionFileAtomic", [snapshotPath, payload]);
            var hash = Convert.ToHexStringLower(SHA256.HashData(Encoding.UTF8.GetBytes(payload)));
            CompleteSession(assembly, session, snapshotPath, hash, ["Probe/First"], 0);
            Assert.Equal(payload, File.ReadAllText(snapshotPath));
            using (var completion = ReadJson(Path.Combine(directory, "completed.json")))
            {
                Assert.Equal(sessionId, Guid.Parse(Property(completion, "sessionId").GetString() ?? ""));
                Assert.Equal(Path.GetFullPath(snapshotPath), Assert.IsType<string>(Property(completion, "snapshotPath").GetString()));
                Assert.Equal(hash, Assert.IsType<string>(Property(completion, "snapshotSha256").GetString()));
                Assert.Equal(1, Property(completion, "caseCount").GetInt32());
                Assert.Equal(0, Property(completion, "exitStatus").GetInt32());
            }
            using (var metadata = ReadJson(Path.Combine(directory, "session.json")))
            {
                Assert.Equal("completed", Assert.IsType<string>(Property(metadata, "status").GetString()));
                Assert.Equal(0, Property(metadata, "exitStatus").GetInt32());
            }
            Assert.Empty(ListIncomplete(assembly, root));
        }
        finally
        {
            Directory.Delete(root, recursive: true);
        }

        static void CompleteSession(Assembly assembly, object session, string snapshotPath, string snapshotSha256, string[] caseNames, int exitStatus)
        {
            BenchmarkReflection.InvokeStatic(assembly, "Lokad.Parquet.Benchmarks.WorkCensusRunner", "CompleteCensusSession", [session, snapshotPath, snapshotSha256, caseNames, exitStatus]);
        }
    }

    [Fact]
    public void CensusSessionDirectoriesAreUnique()
    {
        var root = NewTempRoot();
        try
        {
            var assembly = BenchmarkReflection.BenchmarkAssembly();
            var snapshotPath = Path.Combine(root, "work-census-windows-20260911-120000.json");
            var first = SessionDirectory(BeginSession(assembly, snapshotPath));
            var second = SessionDirectory(BeginSession(assembly, snapshotPath));
            Assert.NotEqual(first, second);
            Assert.True(Directory.Exists(first));
            Assert.True(Directory.Exists(second));
        }
        finally
        {
            Directory.Delete(root, recursive: true);
        }
    }

    [Fact]
    public async Task KilledCensusRetainsCheckpointsWithoutCompletionMarker()
    {
        if (!OperatingSystem.IsLinux())
            return;

        var testOutput = Path.GetDirectoryName(typeof(CensusSessionTests).Assembly.Location) ??
            throw new InvalidOperationException("The test assembly has no output directory.");
        var framework = Path.GetFileName(testOutput);
        var configuration = Directory.GetParent(testOutput)?.Name ??
            throw new InvalidOperationException("The test assembly has no configuration directory.");
        var benchmarkDll = Path.Combine(
            RepositoryTestPaths.Root,
            "bench",
            "Lokad.Parquet.Benchmarks",
            "bin",
            configuration,
            framework,
            "Lokad.Parquet.Benchmarks.dll");
        var workspace = Directory.CreateTempSubdirectory("lokad-census-probe-");
        try
        {
            var artifacts = Path.Combine(workspace.FullName, "artifacts", "benchmarks");
            var startInfo = new ProcessStartInfo("dotnet")
            {
                UseShellExecute = false,
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                CreateNoWindow = true,
                WorkingDirectory = workspace.FullName,
            };
            startInfo.ArgumentList.Add(benchmarkDll);
            startInfo.ArgumentList.Add("--census");
            using var process = Process.Start(startInfo) ??
                throw new InvalidOperationException("The census probe process failed to start.");
            var stdout = process.StandardOutput.ReadToEndAsync();
            var stderr = process.StandardError.ReadToEndAsync();
            try
            {
                var deadline = DateTimeOffset.UtcNow.AddMinutes(3);
                string? sessionDirectory = null;
                while (DateTimeOffset.UtcNow < deadline)
                {
                    if (Directory.Exists(artifacts))
                        sessionDirectory = Directory.GetDirectories(artifacts, "work-census-*.session").SingleOrDefault();
                    if (sessionDirectory is not null &&
                        File.Exists(Path.Combine(sessionDirectory, "checkpoint-01.json")))
                        break;
                    if (process.HasExited)
                        throw new InvalidOperationException("The census probe finished before its second checkpoint. " +
                            (await stdout) + (await stderr));
                    await Task.Delay(250);
                }

                Assert.NotNull(sessionDirectory);
                Assert.True(File.Exists(Path.Combine(sessionDirectory, "checkpoint-01.json")));
                await StopAsync();
                Assert.True(File.Exists(Path.Combine(sessionDirectory, "checkpoint-00.json")));
                Assert.False(File.Exists(Path.Combine(sessionDirectory, "completed.json")));
                Assert.Empty(Directory.GetFiles(artifacts, "work-census-*.json"));
                var incomplete = ListIncomplete(BenchmarkReflection.BenchmarkAssembly(), artifacts);
                Assert.Contains(incomplete, entry =>
                    string.Equals(DescribeIncomplete(entry).Directory, sessionDirectory, StringComparison.Ordinal));
            }
            finally
            {
                await StopAsync();
            }

            async Task StopAsync()
            {
                if (!process.HasExited)
                    process.Kill(entireProcessTree: true);
                using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));
                await process.WaitForExitAsync(timeout.Token);
            }
        }
        finally
        {
            workspace.Delete(recursive: true);
        }
    }

    private static object BeginSession(Assembly assembly, string snapshotPath)
    {
        return BenchmarkReflection.InvokeStatic(assembly, "Lokad.Parquet.Benchmarks.WorkCensusRunner", "BeginCensusSession", [snapshotPath, Guid.NewGuid(), DateTimeOffset.UtcNow]) ??
            throw new InvalidOperationException("The census session failed to begin.");
    }

    private static void RecordCheckpoint(Assembly assembly, object session, int order, string caseName, DateTimeOffset startedAtUtc, DateTimeOffset endedAtUtc, object result)
    {
        BenchmarkReflection.InvokeStatic(assembly, "Lokad.Parquet.Benchmarks.WorkCensusRunner", "RecordCensusCheckpoint", [session, order, caseName, startedAtUtc, endedAtUtc, result]);
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

    private static (string Directory, string? Status, int Checkpoints) DescribeIncomplete(object? entry)
    {
        var type = entry?.GetType() ??
            throw new InvalidOperationException("The incomplete session entry is unavailable.");
        return (
            Assert.IsType<string>(type.GetProperty("SessionDirectory")?.GetValue(entry)),
            type.GetProperty("Status")?.GetValue(entry) as string,
            Assert.IsType<int>(type.GetProperty("CheckpointCount")?.GetValue(entry)));
    }

    private static string SessionDirectory(object session)
    {
        return Assert.IsType<string>(session.GetType().GetProperty("SessionDirectory")?.GetValue(session));
    }

    private static Guid SessionId(object session)
    {
        return Assert.IsType<Guid>(session.GetType().GetProperty("SessionId")?.GetValue(session));
    }

    private static JsonDocument ReadJson(string path)
    {
        return JsonDocument.Parse(File.ReadAllText(path));
    }

    private static JsonElement Property(JsonDocument document, string name)
    {
        return document.RootElement.GetProperty(name);
    }

    private static object ProbeCase(Assembly assembly)
    {
        var caseType = BenchmarkReflection.RequireType(assembly, "Lokad.Parquet.Benchmarks.WorkCensusCase");
        var constructor = Assert.Single(caseType.GetConstructors());
        var arguments = constructor.GetParameters().Select(static parameter => ProbeValue(parameter.ParameterType)).ToArray();
        return constructor.Invoke(arguments);

        static object? ProbeValue(Type type)
        {
            if (type == typeof(string))
                return "probe";
            if (type.IsEnum)
                return Enum.GetValues(type).GetValue(0);
            if (type.IsArray)
            {
                var element = type.GetElementType() ?? typeof(byte);
                return Array.CreateInstance(element, 0);
            }
            if (type.IsInterface && type.IsGenericType)
                return Array.CreateInstance(type.GetGenericArguments()[0], 0);
            if (type.IsValueType)
                return Activator.CreateInstance(type);
            return null;
        }
    }



    private static string NewTempRoot()
    {
        var root = Path.Combine(Path.GetTempPath(), "lokad-census-session-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(root);
        return root;
    }

}
