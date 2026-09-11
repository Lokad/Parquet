using System.Text.Json;

namespace Lokad.Parquet.Benchmarks;

// Lifecycle records for one paired parity session. The session directory sits
// next to its snapshot and carries the session metadata, one atomic checkpoint
// per finished case, and a final completeness marker. A directory without the
// marker is visibly incomplete: it never qualifies as evidence and is never
// merged into another session. File contract (camelCase JSON):
// session.json { sessionId, status ("running", "completed" or "aborted") },
// checkpoint-NN.json { sessionId, order, caseName, result }, completed.json.
// The work census reuses the same contract with census-shaped payloads:
// session.json { sessionId, status, snapshotPath },
// checkpoint-NN.json { sessionId, order, caseName, result }, completed.json
// { sessionId, snapshotPath, snapshotSha256, caseNames }. A failed census keeps
// its checkpoints with an aborted marker and no snapshot, so partial evidence
// stays visibly non-qualifying without rebuilding this recorder.
internal sealed record PairedSessionMetadata(
    Guid SessionId,
    int SchemaVersion,
    string Status,
    DateTimeOffset StartedAtUtc,
    DateTimeOffset? FinishedAtUtc,
    string MachineName,
    int ProcessId,
    string SourceRevision,
    string RunnerFingerprint,
    string PackageLockHash,
    string Runtime,
    string OperatingSystem,
    string Architecture,
    string? SelectedCase,
    string SnapshotPath,
    string? Failure,
    int? ExitStatus);

internal sealed record PairedCaseCheckpoint(
    Guid SessionId,
    int Order,
    string CaseName,
    DateTimeOffset StartedAtUtc,
    DateTimeOffset EndedAtUtc,
    PairedCaseResult Result);

internal sealed record PairedSessionCompletion(
    Guid SessionId,
    DateTimeOffset FinishedAtUtc,
    string SnapshotPath,
    string SnapshotSha256,
    int CaseCount,
    string[] CaseNames,
    bool AllPassed,
    int ExitStatus);

internal sealed record IncompletePairedSession(
    string SessionDirectory,
    string? SessionId,
    string? Status,
    int CheckpointCount);

internal sealed record CensusSessionMetadata(
    Guid SessionId,
    int SchemaVersion,
    string Status,
    DateTimeOffset StartedAtUtc,
    DateTimeOffset? FinishedAtUtc,
    string MachineName,
    int ProcessId,
    string SourceRevision,
    string RunnerFingerprint,
    string PackageLockHash,
    string Runtime,
    string OperatingSystem,
    string Architecture,
    string SnapshotPath,
    string? Failure,
    int? ExitStatus);

internal sealed record CensusCaseCheckpoint(
    Guid SessionId,
    int Order,
    string CaseName,
    DateTimeOffset StartedAtUtc,
    DateTimeOffset EndedAtUtc,
    WorkCensusCase Result);

internal sealed record CensusSessionCompletion(
    Guid SessionId,
    DateTimeOffset FinishedAtUtc,
    string SnapshotPath,
    string SnapshotSha256,
    int CaseCount,
    string[] CaseNames,
    int ExitStatus);

internal sealed record CensusSession(
    string SessionDirectory,
    Guid SessionId,
    DateTimeOffset StartedAtUtc,
    string SnapshotPath);

internal static class PairedSessionRecorder
{
    internal static JsonSerializerOptions SessionJson { get; } = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        WriteIndented = true,
    };

    // Atomic session-file write: a reader never observes a torn checkpoint
    // because the payload is fully written under a temporary name and then
    // renamed over the destination on the same volume.
    internal static void WriteSessionFileAtomic(string path, string content)
    {
        var directory = Path.GetDirectoryName(path) ??
            throw new ArgumentException("The session file needs a directory.", nameof(path));
        Directory.CreateDirectory(directory);
        var temporaryPath = Path.Combine(directory, Path.GetRandomFileName());
        File.WriteAllText(temporaryPath, content);
        File.Move(temporaryPath, path, overwrite: true);
    }

    // Incomplete-session discovery for concurrent-campaign detection: every
    // session directory without a completeness marker is reported, including
    // directories whose metadata never landed after an early process death.
    // Checkpoint-looking temporary files are never parsed.
    internal static IReadOnlyList<IncompletePairedSession> FindIncompleteSessionDirectories(string directory)
    {
        var incomplete = new List<IncompletePairedSession>();
        string[] candidates;
        try
        {
            candidates = Directory.GetDirectories(directory, "*.session");
        }
        catch (DirectoryNotFoundException)
        {
            return incomplete;
        }

        foreach (var candidate in candidates.OrderBy(static path => path, StringComparer.Ordinal))
        {
            if (File.Exists(Path.Combine(candidate, "completed.json")))
                continue;
            var sessionId = default(string?);
            var status = default(string?);
            try
            {
                using var metadata = JsonDocument.Parse(File.ReadAllText(Path.Combine(candidate, "session.json")));
                if (metadata.RootElement.TryGetProperty("sessionId", out var sessionProperty))
                    sessionId = sessionProperty.GetString();
                if (metadata.RootElement.TryGetProperty("status", out var statusProperty))
                    status = statusProperty.GetString();
            }
            catch (IOException)
            {
            }
            catch (JsonException)
            {
            }
            catch (UnauthorizedAccessException)
            {
            }

            incomplete.Add(new IncompletePairedSession(
                candidate,
                sessionId,
                status,
                Directory.GetFiles(candidate, "checkpoint-*.json").Length));
        }

        return incomplete;
    }
}
