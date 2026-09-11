using System.Security.Cryptography;

namespace Lokad.Parquet.Benchmarks;

// Reusable optional diagnostic paired cases: source and shape lanes that stay
// outside the frozen parity claim. Each case pairs one Lokad read path with a
// Parquet.NET counterpart over identical emitted rows; creation truth-checks
// both readers, and the paired protocol records allocation and GC evidence per
// observation. Diagnostic snapshots reuse the paired snapshot writer, while the
// parity report rejects non-catalog cases, so these never join the claim.
internal static class DiagnosticPairedCases
{
    internal static IReadOnlyList<string> Names { get; } =
    [
        "Diagnostic/SourceMemory",
        "Diagnostic/SourceStream",
        "Diagnostic/SourceFile",
        "Diagnostic/SourceCustom",
    ];

    internal static async Task<PairedOperationCase> CreateAsync(string name)
    {
        async Task<PairedOperationCase> CreateSourceCaseAsync(DiagnosticSource source)
        {
            const int rowCount = 65_536;
            var fixture = await ScanFixture.CreateAsync(ScanWorkload.RequiredInt32Plain, rowCount);
            var expected = fixture.Checksum;
            var fixtureHash = Convert.ToHexStringLower(SHA256.HashData(fixture.Bytes));
            var temporaryPath = string.Empty;
            if (source == DiagnosticSource.File)
            {
                temporaryPath = Path.Combine(Path.GetTempPath(), $"lokad-diagnostic-{Guid.NewGuid():N}.parquet");
                await File.WriteAllBytesAsync(temporaryPath, fixture.Bytes);
            }

            async Task<long> RunLokadCheckedAsync()
            {
                var checksum = source switch
                {
                    DiagnosticSource.Memory => await BenchmarkScan.ReadMemoryAsync(fixture.Bytes),
                    DiagnosticSource.Stream => await BenchmarkScan.ReadStreamAsync(fixture.Bytes),
                    DiagnosticSource.File => await BenchmarkScan.ReadFileAsync(temporaryPath),
                    _ => await BenchmarkScan.ReadCustomSourceAsync(fixture.Bytes),
                };
                if (checksum != expected)
                    throw new InvalidOperationException($"The {name} Lokad read produced an invalid checksum.");
                return checksum;
            }

            async Task<long> RunBaselineCheckedAsync()
            {
                // Parquet.NET exposes no custom random-access source: the stream
                // read is the counterpart for the memory, stream and custom lanes,
                // while the file lane compares file against file.
                var checksum = source == DiagnosticSource.File
                    ? await BenchmarkScan.ReadBaselineFileAsync(temporaryPath, expected)
                    : await BenchmarkScan.ReadBaselineStreamAsync(fixture.Bytes, expected);
                if (checksum != expected)
                    throw new InvalidOperationException($"The {name} baseline read produced an invalid checksum.");
                return checksum;
            }

            // Creation runs both readers once, so a broken lane fails here with
            // equal emitted values, nulls and ranges rather than mid-campaign.
            if (await RunLokadCheckedAsync() != expected || await RunBaselineCheckedAsync() != expected)
                throw new InvalidOperationException($"The {name} diagnostic case failed its creation truth check.");
            return new PairedOperationCase(
                name,
                fixtureHash,
                rowCount,
                1,
                0,
                expected,
                RunLokadCheckedAsync,
                RunBaselineCheckedAsync,
                () =>
                {
                    if (temporaryPath.Length != 0 && File.Exists(temporaryPath))
                        File.Delete(temporaryPath);
                    return ValueTask.CompletedTask;
                });
        }

        return name switch
        {
            "Diagnostic/SourceMemory" => await CreateSourceCaseAsync(DiagnosticSource.Memory),
            "Diagnostic/SourceStream" => await CreateSourceCaseAsync(DiagnosticSource.Stream),
            "Diagnostic/SourceFile" => await CreateSourceCaseAsync(DiagnosticSource.File),
            "Diagnostic/SourceCustom" => await CreateSourceCaseAsync(DiagnosticSource.Custom),
            _ => throw new ArgumentException($"Unknown diagnostic paired case '{name}'.", nameof(name)),
        };
    }

    private enum DiagnosticSource
    {
        Memory,
        Stream,
        File,
        Custom,
    }
}
