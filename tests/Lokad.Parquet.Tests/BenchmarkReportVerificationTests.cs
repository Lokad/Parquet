using System.ComponentModel;
using System.Diagnostics;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace Lokad.Parquet.Tests;

// Builds the synthetic report quartets shared by the verifier facts below: a
// schema-8 quartet using the Cornish-Fisher Student-t bound, the same quartet
// with the frozen legacy 1.645 bound, a frozen schema-7 quartet, and the
// schema-7 quartet with a Cornish-Fisher bound. Every quartet carries two
// cases with 400 balanced AB/BA observations each, so -Verify exercises the
// full recompute without depending on real ignored snapshots.
public static class BenchmarkReportQuartet
{
    public static string Build(int schemaVersion, bool legacyInterval, bool materialize)
    {
        var root = Path.Combine(Path.GetTempPath(), "lokad-report-quartet-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(root);
        const string sourceRevision = "synthetic-source-revision";
        const string packageLockHash = "synthetic-package-lock";
        const string scanFixture = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
        const string metaFixture = "fedcba9876543210fedcba9876543210fedcba9876543210fedcba9876543210";
        var catalog = new JsonObject
        {
            ["pairedSchemaVersion"] = schemaVersion,
            ["sourceRevision"] = sourceRevision,
            ["packageLockHash"] = packageLockHash,
            ["cases"] = new JsonArray
            {
                new JsonObject
                {
                    ["name"] = "PreopenedScan/RequiredInt32Plain",
                    ["label"] = "Required INT32, PLAIN",
                },
                new JsonObject
                {
                    ["name"] = "WarmMetadataOpen",
                    ["label"] = "Warm metadata open",
                },
            },
        };
        File.WriteAllText(Path.Combine(root, "catalog.json"), catalog.ToJsonString(new JsonSerializerOptions { WriteIndented = true }));
        var recorded = new DateTimeOffset(2026, 9, 9, 12, 0, 0, TimeSpan.Zero);
        WritePaired(0, "Microsoft Windows 10.0.26100", "synthetic-windows-fingerprint", "0x1", recorded.AddMinutes(0));
        WritePaired(1, "Microsoft Windows 10.0.26100", "synthetic-windows-fingerprint", "0x2", recorded.AddMinutes(5));
        WritePaired(2, "Ubuntu 24.04 Linux 6.8", "synthetic-linux-fingerprint", "0x1", recorded.AddMinutes(10));
        WritePaired(3, "Ubuntu 24.04 Linux 6.8", "synthetic-linux-fingerprint", "0x2", recorded.AddMinutes(15)); var census = new JsonObject
        {
            ["schemaVersion"] = schemaVersion == 9 ? 4 : 2,
            ["recordedAtUtc"] = recorded.ToString("o"),
            ["sourceRevision"] = sourceRevision,
            ["runtime"] = "synthetic-runtime",
            ["operatingSystem"] = "synthetic-os",
            ["architecture"] = "X64",
            ["cases"] = new JsonArray
            {
                new JsonObject
                {
                    ["name"] = "RequiredInt32Plain",
                    ["fixtureHash"] = scanFixture,
                    ["fixtureBytes"] = 262406,
                    ["rowCount"] = 65536,
                    ["columnCount"] = 1,
                    ["utf8PayloadBytes"] = 0,
                    ["sourceReadCalls"] = 4,
                    ["sourceBytesRead"] = 524800,
                    ["maximumConcurrentReads"] = 1,
                    ["publicBatches"] = 17,
                    ["decodedColumnBatches"] = 17,
                    ["totalMoves"] = 19,
                    ["synchronousMoves"] = 19,
                    ["poolRents"] = 3,
                    ["poolReturns"] = 3,
                    ["requestedPoolBytes"] = 524544,
                    ["rentedPoolCapacityBytes"] = 524544,
                    ["peakPooledBytes"] = 200000,
                    ["returnedPoolCapacityBytes"] = 524544,
                    ["logicalOutputBytes"] = 262144,
                    ["sourceCopiedBytes"] = 524800,
                    ["endOfScanRetainedPoolBytes"] = 200000,
                    ["consumerUtf8CopiedBytes"] = 0,
                    ["pooledBytesCleared"] = 524544,
                    ["retainedPoolBytes"] = 0,
                    ["physicalType"] = "int32",
                    ["valueWidthBytes"] = 4,
                    ["nullable"] = false,
                    ["consumer"] = "int32",
                    ["lokadLiveOwnedBytes"] = 300000,
                    ["baselineLiveOwnedBytes"] = 400000,
                    ["passPeaks"] = new JsonArray
                    {
                        new JsonObject
                        {
                            ["peakPooledBytes"] = 200000,
                            ["logicalOutputBytes"] = 262144,
                            ["projection"] = new JsonArray(0),
                            ["target"] = 65536,
                            ["role"] = "cold-instrumented",
                        },
                        new JsonObject
                        {
                            ["peakPooledBytes"] = 150000,
                            ["logicalOutputBytes"] = 262144,
                            ["projection"] = new JsonArray(0),
                            ["target"] = 4096,
                            ["role"] = "warm-instrumented",
                        },
                    },
                },
            },
        };
        if (schemaVersion == 9)
        {
            census["processorAffinity"] = "0x1";
            census["processor"] = "synthetic-processor";
            census["logicalProcessor"] = 0;
            census["serverGarbageCollection"] = false;
            census["gcLatencyMode"] = "Interactive";
            census["tieredCompilation"] = "unrecorded";
            census["tieredPgo"] = "unrecorded";
            census["resolvedOutputPath"] = root;
            census["outputFileSystem"] = "synthetic-fs";
            if (census["cases"] is JsonArray censusCases && censusCases.Count > 0 &&
                censusCases[0] is JsonObject firstCase &&
                firstCase["passPeaks"] is JsonArray peaks)
            {
                var batchCounts = new[] { 17, 4 };
                var index = 0;
                foreach (var peak in peaks)
                {
                    if (peak is JsonObject peakObject)
                    {
                        peakObject["elapsedMilliseconds"] = 0.5 + index;
                        peakObject["batchCount"] = batchCounts[index % batchCounts.Length];
                        peakObject["allocatedBytes"] = 1000;
                        peakObject["gen0Collections"] = 0;
                        peakObject["gen1Collections"] = 0;
                        peakObject["gen2Collections"] = 0;
                    }
                    index++;
                }
            }
        }
        if (schemaVersion == 9 && census["cases"] is JsonArray schema9Cases)
        {
            // The schema-4 diagnostic case set is frozen alongside the paired
            // catalog cases: every extra below carries fully consistent
            // dimensions, denominators, pool accounting and pass evidence.
            schema9Cases.Add(DiagnosticCensusCase(scanFixture, "UnevenInt32Plain", "int32", 4, false, "multi-int32", 8192, 2, 0, null, null, 60000, [(new[] { 0, 1 }, 8192), (new[] { 1 }, 4096)], 0, 0));
            schema9Cases.Add(DiagnosticCensusCase(scanFixture, "NarrowInt32Plain", "int32", 4, false, "int32", 65536, 1, 0, null, null, 200000, [(new[] { 0 }, 65536), (new[] { 0 }, 4096)], 0, 0));
            schema9Cases.Add(DiagnosticCensusCase(scanFixture, "RequiredInt32RowRange", "int32", 4, false, "int32", 4096, 1, 0, 0, 4096, 15000, [(new[] { 0 }, 4096)], 0, 0));
            schema9Cases.Add(DiagnosticCensusCase(scanFixture, "SmallRowGroupsInt32Plain", "int32", 4, false, "int32", 65536, 1, 0, null, null, 200000, [(new[] { 0 }, 65536), (new[] { 0 }, 4096)], 0, 0));
            schema9Cases.Add(DiagnosticCensusCase(scanFixture, "CompressibleInt32Snappy", "int32", 4, false, "int32", 65536, 1, 0, null, null, 200000, [(new[] { 0 }, 65536), (new[] { 0 }, 4096)], 0, 0));
            schema9Cases.Add(DiagnosticCensusCase(scanFixture, "NullableBooleanPlain", "boolean", 1, true, "boolean", 8192, 1, 0, null, null, 50000, [(new[] { 0 }, 8192), (new[] { 0 }, 4096)], 0, 0));
            schema9Cases.Add(DiagnosticCensusCase(scanFixture, "LowCardinalityStringDictionary", "utf8", 0, false, "utf8", 1024, 1, 2048, null, null, 6000, [(new[] { 0 }, 1024), (new[] { 0 }, 64)], 4096, 0));
            schema9Cases.Add(DiagnosticCensusCase(scanFixture, "RequiredInt64Plain", "int64", 8, false, "int64", 65536, 1, 0, null, null, 600000, [(new[] { 0 }, 65536), (new[] { 0 }, 4096)], 0, 0));
            schema9Cases.Add(DiagnosticCensusCase(scanFixture, "RequiredFloatPlain", "float", 4, false, "float", 65536, 1, 0, null, null, 300000, [(new[] { 0 }, 65536), (new[] { 0 }, 4096)], 0, 0));
            schema9Cases.Add(DiagnosticCensusCase(scanFixture, "RequiredDoublePlain", "double", 8, false, "double", 65536, 1, 0, null, null, 600000, [(new[] { 0 }, 65536), (new[] { 0 }, 4096)], 0, 0));
            schema9Cases.Add(DiagnosticCensusCase(scanFixture, "NullableInt64Plain", "int64", 8, true, "nullable-int64", 65536, 1, 0, null, null, 600000, [(new[] { 0 }, 65536), (new[] { 0 }, 4096)], 0, 0));
            schema9Cases.Add(DiagnosticCensusCase(scanFixture, "NullableFixedByteArrayPlain", "fixed", 4, true, "fixed", 1000, 1, 0, null, null, 20000, [(new[] { 0 }, 1000), (new[] { 0 }, 256)], 0, 0));
            schema9Cases.Add(DiagnosticCensusCase(scanFixture, "NullableBinaryPlain", "binary", 0, true, "nullable-binary", 8192, 1, 0, null, null, 50000, [(new[] { 0 }, 8192), (new[] { 0 }, 4096)], 0, 10922));
            schema9Cases.Add(DiagnosticCensusCase(scanFixture, "RequiredInt32V2", "int32", 4, false, "int32", 65536, 1, 0, null, null, 300000, [(new[] { 0 }, 65536), (new[] { 0 }, 4096)], 0, 0));
            schema9Cases.Add(DiagnosticCensusCase(scanFixture, "NullableInt32V2", "int32", 4, true, "nullable-int32", 65536, 1, 0, null, null, 300000, [(new[] { 0 }, 65536), (new[] { 0 }, 4096)], 0, 0));
            schema9Cases.Add(DiagnosticCensusCase(scanFixture, "HighCardinalityStringDictionary", "utf8", 0, false, "utf8", 65536, 1, 388776, null, null, 500000, [(new[] { 0 }, 65536), (new[] { 0 }, 4096)], 777552, 0));
            schema9Cases.Add(DiagnosticCensusCase(scanFixture, "NullableInt32DenseNulls", "int32", 4, true, "nullable-int32", 65536, 1, 0, null, null, 300000, [(new[] { 0 }, 65536), (new[] { 0 }, 4096)], 0, 0));
            schema9Cases.Add(DiagnosticCensusCase(scanFixture, "CrcInt64Dictionary", "int64", 8, false, "int64", 1000, 1, 0, null, null, 30000, [(new[] { 0 }, 1000), (new[] { 0 }, 256)], 0, 0));
            schema9Cases.Add(DiagnosticCensusCase(scanFixture, "CrcBinaryDictionarySnappy", "binary", 0, false, "binary", 1000, 1, 0, null, null, 30000, [(new[] { 1 }, 1000), (new[] { 1 }, 256)], 0, 8000));
            schema9Cases.Add(DiagnosticCensusCase(scanFixture, "MisalignedMultiPage", "int32", 4, false, "multi-int32", 2000, 2, 0, null, null, 60000, [(new[] { 0, 1 }, 2000), (new[] { 0, 1 }, 128)], 0, 0));
        }
        File.WriteAllText(Path.Combine(root, "census.json"), census.ToJsonString(new JsonSerializerOptions { WriteIndented = true }));
        File.WriteAllText(
            Path.Combine(root, "BENCHMARKS.md"),
            "# Synthetic report\n\n<!-- BEGIN GENERATED PARITY REPORT -->\nplaceholder\n<!-- END GENERATED PARITY REPORT -->\n");
        if (materialize)
        {
            var outcome = InvokeReport(root, false);
            if (outcome.ExitCode != 0)
                throw new InvalidOperationException("The synthetic quartet failed to materialize its report:\n" + outcome.Output);
        }

        return root;

        void WritePaired(int index, string operatingSystem, string fingerprint, string affinity, DateTimeOffset start)
        {
            var scan = Summarize(1010.0, 1000.0, 1000.0, legacyInterval);
            var meta = Summarize(1000.0, 1000.0, 1000.0, legacyInterval);
            var snapshot = new JsonObject
            {
                ["schemaVersion"] = schemaVersion,
                ["sessionId"] = Guid.NewGuid().ToString(),
                ["recordedAtUtc"] = start.ToString("o"),
                ["sourceRevision"] = sourceRevision,
                ["runnerFingerprint"] = fingerprint,
                ["packageLockHash"] = packageLockHash,
                ["runtime"] = "synthetic-runtime",
                ["operatingSystem"] = operatingSystem,
                ["architecture"] = "X64",
                ["processor"] = "synthetic-processor",
                ["processPriority"] = "Normal",
                ["processorAffinity"] = affinity,
                ["powerMode"] = "synthetic-power",
                ["instructionMode"] = "synthetic-mode",
                ["stopwatchFrequency"] = 10000000,
                ["randomSeed"] = 24081993,
                ["sampleCount"] = 400,
                ["initialWarmupOperations"] = 64,
                ["minimumStabilizationOperations"] = 16384,
                ["preliminaryStabilizationBlocks"] = 40,
                ["finalStabilizationBlocks"] = 10,
                ["targetBlockMilliseconds"] = 100.0,
                ["estimator"] = "exp(mean(paired log ratio)); one-sided 95% Student-t upper bound",
                ["outlierRule"] = "none; retain every balanced AB/BA observation",
                ["cases"] = new JsonArray
                {
                    new JsonObject
                    {
                        ["name"] = "PreopenedScan/RequiredInt32Plain",
                        ["fixtureHash"] = scanFixture,
                        ["rowCount"] = 65536,
                        ["columnCount"] = 1,
                        ["utf8PayloadBytes"] = 0,
                        ["operationsPerBlock"] = 16,
                        ["pointRatio"] = scan.Point,
                        ["upper95Ratio"] = scan.Upper,
                        ["passed"] = true,
                        ["observations"] = BuildObservations(1010.0, 1000.0, 1000.0, start),
                    },
                    new JsonObject
                    {
                        ["name"] = "WarmMetadataOpen",
                        ["fixtureHash"] = metaFixture,
                        ["rowCount"] = 262144,
                        ["columnCount"] = 1,
                        ["utf8PayloadBytes"] = 0,
                        ["operationsPerBlock"] = 4,
                        ["pointRatio"] = meta.Point,
                        ["upper95Ratio"] = meta.Upper,
                        ["passed"] = true,
                        ["observations"] = BuildObservations(1000.0, 1000.0, 1000.0, start),
                    },
                },
            };
            if (schemaVersion == 9)
            {
                snapshot["logicalProcessor"] = 0;
                snapshot["serverGarbageCollection"] = false;
                snapshot["gcLatencyMode"] = "Interactive";
                snapshot["tieredCompilation"] = "unrecorded";
                snapshot["tieredPgo"] = "unrecorded";
                snapshot["resolvedOutputPath"] = root;
                snapshot["outputFileSystem"] = "synthetic-fs";
            }
            File.WriteAllText(Path.Combine(root, "paired-" + index + ".json"), snapshot.ToJsonString(new JsonSerializerOptions { WriteIndented = true }));
        }
        JsonArray BuildObservations(double firstLokad, double secondLokad, double baseline, DateTimeOffset start)
        {
            var observations = new JsonArray();
            for (var index = 0; index < 400; index++)
            {
                var lokad = index % 2 == 0 ? firstLokad : secondLokad;
                observations.Add(new JsonObject
                {
                    ["recordedAtUtc"] = start.AddSeconds(index).ToString("o"),
                    ["order"] = index % 2 == 0 ? "AB" : "BA",
                    ["lokadNanoseconds"] = lokad,
                    ["parquetNetNanoseconds"] = baseline,
                    ["lokadAllocatedBytes"] = 800,
                    ["parquetNetAllocatedBytes"] = 1600,
                    ["lokadGen0Collections"] = 0,
                    ["parquetNetGen0Collections"] = 0,
                    ["lokadGen1Collections"] = 0,
                    ["parquetNetGen1Collections"] = 0,
                    ["lokadGen2Collections"] = 0,
                    ["parquetNetGen2Collections"] = 0,
                    ["logRatio"] = Math.Log(lokad / baseline),
                });
            }

            return observations;
        }

    }



    // Mirrors the report verifier operation for operation: sequential mean,
    // squared deviations via Math.Pow, and the schema-selected quantile.
    internal static (double Point, double Upper) Summarize(double firstLokad, double secondLokad, double baseline, bool legacyInterval)
    {
        const int count = 400;
        var sum = 0.0;
        for (var index = 0; index < count; index++)
        {
            var lokad = index % 2 == 0 ? firstLokad : secondLokad;
            sum += Math.Log(lokad / baseline);
        }

        var mean = sum / count;
        var squared = 0.0;
        for (var index = 0; index < count; index++)
        {
            var lokad = index % 2 == 0 ? firstLokad : secondLokad;
            squared += Math.Pow(Math.Log(lokad / baseline) - mean, 2);
        }

        var error = Math.Sqrt(squared / (count - 1)) / Math.Sqrt(count);

        static double Quantile(int degreesOfFreedom, bool legacyInterval)
        {
            double[] exact =
            [
                6.314, 2.920, 2.353, 2.132, 2.015, 1.943, 1.895, 1.860, 1.833, 1.812,
            1.796, 1.782, 1.771, 1.761, 1.753, 1.746, 1.740, 1.734, 1.729, 1.725,
            1.721, 1.717, 1.714, 1.711, 1.708, 1.706, 1.703, 1.701, 1.699, 1.697,
        ];
            if (degreesOfFreedom <= exact.Length)
                return exact[degreesOfFreedom - 1];
            if (legacyInterval)
                return 1.645;
            const double z = 1.6448536269514722;
            var inverse = 1.0 / degreesOfFreedom;
            return z + (((z * z) + 1.0) * z) / 4.0 * inverse + ((((5.0 * z * z) + 16.0) * z * z * z) + (3.0 * z)) / 96.0 * inverse * inverse;
        }

        return (Math.Exp(mean), Math.Exp(mean + Quantile(count - 1, legacyInterval) * error));
    }

    private static JsonObject DiagnosticCensusCase(
        string fixtureHash,
        string name,
        string physicalType,
        int valueWidthBytes,
        bool nullable,
        string consumer,
        long rowCount,
        int columnCount,
        long utf8PayloadBytes,
        long? rangeStart,
        long? rangeCount,
        long peakPerPass,
        (int[] Projection, int Target)[] passes,
        long utf8Copied,
        long binaryPayload)
    {
        var slotWidth = physicalType switch
        {
            "boolean" => 1L,
            "int32" => 4L,
            "int64" => 8L,
            "float" => 4L,
            "double" => 8L,
            "fixed" => valueWidthBytes,
            "utf8" => 0L,
            "binary" => 0L,
            _ => throw new InvalidOperationException("The synthetic diagnostic layout is unknown."),
        };
        var logical = physicalType switch
        {
            "utf8" => utf8PayloadBytes + columnCount * (rowCount + 1) * 4,
            "binary" => binaryPayload + columnCount * (rowCount + 1) * 4 + (nullable ? columnCount * ((rowCount + 7) / 8) : 0),
            _ => rowCount * columnCount * slotWidth + (nullable ? columnCount * ((rowCount + 7) / 8) : 0),
        };
        var poolCapacity = Math.Max(200000L, peakPerPass);
        var retainedIdle = Math.Min(1000L, peakPerPass);
        var peaks = new JsonArray();
        var roleIndex = 0;
        foreach (var (projection, target) in passes)
        {
            var passLogical = physicalType switch
            {
                "utf8" => utf8PayloadBytes + projection.Length * (rowCount + 1) * 4,
                "binary" => binaryPayload + projection.Length * (rowCount + 1) * 4 + (nullable ? projection.Length * ((rowCount + 7) / 8) : 0),
                _ => rowCount * projection.Length * slotWidth + (nullable ? projection.Length * ((rowCount + 7) / 8) : 0),
            };
            peaks.Add(new JsonObject
            {
                ["peakPooledBytes"] = peakPerPass,
                ["logicalOutputBytes"] = passLogical,
                ["elapsedMilliseconds"] = 0.5 + roleIndex,
                ["batchCount"] = 5,
                ["allocatedBytes"] = 1000,
                ["gen0Collections"] = 0,
                ["gen1Collections"] = 0,
                ["gen2Collections"] = 0,
                ["projection"] = new JsonArray(projection.Select(static ordinal => (JsonNode)ordinal).ToArray()),
                ["target"] = target,
                ["role"] = roleIndex == 0 ? "cold-instrumented" : "warm-instrumented",
            });
            roleIndex++;
        }

        var entry = new JsonObject
        {
            ["name"] = name,
            ["fixtureHash"] = fixtureHash,
            ["fixtureBytes"] = 100000,
            ["rowCount"] = rowCount,
            ["columnCount"] = columnCount,
            ["utf8PayloadBytes"] = utf8PayloadBytes,
            ["sourceReadCalls"] = 4,
            ["sourceBytesRead"] = 100000,
            ["maximumConcurrentReads"] = 1,
            ["publicBatches"] = 5,
            ["decodedColumnBatches"] = 5,
            ["totalMoves"] = 6,
            ["synchronousMoves"] = 6,
            ["poolRents"] = 4,
            ["poolReturns"] = 4,
            ["requestedPoolBytes"] = poolCapacity,
            ["rentedPoolCapacityBytes"] = poolCapacity,
            ["peakPooledBytes"] = peakPerPass,
            ["returnedPoolCapacityBytes"] = poolCapacity,
            ["logicalOutputBytes"] = logical,
            ["sourceCopiedBytes"] = 100000,
            ["endOfScanRetainedPoolBytes"] = retainedIdle,
            ["consumerUtf8CopiedBytes"] = utf8Copied,
            ["binaryPayloadBytes"] = binaryPayload,
            ["pooledBytesCleared"] = poolCapacity,
            ["retainedPoolBytes"] = 0,
            ["physicalType"] = physicalType,
            ["valueWidthBytes"] = valueWidthBytes,
            ["nullable"] = nullable,
            ["consumer"] = consumer,
            ["lokadLiveOwnedBytes"] = 300000,
            ["baselineLiveOwnedBytes"] = 400000,
            ["passPeaks"] = peaks,
        };
        if (rangeStart.HasValue && rangeCount.HasValue)
        {
            entry["rowRangeStart"] = rangeStart.Value;
            entry["rowRangeCount"] = rangeCount.Value;
        }

        return entry;
    }

    public static (int ExitCode, string Output) InvokeReport(string root, bool verify)
    {
        var script = Path.Combine(RepositoryTestPaths.Root, "benchmark-report.ps1");
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
            var paired = new List<string>();
            for (var index = 0; index < 4; index++)
                paired.Add(Quote(Path.Combine(root, "paired-" + index + ".json")));
            var command = "& " + Quote(script) + " -PairedSnapshot @(" + string.Join(",", paired) + ")"
                + " -CensusSnapshot " + Quote(Path.Combine(root, "census.json"))
                + " -Catalog " + Quote(Path.Combine(root, "catalog.json"))
                + " -Document " + Quote(Path.Combine(root, "BENCHMARKS.md"))
                + (verify ? " -Verify" : string.Empty);
            startInfo.ArgumentList.Add("-NoProfile");
            startInfo.ArgumentList.Add("-Command");
            startInfo.ArgumentList.Add(command);
            Process? process = null;
            try
            {
                process = Process.Start(startInfo);
            }
            catch (Win32Exception)
            {
            }

            if (process is null)
                continue;
            using (process)
            {
                var completed = process.WaitForExit(180000);
                var output = process.StandardOutput.ReadToEnd() + "\n" + process.StandardError.ReadToEnd();
                // PowerShell renders long terminating errors with host-width word wrapping
                // (80 columns on the Linux CI runner) and emits ANSI color codes into the
                // redirected error stream, splitting single-line gate messages across pipe continuations.
                // Strip the color sequences and unwrap those continuations so message
                // assertions hold on every host; all gate messages are single-line.
                var escape = new string((char)27, 1);
                output = System.Text.RegularExpressions.Regex.Replace(output, escape + @"\[[0-9;?]*[a-zA-Z]", "");
                output = System.Text.RegularExpressions.Regex.Replace(output, @"\s*\r?\n\s*\|\s*", " ");
                output = System.Text.RegularExpressions.Regex.Replace(output, @" {2,}", " ");
                if (!completed)
                {
                    try
                    {
                        process.Kill();
                    }
                    catch (InvalidOperationException)
                    {
                    }
                    catch (Win32Exception)
                    {
                    }

                    throw new InvalidOperationException("The benchmark report script timed out.");
                }

                return (process.ExitCode, output);
            }
        }

        static string Quote(string value)
        {
            return "'" + value.Replace("'", "''") + "'";
        }

        throw new InvalidOperationException("Neither pwsh nor powershell is available to verify the benchmark report.");
    }
}
public sealed class BenchmarkReportQuartetFixture : IDisposable
{
    public BenchmarkReportQuartetFixture()
    {
        V8Root = BenchmarkReportQuartet.Build(8, false, true);
        V8LegacyRoot = BenchmarkReportQuartet.Build(8, true, false);
        V7Root = BenchmarkReportQuartet.Build(7, true, true);
        V7FutureRoot = BenchmarkReportQuartet.Build(7, false, false);
        V9Root = BenchmarkReportQuartet.Build(9, false, true);
    }

    public string V8Root { get; }

    public string V8LegacyRoot { get; }

    public string V7Root { get; }

    public string V7FutureRoot { get; }

    public string V9Root { get; }

    public void Dispose()
    {
        foreach (var root in new[] { V8Root, V8LegacyRoot, V7Root, V7FutureRoot, V9Root })
        {
            try
            {
                Directory.Delete(root, true);
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

public sealed class BenchmarkReportVerificationTests : IClassFixture<BenchmarkReportQuartetFixture>
{
    private readonly BenchmarkReportQuartetFixture _quartets;

    public BenchmarkReportVerificationTests(BenchmarkReportQuartetFixture quartets)
    {
        _quartets = quartets;
    }

    [Fact]
    public void CleanSchema8QuartetPassesVerify()
    {
        var outcome = RunVerify(_quartets.V8Root);
        Assert.Equal(0, outcome.ExitCode);
        Assert.Contains("matches the supplied snapshots", outcome.Output);
    }

    [Fact]
    public void CleanSchema7QuartetPassesVerify()
    {
        var outcome = RunVerify(_quartets.V7Root);
        Assert.Equal(0, outcome.ExitCode);
        Assert.Contains("matches the supplied snapshots", outcome.Output);
    }

    [Fact]
    public void ScaledRawTimingsAreRejected()
    {
        var outcome = VerifyAfterPairedMutation(_quartets.V8Root, 0, "PreopenedScan/RequiredInt32Plain", static target =>
        {
            var observations = Assert.IsType<JsonArray>(target["observations"]);
            Assert.IsType<JsonObject>(observations[0])["lokadNanoseconds"] = 101000.0;
        });
        Assert.NotEqual(0, outcome.ExitCode);
        Assert.Contains("does not match its raw timings", outcome.Output);
    }

    [Fact]
    public void FudgedStoredLogRatioIsRejected()
    {
        var outcome = VerifyAfterPairedMutation(_quartets.V8Root, 0, "PreopenedScan/RequiredInt32Plain", static target =>
        {
            var observations = Assert.IsType<JsonArray>(target["observations"]);
            var first = Assert.IsType<JsonObject>(observations[0]);
            var stored = Assert.IsAssignableFrom<JsonValue>(first["logRatio"]).GetValue<double>();
            first["logRatio"] = stored + 0.5;
        });
        Assert.NotEqual(0, outcome.ExitCode);
        Assert.Contains("does not match its raw timings", outcome.Output);
    }

    [Fact]
    public void BumpedStoredUpperBoundIsRejected()
    {
        var outcome = VerifyAfterPairedMutation(_quartets.V8Root, 0, "PreopenedScan/RequiredInt32Plain", static target =>
        {
            var stored = Assert.IsAssignableFrom<JsonValue>(target["upper95Ratio"]).GetValue<double>();
            target["upper95Ratio"] = stored + 0.001;
        });
        Assert.NotEqual(0, outcome.ExitCode);
        Assert.Contains("upper bound does not match recomputed evidence", outcome.Output);
    }

    [Fact]
    public void LoweredStoredUpperBoundIsRejected()
    {
        var outcome = VerifyAfterPairedMutation(_quartets.V8Root, 0, "PreopenedScan/RequiredInt32Plain", static target =>
        {
            var point = Assert.IsAssignableFrom<JsonValue>(target["pointRatio"]).GetValue<double>();
            target["upper95Ratio"] = point - 0.0001;
        });
        Assert.NotEqual(0, outcome.ExitCode);
        Assert.Contains("upper bound does not match recomputed evidence", outcome.Output);
    }

    [Fact]
    public void MissingOrderIsRejected()
    {
        var outcome = VerifyAfterPairedMutation(_quartets.V8Root, 0, "PreopenedScan/RequiredInt32Plain", static target =>
        {
            var observations = Assert.IsType<JsonArray>(target["observations"]);
            Assert.IsType<JsonObject>(observations[0]).Remove("order");
        });
        Assert.NotEqual(0, outcome.ExitCode);
        Assert.Contains("without a balanced AB/BA order", outcome.Output);
    }

    [Fact]
    public void NullTimingIsRejected()
    {
        var outcome = VerifyAfterPairedMutation(_quartets.V8Root, 0, "PreopenedScan/RequiredInt32Plain", static target =>
        {
            var observations = Assert.IsType<JsonArray>(target["observations"]);
            Assert.IsType<JsonObject>(observations[0])["lokadNanoseconds"] = null;
        });
        Assert.NotEqual(0, outcome.ExitCode);
        Assert.Contains("with missing timings", outcome.Output);
    }

    [Fact]
    public void ZeroTimingIsRejected()
    {
        var outcome = VerifyAfterPairedMutation(_quartets.V8Root, 0, "PreopenedScan/RequiredInt32Plain", static target =>
        {
            var observations = Assert.IsType<JsonArray>(target["observations"]);
            Assert.IsType<JsonObject>(observations[0])["lokadNanoseconds"] = 0.0;
        });
        Assert.NotEqual(0, outcome.ExitCode);
        Assert.Contains("with non-positive timings", outcome.Output);
    }

    [Fact]
    public void TruncatedObservationsAreRejected()
    {
        var outcome = VerifyAfterPairedMutation(_quartets.V8Root, 0, "PreopenedScan/RequiredInt32Plain", static target =>
        {
            var observations = Assert.IsType<JsonArray>(target["observations"]);
            observations.RemoveAt(observations.Count - 1);
        });
        Assert.NotEqual(0, outcome.ExitCode);
        Assert.Contains("does not contain 400 observations", outcome.Output);
    }
    [Fact]
    public void ImbalancedOrdersAreRejected()
    {
        var outcome = VerifyAfterPairedMutation(_quartets.V8Root, 0, "PreopenedScan/RequiredInt32Plain", static target =>
        {
            var observations = Assert.IsType<JsonArray>(target["observations"]);
            var flipped = 0;
            foreach (var entry in observations)
            {
                if (flipped >= 10)
                    break;
                var observation = Assert.IsType<JsonObject>(entry);
                if (string.Equals(observation["order"]?.GetValue<string>(), "BA", StringComparison.Ordinal))
                {
                    observation["order"] = "AB";
                    flipped++;
                }
            }
        });
        Assert.NotEqual(0, outcome.ExitCode);
        Assert.Contains("does not contain balanced AB/BA observations", outcome.Output);
    }

    [Fact]
    public void CensusRevisionMismatchIsRejected()
    {
        var outcome = VerifyAfterCensusMutation(_quartets.V8Root, static census =>
        {
            census["sourceRevision"] = "different-revision";
        });
        Assert.NotEqual(0, outcome.ExitCode);
        Assert.Contains("has a different source revision", outcome.Output);
    }

    [Fact]
    public void FixtureHashMismatchIsRejected()
    {
        var outcome = VerifyAfterPairedMutation(_quartets.V8Root, 1, "PreopenedScan/RequiredInt32Plain", static target =>
        {
            target["fixtureHash"] = "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff";
        });
        Assert.NotEqual(0, outcome.ExitCode);
        Assert.Contains("fixture hashes differ across sessions", outcome.Output);
    }

    [Fact]
    public void Utf8PayloadMismatchIsRejected()
    {
        var outcome = VerifyAfterPairedMutation(_quartets.V8Root, 1, "PreopenedScan/RequiredInt32Plain", static target =>
        {
            target["utf8PayloadBytes"] = 7;
        });
        Assert.NotEqual(0, outcome.ExitCode);
        Assert.Contains("utf8PayloadBytes differs across sessions", outcome.Output);
    }

    [Fact]
    public void CensusPoolImbalanceIsRejected()
    {
        var outcome = VerifyAfterCensusMutation(_quartets.V8Root, static census =>
        {
            var cases = Assert.IsType<JsonArray>(census["cases"]);
            var entry = Assert.IsType<JsonObject>(cases[0]);
            var rents = Assert.IsAssignableFrom<JsonValue>(entry["poolRents"]).GetValue<int>();
            entry["poolRents"] = rents + 1;
        });
        Assert.NotEqual(0, outcome.ExitCode);
        Assert.Contains("unbalanced pool activity", outcome.Output);
    }

    [Fact]
    public void NegativeAllocationIsRejected()
    {
        var outcome = VerifyAfterPairedMutation(_quartets.V8Root, 0, "PreopenedScan/RequiredInt32Plain", static target =>
        {
            var observations = Assert.IsType<JsonArray>(target["observations"]);
            Assert.IsType<JsonObject>(observations[0])["lokadAllocatedBytes"] = -1;
        });
        Assert.NotEqual(0, outcome.ExitCode);
        Assert.Contains("with missing allocation counts", outcome.Output);
    }

    [Fact]
    public void Schema8LegacyUpperBoundIsRejected()
    {
        var root = CopyQuartet(_quartets.V8LegacyRoot);
        try
        {
            var outcome = RunVerify(root);
            Assert.NotEqual(0, outcome.ExitCode);
            Assert.Contains("upper bound does not match recomputed evidence", outcome.Output);
        }
        finally
        {
            Directory.Delete(root, true);
        }
    }

    [Fact]
    public void LegacySchema7CornishFisherUpperBoundIsRejected()
    {
        var root = CopyQuartet(_quartets.V7FutureRoot);
        try
        {
            var outcome = RunVerify(root);
            Assert.NotEqual(0, outcome.ExitCode);
            Assert.Contains("upper bound does not match recomputed evidence", outcome.Output);
        }
        finally
        {
            Directory.Delete(root, true);
        }
    }

    [Fact]
    public void NonFiniteTimingsAreRejected()
    {
        var root = CopyQuartet(_quartets.V8Root);
        try
        {
            var path = Path.Combine(root, "paired-0.json");
            File.WriteAllText(path, ReplaceFirst(File.ReadAllText(path), "\"lokadNanoseconds\": 1010", "\"lokadNanoseconds\": 1e999"));
            var outcome = RunVerify(root);
            Assert.NotEqual(0, outcome.ExitCode);
            Assert.Contains("with non-finite timings", outcome.Output);
        }
        finally
        {
            Directory.Delete(root, true);
        }

        static string ReplaceFirst(string text, string oldText, string newText)
        {
            var index = text.IndexOf(oldText, StringComparison.Ordinal);
            Assert.True(index >= 0, "The synthetic snapshot does not contain the expected timing.");
            return text.Substring(0, index) + newText + text.Substring(index + oldText.Length);
        }
    }

    [Fact]
    public void DuplicateSessionIdIsRejected()
    {
        var root = CopyQuartet(_quartets.V8Root);
        try
        {
            var first = Assert.IsType<JsonObject>(JsonNode.Parse(File.ReadAllText(Path.Combine(root, "paired-0.json"))));
            var session = Assert.IsAssignableFrom<JsonValue>(first["sessionId"]).GetValue<string>();
            var secondPath = Path.Combine(root, "paired-1.json");
            var second = Assert.IsType<JsonObject>(JsonNode.Parse(File.ReadAllText(secondPath)));
            second["sessionId"] = session;
            File.WriteAllText(secondPath, second.ToJsonString(new JsonSerializerOptions { WriteIndented = true }));
            var outcome = RunVerify(root);
            Assert.NotEqual(0, outcome.ExitCode);
            Assert.Contains("four distinct sessions", outcome.Output);
        }
        finally
        {
            Directory.Delete(root, true);
        }
    }

    [Fact]
    public void CleanSchema9QuartetPassesVerify()
    {
        var outcome = RunVerify(_quartets.V9Root);
        Assert.Equal(0, outcome.ExitCode);
        Assert.Contains("matches the supplied snapshots", outcome.Output);
    }

    [Fact]
    public void MissingPairedHostEvidenceIsRejected()
    {
        var outcome = VerifyAfterRunMutation(_quartets.V9Root, 0, static target =>
        {
            target.Remove("tieredPgo");
        });
        Assert.NotEqual(0, outcome.ExitCode);
        Assert.Contains("missing its runtime tuning evidence", outcome.Output);
    }

    [Fact]
    public void MissingPairedOutputEvidenceIsRejected()
    {
        var outcome = VerifyAfterRunMutation(_quartets.V9Root, 0, static target =>
        {
            target.Remove("resolvedOutputPath");
        });
        Assert.NotEqual(0, outcome.ExitCode);
        Assert.Contains("missing its output storage evidence", outcome.Output);
    }

    [Fact]
    public void CensusAffinityViolationIsRejected()
    {
        var outcome = VerifyAfterCensusMutation(_quartets.V9Root, static census =>
        {
            census["processorAffinity"] = "0x3";
        });
        Assert.NotEqual(0, outcome.ExitCode);
        Assert.Contains("must be bound to exactly one logical processor", outcome.Output);
    }

    [Fact]
    public void CensusTuningEvidenceMissingIsRejected()
    {
        var outcome = VerifyAfterCensusMutation(_quartets.V9Root, static census =>
        {
            census.Remove("tieredCompilation");
        });
        Assert.NotEqual(0, outcome.ExitCode);
        Assert.Contains("missing its runtime tuning evidence", outcome.Output);
    }

    [Fact]
    public void CensusPassTimingMissingIsRejected()
    {
        var outcome = VerifyAfterCensusMutation(_quartets.V9Root, static census =>
        {
            var cases = Assert.IsType<JsonArray>(census["cases"]);
            var entry = Assert.IsType<JsonObject>(cases[0]);
            var peaks = Assert.IsType<JsonArray>(entry["passPeaks"]);
            Assert.IsType<JsonObject>(peaks[0]).Remove("elapsedMilliseconds");
        });
        Assert.NotEqual(0, outcome.ExitCode);
        Assert.Contains("missing its pass timing evidence", outcome.Output);
    }

    [Fact]
    public void CensusPassBatchCountMissingIsRejected()
    {
        var outcome = VerifyAfterCensusMutation(_quartets.V9Root, static census =>
        {
            var cases = Assert.IsType<JsonArray>(census["cases"]);
            var entry = Assert.IsType<JsonObject>(cases[0]);
            var peaks = Assert.IsType<JsonArray>(entry["passPeaks"]);
            Assert.IsType<JsonObject>(peaks[1]).Remove("batchCount");
        });
        Assert.NotEqual(0, outcome.ExitCode);
        Assert.Contains("missing its pass batch count", outcome.Output);
    }

    private static (int ExitCode, string Output) VerifyAfterRunMutation(string source, int snapshotIndex, Action<JsonObject> mutate)
    {
        var root = CopyQuartet(source);
        try
        {
            var path = Path.Combine(root, "paired-" + snapshotIndex + ".json");
            var snapshot = Assert.IsType<JsonObject>(JsonNode.Parse(File.ReadAllText(path)));
            mutate(snapshot);
            File.WriteAllText(path, snapshot.ToJsonString(new JsonSerializerOptions { WriteIndented = true }));
            return RunVerify(root);
        }
        finally
        {
            Directory.Delete(root, true);
        }
    }

    private static string CopyQuartet(string source)
    {
        var destination = Path.Combine(Path.GetTempPath(), "lokad-report-copy-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(destination);
        foreach (var path in Directory.EnumerateFiles(source))
        {
            var name = Path.GetFileName(path) ?? throw new InvalidOperationException("A synthetic file has no name.");
            File.Copy(path, Path.Combine(destination, name));
        }

        return destination;
    }

    private static (int ExitCode, string Output) RunVerify(string root)
    {
        return BenchmarkReportQuartet.InvokeReport(root, true);
    }

    private static (int ExitCode, string Output) VerifyAfterPairedMutation(string source, int snapshotIndex, string caseName, Action<JsonObject> mutate)
    {
        var root = CopyQuartet(source);
        try
        {
            var path = Path.Combine(root, "paired-" + snapshotIndex + ".json");
            var snapshot = Assert.IsType<JsonObject>(JsonNode.Parse(File.ReadAllText(path)));
            var cases = Assert.IsType<JsonArray>(snapshot["cases"]);
            JsonObject? found = null;
            foreach (var entry in cases)
            {
                var candidate = Assert.IsType<JsonObject>(entry);
                if (string.Equals(candidate["name"]?.GetValue<string>(), caseName, StringComparison.Ordinal))
                    found = candidate;
            }

            mutate(Assert.IsType<JsonObject>(found));
            File.WriteAllText(path, snapshot.ToJsonString(new JsonSerializerOptions { WriteIndented = true }));
            return RunVerify(root);
        }
        finally
        {
            Directory.Delete(root, true);
        }
    }

    private static (int ExitCode, string Output) VerifyAfterCensusMutation(string source, Action<JsonObject> mutate)
    {
        var root = CopyQuartet(source);
        try
        {
            var path = Path.Combine(root, "census.json");
            var census = Assert.IsType<JsonObject>(JsonNode.Parse(File.ReadAllText(path)));
            mutate(census);
            File.WriteAllText(path, census.ToJsonString(new JsonSerializerOptions { WriteIndented = true }));
            return RunVerify(root);
        }
        finally
        {
            Directory.Delete(root, true);
        }
    }

    [Fact]
    public void CensusMissingPassPeaksIsRejected()
    {
        var outcome = VerifyAfterCensusMutation(_quartets.V9Root, static census =>
        {
            var cases = Assert.IsType<JsonArray>(census["cases"]);
            Assert.IsType<JsonObject>(cases[0]).Remove("passPeaks");
        });
        Assert.NotEqual(0, outcome.ExitCode);
        Assert.Contains("expected pass set", outcome.Output);
    }

    [Fact]
    public void CensusPassRemovedIsRejected()
    {
        var outcome = VerifyAfterCensusMutation(_quartets.V9Root, static census =>
        {
            var cases = Assert.IsType<JsonArray>(census["cases"]);
            var peaks = Assert.IsType<JsonArray>(Assert.IsType<JsonObject>(cases[0])["passPeaks"]);
            peaks.RemoveAt(1);
        });
        Assert.NotEqual(0, outcome.ExitCode);
        Assert.Contains("expected pass set", outcome.Output);
    }

    [Fact]
    public void CensusDiagnosticCaseRemovedIsRejected()
    {
        var outcome = VerifyAfterCensusMutation(_quartets.V9Root, static census =>
        {
            var cases = Assert.IsType<JsonArray>(census["cases"]);
            cases.RemoveAt(cases.Count - 1);
        });
        Assert.NotEqual(0, outcome.ExitCode);
        Assert.Contains("frozen case set", outcome.Output);
    }

    [Fact]
    public void CensusUnknownExtraCaseIsRejected()
    {
        var outcome = VerifyAfterCensusMutation(_quartets.V9Root, static census =>
        {
            var cases = Assert.IsType<JsonArray>(census["cases"]);
            var clone = Assert.IsType<JsonObject>(JsonNode.Parse(Assert.IsType<JsonObject>(cases[0]).ToJsonString()));
            clone["name"] = "UnknownExtraLane";
            cases.Add(clone);
        });
        Assert.NotEqual(0, outcome.ExitCode);
        Assert.Contains("frozen case set", outcome.Output);
    }

    [Fact]
    public void CensusZeroReadsIsRejected()
    {
        var outcome = VerifyAfterCensusMutation(_quartets.V9Root, static census =>
        {
            var cases = Assert.IsType<JsonArray>(census["cases"]);
            var entry = Assert.IsType<JsonObject>(cases[0]);
            entry["sourceReadCalls"] = 0;
            entry["sourceBytesRead"] = 0;
        });
        Assert.NotEqual(0, outcome.ExitCode);
        Assert.Contains("source read evidence", outcome.Output);
    }

    [Fact]
    public void CensusZeroReadsFailsGeneration()
    {
        var root = CopyQuartet(_quartets.V9Root);
        try
        {
            var path = Path.Combine(root, "census.json");
            var census = Assert.IsType<JsonObject>(JsonNode.Parse(File.ReadAllText(path)));
            var entry = Assert.IsType<JsonObject>(Assert.IsType<JsonArray>(census["cases"])[0]);
            entry["sourceReadCalls"] = 0;
            entry["sourceBytesRead"] = 0;
            File.WriteAllText(path, census.ToJsonString(new JsonSerializerOptions { WriteIndented = true }));
            var outcome = BenchmarkReportQuartet.InvokeReport(root, false);
            Assert.NotEqual(0, outcome.ExitCode);
            Assert.Contains("source read evidence", outcome.Output);
        }
        finally
        {
            Directory.Delete(root, true);
        }
    }

    [Fact]
    public void CensusNegativeCounterIsRejected()
    {
        var outcome = VerifyAfterCensusMutation(_quartets.V9Root, static census =>
        {
            var cases = Assert.IsType<JsonArray>(census["cases"]);
            Assert.IsType<JsonObject>(cases[0])["requestedPoolBytes"] = -1;
        });
        Assert.NotEqual(0, outcome.ExitCode);
        Assert.Contains("invalid requestedPoolBytes evidence", outcome.Output);
    }

    [Fact]
    public void CensusLayoutIdentityChangedIsRejected()
    {
        var outcome = VerifyAfterCensusMutation(_quartets.V9Root, static census =>
        {
            var cases = Assert.IsType<JsonArray>(census["cases"]);
            Assert.IsType<JsonObject>(cases[0])["physicalType"] = "boolean";
        });
        Assert.NotEqual(0, outcome.ExitCode);
        Assert.Contains("census layout", outcome.Output);
    }

    [Fact]
    public void CensusConsumerChangedIsRejected()
    {
        var outcome = VerifyAfterCensusMutation(_quartets.V9Root, static census =>
        {
            var cases = Assert.IsType<JsonArray>(census["cases"]);
            Assert.IsType<JsonObject>(cases[0])["consumer"] = "int33";
        });
        Assert.NotEqual(0, outcome.ExitCode);
        Assert.Contains("unknown consumer identity", outcome.Output);
    }

    [Fact]
    public void CensusPassProjectionMissingIsRejected()
    {
        var outcome = VerifyAfterCensusMutation(_quartets.V9Root, static census =>
        {
            var cases = Assert.IsType<JsonArray>(census["cases"]);
            var peaks = Assert.IsType<JsonArray>(Assert.IsType<JsonObject>(cases[0])["passPeaks"]);
            Assert.IsType<JsonObject>(peaks[0]).Remove("projection");
        });
        Assert.NotEqual(0, outcome.ExitCode);
        Assert.Contains("missing its projection", outcome.Output);
    }

    [Fact]
    public void CatalogIdentityMismatchIsRejected()
    {
        var outcome = VerifyAfterCatalogMutation(_quartets.V9Root, static catalog =>
        {
            catalog["sourceRevision"] = "different-revision";
        });
        Assert.NotEqual(0, outcome.ExitCode);
        Assert.Contains("catalog has a different source revision", outcome.Output);
    }

    [Fact]
    public void CatalogIdentityMissingIsRejected()
    {
        var outcome = VerifyAfterCatalogMutation(_quartets.V9Root, static catalog =>
        {
            catalog.Remove("sourceRevision");
        });
        Assert.NotEqual(0, outcome.ExitCode);
        Assert.Contains("does not record its source identity", outcome.Output);
    }

    [Fact]
    public void FailedScanLaneRendersTruthfullyAndFailsGeneration()
    {
        var root = CopyQuartet(_quartets.V9Root);
        try
        {
            FailScanLane(root, "PreopenedScan/RequiredInt32Plain", 1.2);
            var outcome = BenchmarkReportQuartet.InvokeReport(root, false);
            Assert.NotEqual(0, outcome.ExitCode);
            Assert.Contains("parity claim fails on PreopenedScan/RequiredInt32Plain", outcome.Output);
            var document = File.ReadAllText(Path.Combine(root, "BENCHMARKS.md"));
            Assert.Contains("| FAIL |", document);
            Assert.Contains("Parity claim (upper 95% bound no greater than 1.05 on every pre-opened scan lane): FAIL (PreopenedScan/RequiredInt32Plain)", document);
            Assert.Contains("| pass |", document);
        }
        finally
        {
            Directory.Delete(root, true);
        }
    }

    [Fact]
    public void FailedScanLaneVerifyFailsWithoutWriting()
    {
        var root = CopyQuartet(_quartets.V9Root);
        try
        {
            FailScanLane(root, "PreopenedScan/RequiredInt32Plain", 1.2);
            var generated = BenchmarkReportQuartet.InvokeReport(root, false);
            Assert.NotEqual(0, generated.ExitCode);
            var before = File.ReadAllBytes(Path.Combine(root, "BENCHMARKS.md"));
            var outcome = RunVerify(root);
            Assert.NotEqual(0, outcome.ExitCode);
            Assert.Contains("parity claim fails", outcome.Output);
            Assert.Equal(before, File.ReadAllBytes(Path.Combine(root, "BENCHMARKS.md")));
        }
        finally
        {
            Directory.Delete(root, true);
        }
    }

    [Fact]
    public void FailedWarmLaneDoesNotFailClaim()
    {
        var root = CopyQuartet(_quartets.V9Root);
        try
        {
            FailScanLane(root, "WarmMetadataOpen", 1.3);
            var outcome = BenchmarkReportQuartet.InvokeReport(root, false);
            Assert.Equal(0, outcome.ExitCode);
            var document = File.ReadAllText(Path.Combine(root, "BENCHMARKS.md"));
            Assert.Contains("Parity claim (upper 95% bound no greater than 1.05 on every pre-opened scan lane): PASS", document);
            Assert.Contains("accepted parity limitation", document);
        }
        finally
        {
            Directory.Delete(root, true);
        }
    }

    [Fact]
    public void UnequalBlockCountsNormalizePerSession()
    {
        // B03: operations per block vary across sessions, so bytes normalize by
        // each session's own operation count instead of pooling per observation.
        var root = CopyQuartet(_quartets.V8Root);
        try
        {
            foreach (var index in new[] { 2, 3 })
            {
                var path = Path.Combine(root, "paired-" + index + ".json");
                var snapshot = Assert.IsType<JsonObject>(JsonNode.Parse(File.ReadAllText(path)));
                var cases = Assert.IsType<JsonArray>(snapshot["cases"]);
                var target = Assert.IsType<JsonObject>(cases[0]);
                target["operationsPerBlock"] = 32;
                foreach (var entry in Assert.IsType<JsonArray>(target["observations"]))
                {
                    var observation = Assert.IsType<JsonObject>(entry);
                    observation["lokadAllocatedBytes"] = 1600;
                    observation["parquetNetAllocatedBytes"] = 3200;
                }
                File.WriteAllText(path, snapshot.ToJsonString(new JsonSerializerOptions { WriteIndented = true }));
            }
            var outcome = BenchmarkReportQuartet.InvokeReport(root, false);
            Assert.Equal(0, outcome.ExitCode);
            var document = File.ReadAllText(Path.Combine(root, "BENCHMARKS.md"));
            Assert.DoesNotContain("Lokad mean B/obs", document);
            Assert.Contains("| Required INT32, PLAIN | Windows 1 | 50.000 | 0.0122 | 100.000 | 0.0244 | 0/0/0 | 0/0/0 | pass | pass | pass |", document);
            Assert.Contains("| Required INT32, PLAIN | Windows 2 | 50.000 | 0.0122 | 100.000 | 0.0244 | 0/0/0 | 0/0/0 | pass | pass | pass |", document);
            Assert.Contains("| Required INT32, PLAIN | Linux 1 | 50.000 | 0.0244 | 100.000 | 0.0488 | 0/0/0 | 0/0/0 | pass | pass | pass |", document);
            Assert.Contains("| Required INT32, PLAIN | Linux 2 | 50.000 | 0.0244 | 100.000 | 0.0488 | 0/0/0 | 0/0/0 | pass | pass | pass |", document);
        }
        finally
        {
            Directory.Delete(root, true);
        }
    }

    [Fact]
    public void FailingAllocationBudgetRendersAndFails()
    {
        var root = CopyQuartet(_quartets.V8Root);
        try
        {
            var path = Path.Combine(root, "paired-0.json");
            var snapshot = Assert.IsType<JsonObject>(JsonNode.Parse(File.ReadAllText(path)));
            var cases = Assert.IsType<JsonArray>(snapshot["cases"]);
            var observations = Assert.IsType<JsonArray>(Assert.IsType<JsonObject>(cases[0])["observations"]);
            foreach (var entry in observations)
                Assert.IsType<JsonObject>(entry)["lokadAllocatedBytes"] = 16000;
            File.WriteAllText(path, snapshot.ToJsonString(new JsonSerializerOptions { WriteIndented = true }));
            var outcome = BenchmarkReportQuartet.InvokeReport(root, false);
            Assert.NotEqual(0, outcome.ExitCode);
            Assert.Contains("PreopenedScan/RequiredInt32Plain (Windows 1) allocates 0.2441 fixed-width B/cell, above the 0.10 budget", outcome.Output);
            var document = File.ReadAllText(Path.Combine(root, "BENCHMARKS.md"));
            Assert.Contains("| Required INT32, PLAIN | Windows 1 | 1000.000 | 0.2441 | 100.000 | 0.0244 | 0/0/0 | 0/0/0 | FAIL | pass | pass |", document);
            Assert.Contains("| Required INT32, PLAIN | Windows 2 | 50.000 | 0.0122 | 100.000 | 0.0244 | 0/0/0 | 0/0/0 | pass | pass | pass |", document);
        }
        finally
        {
            Directory.Delete(root, true);
        }
    }

    [Fact]
    public void FailingGcBudgetRendersAndFails()
    {
        var root = CopyQuartet(_quartets.V8Root);
        try
        {
            var path = Path.Combine(root, "paired-1.json");
            var snapshot = Assert.IsType<JsonObject>(JsonNode.Parse(File.ReadAllText(path)));
            var cases = Assert.IsType<JsonArray>(snapshot["cases"]);
            var observations = Assert.IsType<JsonArray>(Assert.IsType<JsonObject>(cases[0])["observations"]);
            Assert.IsType<JsonObject>(observations[0])["lokadGen0Collections"] = 1;
            File.WriteAllText(path, snapshot.ToJsonString(new JsonSerializerOptions { WriteIndented = true }));
            var outcome = BenchmarkReportQuartet.InvokeReport(root, false);
            Assert.NotEqual(0, outcome.ExitCode);
            Assert.Contains("PreopenedScan/RequiredInt32Plain (Windows 2) collects 1/0/0 against the zero-GC budget", outcome.Output);
            var document = File.ReadAllText(Path.Combine(root, "BENCHMARKS.md"));
            Assert.Contains("| FAIL |", document);
        }
        finally
        {
            Directory.Delete(root, true);
        }
    }

    [Fact]
    public void FailingCpuBudgetRendersAndFails()
    {
        var root = CopyQuartet(_quartets.V8Root);
        try
        {
            ScaleSingleSession(root, 2, "PreopenedScan/RequiredInt32Plain", 4.0);
            var outcome = BenchmarkReportQuartet.InvokeReport(root, false);
            Assert.NotEqual(0, outcome.ExitCode);
            Assert.Contains("PreopenedScan/RequiredInt32Plain (Linux 1)", outcome.Output);
            Assert.Contains("against the 3x CPU budget", outcome.Output);
            var document = File.ReadAllText(Path.Combine(root, "BENCHMARKS.md"));
            Assert.Contains("| FAIL |", document);
        }
        finally
        {
            Directory.Delete(root, true);
        }

        static void ScaleSingleSession(string root, int snapshotIndex, string caseName, double factor)
        {
            var path = Path.Combine(root, "paired-" + snapshotIndex + ".json");
            var snapshot = Assert.IsType<JsonObject>(JsonNode.Parse(File.ReadAllText(path)));
            var cases = Assert.IsType<JsonArray>(snapshot["cases"]);
            JsonObject? found = null;
            foreach (var entry in cases)
            {
                var candidate = Assert.IsType<JsonObject>(entry);
                if (string.Equals(candidate["name"]?.GetValue<string>(), caseName, StringComparison.Ordinal))
                    found = candidate;
            }

            var target = Assert.IsType<JsonObject>(found);
            var observations = Assert.IsType<JsonArray>(target["observations"]);
            var firstLokad = 0.0;
            var secondLokad = 0.0;
            var baseline = 0.0;
            var seen = 0;
            foreach (var entry in observations)
            {
                var observation = Assert.IsType<JsonObject>(entry);
                var scaled = Assert.IsAssignableFrom<JsonValue>(observation["lokadNanoseconds"]).GetValue<double>() * factor;
                var reference = Assert.IsAssignableFrom<JsonValue>(observation["parquetNetNanoseconds"]).GetValue<double>();
                observation["lokadNanoseconds"] = scaled;
                observation["logRatio"] = Math.Log(scaled / reference);
                if (seen == 0)
                {
                    firstLokad = scaled;
                    baseline = reference;
                }
                if (seen == 1)
                    secondLokad = scaled;
                seen++;
            }

            var summary = BenchmarkReportQuartet.Summarize(firstLokad, secondLokad, baseline, false);
            target["pointRatio"] = summary.Point;
            target["upper95Ratio"] = summary.Upper;
            target["passed"] = summary.Upper <= 1.05;
            File.WriteAllText(path, snapshot.ToJsonString(new JsonSerializerOptions { WriteIndented = true }));
        }
    }

    [Fact]
    public void FailingCensusPoolBudgetRendersAndFails()
    {
        var root = CopyQuartet(_quartets.V8Root);
        try
        {
            var path = Path.Combine(root, "census.json");
            var census = Assert.IsType<JsonObject>(JsonNode.Parse(File.ReadAllText(path)));
            var cases = Assert.IsType<JsonArray>(census["cases"]);
            Assert.IsType<JsonObject>(cases[0])["peakPooledBytes"] = 2000000;
            File.WriteAllText(path, census.ToJsonString(new JsonSerializerOptions { WriteIndented = true }));
            var outcome = BenchmarkReportQuartet.InvokeReport(root, false);
            Assert.NotEqual(0, outcome.ExitCode);
            Assert.Contains("fails its 6x pooled-memory budget", outcome.Output);
            Assert.Contains("RequiredInt32Plain case peaks at 2000000 B against 262144 B layout", outcome.Output);
            var document = File.ReadAllText(Path.Combine(root, "BENCHMARKS.md"));
            Assert.Contains("| FAIL |", document);
        }
        finally
        {
            Directory.Delete(root, true);
        }
    }

    [Fact]
    public void FailingCensusPoolBudgetVerifyFailsAfterMatch()
    {
        // Valid failing evidence round-trips: generation renders the FAIL markers
        // and verification then fails on the budget outcome, not on a mismatch.
        var root = CopyQuartet(_quartets.V8Root);
        try
        {
            var path = Path.Combine(root, "census.json");
            var census = Assert.IsType<JsonObject>(JsonNode.Parse(File.ReadAllText(path)));
            var cases = Assert.IsType<JsonArray>(census["cases"]);
            Assert.IsType<JsonObject>(cases[0])["peakPooledBytes"] = 2000000;
            File.WriteAllText(path, census.ToJsonString(new JsonSerializerOptions { WriteIndented = true }));
            var generated = BenchmarkReportQuartet.InvokeReport(root, false);
            Assert.NotEqual(0, generated.ExitCode);
            var outcome = RunVerify(root);
            Assert.NotEqual(0, outcome.ExitCode);
            Assert.Contains("fails its 6x pooled-memory budget", outcome.Output);
        }
        finally
        {
            Directory.Delete(root, true);
        }
    }
    [Fact]
    public void TamperedEvidenceRejectedInGenerationMode()
    {
        var root = CopyQuartet(_quartets.V8Root);
        try
        {
            var path = Path.Combine(root, "paired-0.json");
            var snapshot = Assert.IsType<JsonObject>(JsonNode.Parse(File.ReadAllText(path)));
            var cases = Assert.IsType<JsonArray>(snapshot["cases"]);
            var target = Assert.IsType<JsonObject>(cases[0]);
            var observations = Assert.IsType<JsonArray>(target["observations"]);
            var first = Assert.IsType<JsonObject>(observations[0]);
            var stored = Assert.IsAssignableFrom<JsonValue>(first["logRatio"]).GetValue<double>();
            first["logRatio"] = stored + 0.5;
            File.WriteAllText(path, snapshot.ToJsonString(new JsonSerializerOptions { WriteIndented = true }));
            var before = File.ReadAllText(Path.Combine(root, "BENCHMARKS.md"));
            var outcome = BenchmarkReportQuartet.InvokeReport(root, false);
            Assert.NotEqual(0, outcome.ExitCode);
            Assert.Contains("does not match its raw timings", outcome.Output);
            Assert.Equal(before, File.ReadAllText(Path.Combine(root, "BENCHMARKS.md")));
        }
        finally
        {
            Directory.Delete(root, true);
        }
    }

    [Fact]
    public void ReportTablesRenderRecordedFields()
    {
        var root = CopyQuartet(_quartets.V9Root);
        try
        {
            var outcome = BenchmarkReportQuartet.InvokeReport(root, false);
            Assert.Equal(0, outcome.ExitCode);
            var document = File.ReadAllText(Path.Combine(root, "BENCHMARKS.md"));
            Assert.Contains("| Workload | Session | Lokad B/op | Lokad B/cell | Parquet.NET B/op | Parquet.NET B/cell | Lokad GC 0/1/2 | Parquet.NET GC 0/1/2 | Alloc | GC | CPU |", document);
            Assert.Contains("| Case | Pass | Projection | Target | Role | Batches | ms | Allocated B | GC 0/1/2 | Peak / output | Budget |", document);
            Assert.Contains("| Case | Layout | Nullable | Consumer | Range | Lokad live B | Baseline live B | Lokad retained | Baseline retained | End-scan retained | Retained |", document);
            Assert.Contains("cold-instrumented", document);
            Assert.Contains("int32/4", document);
            Assert.Contains("300000", document);
        }
        finally
        {
            Directory.Delete(root, true);
        }
    }

    private static void FailScanLane(string root, string caseName, double factor)
    {
        for (var index = 0; index < 4; index++)
        {
            var path = Path.Combine(root, "paired-" + index + ".json");
            var snapshot = Assert.IsType<JsonObject>(JsonNode.Parse(File.ReadAllText(path)));
            var cases = Assert.IsType<JsonArray>(snapshot["cases"]);
            JsonObject? found = null;
            foreach (var entry in cases)
            {
                var candidate = Assert.IsType<JsonObject>(entry);
                if (string.Equals(candidate["name"]?.GetValue<string>(), caseName, StringComparison.Ordinal))
                    found = candidate;
            }

            var target = Assert.IsType<JsonObject>(found);
            var observations = Assert.IsType<JsonArray>(target["observations"]);
            var first = Assert.IsType<JsonObject>(observations[0]);
            var second = Assert.IsType<JsonObject>(observations[1]);
            var firstLokad = Assert.IsAssignableFrom<JsonValue>(first["lokadNanoseconds"]).GetValue<double>() * factor;
            var secondLokad = Assert.IsAssignableFrom<JsonValue>(second["lokadNanoseconds"]).GetValue<double>() * factor;
            var baseline = Assert.IsAssignableFrom<JsonValue>(first["parquetNetNanoseconds"]).GetValue<double>();
            foreach (var entry in observations)
            {
                var observation = Assert.IsType<JsonObject>(entry);
                var scaled = Assert.IsAssignableFrom<JsonValue>(observation["lokadNanoseconds"]).GetValue<double>() * factor;
                var reference = Assert.IsAssignableFrom<JsonValue>(observation["parquetNetNanoseconds"]).GetValue<double>();
                observation["lokadNanoseconds"] = scaled;
                observation["logRatio"] = Math.Log(scaled / reference);
            }

            var summary = BenchmarkReportQuartet.Summarize(firstLokad, secondLokad, baseline, false);
            target["pointRatio"] = summary.Point;
            target["upper95Ratio"] = summary.Upper;
            target["passed"] = summary.Upper <= 1.05;
            File.WriteAllText(path, snapshot.ToJsonString(new JsonSerializerOptions { WriteIndented = true }));
        }
    }


    private static (int ExitCode, string Output) VerifyAfterCatalogMutation(string source, Action<JsonObject> mutate)
    {
        var root = CopyQuartet(source);
        try
        {
            var path = Path.Combine(root, "catalog.json");
            var catalog = Assert.IsType<JsonObject>(JsonNode.Parse(File.ReadAllText(path)));
            mutate(catalog);
            File.WriteAllText(path, catalog.ToJsonString(new JsonSerializerOptions { WriteIndented = true }));
            return RunVerify(root);
        }
        finally
        {
            Directory.Delete(root, true);
        }
    }


}


