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
                    ["logicalOutputBytes"] = 100000,
                    ["sourceCopiedBytes"] = 524800,
                    ["endOfScanRetainedPoolBytes"] = 200000,
                    ["consumerUtf8CopiedBytes"] = 0,
                    ["pooledBytesCleared"] = 524544,
                    ["retainedPoolBytes"] = 0,
                    ["passPeaks"] = new JsonArray
                    {
                        new JsonObject
                        {
                            ["peakPooledBytes"] = 200000,
                            ["logicalOutputBytes"] = 100000,
                        },
                        new JsonObject
                        {
                            ["peakPooledBytes"] = 150000,
                            ["logicalOutputBytes"] = 100000,
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
            var scan = Summarize(1010.0, 1000.0, 1000.0);
            var meta = Summarize(1000.0, 1000.0, 1000.0);
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

        // Mirrors the report verifier operation for operation: sequential mean,
        // squared deviations via Math.Pow, and the schema-selected quantile.
        (double Point, double Upper) Summarize(double firstLokad, double secondLokad, double baseline)
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
            return (Math.Exp(mean), Math.Exp(mean + Quantile(count - 1) * error));
        }

        double Quantile(int degreesOfFreedom)
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
}
