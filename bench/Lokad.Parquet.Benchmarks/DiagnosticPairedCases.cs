using System.Security.Cryptography;
using System.Text;
using BaselineParquetReader = Parquet.ParquetReader;

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
        "Diagnostic/RequiredInt64Plain",
        "Diagnostic/RequiredFloatPlain",
        "Diagnostic/RequiredDoublePlain",
        "Diagnostic/NullableBinaryPlain",
        "Diagnostic/NullableFixedPlain",
        "Diagnostic/RequiredInt32V2",
        "Diagnostic/NullableInt32V2",
        "Diagnostic/RequiredInt32RowRange",
        "Diagnostic/SmallRowGroupsInt32",
        "Diagnostic/HighCardinalityStringDictionary",
        "Diagnostic/MisalignedMultiPage",
        "Diagnostic/CrcInt32Plain",
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

        async Task<PairedOperationCase> CreateShapeCaseAsync(DiagnosticShapeSpec spec)
        {
            var lokadStream = new MemoryStream(spec.FixtureBytes, writable: false);
            var baselineStream = new MemoryStream(spec.FixtureBytes, writable: false);
            ParquetFile? lokadFile = null;
            BaselineParquetReader? baselineReader = null;
            try
            {
                lokadFile = await ParquetFile.OpenAsync(lokadStream);
                baselineReader = await BaselineParquetReader.CreateAsync(baselineStream);
            }
            catch
            {
                if (lokadFile is not null)
                    await lokadFile.DisposeAsync();
                if (baselineReader is not null)
                    await baselineReader.DisposeAsync();
                lokadStream.Dispose();
                baselineStream.Dispose();
                throw;
            }

            var openLokadFile = lokadFile ?? throw new InvalidOperationException($"The {name} diagnostic case failed to open its fixture.");
            var openBaselineReader = baselineReader ?? throw new InvalidOperationException($"The {name} diagnostic case failed to open its fixture.");
            try
            {
                var columns = spec.Projection.Select(ordinal => openLokadFile.Metadata.Schema.Columns[ordinal]).ToArray();
                var baselineFields = openBaselineReader.Schema.DataFields.ToArray();
                var fields = spec.Projection.Select(ordinal => baselineFields[ordinal]).ToArray();
                var maximumGroupRows = 0;
                for (var groupOrdinal = 0; groupOrdinal < openBaselineReader.RowGroupCount; groupOrdinal++)
                {
                    using var group = openBaselineReader.OpenRowGroupReader(groupOrdinal);
                    maximumGroupRows = Math.Max(maximumGroupRows, checked((int)group.RowCount));
                }

                var destinations = LiveSessionRetention.BaselineDestinations.Create(fields, spec.Workload, maximumGroupRows);
                var lokadValueChains = new long[columns.Length];
                var lokadNullChains = new long[columns.Length];
                var baselineValueChains = new long[fields.Length];
                var baselineNullChains = new long[fields.Length];
                Utf8ScanSink? lokadSink = ScanWorkloadCatalog.IsString(spec.Workload) ? new Utf8ScanSink(spec.EmittedRows, spec.Utf8PayloadBytes) : null;
                Utf8ScanSink? baselineSink = ScanWorkloadCatalog.IsString(spec.Workload) ? new Utf8ScanSink(spec.EmittedRows, spec.Utf8PayloadBytes) : null;
                ParquetRowRange? rowRange = spec.RangeStart.HasValue && spec.RangeCount.HasValue ? new ParquetRowRange(spec.RangeStart.Value, spec.RangeCount.Value) : null;
                async Task<long> RunLokadAsync()
                {
                    Array.Fill(lokadValueChains, ScanChecksum.Seed);
                    Array.Fill(lokadNullChains, ScanChecksum.Seed);
                    lokadSink?.Reset();
                    var outcome = await WorkCensusRunner.RunCensusPassAsync(
                        openLokadFile, spec.Workload, spec.EmittedRows, spec.Utf8PayloadBytes, columns, spec.EmittedRows,
                        spec.ExpectedChecksum, spec.ColumnHashes, spec.NullCounts, rowRange,
                        lokadValueChains, lokadNullChains, lokadSink);
                    return outcome.Checksum;
                }

                async Task<long> RunBaselineAsync()
                {
                    baselineSink?.Reset();
                    return await LiveSessionRetention.ScanBaselineSessionAsync(
                        openBaselineReader, fields, spec.Workload, spec.EmittedRows, spec.Utf8PayloadBytes,
                        spec.RangeStart ?? 0, spec.RangeCount ?? spec.EmittedRows,
                        baselineValueChains, baselineNullChains, destinations, baselineSink);
                }

                async ValueTask DisposeCoreAsync()
                {
                    await openLokadFile.DisposeAsync();
                    await openBaselineReader.DisposeAsync();
                    lokadStream.Dispose();
                    baselineStream.Dispose();
                }

                if (await RunLokadAsync() != spec.ExpectedChecksum || await RunBaselineAsync() != spec.ExpectedChecksum)
                    throw new InvalidOperationException($"The {name} diagnostic case failed its creation truth check.");
                return new PairedOperationCase(
                    name,
                    Convert.ToHexStringLower(SHA256.HashData(spec.FixtureBytes)),
                    spec.EmittedRows,
                    columns.Length,
                    spec.Utf8PayloadBytes,
                    spec.ExpectedChecksum,
                    RunLokadAsync,
                    RunBaselineAsync,
                    DisposeCoreAsync);
            }
            catch
            {
                await openLokadFile.DisposeAsync();
                await openBaselineReader.DisposeAsync();
                lokadStream.Dispose();
                baselineStream.Dispose();
                throw;
            }
        }
        DiagnosticShapeSpec BuildRequiredInt64()
        {
            const int rows = 65_536;
            var values = new long[rows];
            var chain = ScanChecksum.Seed;
            for (var row = 0; row < rows; row++)
            {
                values[row] = ScanChecksum.CreateInt32(row);
                chain = ScanChecksum.MixInt64(chain, values[row]);
            }

            var folded = ScanChecksum.CombineColumn(chain, ScanChecksum.Seed);
            var bytes = Lokad.Parquet.Tests.ParquetFixtureBuilder.CreateInt32(new Lokad.Parquet.Tests.ParquetFixtureOptions
            {
                PhysicalTypeCode = (int)ParquetPhysicalType.Int64,
                PhysicalValues = values,
            });
            return new DiagnosticShapeSpec(ScanWorkload.RequiredInt64Plain, bytes, [0], null, null, rows, 0, folded, [folded], [0]);
        }

        DiagnosticShapeSpec BuildRequiredFloat()
        {
            const int rows = 65_536;
            var values = new float[rows];
            var chain = ScanChecksum.Seed;
            for (var row = 0; row < rows; row++)
            {
                values[row] = row * 1.5f + 1.0f;
                chain = ScanChecksum.MixFloat(chain, values[row]);
            }

            var folded = ScanChecksum.CombineColumn(chain, ScanChecksum.Seed);
            var bytes = Lokad.Parquet.Tests.ParquetFixtureBuilder.CreateInt32(new Lokad.Parquet.Tests.ParquetFixtureOptions
            {
                PhysicalTypeCode = (int)ParquetPhysicalType.Float,
                PhysicalValues = values,
            });
            return new DiagnosticShapeSpec(ScanWorkload.RequiredFloatPlain, bytes, [0], null, null, rows, 0, folded, [folded], [0]);
        }

        DiagnosticShapeSpec BuildRequiredDouble()
        {
            const int rows = 65_536;
            var values = new double[rows];
            var chain = ScanChecksum.Seed;
            for (var row = 0; row < rows; row++)
            {
                values[row] = row * 1.25 + 1.0;
                chain = ScanChecksum.MixDouble(chain, values[row]);
            }

            var folded = ScanChecksum.CombineColumn(chain, ScanChecksum.Seed);
            var bytes = Lokad.Parquet.Tests.ParquetFixtureBuilder.CreateInt32(new Lokad.Parquet.Tests.ParquetFixtureOptions
            {
                PhysicalTypeCode = (int)ParquetPhysicalType.Double,
                PhysicalValues = values,
            });
            return new DiagnosticShapeSpec(ScanWorkload.RequiredDoublePlain, bytes, [0], null, null, rows, 0, folded, [folded], [0]);
        }

        DiagnosticShapeSpec BuildNullableBinary()
        {
            const int rows = 8192;
            var payloads = new byte[rows][];
            var validity = new bool[rows];
            var chain = ScanChecksum.Seed;
            var nulls = ScanChecksum.Seed;
            var nullCount = 0;
            for (var row = 0; row < rows; row++)
            {
                if (row % 3 == 2)
                {
                    payloads[row] = [];
                    validity[row] = false;
                    nulls = ScanChecksum.Mix(nulls, row);
                    nullCount++;
                }
                else
                {
                    payloads[row] = [(byte)(row & 255), (byte)((row >> 8) & 255)];
                    validity[row] = true;
                    chain = ScanChecksum.MixBytes(chain, payloads[row]);
                }
            }

            var folded = ScanChecksum.CombineColumn(chain, nulls);
            var bytes = Lokad.Parquet.Tests.ParquetFixtureBuilder.CreateInt32(new Lokad.Parquet.Tests.ParquetFixtureOptions
            {
                PhysicalTypeCode = (int)ParquetPhysicalType.ByteArray,
                PhysicalValues = payloads,
                Repetition = ParquetRepetition.Optional,
                Validity = validity,
            });
            return new DiagnosticShapeSpec(ScanWorkload.NullableBinaryPlain, bytes, [0], null, null, rows, 0, folded, [folded], [nullCount]);
        }

        DiagnosticShapeSpec BuildNullableFixed()
        {
            const int rows = 4096;
            var payloads = new byte[rows][];
            var validity = new bool[rows];
            var chain = ScanChecksum.Seed;
            var nulls = ScanChecksum.Seed;
            var nullCount = 0;
            for (var row = 0; row < rows; row++)
            {
                payloads[row] = [(byte)row, (byte)(row >> 8), (byte)(row >> 16), (byte)(row >> 24)];
                validity[row] = (row & 3) != 0;
                if (validity[row])
                    chain = ScanChecksum.MixBytes(chain, payloads[row]);
                else
                {
                    nulls = ScanChecksum.Mix(nulls, row);
                    nullCount++;
                }
            }

            var folded = ScanChecksum.CombineColumn(chain, nulls);
            var bytes = Lokad.Parquet.Tests.ParquetFixtureBuilder.CreateInt32(new Lokad.Parquet.Tests.ParquetFixtureOptions
            {
                PhysicalTypeCode = (int)ParquetPhysicalType.FixedLengthByteArray,
                TypeLength = 4,
                PhysicalValues = payloads,
                Repetition = ParquetRepetition.Optional,
                Validity = validity,
            });
            return new DiagnosticShapeSpec(ScanWorkload.NullableFixedByteArrayPlain, bytes, [0], null, null, rows, 0, folded, [folded], [nullCount]);
        }

        DiagnosticShapeSpec BuildRequiredInt32V2()
        {
            const int rows = 65_536;
            var values = Enumerable.Range(0, rows).Select(ScanChecksum.CreateInt32).ToArray();
            var folded = ScanChecksum.CombineColumn(ScanChecksum.ConsumeRequired(ScanChecksum.Seed, values), ScanChecksum.Seed);
            var bytes = Lokad.Parquet.Tests.ParquetFixtureBuilder.CreateInt32(new Lokad.Parquet.Tests.ParquetFixtureOptions
            {
                Values = values,
                PageVersion = Lokad.Parquet.Tests.FixturePageVersion.DataPageV2,
            });
            return new DiagnosticShapeSpec(ScanWorkload.RequiredInt32V2, bytes, [0], null, null, rows, 0, folded, [folded], [0]);
        }

        DiagnosticShapeSpec BuildNullableInt32V2()
        {
            const int rows = 65_536;
            var values = Enumerable.Range(0, rows).Select(ScanChecksum.CreateInt32).ToArray();
            var validity = Enumerable.Range(0, rows).Select(static row => (row & 7) != 0).ToArray();
            var chain = ScanChecksum.Seed;
            var nulls = ScanChecksum.Seed;
            var nullCount = 0;
            for (var row = 0; row < rows; row++)
            {
                if (validity[row])
                    chain = ScanChecksum.Mix(chain, values[row]);
                else
                {
                    nulls = ScanChecksum.Mix(nulls, row);
                    nullCount++;
                }
            }

            var folded = ScanChecksum.CombineColumn(chain, nulls);
            var bytes = Lokad.Parquet.Tests.ParquetFixtureBuilder.CreateInt32(new Lokad.Parquet.Tests.ParquetFixtureOptions
            {
                Values = values,
                Repetition = ParquetRepetition.Optional,
                Validity = validity,
                PageVersion = Lokad.Parquet.Tests.FixturePageVersion.DataPageV2,
            });
            return new DiagnosticShapeSpec(ScanWorkload.NullableInt32V2, bytes, [0], null, null, rows, 0, folded, [folded], [nullCount]);
        }

        async Task<DiagnosticShapeSpec> BuildRequiredInt32RowRangeAsync()
        {
            const int rows = 65_536;
            const int rangeStart = 4096;
            const int rangeCount = 4096;
            var fixture = await ScanFixture.CreateAsync(ScanWorkload.RequiredInt32Plain, rows);
            var chain = ScanChecksum.Seed;
            for (var row = rangeStart; row < rangeStart + rangeCount; row++)
                chain = ScanChecksum.Mix(chain, ScanChecksum.CreateInt32(row));
            var folded = ScanChecksum.CombineColumn(chain, ScanChecksum.Seed);
            return new DiagnosticShapeSpec(ScanWorkload.RequiredInt32Plain, fixture.Bytes, [0], rangeStart, rangeCount, rangeCount, 0, folded, [folded], [0]);
        }

        DiagnosticShapeSpec BuildSmallRowGroups()
        {
            const int groups = 8;
            const int groupRows = 8192;
            var pages = new int[groups][];
            var chain = ScanChecksum.Seed;
            for (var group = 0; group < groups; group++)
            {
                pages[group] = new int[groupRows];
                for (var row = 0; row < groupRows; row++)
                {
                    pages[group][row] = checked((group * groupRows + row) * 3 + 1);
                    chain = ScanChecksum.Mix(chain, pages[group][row]);
                }
            }

            var folded = ScanChecksum.CombineColumn(chain, ScanChecksum.Seed);
            var bytes = Lokad.Parquet.Tests.ParquetFixtureBuilder.CreateRequiredInt32RowGroups(pages);
            return new DiagnosticShapeSpec(ScanWorkload.RequiredInt32Plain, bytes, [0], null, null, groups * groupRows, 0, folded, [folded], [0]);
        }

        DiagnosticShapeSpec BuildHighCardinalityDictionary()
        {
            const int rows = 65_536;
            const int cardinality = 16_384;
            var dictionary = new byte[cardinality][];
            for (var index = 0; index < cardinality; index++)
                dictionary[index] = Encoding.UTF8.GetBytes("v-" + index);
            var values = new byte[rows][];
            var indices = new int[rows];
            var chain = ScanChecksum.Seed;
            var payloadBytes = 0;
            for (var row = 0; row < rows; row++)
            {
                indices[row] = row % cardinality;
                values[row] = dictionary[indices[row]];
                foreach (var value in values[row])
                {
                    chain = ScanChecksum.Mix(chain, value);
                    payloadBytes++;
                }

                chain = ScanChecksum.Mix(chain, ScanChecksum.ValueSeparator);
            }

            // The UTF-8 annotation is load-bearing: without it the baseline maps
            // BYTE_ARRAY to binary and never reaches its string consumer.
            var bytes = Lokad.Parquet.Tests.ParquetFixtureBuilder.CreateInt32(new Lokad.Parquet.Tests.ParquetFixtureOptions
            {
                PhysicalTypeCode = (int)ParquetPhysicalType.ByteArray,
                PhysicalValues = values,
                DictionaryValues = dictionary,
                DictionaryIndices = indices,
                ConvertedType = (int)ParquetConvertedType.Utf8,
                LogicalTypeDiscriminator = (int)ParquetLogicalTypeKind.String,
            });
            return new DiagnosticShapeSpec(ScanWorkload.RequiredStringDictionary, bytes, [0], null, null, rows, payloadBytes, chain, [chain], [0]);
        }

        DiagnosticShapeSpec BuildMisalignedMultiPage()
        {
            var leftPages = new[]
            {
                Enumerable.Range(0, 1000).Select(ScanChecksum.CreateInt32).ToArray(),
                Enumerable.Range(1000, 1000).Select(ScanChecksum.CreateInt32).ToArray(),
            };
            var rightPages = new[]
            {
                Enumerable.Range(0, 1500).Select(static row => ScanChecksum.CreateInt32(100000 + row)).ToArray(),
                Enumerable.Range(1500, 500).Select(static row => ScanChecksum.CreateInt32(100000 + row)).ToArray(),
            };
            var leftFolded = ScanChecksum.CombineColumn(
                ScanChecksum.ConsumeRequired(ScanChecksum.Seed, leftPages[0].Concat(leftPages[1]).ToArray()), ScanChecksum.Seed);
            var rightFolded = ScanChecksum.CombineColumn(
                ScanChecksum.ConsumeRequired(ScanChecksum.Seed, rightPages[0].Concat(rightPages[1]).ToArray()), ScanChecksum.Seed);
            var bytes = Lokad.Parquet.Tests.ParquetFixtureBuilder.CreateRequiredInt32Columns(
            [
                new Lokad.Parquet.Tests.RequiredInt32FixtureColumn { Name = "left", Pages = leftPages },
                new Lokad.Parquet.Tests.RequiredInt32FixtureColumn { Name = "right", Pages = rightPages },
            ]);
            var combined = ScanChecksum.CombineColumns([leftFolded, rightFolded]);
            return new DiagnosticShapeSpec(ScanWorkload.TwoRequiredInt32Plain, bytes, [0, 1], null, null, 2000, 0, combined, [leftFolded, rightFolded], [0, 0]);
        }

        DiagnosticShapeSpec BuildCrcInt32Plain()
        {
            // The Lokad reader validates every present page CRC while the
            // baseline ignores CRCs, so this lane prices validation explicitly.
            const int rows = 4096;
            var values = Enumerable.Range(0, rows).Select(ScanChecksum.CreateInt32).ToArray();
            var folded = ScanChecksum.CombineColumn(ScanChecksum.ConsumeRequired(ScanChecksum.Seed, values), ScanChecksum.Seed);
            var bytes = Lokad.Parquet.Tests.ParquetFixtureBuilder.CreateInt32(new Lokad.Parquet.Tests.ParquetFixtureOptions
            {
                Values = values,
                CrcMode = Lokad.Parquet.Tests.FixtureCrcMode.Valid,
            });
            return new DiagnosticShapeSpec(ScanWorkload.RequiredInt32Plain, bytes, [0], null, null, rows, 0, folded, [folded], [0]);
        }
        return name switch
        {
            "Diagnostic/SourceMemory" => await CreateSourceCaseAsync(DiagnosticSource.Memory),
            "Diagnostic/SourceStream" => await CreateSourceCaseAsync(DiagnosticSource.Stream),
            "Diagnostic/SourceFile" => await CreateSourceCaseAsync(DiagnosticSource.File),
            "Diagnostic/SourceCustom" => await CreateSourceCaseAsync(DiagnosticSource.Custom),
            "Diagnostic/RequiredInt64Plain" => await CreateShapeCaseAsync(BuildRequiredInt64()),
            "Diagnostic/RequiredFloatPlain" => await CreateShapeCaseAsync(BuildRequiredFloat()),
            "Diagnostic/RequiredDoublePlain" => await CreateShapeCaseAsync(BuildRequiredDouble()),
            "Diagnostic/NullableBinaryPlain" => await CreateShapeCaseAsync(BuildNullableBinary()),
            "Diagnostic/NullableFixedPlain" => await CreateShapeCaseAsync(BuildNullableFixed()),
            "Diagnostic/RequiredInt32V2" => await CreateShapeCaseAsync(BuildRequiredInt32V2()),
            "Diagnostic/NullableInt32V2" => await CreateShapeCaseAsync(BuildNullableInt32V2()),
            "Diagnostic/RequiredInt32RowRange" => await CreateShapeCaseAsync(await BuildRequiredInt32RowRangeAsync()),
            "Diagnostic/SmallRowGroupsInt32" => await CreateShapeCaseAsync(BuildSmallRowGroups()),
            "Diagnostic/HighCardinalityStringDictionary" => await CreateShapeCaseAsync(BuildHighCardinalityDictionary()),
            "Diagnostic/MisalignedMultiPage" => await CreateShapeCaseAsync(BuildMisalignedMultiPage()),
            "Diagnostic/CrcInt32Plain" => await CreateShapeCaseAsync(BuildCrcInt32Plain()),
            _ => throw new ArgumentException($"Unknown diagnostic paired case '{name}'.", nameof(name)),
        };
    }

    /// <summary>Frozen shape of one diagnostic paired case with its truth.</summary>
    /// <param name="Workload">The workload token selecting the fixture and consumers.</param>
    /// <param name="FixtureBytes">The fixture both readers scan.</param>
    /// <param name="Projection">The projected column ordinals in scan order.</param>
    /// <param name="RangeStart">The first selected row, or null for a full scan.</param>
    /// <param name="RangeCount">The selected row count, or null for a full scan.</param>
    /// <param name="EmittedRows">The number of emitted rows.</param>
    /// <param name="Utf8PayloadBytes">The UTF-8 payload bytes.</param>
    /// <param name="ExpectedChecksum">The truth checksum both readers must reproduce.</param>
    /// <param name="ColumnHashes">The per-column truth hashes indexed by ordinal.</param>
    /// <param name="NullCounts">The per-column truth null counts indexed by ordinal.</param>
    internal sealed record DiagnosticShapeSpec(
        ScanWorkload Workload,
        byte[] FixtureBytes,
        int[] Projection,
        long? RangeStart,
        long? RangeCount,
        int EmittedRows,
        int Utf8PayloadBytes,
        long ExpectedChecksum,
        long[] ColumnHashes,
        int[] NullCounts);
    private enum DiagnosticSource
    {
        Memory,
        Stream,
        File,
        Custom,
    }
}
