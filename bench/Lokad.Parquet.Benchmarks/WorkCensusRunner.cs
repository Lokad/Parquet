using Parquet;
using Parquet.Schema;
using System.Buffers.Binary;
using BaselineParquetReader = Parquet.ParquetReader;
using System.Diagnostics;
using System.Text;
using BaselineParquetSchema = Parquet.Schema.ParquetSchema;
using BaselineParquetWriter = Parquet.ParquetWriter;
using System.Reflection;
using System.Runtime.InteropServices;
using System.Security.Cryptography;
using System.Text.Json;

namespace Lokad.Parquet.Benchmarks;

internal static class WorkCensusRunner
{
    private const int RowCount = 65_536;
    internal const int SnapshotSchemaVersion = 4;
    private static readonly Type PoolType =
        typeof(ParquetFile).Assembly
            .GetType("Lokad.Parquet.Internal.ParquetArrayPool", throwOnError: true, ignoreCase: false)
        ?? throw new InvalidOperationException("The internal pool facade is unavailable.");
    private static readonly PropertyInfo PoolRentObserverProperty =
        PoolType.GetProperty("RentObserver", BindingFlags.Public | BindingFlags.Static)
        ?? throw new InvalidOperationException("The internal pool rent observer is unavailable.");
    private static readonly PropertyInfo PoolReturnObserverProperty =
        PoolType.GetProperty("ReturnObserver", BindingFlags.Public | BindingFlags.Static)
        ?? throw new InvalidOperationException("The internal pool return observer is unavailable.");

    public static Task<int> RunAsync() => RunCensusAsync(16, CensusOutputPath(false));

    // Q01 public-validation smoke: the same orchestration with one retention
    // repetition instead of sixteen. Truth, pool-balance and catalog checks are
    // unchanged; only repeat timing is reduced, and the snapshot lands in an
    // isolated temp directory so smoke evidence can never qualify as a campaign.
    public static Task<int> RunSmokeAsync() => RunCensusAsync(1, CensusOutputPath(true));

    private static string CensusOutputPath(bool smoke)
    {
        var platform = OperatingSystem.IsWindows() ? "windows" : "linux";
        var leaf = "work-census-" + platform + "-" + DateTimeOffset.UtcNow.ToString("yyyyMMdd-HHmmss") + ".json";
        if (smoke)
            return Path.Combine(Path.GetTempPath(), "lokad-census-smoke-" + Guid.NewGuid().ToString("N"), leaf);
        return Path.Combine("artifacts", "benchmarks", leaf);
    }

    private static async Task<int> RunCensusAsync(int retentionRepetitions, string outputPath)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(retentionRepetitions);
        ArgumentException.ThrowIfNullOrWhiteSpace(outputPath);
        var sessionId = Guid.NewGuid();
        var sessionStartedAt = DateTimeOffset.UtcNow;
        var session = BeginCensusSession(outputPath, sessionId, sessionStartedAt);
        var snapshotParent = Path.GetDirectoryName(Path.GetFullPath(outputPath)) ??
            throw new InvalidOperationException("The work-census output has no directory.");
        foreach (var incomplete in PairedSessionRecorder.FindIncompleteSessionDirectories(snapshotParent))
        {
            if (!string.Equals(incomplete.SessionDirectory, session.SessionDirectory, StringComparison.Ordinal))
                Console.WriteLine($"Warning: incomplete census session {incomplete.SessionDirectory} has no completeness marker and never qualifies as evidence.");
        }
        var results = new List<WorkCensusCase>();
        var order = 0;
        async Task<WorkCensusCase> RunCensusCaseAsync(Func<Task<WorkCensusCase>> measureAsync)
        {
            var caseStartedAt = DateTimeOffset.UtcNow;
            var result = await measureAsync();
            var caseEndedAt = DateTimeOffset.UtcNow;
            RecordCensusCheckpoint(session, order, result.Name, caseStartedAt, caseEndedAt, result);
            results.Add(result);
            order++;
            return result;
        }
        try
        {
            foreach (var workload in ScanWorkloadCatalog.ParityWorkloads)
                await RunCensusCaseAsync(() => MeasureAsync(workload, retentionRepetitions));
            {
                // Uneven multi-row-group case: the catalog writer emits one page per
                // column chunk, so uneven batch partitioning is covered here with a
                // fully known oracle instead.
                const int firstGroupRows = 2048;
                const int secondGroupRows = 6144;
                var (unevenBytes, unevenFolded, unevenExpected) = await ScanTruthVerification.WriteUnevenTwoColumnFixtureAsync(firstGroupRows, secondGroupRows);
                await RunCensusCaseAsync(() => MeasureCustomAsync(
                    "UnevenInt32Plain",
                    unevenBytes,
                    firstGroupRows + secondGroupRows,
                    2,
                    0,
                    0,
                    unevenExpected,
                    unevenFolded,
                    [0, 0],
                    ScanWorkload.TwoRequiredInt32Plain,
                    new CensusCaseLayout(CensusPhysicalType.Int32, sizeof(int), false, CensusConsumer.MultiInt32),
                    [new CensusPassSpec([0, 1], firstGroupRows + secondGroupRows),
                     new CensusPassSpec([1], 4096)],
                    null, retentionRepetitions));
            }
            {
                // Narrow projection in a wide schema: one column of eight at the
                // full target, then a different single column at a small target.
                var wide = await ScanFixture.CreateAsync(ScanWorkload.EightRequiredInt32Plain, RowCount);
                await RunCensusCaseAsync(() => MeasureCustomAsync(
                    "NarrowInt32Plain",
                    wide.Bytes,
                    RowCount,
                    1,
                    0,
                    0,
                    wide.Checksum,
                    wide.ColumnChecksums,
                    wide.NullCounts,
                    ScanWorkload.EightRequiredInt32Plain,
                    new CensusCaseLayout(CensusPhysicalType.Int32, sizeof(int), false, CensusConsumer.Int32),
                    [new CensusPassSpec([0], RowCount),
                     new CensusPassSpec([1], 4096)],
                    null, retentionRepetitions));
            }
            {
                // Caller-selected small row range: the oracle covers exactly the
                // selected rows while source accounting observes only the bounded
                // slice reads underneath; the group cursor still advances past
                // each whole page.
                const int rangeStart = 4096;
                const int rangeCount = 4096;
                var fixture = await ScanFixture.CreateAsync(ScanWorkload.RequiredInt32Plain, RowCount);
                var chain = ScanChecksum.Seed;
                for (var row = rangeStart; row < rangeStart + rangeCount; row++)
                    chain = ScanChecksum.Mix(chain, ScanChecksum.CreateInt32(row));
                var folded = ScanChecksum.CombineColumn(chain, ScanChecksum.Seed);
                await RunCensusCaseAsync(() => MeasureCustomAsync(
                    "RequiredInt32RowRange",
                    fixture.Bytes,
                    rangeCount,
                    1,
                    0,
                    0,
                    folded,
                    [folded],
                    [0],
                    ScanWorkload.RequiredInt32Plain,
                    new CensusCaseLayout(CensusPhysicalType.Int32, sizeof(int), false, CensusConsumer.Int32),
                    [new CensusPassSpec([0], rangeCount)],
                    new ParquetRowRange(rangeStart, rangeCount), retentionRepetitions));
            }
            {
                // Many small row groups stand in for many small pages, with page
                // boundary stress across eight groups.
                const int smallGroups = 8;
                const int smallGroupRows = 8192;
                var (smallBytes, smallFolded) = await WriteSmallRowGroupsFixtureAsync(smallGroups, smallGroupRows);
                await RunCensusCaseAsync(() => MeasureCustomAsync(
                    "SmallRowGroupsInt32Plain",
                    smallBytes,
                    smallGroups * smallGroupRows,
                    1,
                    0,
                    0,
                    smallFolded,
                    [smallFolded],
                    [0],
                    ScanWorkload.RequiredInt32Plain,
                    new CensusCaseLayout(CensusPhysicalType.Int32, sizeof(int), false, CensusConsumer.Int32),
                    [new CensusPassSpec([0], smallGroups * smallGroupRows),
                     new CensusPassSpec([0], 4096)],
                    null, retentionRepetitions));
            }
            {
                // Highly compressible Snappy lane: zeros exercise copy-heavy
                // decoding against the hash-valued literal-heavy catalog lane.
                var zeros = new int[RowCount];
                var compressibleField = new DataField<int>("value", nullable: false);
                using var compressibleStream = new MemoryStream();
                var compressibleOptions = new ParquetOptions { CompressionMethod = CompressionMethod.Snappy, DictionaryEncodingThreshold = 0 };
                await using (var writer = await BaselineParquetWriter.CreateAsync(new BaselineParquetSchema(compressibleField), compressibleStream, compressibleOptions))
                {
                    using var rowGroup = writer.CreateRowGroup();
                    await rowGroup.WriteAsync<int>(compressibleField, zeros);
                    rowGroup.CompleteValidate();
                }
                var compressibleChain = ScanChecksum.ConsumeRequired(ScanChecksum.Seed, zeros);
                var compressibleFolded = ScanChecksum.CombineColumn(compressibleChain, ScanChecksum.Seed);
                await RunCensusCaseAsync(() => MeasureCustomAsync(
                    "CompressibleInt32Snappy",
                    compressibleStream.ToArray(),
                    RowCount,
                    1,
                    0,
                    0,
                    compressibleFolded,
                    [compressibleFolded],
                    [0],
                    ScanWorkload.RequiredInt32Snappy,
                    new CensusCaseLayout(CensusPhysicalType.Int32, sizeof(int), false, CensusConsumer.Int32),
                    [new CensusPassSpec([0], RowCount),
                     new CensusPassSpec([0], 4096)],
                    null, retentionRepetitions));
            }
            {
                // Optional non-INT32 lane: nullable booleans carry their own workload
                // token with bool-lane validity accounting.
                const int booleanRows = 8192;
                var (booleanBytes, booleanFolded, booleanNulls) = await WriteNullableBooleanFixtureAsync(booleanRows);
                await RunCensusCaseAsync(() => MeasureCustomAsync(
                    "NullableBooleanPlain",
                    booleanBytes,
                    booleanRows,
                    1,
                    0,
                    0,
                    booleanFolded,
                    [booleanFolded],
                    [booleanNulls],
                    ScanWorkload.NullableBooleanPlain,
                    new CensusCaseLayout(CensusPhysicalType.Boolean, 1, true, CensusConsumer.Boolean),
                    [new CensusPassSpec([0], booleanRows),
                     new CensusPassSpec([0], 4096)],
                    null, retentionRepetitions));
            }
            {
                // Minimal dictionary cardinality with maximal reuse: two values
                // over the full row count.
                const int dictionaryRows = 65536;
                var (dictionaryBytes, dictionaryChecksum, dictionaryUtf8Bytes) = await WriteLowCardinalityDictionaryFixtureAsync(dictionaryRows);
                await RunCensusCaseAsync(() => MeasureCustomAsync(
                    "LowCardinalityStringDictionary",
                    dictionaryBytes,
                    dictionaryRows,
                    1,
                    dictionaryUtf8Bytes,
                    0,
                    dictionaryChecksum,
                    [dictionaryChecksum],
                    [0],
                    ScanWorkload.RequiredStringDictionary,
                    new CensusCaseLayout(CensusPhysicalType.Utf8, 0, false, CensusConsumer.Utf8),
                    [new CensusPassSpec([0], dictionaryRows),
                     new CensusPassSpec([0], 4096)],
                    null, retentionRepetitions));
            }

            {
                // Wider required primitives: INT64, FLOAT and DOUBLE decode through
                // dedicated physical paths with their own checksums and layouts.
                const int wideRows = 65536;
                var int64Field = new DataField<long>("value", nullable: false);
                var int64Values = new long[wideRows];
                var int64Chain = ScanChecksum.Seed;
                for (var row = 0; row < int64Values.Length; row++)
                {
                    int64Values[row] = ScanChecksum.CreateInt32(row);
                    int64Chain = ScanChecksum.MixInt64(int64Chain, int64Values[row]);
                }

                var int64Folded = ScanChecksum.CombineColumn(int64Chain, ScanChecksum.Seed);
                using var int64Stream = new MemoryStream();
                await using (var writer = await BaselineParquetWriter.CreateAsync(
                    new BaselineParquetSchema(int64Field), int64Stream,
                    new ParquetOptions { CompressionMethod = CompressionMethod.None, DictionaryEncodingThreshold = 0 }))
                {
                    using var rowGroup = writer.CreateRowGroup();
                    await rowGroup.WriteAsync<long>(int64Field, int64Values);
                    rowGroup.CompleteValidate();
                }

                await RunCensusCaseAsync(() => MeasureCustomAsync(
                    "RequiredInt64Plain",
                    int64Stream.ToArray(),
                    wideRows,
                    1,
                    0,
                    0,
                    int64Folded,
                    [int64Folded],
                    [0],
                    ScanWorkload.RequiredInt64Plain,
                    new CensusCaseLayout(CensusPhysicalType.Int64, sizeof(long), false, CensusConsumer.Int64),
                    [new CensusPassSpec([0], wideRows),
                     new CensusPassSpec([0], 4096)],
                    null, retentionRepetitions));
            }
            {
                const int wideRows = 65536;
                var floatField = new DataField<float>("value", nullable: false);
                var floatValues = new float[wideRows];
                var floatChain = ScanChecksum.Seed;
                for (var row = 0; row < floatValues.Length; row++)
                {
                    floatValues[row] = (float)row * 0.5f + 1f;
                    floatChain = ScanChecksum.MixFloat(floatChain, floatValues[row]);
                }

                var floatFolded = ScanChecksum.CombineColumn(floatChain, ScanChecksum.Seed);
                using var floatStream = new MemoryStream();
                await using (var writer = await BaselineParquetWriter.CreateAsync(
                    new BaselineParquetSchema(floatField), floatStream,
                    new ParquetOptions { CompressionMethod = CompressionMethod.None, DictionaryEncodingThreshold = 0 }))
                {
                    using var rowGroup = writer.CreateRowGroup();
                    await rowGroup.WriteAsync<float>(floatField, floatValues);
                    rowGroup.CompleteValidate();
                }

                await RunCensusCaseAsync(() => MeasureCustomAsync(
                    "RequiredFloatPlain",
                    floatStream.ToArray(),
                    wideRows,
                    1,
                    0,
                    0,
                    floatFolded,
                    [floatFolded],
                    [0],
                    ScanWorkload.RequiredFloatPlain,
                    new CensusCaseLayout(CensusPhysicalType.Float, sizeof(float), false, CensusConsumer.Float),
                    [new CensusPassSpec([0], wideRows),
                     new CensusPassSpec([0], 4096)],
                    null, retentionRepetitions));
            }
            {
                const int wideRows = 65536;
                var doubleField = new DataField<double>("value", nullable: false);
                var doubleValues = new double[wideRows];
                var doubleChain = ScanChecksum.Seed;
                for (var row = 0; row < doubleValues.Length; row++)
                {
                    doubleValues[row] = (double)row * 0.5 + 1.0;
                    doubleChain = ScanChecksum.MixDouble(doubleChain, doubleValues[row]);
                }

                var doubleFolded = ScanChecksum.CombineColumn(doubleChain, ScanChecksum.Seed);
                using var doubleStream = new MemoryStream();
                await using (var writer = await BaselineParquetWriter.CreateAsync(
                    new BaselineParquetSchema(doubleField), doubleStream,
                    new ParquetOptions { CompressionMethod = CompressionMethod.None, DictionaryEncodingThreshold = 0 }))
                {
                    using var rowGroup = writer.CreateRowGroup();
                    await rowGroup.WriteAsync<double>(doubleField, doubleValues);
                    rowGroup.CompleteValidate();
                }

                await RunCensusCaseAsync(() => MeasureCustomAsync(
                    "RequiredDoublePlain",
                    doubleStream.ToArray(),
                    wideRows,
                    1,
                    0,
                    0,
                    doubleFolded,
                    [doubleFolded],
                    [0],
                    ScanWorkload.RequiredDoublePlain,
                    new CensusCaseLayout(CensusPhysicalType.Double, sizeof(double), false, CensusConsumer.Double),
                    [new CensusPassSpec([0], wideRows),
                     new CensusPassSpec([0], 4096)],
                    null, retentionRepetitions));
            }

            {
                // Nullable INT64 mirrors the nullable INT32 lane shape with eight-byte slots.
                const int nullableInt64Rows = 65536;
                var nullableInt64Field = new DataField<long?>("value");
                var nullableInt64Values = new long?[nullableInt64Rows];
                var nullableInt64Chain = ScanChecksum.Seed;
                var nullableInt64Nulls = ScanChecksum.Seed;
                var nullableInt64NullCount = 0;
                for (var row = 0; row < nullableInt64Values.Length; row++)
                {
                    if ((row & 7) == 0)
                    {
                        nullableInt64Values[row] = null;
                        nullableInt64Nulls = ScanChecksum.Mix(nullableInt64Nulls, row);
                        nullableInt64NullCount++;
                    }
                    else
                    {
                        var nullableInt64Value = ScanChecksum.CreateInt32(row);
                        nullableInt64Values[row] = nullableInt64Value;
                        nullableInt64Chain = ScanChecksum.MixInt64(nullableInt64Chain, nullableInt64Value);
                    }
                }

                var nullableInt64Folded = ScanChecksum.CombineColumn(nullableInt64Chain, nullableInt64Nulls);
                using var nullableInt64Stream = new MemoryStream();
                await using (var writer = await BaselineParquetWriter.CreateAsync(
                    new BaselineParquetSchema(nullableInt64Field), nullableInt64Stream,
                    new ParquetOptions { CompressionMethod = CompressionMethod.None, DictionaryEncodingThreshold = 0 }))
                {
                    using var rowGroup = writer.CreateRowGroup();
                    await rowGroup.WriteAsync<long>(nullableInt64Field, nullableInt64Values);
                    rowGroup.CompleteValidate();
                }

                await RunCensusCaseAsync(() => MeasureCustomAsync(
                    "NullableInt64Plain",
                    nullableInt64Stream.ToArray(),
                    nullableInt64Rows,
                    1,
                    0,
                    0,
                    nullableInt64Folded,
                    [nullableInt64Folded],
                    [nullableInt64NullCount],
                    ScanWorkload.NullableInt64Plain,
                    new CensusCaseLayout(CensusPhysicalType.Int64, sizeof(long), true, CensusConsumer.NullableInt64),
                    [new CensusPassSpec([0], nullableInt64Rows),
                     new CensusPassSpec([0], 4096)],
                    null, retentionRepetitions));
            }
            {
                // Dense nulls: every other row is null, stressing definition-level
                // runs and validity expansion far beyond the one-in-eight lane.
                const int denseRows = 65536;
                var denseField = new DataField<int?>("value");
                var denseValues = new int?[denseRows];
                var denseChain = ScanChecksum.Seed;
                var denseNulls = ScanChecksum.Seed;
                var denseNullCount = 0;
                for (var row = 0; row < denseValues.Length; row++)
                {
                    if ((row & 1) == 0)
                    {
                        denseValues[row] = null;
                        denseNulls = ScanChecksum.Mix(denseNulls, row);
                        denseNullCount++;
                    }
                    else
                    {
                        var denseValue = ScanChecksum.CreateInt32(row);
                        denseValues[row] = denseValue;
                        denseChain = ScanChecksum.Mix(denseChain, denseValue);
                    }
                }

                var denseFolded = ScanChecksum.CombineColumn(denseChain, denseNulls);
                using var denseStream = new MemoryStream();
                await using (var writer = await BaselineParquetWriter.CreateAsync(
                    new BaselineParquetSchema(denseField), denseStream,
                    new ParquetOptions { CompressionMethod = CompressionMethod.None, DictionaryEncodingThreshold = 0 }))
                {
                    using var rowGroup = writer.CreateRowGroup();
                    await rowGroup.WriteAsync<int>(denseField, denseValues);
                    rowGroup.CompleteValidate();
                }

                await RunCensusCaseAsync(() => MeasureCustomAsync(
                    "NullableInt32DenseNulls",
                    denseStream.ToArray(),
                    denseRows,
                    1,
                    0,
                    0,
                    denseFolded,
                    [denseFolded],
                    [denseNullCount],
                    ScanWorkload.NullableInt32Plain,
                    new CensusCaseLayout(CensusPhysicalType.Int32, sizeof(int), true, CensusConsumer.NullableInt32),
                    [new CensusPassSpec([0], denseRows),
                     new CensusPassSpec([0], 4096)],
                    null, retentionRepetitions));
            }
            {
                // High dictionary cardinality: 16,384 distinct values over the full
                // row count with dictionary encoding forced, mirroring the
                // low-cardinality lane oracle shape.
                const int highCardRows = 65536;
                var highCardField = new DataField<string>("value", nullable: false);
                var highCardEncoded = new ReadOnlyMemory<char>[highCardRows];
                var highCardChain = ScanChecksum.Seed;
                var highCardPayload = 0;
                for (var row = 0; row < highCardEncoded.Length; row++)
                {
                    var text = "v-" + (row % 16384);
                    highCardEncoded[row] = string.Intern(text).AsMemory();
                    foreach (var value in Encoding.UTF8.GetBytes(text))
                    {
                        highCardChain = ScanChecksum.Mix(highCardChain, value);
                        highCardPayload++;
                    }

                    highCardChain = ScanChecksum.Mix(highCardChain, ScanChecksum.ValueSeparator);
                }

                using var highCardStream = new MemoryStream();
                var highCardOptions = new ParquetOptions { CompressionMethod = CompressionMethod.None, DictionaryEncodingThreshold = 1 };
                highCardOptions.ColumnEncodingHints[highCardField.Path.ToString()] = EncodingHint.Dictionary;
                await using (var writer = await BaselineParquetWriter.CreateAsync(new BaselineParquetSchema(highCardField), highCardStream, highCardOptions))
                {
                    using var rowGroup = writer.CreateRowGroup();
                    await rowGroup.WriteAsync<ReadOnlyMemory<char>>(highCardField, highCardEncoded);
                    rowGroup.CompleteValidate();
                }

                var highCardBytes = highCardStream.ToArray();
                await using (var inspection = await ParquetFile.OpenAsync((ReadOnlyMemory<byte>)highCardBytes))
                {
                    var chunk = inspection.Metadata.RowGroups[0].Columns[0];
                    var hasDictionaryEncoding = chunk.EncodingCodes.Contains((int)ParquetEncoding.PlainDictionary) ||
                        chunk.EncodingCodes.Contains((int)ParquetEncoding.RunLengthDictionary);
                    if (!hasDictionaryEncoding)
                        throw new InvalidOperationException("The high-cardinality fixture did not use dictionary encoding: " + string.Join(",", chunk.EncodingCodes) + ".");
                }

                await RunCensusCaseAsync(() => MeasureCustomAsync(
                    "HighCardinalityStringDictionary",
                    highCardBytes,
                    highCardRows,
                    1,
                    highCardPayload,
                    0,
                    highCardChain,
                    [highCardChain],
                    [0],
                    ScanWorkload.RequiredStringDictionary,
                    new CensusCaseLayout(CensusPhysicalType.Utf8, 0, false, CensusConsumer.Utf8),
                    [new CensusPassSpec([0], highCardRows),
                     new CensusPassSpec([0], 4096)],
                    null, retentionRepetitions));
            }

            {
                // Data Page V2 layouts: the hand-built V2 value sections exercise V2
                // level handling that the Parquet.NET writer cannot produce.
                const int v2Rows = 65536;
                var v2Values = Enumerable.Range(0, v2Rows).Select(ScanChecksum.CreateInt32).ToArray();
                var v2Chain = ScanChecksum.ConsumeRequired(ScanChecksum.Seed, v2Values);
                var v2Folded = ScanChecksum.CombineColumn(v2Chain, ScanChecksum.Seed);
                var v2Bytes = Lokad.Parquet.Tests.ParquetFixtureBuilder.CreateInt32(new Lokad.Parquet.Tests.ParquetFixtureOptions
                {
                    Values = v2Values,
                    PageVersion = Lokad.Parquet.Tests.FixturePageVersion.DataPageV2,
                });
                await RunCensusCaseAsync(() => MeasureCustomAsync(
                    "RequiredInt32V2",
                    v2Bytes,
                    v2Rows,
                    1,
                    0,
                    0,
                    v2Folded,
                    [v2Folded],
                    [0],
                    ScanWorkload.RequiredInt32V2,
                    new CensusCaseLayout(CensusPhysicalType.Int32, sizeof(int), false, CensusConsumer.Int32),
                    [new CensusPassSpec([0], v2Rows),
                     new CensusPassSpec([0], 4096)],
                    null, retentionRepetitions));
            }
            {
                const int nullableV2Rows = 65536;
                var nullableV2Values = Enumerable.Range(0, nullableV2Rows).Select(ScanChecksum.CreateInt32).ToArray();
                var nullableV2Validity = Enumerable.Range(0, nullableV2Rows).Select(static row => (row & 7) != 0).ToArray();
                var nullableV2Chain = ScanChecksum.Seed;
                var nullableV2Nulls = ScanChecksum.Seed;
                var nullableV2NullCount = 0;
                for (var row = 0; row < nullableV2Rows; row++)
                {
                    if (nullableV2Validity[row])
                        nullableV2Chain = ScanChecksum.Mix(nullableV2Chain, nullableV2Values[row]);
                    else
                    {
                        nullableV2Nulls = ScanChecksum.Mix(nullableV2Nulls, row);
                        nullableV2NullCount++;
                    }
                }

                var nullableV2Folded = ScanChecksum.CombineColumn(nullableV2Chain, nullableV2Nulls);
                var nullableV2Bytes = Lokad.Parquet.Tests.ParquetFixtureBuilder.CreateInt32(new Lokad.Parquet.Tests.ParquetFixtureOptions
                {
                    Values = nullableV2Values,
                    Repetition = ParquetRepetition.Optional,
                    Validity = nullableV2Validity,
                    PageVersion = Lokad.Parquet.Tests.FixturePageVersion.DataPageV2,
                });
                await RunCensusCaseAsync(() => MeasureCustomAsync(
                    "NullableInt32V2",
                    nullableV2Bytes,
                    nullableV2Rows,
                    1,
                    0,
                    0,
                    nullableV2Folded,
                    [nullableV2Folded],
                    [nullableV2NullCount],
                    ScanWorkload.NullableInt32V2,
                    new CensusCaseLayout(CensusPhysicalType.Int32, sizeof(int), true, CensusConsumer.NullableInt32),
                    [new CensusPassSpec([0], nullableV2Rows),
                     new CensusPassSpec([0], 4096)],
                    null, retentionRepetitions));
            }
            {
                // Nullable variable-width binary: optional BYTE_ARRAY pages with null
                // rows exercise definition levels over offsets without UTF-8 validation.
                const int binaryRows = 8192;
                var binaryValues = new byte[binaryRows][];
                var binaryValidity = new bool[binaryRows];
                var binaryChain = ScanChecksum.Seed;
                var binaryNulls = ScanChecksum.Seed;
                var binaryNullCount = 0;
                var binaryPayloadBytes = 0;
                for (var row = 0; row < binaryRows; row++)
                {
                    if (row % 3 == 2)
                    {
                        binaryValues[row] = [];
                        binaryValidity[row] = false;
                        binaryNulls = ScanChecksum.Mix(binaryNulls, row);
                        binaryNullCount++;
                    }
                    else
                    {
                        binaryValues[row] = [(byte)(row & 255), (byte)((row >> 8) & 255)];
                        binaryValidity[row] = true;
                        binaryChain = ScanChecksum.MixBytes(binaryChain, binaryValues[row]);
                        binaryPayloadBytes += binaryValues[row].Length;
                    }
                }

                var binaryFolded = ScanChecksum.CombineColumn(binaryChain, binaryNulls);
                var binaryBytes = Lokad.Parquet.Tests.ParquetFixtureBuilder.CreateInt32(new Lokad.Parquet.Tests.ParquetFixtureOptions
                {
                    PhysicalTypeCode = (int)ParquetPhysicalType.ByteArray,
                    PhysicalValues = binaryValues,
                    Repetition = ParquetRepetition.Optional,
                    Validity = binaryValidity,
                });
                await RunCensusCaseAsync(() => MeasureCustomAsync(
                    "NullableBinaryPlain",
                    binaryBytes,
                    binaryRows,
                    1,
                    0,
                    binaryPayloadBytes,
                    binaryFolded,
                    [binaryFolded],
                    [binaryNullCount],
                    ScanWorkload.NullableBinaryPlain,
                    new CensusCaseLayout(CensusPhysicalType.ByteArray, 0, true, CensusConsumer.NullableBinary),
                    [new CensusPassSpec([0], binaryRows),
                     new CensusPassSpec([0], 4096)],
                    null, retentionRepetitions));
            }
            {
                // Real multi-page misalignment within one row group: the two columns
                // split their pages at different rows, so small-target projected
                // batches slice partial pages on both sides instead of transferring.
                const int misalignedRows = 2000;
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
                var misalignedBytes = Lokad.Parquet.Tests.ParquetFixtureBuilder.CreateRequiredInt32Columns(
                [
                    new Lokad.Parquet.Tests.RequiredInt32FixtureColumn { Name = "left", Pages = leftPages },
                    new Lokad.Parquet.Tests.RequiredInt32FixtureColumn { Name = "right", Pages = rightPages },
                ]);
                var misalignedChecksum = ScanChecksum.CombineColumns([leftFolded, rightFolded]);
                await RunCensusCaseAsync(() => MeasureCustomAsync(
                    "MisalignedMultiPage",
                    misalignedBytes,
                    misalignedRows,
                    2,
                    0,
                    0,
                    misalignedChecksum,
                    [leftFolded, rightFolded],
                    [0, 0],
                    ScanWorkload.TwoRequiredInt32Plain,
                    new CensusCaseLayout(CensusPhysicalType.Int32, sizeof(int), false, CensusConsumer.MultiInt32),
                    [new CensusPassSpec([0, 1], misalignedRows),
                     new CensusPassSpec([0, 1], 128)],
                    null, retentionRepetitions));
            }

            {
                // Committed producer fixtures with page CRCs: truth comes from the
                // pinned independent column hashes plus Lokad/baseline agreement,
                // established before any timing runs. The pinned baseline never
                // reads or validates page CRCs (its read path has no CRC handling);
                // Lokad validates every present CRC before trusting payload bytes.
                var repositoryRoot = Environment.GetEnvironmentVariable("LOKAD_PARQUET_REPOSITORY_ROOT") ??
                    throw new InvalidOperationException("The benchmark repository root is unavailable.");
                var plainChecksumBytes = await File.ReadAllBytesAsync(Path.Combine(
                    repositoryRoot, "tests", "fixtures", "apache-parquet-testing", "plain-dict-uncompressed-checksum.parquet"));
                var plainTruth = await EstablishCommittedTruthAsync(
                    plainChecksumBytes, 0, "b4e1c8ce8ea209fb64ee37db3c5b356b0952a756d919b5b31f69e1dc49087cf2");
                await RunCensusCaseAsync(() => MeasureCustomAsync(
                    "CrcInt64Dictionary",
                    plainChecksumBytes,
                    plainTruth.RowCount,
                    1,
                    0,
                    0,
                    plainTruth.Checksum,
                    plainTruth.ColumnHashes,
                    plainTruth.NullCounts,
                    ScanWorkload.CrcInt64Dictionary,
                    new CensusCaseLayout(CensusPhysicalType.Int64, sizeof(long), false, CensusConsumer.Int64),
                    [new CensusPassSpec([0], plainTruth.RowCount),
                     new CensusPassSpec([0], 256)],
                    null, retentionRepetitions));
                var snappyChecksumBytes = await File.ReadAllBytesAsync(Path.Combine(
                    repositoryRoot, "tests", "fixtures", "apache-parquet-testing", "rle-dict-snappy-checksum.parquet"));
                var snappyTruth = await EstablishCommittedTruthAsync(
                    snappyChecksumBytes, 1, "7466464aa99cfabca1449d81db1f859a10208da4e72ba5b5d12df8ecf4f42a59");
                await RunCensusCaseAsync(() => MeasureCustomAsync(
                    "CrcBinaryDictionarySnappy",
                    snappyChecksumBytes,
                    snappyTruth.RowCount,
                    1,
                    0,
                    snappyTruth.BinaryPayloadBytes,
                    snappyTruth.Checksum,
                    snappyTruth.ColumnHashes,
                    snappyTruth.NullCounts,
                    ScanWorkload.CrcBinaryDictionarySnappy,
                    new CensusCaseLayout(CensusPhysicalType.ByteArray, 0, false, CensusConsumer.Binary),
                    [new CensusPassSpec([1], snappyTruth.RowCount),
                     new CensusPassSpec([1], 256)],
                    null, retentionRepetitions));
                var fixedBytes = await File.ReadAllBytesAsync(Path.Combine(
                    repositoryRoot, "tests", "fixtures", "apache-parquet-testing", "fixed_length_byte_array.parquet"));
                var fixedTruth = await EstablishCommittedTruthAsync(
                    fixedBytes, 0, "ccef1cbacb37a62e63bd7be4128dc95a1808f97b2f1d7eed79755d916bcc9abd");
                await using (var fixedInspection = await ParquetFile.OpenAsync((ReadOnlyMemory<byte>)fixedBytes))
                {
                    if (fixedInspection.Metadata.Schema.Columns[0].SchemaElement.TypeLength != 4)
                        throw new InvalidOperationException("The committed fixed fixture changed its type width.");
                }

                await RunCensusCaseAsync(() => MeasureCustomAsync(
                    "NullableFixedByteArrayPlain",
                    fixedBytes,
                    fixedTruth.RowCount,
                    1,
                    0,
                    0,
                    fixedTruth.Checksum,
                    fixedTruth.ColumnHashes,
                    fixedTruth.NullCounts,
                    ScanWorkload.NullableFixedByteArrayPlain,
                    new CensusCaseLayout(CensusPhysicalType.FixedLengthByteArray, 4, true, CensusConsumer.Fixed),
                    [new CensusPassSpec([0], fixedTruth.RowCount),
                     new CensusPassSpec([0], 256)],
                    null, retentionRepetitions));
            }
            {
                // The static catalog is load-bearing: every measured case resolves
                // to exactly one entry with a matching layout and pass set, so a
                // renamed, added, or reshaped case fails here instead of silently
                // drifting from the exported and reconciled catalog.
                var catalogByName = new Dictionary<string, CensusCatalogCase>(StringComparer.Ordinal);
                foreach (var catalogEntry in CensusCatalog.Cases)
                    catalogByName.Add(catalogEntry.Name, catalogEntry);
                var reconciled = new HashSet<string>(StringComparer.Ordinal);
                foreach (var result in results)
                {
                    if (!catalogByName.TryGetValue(result.Name, out var catalogEntry))
                        throw new InvalidOperationException($"The work census measured an unknown case '{result.Name}'.");
                    if (!reconciled.Add(result.Name))
                        throw new InvalidOperationException($"The work census measured a duplicate case '{result.Name}'.");
                    if (!string.Equals(result.PhysicalType, CensusLayout.NameOf(catalogEntry.PhysicalType), StringComparison.Ordinal) ||
                        result.ValueWidthBytes != catalogEntry.TypeWidthBytes ||
                        result.Nullable != catalogEntry.Nullable ||
                        !string.Equals(result.Consumer, CensusConsumerNames.SnapshotName(catalogEntry.Consumer), StringComparison.Ordinal))
                        throw new InvalidOperationException($"The work census case '{result.Name}' does not match its catalog layout.");
                    if (result.PassPeaks.Count != catalogEntry.PassProjections.Count)
                        throw new InvalidOperationException($"The work census case '{result.Name}' does not carry its catalog passes.");
                    for (var pass = 0; pass < result.PassPeaks.Count; pass++)
                    {
                        if (!result.PassPeaks[pass].Projection.SequenceEqual(catalogEntry.PassProjections[pass]))
                            throw new InvalidOperationException($"The work census case '{result.Name}' does not match its catalog passes.");
                    }
                }
                if (reconciled.Count != CensusCatalog.Cases.Count)
                    throw new InvalidOperationException("The work census is missing catalog cases.");
            }
            var affinityMask = BenchmarkHostPolicy.ApplySingleProcessorAffinity();
            var outputEvidence = BenchmarkHostPolicy.CheckNativeWorkspacePath(outputPath, "work census output");
            var snapshot = new WorkCensusSnapshot(
                SnapshotSchemaVersion,
                DateTimeOffset.UtcNow,
                Environment.GetEnvironmentVariable("LOKAD_PARQUET_SOURCE_REVISION") ?? "unrecorded",
                RuntimeInformation.FrameworkDescription,
                RuntimeInformation.OSDescription,
                RuntimeInformation.ProcessArchitecture.ToString(),
                BenchmarkHostPolicy.GetProcessorName(),
                BenchmarkHostPolicy.FormatAffinity(affinityMask),
                BenchmarkHostPolicy.GetSelectedLogicalProcessor(affinityMask),
                BenchmarkHostPolicy.GetServerGarbageCollection(),
                BenchmarkHostPolicy.GetGcLatencyMode(),
                BenchmarkHostPolicy.GetTieredCompilation(),
                BenchmarkHostPolicy.GetTieredPgo(),
                outputEvidence.ResolvedPath,
                outputEvidence.FileSystem,
                PairedParityRunner.GetRunnerFingerprint(),
                Environment.GetEnvironmentVariable("LOKAD_PARQUET_PACKAGE_LOCK_HASH") ?? "unrecorded",
                results);
            var snapshotJson = JsonSerializer.Serialize(snapshot, PairedSessionRecorder.SessionJson);
            PairedSessionRecorder.WriteSessionFileAtomic(outputPath, snapshotJson);
            var snapshotHash = Convert.ToHexStringLower(SHA256.HashData(await File.ReadAllBytesAsync(outputPath)));

            Console.WriteLine($"Work census: {Path.GetFullPath(outputPath)}");
            foreach (var result in results)
            {
                Console.WriteLine(
                    $"{result.Name}: reads={result.SourceReadCalls}/{result.SourceBytesRead} B, " +
                    $"pool={result.PoolRents} rents/{result.PeakPooledBytes} B peak/" +
                    $"{result.PooledBytesCleared} B cleared, moves={result.SynchronousMoves}/" +
                    $"{result.TotalMoves} synchronous.");
            }
            CompleteCensusSession(session, outputPath, snapshotHash, results.Select(static result => result.Name).ToArray(), 0);
            return 0;
        }
        catch (Exception exception)
        {
            AbortCensusSession(session, exception.Message);
            Console.Error.WriteLine($"The work census session {sessionId} aborted: {exception.Message}");
            return 1;
        }

        static async Task<WorkCensusCase> MeasureAsync(ScanWorkload workload, int retentionRepetitions)
        {
            var fixture = await ScanFixture.CreateAsync(workload, RowCount);
            return await MeasureCustomAsync(
                workload.ToString(),
                fixture.Bytes,
                RowCount,
                fixture.ColumnCount,
                fixture.Utf8PayloadBytes,
                0,
                fixture.Checksum,
                fixture.ColumnChecksums,
                fixture.NullCounts,
                workload,
                CensusCaseLayout.ForCatalogLane(workload),
                [new CensusPassSpec(Enumerable.Range(0, fixture.ColumnCount).ToArray(), RowCount),
                 new CensusPassSpec(Enumerable.Range(fixture.ColumnCount / 2, fixture.ColumnCount - fixture.ColumnCount / 2).ToArray(), 4096)],
                null, retentionRepetitions);
        }

        // Two-value dictionary over the full row count: minimal cardinality with
        // maximal dictionary reuse, mirroring the catalog string oracle.
        async Task<(byte[] Bytes, long Checksum, int Utf8Bytes)> WriteLowCardinalityDictionaryFixtureAsync(int count)
        {
            var field = new DataField<string>("value", nullable: false);
            var encoded = new ReadOnlyMemory<char>[count];
            var checksum = ScanChecksum.Seed;
            var utf8Bytes = 0;
            for (var row = 0; row < encoded.Length; row++)
            {
                var text = row % 2 == 0 ? "yes" : "no";
                encoded[row] = text.AsMemory();
                foreach (var value in Encoding.UTF8.GetBytes(text))
                {
                    checksum = ScanChecksum.Mix(checksum, value);
                    utf8Bytes++;
                }
                checksum = ScanChecksum.Mix(checksum, ScanChecksum.ValueSeparator);
            }
            using var stream = new MemoryStream();
            var options = new ParquetOptions { CompressionMethod = CompressionMethod.None, DictionaryEncodingThreshold = 1 };
            options.ColumnEncodingHints[field.Path.ToString()] = EncodingHint.Dictionary;
            await using (var writer = await BaselineParquetWriter.CreateAsync(new BaselineParquetSchema(field), stream, options))
            {
                using var rowGroup = writer.CreateRowGroup();
                await rowGroup.WriteAsync<ReadOnlyMemory<char>>(field, encoded);
                rowGroup.CompleteValidate();
            }
            var bytes = stream.ToArray();
            await using var inspection = await ParquetFile.OpenAsync((ReadOnlyMemory<byte>)bytes);
            var chunk = inspection.Metadata.RowGroups[0].Columns[0];
            var hasDictionaryEncoding = chunk.EncodingCodes.Contains((int)ParquetEncoding.PlainDictionary) ||
                chunk.EncodingCodes.Contains((int)ParquetEncoding.RunLengthDictionary);
            if (!hasDictionaryEncoding)
                throw new InvalidOperationException("The low-cardinality fixture did not use dictionary encoding: " + string.Join(',', chunk.EncodingCodes) + ".");
            return (bytes, checksum, utf8Bytes);
        }
        // Optional booleans are the optional non-INT32 lane: nulls every eighth
        // row, alternating values otherwise, with a matching oracle.
        async Task<(byte[] Bytes, long Folded, int Nulls)> WriteNullableBooleanFixtureAsync(int count)
        {
            var field = new DataField<bool?>("value");
            var values = new bool?[count];
            var valueChain = ScanChecksum.Seed;
            var nullChain = ScanChecksum.Seed;
            var nulls = 0;
            for (var row = 0; row < values.Length; row++)
            {
                if ((row & 7) == 7)
                {
                    values[row] = null;
                    nullChain = ScanChecksum.Mix(nullChain, row);
                    nulls++;
                }
                else
                {
                    var value = (row & 1) == 0;
                    values[row] = value;
                    valueChain = ScanChecksum.Mix(valueChain, value ? 1 : 0);
                }
            }
            using var stream = new MemoryStream();
            var options = new ParquetOptions { CompressionMethod = CompressionMethod.None, DictionaryEncodingThreshold = 0 };
            await using (var writer = await BaselineParquetWriter.CreateAsync(new BaselineParquetSchema(field), stream, options))
            {
                using var rowGroup = writer.CreateRowGroup();
                await rowGroup.WriteAsync<bool>(field, values);
                rowGroup.CompleteValidate();
            }
            return (stream.ToArray(), ScanChecksum.CombineColumn(valueChain, nullChain), nulls);
        }
        // Eight small row groups stand in for many small pages: the catalog
        // writer emits one page per column chunk, so row groups are the only
        // available page-boundary stress with a fully known oracle.
        async Task<(byte[] Bytes, long Folded)> WriteSmallRowGroupsFixtureAsync(int groups, int groupRows)
        {
            var field = new DataField<int>("value", nullable: false);
            using var stream = new MemoryStream();
            var options = new ParquetOptions { CompressionMethod = CompressionMethod.None, DictionaryEncodingThreshold = 0 };
            await using (var writer = await BaselineParquetWriter.CreateAsync(new BaselineParquetSchema(field), stream, options))
            {
                for (var group = 0; group < groups; group++)
                {
                    var values = new int[groupRows];
                    for (var row = 0; row < values.Length; row++)
                        values[row] = checked((group * groupRows + row) * 3 + 1);
                    using var rowGroup = writer.CreateRowGroup();
                    await rowGroup.WriteAsync<int>(field, values);
                    rowGroup.CompleteValidate();
                }
            }
            var chain = ScanChecksum.Seed;
            for (var row = 0; row < groups * groupRows; row++)
                chain = ScanChecksum.Mix(chain, checked(row * 3 + 1));
            return (stream.ToArray(), ScanChecksum.CombineColumn(chain, ScanChecksum.Seed));
        }
        static async Task<WorkCensusCase> MeasureCustomAsync(
            string name,
            byte[] fixtureBytes,
            int rowCount,
            int columnCount,
            int utf8PayloadBytes,
            int binaryPayloadBytes,
            long checksum,
            long[] columnHashes,
            int[] nullCounts,
            ScanWorkload workload,
            CensusCaseLayout layout,
            IReadOnlyList<CensusPassSpec> passSpecs,
            ParquetRowRange? rowRange,
            int retentionRepetitions)
        {
            using var stream = new CountingMemoryStream(fixtureBytes);
            await using var file = await ParquetFile.OpenAsync(stream);
            stream.Reset();

            var pool = new PoolCensus();
            var previousRentObserver = PoolRentObserverProperty.GetValue(null);
            var previousReturnObserver = PoolReturnObserverProperty.GetValue(null);
            PoolRentObserverProperty.SetValue(null, (Action<Array, int>)((array, requestedLength) =>
            {
                var capacityBytes = Buffer.ByteLength(array);
                pool.RentCount++;
                pool.RequestedBytes += checked((long)requestedLength *
                    (array.Length == 0 ? 0 : capacityBytes / array.Length));
                pool.RentedCapacityBytes += capacityBytes;
                pool.OutstandingBytes += capacityBytes;
                pool.PeakBytes = Math.Max(pool.PeakBytes, pool.OutstandingBytes);
            }));
            PoolReturnObserverProperty.SetValue(null, (Action<Array, int>)((array, returnedLength) =>
            {
                if (array.Length != returnedLength)
                    throw new InvalidOperationException("The work-census pool return length is inconsistent.");
                var capacityBytes = Buffer.ByteLength(array);
                pool.ReturnCount++;
                pool.ReturnedCapacityBytes += capacityBytes;
                pool.ClearedBytes += capacityBytes;
                pool.OutstandingBytes -= capacityBytes;
                if (pool.OutstandingBytes < 0)
                    throw new InvalidOperationException("The work-census pool accounting is negative.");
            }));
            var publicBatchCount = 0;
            var totalMoves = 0;
            var synchronousMoves = 0;
            var logicalOutputBytes = 0L;
            var decodedBatches = 0;
            var consumerUtf8Bytes = 0L;
            var peakPooledBytes = 0L;
            var passPeaks = new List<CensusPassMeasurement>();
            var endOfScanRetainedBytes = 0L;
            var poolRents = 0;
            var poolReturns = 0;
            var requestedBytes = 0L;
            var rentedCapacityBytes = 0L;
            var returnedCapacityBytes = 0L;
            var clearedBytes = 0L;
            var retentionOrdinals = passSpecs[0].Ordinals.ToArray();
            var retentionExpected = checksum;
            try
            {

                // Pass shapes arrive per case: the standard full projection at the
                // full-row target plus a narrow small-target rotation covers
                // multi-batch slicing and projection changes in one truth-checked,
                // pool-balanced case. Cross-column page misalignment cannot come from
                // the single-page baseline writer; it is covered by the partitioning
                // tests against synthetic uneven fixtures.
                var allColumns = file.Metadata.Schema.Columns;
                for (var passIndex = 0; passIndex < passSpecs.Count; passIndex++)
                {
                    var spec = passSpecs[passIndex];
                    var columns = spec.Ordinals.Select(ordinal => allColumns[ordinal]).ToArray();
                    var expected = CensusExpectedChecksum(checksum, columnHashes, allColumns.Count, columns);
                    var logicalBytes = CensusLayout.LogicalOutputBytes(layout.PhysicalType, layout.TypeWidthBytes, layout.Nullable, rowCount, columns.Length, utf8PayloadBytes, binaryPayloadBytes);
                    // File-owned caches carry retained buffers across passes without
                    // touching the shared pool, so a pass that reuses them would record
                    // a zero peak. The carried outstanding bytes are the storage the
                    // pass actually occupies, and therefore the honest peak floor.
                    var carryBytes = pool.OutstandingBytes;
                    pool.PeakBytes = 0;
                    var allocatedBefore = GC.GetTotalAllocatedBytes(false);
                    var gen0Before = GC.CollectionCount(0);
                    var gen1Before = GC.CollectionCount(1);
                    var gen2Before = GC.CollectionCount(2);
                    var started = Stopwatch.GetTimestamp();
                    var passValueChains = new long[columns.Length];
                    var passNullChains = new long[columns.Length];
                    Utf8ScanSink? passSink = ScanWorkloadCatalog.IsString(workload)
                        ? new Utf8ScanSink(rowCount, utf8PayloadBytes)
                        : null;
                    var outcome = await RunCensusPassAsync(
                        file, workload, rowCount, utf8PayloadBytes, columns, spec.Target,
                        expected, columnHashes, nullCounts, rowRange,
                        passValueChains, passNullChains, passSink);
                    var elapsedMilliseconds = (Stopwatch.GetTimestamp() - started) * 1000.0 / Stopwatch.Frequency;
                    publicBatchCount += outcome.PassBatches;
                    totalMoves += outcome.TotalMoves;
                    synchronousMoves += outcome.SynchronousMoves;
                    consumerUtf8Bytes += outcome.ConsumerUtf8Bytes;
                    decodedBatches = checked(decodedBatches + outcome.PassBatches * columns.Length);
                    logicalOutputBytes += logicalBytes;
                    var passPeakBytes = Math.Max(pool.PeakBytes, carryBytes);
                    peakPooledBytes = Math.Max(peakPooledBytes, passPeakBytes);
                    passPeaks.Add(new CensusPassMeasurement(
                        passPeakBytes,
                        logicalBytes,
                        elapsedMilliseconds,
                        outcome.PassBatches,
                        GC.GetTotalAllocatedBytes(false) - allocatedBefore,
                        GC.CollectionCount(0) - gen0Before,
                        GC.CollectionCount(1) - gen1Before,
                        GC.CollectionCount(2) - gen2Before,
                        spec.Ordinals.ToArray(),
                        spec.Target,
                        passIndex == 0 ? "cold-instrumented" : "warm-instrumented"));
                }
                // Retained storage still held by file-owned caches after the scans,
                // sampled separately from the zero-after-disposal check below.
                endOfScanRetainedBytes = pool.OutstandingBytes;
                // The retention probes below scan the first-pass projection, so their
                // truth checksum is the projected combination, not the full-case
                // checksum; a narrow first pass over a wide fixture would otherwise
                // fail its truth check against columns it never scans.
                retentionExpected = CensusExpectedChecksum(
                    checksum, columnHashes, allColumns.Count,
                    retentionOrdinals.Select(ordinal => allColumns[ordinal]).ToArray());
                await file.DisposeAsync().ConfigureAwait(false);


                if (stream.ReadCount == 0)
                    throw new InvalidOperationException($"The {name} work census observed no source reads.");
                if (pool.OutstandingBytes != 0 || pool.RentCount != pool.ReturnCount)
                    throw new InvalidOperationException($"The {name} work-census pool accounting is unbalanced.");
                poolRents = pool.RentCount;
                poolReturns = pool.ReturnCount;
                requestedBytes = pool.RequestedBytes;
                rentedCapacityBytes = pool.RentedCapacityBytes;
                returnedCapacityBytes = pool.ReturnedCapacityBytes;
                clearedBytes = pool.ClearedBytes;
            }
            finally
            {
                PoolRentObserverProperty.SetValue(null, previousRentObserver);
                PoolReturnObserverProperty.SetValue(null, previousReturnObserver);
            }
            // Live-session retention probes run after the pool observers are restored
            // so their rents never pollute the pass accounting above. Each probe holds
            // one pre-opened reader with reused destinations and sink alive through the
            // observation window and verifies the truth checksum on every repetition,
            // so both sides scan identical emitted rows, columns and ranges. Lokad
            // batching follows the first pass target; the baseline reads whole row
            // groups and checksums the same emitted rows while holding group-sized
            // reusable destinations.
            var lokadLive = await LiveSessionRetention.MeasureLokadAsync(
                fixtureBytes, retentionOrdinals, passSpecs[0].Target,
                rowRange?.Start, rowRange?.Count, rowCount, utf8PayloadBytes, workload,
                retentionExpected, columnHashes, nullCounts, retentionRepetitions, name);
            var baselineLive = await LiveSessionRetention.MeasureBaselineAsync(
                fixtureBytes, retentionOrdinals,
                rowRange?.Start, rowRange?.Count, rowCount, utf8PayloadBytes, workload,
                retentionExpected, retentionRepetitions, name);
            return new WorkCensusCase(
                    name,
                    Convert.ToHexStringLower(SHA256.HashData(fixtureBytes)),
                    fixtureBytes.Length,
                    rowCount,
                    columnCount,
                    utf8PayloadBytes,
                    binaryPayloadBytes,
                    stream.ReadCount,
                    stream.BytesRead,
                    stream.MaximumConcurrentReads,
                    publicBatchCount,
                    decodedBatches,
                    totalMoves,
                    synchronousMoves,
                    poolRents,
                    poolReturns,
                    requestedBytes,
                    rentedCapacityBytes,
                    peakPooledBytes,
                    returnedCapacityBytes,
                    logicalOutputBytes,
                    stream.CopiedBytes,
                    endOfScanRetainedBytes,
                    consumerUtf8Bytes,
                    clearedBytes,
                    pool.OutstandingBytes,
                    passPeaks,
                    lokadLive.PostDisposalBytes,
                    lokadLive.PostDisposalProcessPrivateBytes,
                    baselineLive.PostDisposalBytes,
                    baselineLive.PostDisposalProcessPrivateBytes,
                    CensusLayout.NameOf(layout.PhysicalType),
                    layout.TypeWidthBytes,
                    layout.Nullable,
                    CensusConsumerNames.SnapshotName(layout.Consumer),
                    rowRange?.Start,
                    rowRange?.Count,
                    lokadLive.LiveOwnedBytes,
                    baselineLive.LiveOwnedBytes);
        }
    }

    internal sealed record CommittedLaneTruth(
        long Checksum,
        long[] ColumnHashes,
        int[] NullCounts,
        int RowCount,
        int BinaryPayloadBytes);

    // Committed producer fixtures carry no writer-time oracle in this
    // repository: truth is established by decoding every value on both readers,
    // verifying the pinned independent SHA-256 column hash on each side, and
    // requiring byte-for-byte agreement before any timing runs. The folding
    // below mirrors the independent test oracle value for value (length
    // prefixes plus null markers) rather than sharing the measured consumer,
    // so agreement is genuinely independent of the timed path.
    internal static async Task<CommittedLaneTruth> EstablishCommittedTruthAsync(
        byte[] fixtureBytes,
        int projectedOrdinal,
        string pinnedColumnHash)
    {
        using var stream = new MemoryStream(fixtureBytes, writable: false);
        await using var file = await ParquetFile.OpenAsync(stream);
        var columns = file.Metadata.Schema.Columns;
        if ((uint)projectedOrdinal >= (uint)columns.Count)
            throw new ArgumentOutOfRangeException(nameof(projectedOrdinal));
        var rowCount = checked((int)file.Metadata.RowCount);
        var valueChain = ScanChecksum.Seed;
        var nullChain = ScanChecksum.Seed;
        var nullCount = 0;
        var consumed = 0;
        var payloadBytes = 0;
        using var hash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        var lengthBytes = new byte[sizeof(int)];
        var int64Bytes = new byte[sizeof(long)];
        await foreach (var batch in file.ScanAsync(new ParquetScanOptions([columns[projectedOrdinal]])))
        {
            using (batch)
            {
                var view = batch.Columns[0];
                for (var row = 0; row < batch.RowCount; row++)
                {
                    if (!view.Validity.IsValid(row))
                    {
                        hash.AppendData([0]);
                        nullChain = ScanChecksum.Mix(nullChain, consumed + row);
                        nullCount++;
                        continue;
                    }

                    hash.AppendData([1]);
                    if (view is ParquetPrimitiveColumnBatch<long> longs)
                    {
                        BinaryPrimitives.WriteInt32LittleEndian(lengthBytes, sizeof(long));
                        hash.AppendData(lengthBytes);
                        BinaryPrimitives.WriteInt64LittleEndian(int64Bytes, longs.Values.Span[row]);
                        hash.AppendData(int64Bytes);
                        valueChain = ScanChecksum.MixInt64(valueChain, longs.Values.Span[row]);
                    }
                    else if (view is ParquetBinaryColumnBatch binary)
                    {
                        var offsets = binary.Offsets.Span;
                        var bytes = binary.Payload.Span[offsets[row]..offsets[row + 1]];
                        BinaryPrimitives.WriteInt32LittleEndian(lengthBytes, bytes.Length);
                        hash.AppendData(lengthBytes);
                        hash.AppendData(bytes);
                        valueChain = ScanChecksum.MixBytes(valueChain, bytes.ToArray());
                        payloadBytes += bytes.Length;
                    }
                    else if (view is ParquetFixedLengthByteArrayColumnBatch fixedBytes)
                    {
                        var bytes = fixedBytes.Payload.Span.Slice(row * fixedBytes.TypeWidth, fixedBytes.TypeWidth);
                        BinaryPrimitives.WriteInt32LittleEndian(lengthBytes, bytes.Length);
                        hash.AppendData(lengthBytes);
                        hash.AppendData(bytes);
                        valueChain = ScanChecksum.MixBytes(valueChain, bytes.ToArray());
                    }
                    else
                    {
                        throw new InvalidOperationException("The committed fixture has an unexpected physical view.");
                    }
                }

                consumed += batch.RowCount;
            }
        }

        if (consumed != rowCount)
            throw new InvalidOperationException("The committed fixture scan missed rows.");
        var lokadHash = Convert.ToHexStringLower(hash.GetHashAndReset());
        if (!lokadHash.Equals(pinnedColumnHash, StringComparison.OrdinalIgnoreCase))
            throw new InvalidOperationException("The committed fixture Lokad hash does not match its pinned hash.");
        var folded = ScanChecksum.CombineColumn(valueChain, nullChain);
        using var baselineStream = new MemoryStream(fixtureBytes, writable: false);
        await using var baseline = await BaselineParquetReader.CreateAsync(baselineStream);
        var baselineField = baseline.Schema.DataFields[projectedOrdinal];
        var baselineValueChain = ScanChecksum.Seed;
        var baselineNullChain = ScanChecksum.Seed;
        using var baselineHash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        var baselineLengthBytes = new byte[sizeof(int)];
        var baselineInt64Bytes = new byte[sizeof(long)];
        var baselineConsumed = 0;
        for (var groupOrdinal = 0; groupOrdinal < baseline.RowGroupCount; groupOrdinal++)
        {
            using var group = baseline.OpenRowGroupReader(groupOrdinal);
            var groupRows = checked((int)group.RowCount);
            if (baselineField.ClrType == typeof(long))
            {
                if (baselineField.IsNullable)
                {
                    var values = new long?[groupRows];
                    await group.ReadAsync<long>(baselineField, values);
                    for (var row = 0; row < groupRows; row++)
                    {
                        if (values[row] is long value)
                        {
                            baselineHash.AppendData([1]);
                            BinaryPrimitives.WriteInt32LittleEndian(baselineLengthBytes, sizeof(long));
                            baselineHash.AppendData(baselineLengthBytes);
                            BinaryPrimitives.WriteInt64LittleEndian(baselineInt64Bytes, value);
                            baselineHash.AppendData(baselineInt64Bytes);
                            baselineValueChain = ScanChecksum.MixInt64(baselineValueChain, value);
                        }
                        else
                        {
                            baselineHash.AppendData([0]);
                            baselineNullChain = ScanChecksum.Mix(baselineNullChain, baselineConsumed + row);
                        }
                    }
                }
                else
                {
                    var values = new long[groupRows];
                    await group.ReadAsync<long>(baselineField, values);
                    for (var row = 0; row < groupRows; row++)
                    {
                        baselineHash.AppendData([1]);
                        BinaryPrimitives.WriteInt32LittleEndian(baselineLengthBytes, sizeof(long));
                        baselineHash.AppendData(baselineLengthBytes);
                        BinaryPrimitives.WriteInt64LittleEndian(baselineInt64Bytes, values[row]);
                        baselineHash.AppendData(baselineInt64Bytes);
                        baselineValueChain = ScanChecksum.MixInt64(baselineValueChain, values[row]);
                    }
                }
            }
            else if (baselineField.ClrType == typeof(ReadOnlyMemory<byte>))
            {
                if (baselineField.IsNullable)
                {
                    var values = new ReadOnlyMemory<byte>?[groupRows];
                    await group.ReadAsync<ReadOnlyMemory<byte>>(baselineField, values);
                    for (var row = 0; row < groupRows; row++)
                    {
                        if (values[row] is ReadOnlyMemory<byte> bytes)
                            AppendBaselineBinary(bytes.Span);
                        else
                        {
                            baselineHash.AppendData([0]);
                            baselineNullChain = ScanChecksum.Mix(baselineNullChain, baselineConsumed + row);
                        }
                    }
                }
                else
                {
                    var values = new ReadOnlyMemory<byte>[groupRows];
                    await group.ReadAsync<ReadOnlyMemory<byte>>(baselineField, values);
                    for (var row = 0; row < groupRows; row++)
                        AppendBaselineBinary(values[row].Span);
                }
            }
            else
            {
                throw new InvalidOperationException("The committed baseline column has an unexpected field type.");
            }

            void AppendBaselineBinary(ReadOnlySpan<byte> bytes)
            {
                baselineHash.AppendData([1]);
                BinaryPrimitives.WriteInt32LittleEndian(baselineLengthBytes, bytes.Length);
                baselineHash.AppendData(baselineLengthBytes);
                baselineHash.AppendData(bytes.ToArray());
                baselineValueChain = ScanChecksum.MixBytes(baselineValueChain, bytes.ToArray());
            }

            baselineConsumed += groupRows;
        }

        if (baselineConsumed != rowCount)
            throw new InvalidOperationException("The committed baseline scan missed rows.");
        var baselineFolded = ScanChecksum.CombineColumn(baselineValueChain, baselineNullChain);
        var baselineHashed = Convert.ToHexStringLower(baselineHash.GetHashAndReset());
        if (!baselineHashed.Equals(pinnedColumnHash, StringComparison.OrdinalIgnoreCase) || baselineFolded != folded)
            throw new InvalidOperationException("The committed baseline output does not match the pinned truth.");
        var hashes = new long[columns.Count];
        hashes[projectedOrdinal] = folded;
        var nulls = new int[columns.Count];
        nulls[projectedOrdinal] = nullCount;
        return new CommittedLaneTruth(folded, hashes, nulls, rowCount, payloadBytes);
    }
    internal sealed record CensusPassOutcome(int PassBatches, int TotalMoves, int SynchronousMoves, long ConsumerUtf8Bytes, long Checksum);

    // Shared Census consumer: the work census and the truth verification run this
    // same pass. Besides the checksum it asserts row counts, per-column folded
    // hashes, and null counts against the caller-supplied expectations.
    private delegate long MixValue<T>(long checksum, T value)
        where T : unmanaged;

    private static void DecodeNullablePrimitive<T>(
        ReadOnlySpan<T> values,
        ParquetValidity validity,
        MixValue<T> mix,
        ref long valueChain,
        ref long nullChain,
        ref int nullCount,
        int consumedBase)
        where T : unmanaged
    {
        if (validity.IsAllValid)
        {
            foreach (var value in values)
                valueChain = mix(valueChain, value);
        }
        else
        {
            var bits = validity.Bits.Span;
            for (var row = 0; row < values.Length; row++)
            {
                if ((bits[row >> 3] & (1 << (row & 7))) != 0)
                    valueChain = mix(valueChain, values[row]);
                else
                {
                    nullChain = ScanChecksum.Mix(nullChain, consumedBase + row);
                    nullCount++;
                }
            }
        }
    }

    internal static async Task<CensusPassOutcome> RunCensusPassAsync(
        ParquetFile file,
        ScanWorkload workload,
        int rowCount,
        int utf8PayloadBytes,
        IReadOnlyList<ParquetColumn> projection,
        int target,
        long expectedChecksum,
        long[] expectedColumnHashes,
        int[] expectedNullCounts,
        ParquetRowRange? rowRange,
        long[] valueChains,
        long[] nullChains,
        Utf8ScanSink? utf8Sink)
    {
        ArgumentNullException.ThrowIfNull(valueChains);
        ArgumentNullException.ThrowIfNull(nullChains);
        if (valueChains.Length < projection.Count || nullChains.Length < projection.Count)
            throw new ArgumentException("The census destination chains cover fewer columns than the projection.", nameof(valueChains));
        var options = new ParquetScanOptions(projection, null, rowRange, target);
        var nullCounts = new int[projection.Count];
        var consumed = new int[projection.Count];
        Array.Fill(valueChains, ScanChecksum.Seed);
        Array.Fill(nullChains, ScanChecksum.Seed);
        Array.Fill(consumed, rowRange is null ? 0 : checked((int)rowRange.Value.Start));
        var expectedRows = (int)(rowRange?.Count ?? rowCount);
        utf8Sink?.Reset();
        var passBatches = 0;
        var totalMoves = 0;
        var synchronousMoves = 0;
        var consumerUtf8Bytes = 0L;
        var rows = 0;
        var enumerator = file.ScanAsync(options).GetAsyncEnumerator();
        try
        {
            while (true)
            {
                var moving = enumerator.MoveNextAsync();
                totalMoves++;
                bool moved;
                if (moving.IsCompletedSuccessfully)
                {
                    synchronousMoves++;
                    moved = moving.Result;
                }
                else
                {
                    moved = await moving.ConfigureAwait(false);
                }
                if (!moved)
                    break;

                using var batch = enumerator.Current;
                passBatches++;
                rows += batch.RowCount;
                for (var columnIndex = 0; columnIndex < batch.Columns.Count; columnIndex++)
                {
                    var untypedColumn = batch.Columns[columnIndex];
                    if (untypedColumn is ParquetBinaryColumnBatch binary && utf8Sink is null)
                    {
                        // Raw binary diagnostic lane: fold payload bytes without UTF-8 validation.
                        if (projection.Count > 1)
                            throw new InvalidOperationException("A multi-column binary census value is unsupported.");
                        var offsets = binary.Offsets.Span;
                        if (binary.Validity.IsAllValid)
                        {
                            for (var row = 0; row < binary.RowCount; row++)
                                valueChains[0] = ScanChecksum.MixBytes(valueChains[0], binary.Payload.Span[offsets[row]..offsets[row + 1]].ToArray());
                        }
                        else
                        {
                            var payload = binary.Payload.Span;
                            var bits = binary.Validity.Bits.Span;
                            for (var row = 0; row < binary.RowCount; row++)
                            {
                                if ((bits[row >> 3] & (1 << (row & 7))) != 0)
                                    valueChains[0] = ScanChecksum.MixBytes(valueChains[0], payload[offsets[row]..offsets[row + 1]].ToArray());
                                else
                                {
                                    nullChains[0] = ScanChecksum.Mix(nullChains[0], consumed[0] + row);
                                    nullCounts[0]++;
                                }
                            }
                        }
                    }
                    else if (untypedColumn is ParquetPrimitiveColumnBatch<long> longs)
                    {
                        if (projection.Count > 1)
                            throw new InvalidOperationException("A multi-column INT64 census value is unsupported.");
                        DecodeNullablePrimitive(
                            longs.Values.Span, longs.Validity,
                            static (chain, value) => ScanChecksum.MixInt64(chain, value),
                            ref valueChains[0], ref nullChains[0], ref nullCounts[0], consumed[0]);
                    }
                    else if (untypedColumn is ParquetPrimitiveColumnBatch<float> singles)
                    {
                        if (projection.Count > 1)
                            throw new InvalidOperationException("A multi-column FLOAT census value is unsupported.");
                        DecodeNullablePrimitive(
                            singles.Values.Span, singles.Validity,
                            static (chain, value) => ScanChecksum.MixFloat(chain, value),
                            ref valueChains[0], ref nullChains[0], ref nullCounts[0], consumed[0]);
                    }
                    else if (untypedColumn is ParquetPrimitiveColumnBatch<double> doubles)
                    {
                        if (projection.Count > 1)
                            throw new InvalidOperationException("A multi-column DOUBLE census value is unsupported.");
                        DecodeNullablePrimitive(
                            doubles.Values.Span, doubles.Validity,
                            static (chain, value) => ScanChecksum.MixDouble(chain, value),
                            ref valueChains[0], ref nullChains[0], ref nullCounts[0], consumed[0]);
                    }
                    else if (untypedColumn is ParquetFixedLengthByteArrayColumnBatch fixedBytes)
                    {
                        if (projection.Count > 1)
                            throw new InvalidOperationException("A multi-column fixed binary census value is unsupported.");
                        var payload = fixedBytes.Payload.Span;
                        var width = fixedBytes.TypeWidth;
                        if (fixedBytes.Validity.IsAllValid)
                        {
                            for (var row = 0; row < fixedBytes.RowCount; row++)
                                valueChains[0] = ScanChecksum.MixBytes(valueChains[0], payload.Slice(row * width, width).ToArray());
                        }
                        else
                        {
                            var bits = fixedBytes.Validity.Bits.Span;
                            for (var row = 0; row < fixedBytes.RowCount; row++)
                            {
                                if ((bits[row >> 3] & (1 << (row & 7))) != 0)
                                    valueChains[0] = ScanChecksum.MixBytes(valueChains[0], payload.Slice(row * width, width).ToArray());
                                else
                                {
                                    nullChains[0] = ScanChecksum.Mix(nullChains[0], consumed[0] + row);
                                    nullCounts[0]++;
                                }
                            }
                        }
                    }
                    else if (untypedColumn is ParquetBinaryColumnBatch strings)
                    {
                        if (!strings.Validity.IsAllValid || utf8Sink is null)
                            throw new InvalidOperationException("The required UTF-8 census column is invalid.");
                        var offsets = strings.Offsets.Span;
                        for (var row = 0; row < strings.RowCount; row++)
                            utf8Sink.AppendUtf8(
                                strings.Payload.Span[offsets[row]..offsets[row + 1]]);
                    }
                    else if (untypedColumn is ParquetPrimitiveColumnBatch<bool> flags)
                    {
                        if (projection.Count > 1)
                            throw new InvalidOperationException("A multi-column boolean census value is unsupported.");
                        if (flags.Validity.IsAllValid)
                        {
                            foreach (var value in flags.Values.Span)
                                valueChains[0] = ScanChecksum.Mix(valueChains[0], value ? 1 : 0);
                        }
                        else
                        {
                            var values = flags.Values.Span;
                            var bits = flags.Validity.Bits.Span;
                            for (var row = 0; row < values.Length; row++)
                            {
                                if ((bits[row >> 3] & (1 << (row & 7))) != 0)
                                    valueChains[0] = ScanChecksum.Mix(valueChains[0], values[row] ? 1 : 0);
                                else
                                {
                                    nullChains[0] = ScanChecksum.Mix(nullChains[0], consumed[0] + row);
                                    nullCounts[0]++;
                                }
                            }
                        }
                    }
                    else if (untypedColumn is ParquetPrimitiveColumnBatch<int> column)
                    {
                        var values = column.Values.Span;
                        if (projection.Count > 1)
                        {
                            if (!column.Validity.IsAllValid)
                                throw new InvalidOperationException("A required multi-column census value decoded as null.");
                            valueChains[columnIndex] = ScanChecksum.ConsumeRequired(valueChains[columnIndex], values);
                        }
                        else if (column.Validity.IsAllValid)
                        {
                            foreach (var value in values)
                                valueChains[0] = ScanChecksum.Mix(valueChains[0], value);
                        }
                        else
                        {
                            var bits = column.Validity.Bits.Span;
                            for (var row = 0; row < values.Length; row++)
                            {
                                if ((bits[row >> 3] & (1 << (row & 7))) != 0)
                                    valueChains[0] = ScanChecksum.Mix(valueChains[0], values[row]);
                                else
                                {
                                    nullChains[0] = ScanChecksum.Mix(nullChains[0], consumed[0] + row);
                                    nullCounts[0]++;
                                }
                            }
                        }
                    }
                    else
                    {
                        throw new InvalidOperationException("The census consumer met an unexpected batch type.");
                    }

                    consumed[columnIndex] += batch.Columns[columnIndex].RowCount;
                }
            }
        }
        finally
        {
            await enumerator.DisposeAsync().ConfigureAwait(false);
        }

        long checksum;
        if (utf8Sink is not null)
        {
            checksum = utf8Sink.Complete(expectedRows, utf8PayloadBytes);
            consumerUtf8Bytes += utf8PayloadBytes;
        }
        else if (projection.Count > 1)
        {
            checksum = ScanChecksum.CombineColumns(valueChains.AsSpan(0, projection.Count), nullChains.AsSpan(0, projection.Count));
        }
        else
        {
            checksum = ScanChecksum.CombineColumn(valueChains[0], nullChains[0]);
        }

        if (checksum != expectedChecksum)
            throw new InvalidOperationException($"The {workload} work-census truth check failed.");
        if (rows != expectedRows)
            throw new InvalidOperationException($"The {workload} work-census row count check failed.");
        if (utf8Sink is null)
        {
            for (var columnIndex = 0; columnIndex < projection.Count; columnIndex++)
            {
                var ordinal = projection[columnIndex].Ordinal;
                if (nullCounts[columnIndex] != expectedNullCounts[ordinal])
                    throw new InvalidOperationException($"The {workload} work-census null count check failed.");
                var folded = ScanChecksum.CombineColumn(valueChains[columnIndex], nullChains[columnIndex]);
                if (folded != expectedColumnHashes[ordinal])
                    throw new InvalidOperationException($"The {workload} work-census column hash check failed.");
            }
        }
        return new CensusPassOutcome(passBatches, totalMoves, synchronousMoves, consumerUtf8Bytes, checksum);
    }

    internal static CensusSession BeginCensusSession(string snapshotPath, Guid sessionId, DateTimeOffset startedAtUtc)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(snapshotPath);
        var leaf = Path.GetFileNameWithoutExtension(snapshotPath);
        if (string.IsNullOrEmpty(leaf))
            leaf = "work-census";
        var parent = Path.GetDirectoryName(Path.GetFullPath(snapshotPath)) ??
            throw new InvalidOperationException("The work-census output has no directory.");
        var session = new CensusSession(
            Path.Combine(parent, leaf + "." + sessionId.ToString("N")[..8] + ".session"),
            sessionId,
            startedAtUtc,
            Path.GetFullPath(snapshotPath));
        WriteCensusSessionMetadata(session, "running", null, null, null);
        return session;
    }

    internal static void RecordCensusCheckpoint(CensusSession session, int order, string caseName, DateTimeOffset startedAtUtc, DateTimeOffset endedAtUtc, WorkCensusCase result)
    {
        ArgumentNullException.ThrowIfNull(session);
        ArgumentOutOfRangeException.ThrowIfNegative(order);
        ArgumentException.ThrowIfNullOrWhiteSpace(caseName);
        ArgumentNullException.ThrowIfNull(result);
        var checkpoint = new CensusCaseCheckpoint(session.SessionId, order, caseName, startedAtUtc, endedAtUtc, result);
        PairedSessionRecorder.WriteSessionFileAtomic(
            Path.Combine(session.SessionDirectory, $"checkpoint-{order:D2}.json"),
            JsonSerializer.Serialize(checkpoint, PairedSessionRecorder.SessionJson));
    }

    internal static void CompleteCensusSession(CensusSession session, string snapshotPath, string snapshotSha256, string[] caseNames, int exitStatus)
    {
        ArgumentNullException.ThrowIfNull(session);
        ArgumentException.ThrowIfNullOrWhiteSpace(snapshotPath);
        ArgumentException.ThrowIfNullOrWhiteSpace(snapshotSha256);
        ArgumentNullException.ThrowIfNull(caseNames);
        var finishedAt = DateTimeOffset.UtcNow;
        var completion = new CensusSessionCompletion(
            session.SessionId, finishedAt, Path.GetFullPath(snapshotPath), snapshotSha256, caseNames.Length, caseNames, exitStatus);
        PairedSessionRecorder.WriteSessionFileAtomic(
            Path.Combine(session.SessionDirectory, "completed.json"),
            JsonSerializer.Serialize(completion, PairedSessionRecorder.SessionJson));
        WriteCensusSessionMetadata(session, "completed", null, exitStatus, finishedAt);
    }

    internal static void AbortCensusSession(CensusSession session, string failure)
    {
        ArgumentNullException.ThrowIfNull(session);
        ArgumentException.ThrowIfNullOrWhiteSpace(failure);
        WriteCensusSessionMetadata(session, "aborted", failure, null, DateTimeOffset.UtcNow);
    }

    private static void WriteCensusSessionMetadata(CensusSession session, string status, string? failure, int? exitStatus, DateTimeOffset? finishedAt)
    {
        var metadata = new CensusSessionMetadata(
            session.SessionId,
            SnapshotSchemaVersion,
            status,
            session.StartedAtUtc,
            finishedAt,
            Environment.MachineName,
            Environment.ProcessId,
            Environment.GetEnvironmentVariable("LOKAD_PARQUET_SOURCE_REVISION") ?? "unrecorded",
            PairedParityRunner.GetRunnerFingerprint(),
            Environment.GetEnvironmentVariable("LOKAD_PARQUET_PACKAGE_LOCK_HASH") ?? "unrecorded",
            RuntimeInformation.FrameworkDescription,
            RuntimeInformation.OSDescription,
            RuntimeInformation.ProcessArchitecture.ToString(),
            session.SnapshotPath,
            failure,
            exitStatus);
        PairedSessionRecorder.WriteSessionFileAtomic(
            Path.Combine(session.SessionDirectory, "session.json"),
            JsonSerializer.Serialize(metadata, PairedSessionRecorder.SessionJson));
    }

    internal static long CensusExpectedChecksum(
        long fullChecksum,
        long[] columnHashes,
        int allColumnCount,
        IReadOnlyList<ParquetColumn> projection)
    {
        // The stored full checksum combines columns in stored order, so it only
        // stands for a projection that selects every column in stored order; any
        // narrower or reordered projection combines its own columns in scan order.
        if (projection.Count == allColumnCount)
        {
            var identity = true;
            for (var ordinal = 0; ordinal < projection.Count; ordinal++)
            {
                if (projection[ordinal].Ordinal != ordinal)
                {
                    identity = false;
                    break;
                }
            }
            if (identity)
                return fullChecksum;
        }
        return ScanChecksum.CombineColumns(projection.Select(column => columnHashes[column.Ordinal]).ToArray());
    }

    private sealed class CountingMemoryStream : MemoryStream
    {
        private int _activeReads;

        public CountingMemoryStream(byte[] bytes) : base(bytes, writable: false) { }

        public int ReadCount { get; private set; }
        public long BytesRead { get; private set; }
        public long CopiedBytes { get; private set; }
        public int MaximumConcurrentReads { get; private set; }

        public override int Read(Span<byte> buffer)
        {
            var active = Interlocked.Increment(ref _activeReads);
            MaximumConcurrentReads = Math.Max(MaximumConcurrentReads, active);
            try
            {
                var read = base.Read(buffer);
                ObserveRead(read);
                return read;
            }
            finally
            {
                Interlocked.Decrement(ref _activeReads);
            }
        }

        // The source adapter routes stream reads through ReadAsync(Memory<byte>),
        // which never calls the synchronous override above; without this override
        // every census workload reported zero reads with zero concurrency. The base
        // MemoryStream implementation completes synchronously, so this override does
        // too, keeping the sequential single-read accounting exact.
        public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var active = Interlocked.Increment(ref _activeReads);
            MaximumConcurrentReads = Math.Max(MaximumConcurrentReads, active);
            try
            {
                var read = base.Read(buffer.Span);
                ObserveRead(read);
                return new ValueTask<int>(read);
            }
            finally
            {
                Interlocked.Decrement(ref _activeReads);
            }
        }

        public void Reset()
        {
            ReadCount = 0;
            BytesRead = 0;
            CopiedBytes = 0;
            MaximumConcurrentReads = 0;
        }

        private void ObserveRead(int read)
        {
            ReadCount++;
            BytesRead += read;
            // On the measured stream path every read lands in a rented pool buffer,
            // so each delivered byte is a source copy; borrows apply only to exact
            // MemoryStream and direct-memory sources, which bypass this stream.
            CopiedBytes += read;
        }
    }

    private sealed class PoolCensus
    {
        public int RentCount { get; set; }
        public int ReturnCount { get; set; }
        public long RequestedBytes { get; set; }
        public long RentedCapacityBytes { get; set; }
        public long ReturnedCapacityBytes { get; set; }
        public long ClearedBytes { get; set; }
        public long OutstandingBytes { get; set; }
        public long PeakBytes { get; set; }
    }
}

/// <summary>Measured per-scan work for one benchmark workload. Everything below is
/// measured at observed events except DecodedColumnBatches and LogicalOutputBytes,
/// which are derived from inputs; per-field notes say which is which.</summary>
/// <param name="DecodedColumnBatches">Derived estimate of internal batches (public batches times projected columns); projected or realigned scans consume fewer, larger source batches.</param>
/// <param name="LogicalOutputBytes">Derived decoded-layout size across both passes: value bytes plus validity bitmap bytes where lanes can produce nulls, or UTF-8 payload plus offsets.</param>
/// <param name="SourceCopiedBytes">Measured bytes delivered by the counting stream. On the measured stream-subclass path every read lands in a rented pool buffer, so deliveries coincide with reads; exact-MemoryStream and direct-memory borrows bypass this stream.</param>
/// <param name="PooledBytesCleared">Measured at pool-return events: returns always carry full-length arrays (asserted by the observer) and the pool clears whole returned arrays.</param>
/// <param name="EndOfScanRetainedPoolBytes">Measured pooled bytes still retained by file-owned caches after the scans, before file disposal.</param>
/// <param name="RetainedPoolBytes">Measured pooled bytes outstanding after file disposal; always zero.</param>
/// <param name="PassPeaks">Measured per-pass peak pooled bytes with that pass decoded-layout size, so each pass carries its own budget envelope.</param>
/// <param name="PhysicalType">Snapshot layout name of the projected physical type, so the verifier can reconstruct denominators without workload tokens.</param>
/// <param name="ValueWidthBytes">Snapshot byte width of one fixed-width value slot.</param>
/// <param name="Nullable">Snapshot flag for lanes that can produce nulls and therefore a validity bitmap.</param>
/// <param name="Consumer">Snapshot name of the census consumer that drains the case batches.</param>
/// <param name="RowRangeStart">Snapshot first selected row for range cases, or null for full scans.</param>
/// <param name="RowRangeCount">Snapshot selected row count for range cases, or null for full scans.</param>
/// <param name="LokadLiveOwnedBytes">Managed-heap growth with the pre-opened Lokad reader, reused destinations and sink held alive.</param>
/// <param name="BaselineLiveOwnedBytes">Managed-heap growth with the pre-opened competitor reader, reused destinations and sink held alive.</param>
/// <param name="LokadRetainedManagedBytes">Measured managed-heap growth after tearing down the warmed live Lokad session.</param>
/// <param name="LokadRetainedProcessPrivateBytes">Measured process-private growth after tearing down the warmed live Lokad session.</param>
/// <param name="BaselineRetainedManagedBytes">Measured managed-heap growth after tearing down the warmed live competitor session.</param>
/// <param name="BaselineRetainedProcessPrivateBytes">Measured process-private growth after tearing down the warmed live competitor session.</param>
internal sealed record WorkCensusCase(
    string Name,
    string FixtureHash,
    int FixtureBytes,
    int RowCount,
    int ColumnCount,
    int Utf8PayloadBytes,
    int BinaryPayloadBytes,
    int SourceReadCalls,
    long SourceBytesRead,
    int MaximumConcurrentReads,
    int PublicBatches,
    int DecodedColumnBatches,
    int TotalMoves,
    int SynchronousMoves,
    int PoolRents,
    int PoolReturns,
    long RequestedPoolBytes,
    long RentedPoolCapacityBytes,
    long PeakPooledBytes,
    long ReturnedPoolCapacityBytes,
    long LogicalOutputBytes,
    long SourceCopiedBytes,
    long EndOfScanRetainedPoolBytes,
    long ConsumerUtf8CopiedBytes,
    long PooledBytesCleared,
    long RetainedPoolBytes,
    IReadOnlyList<CensusPassMeasurement> PassPeaks,
    long LokadRetainedManagedBytes,
    long LokadRetainedProcessPrivateBytes,
    long BaselineRetainedManagedBytes,
    long BaselineRetainedProcessPrivateBytes,
    string PhysicalType,
    int ValueWidthBytes,
    bool Nullable,
    string Consumer,
    long? RowRangeStart,
    long? RowRangeCount,
    long LokadLiveOwnedBytes,
    long BaselineLiveOwnedBytes);

/// <summary>Measured peak pooled bytes with the decoded-layout size of one census pass.</summary>
/// <param name="PeakPooledBytes">Measured maximum outstanding pooled bytes during the pass.</param>
/// <param name="LogicalOutputBytes">Derived decoded-layout size of the pass.</param>
internal sealed record CensusPassSpec(IReadOnlyList<int> Ordinals, int Target);

/// <summary>Measured peak pooled bytes with the decoded-layout size and projection of one census pass.</summary>
/// <param name="Projection">Snapshot column ordinals projected by the pass.</param>
/// <param name="Target">Snapshot preferred output rows per batch for the pass.</param>
/// <param name="Role">Snapshot pass role: the first pass on a freshly opened file is cold-instrumented, later passes reusing file-owned caches are warm-instrumented. Instrumented time includes consumer, checksum and sink work, so differently shaped passes are never a comparative timing sample.</param>
internal sealed record CensusPassMeasurement(long PeakPooledBytes, long LogicalOutputBytes, double ElapsedMilliseconds, int BatchCount, long AllocatedBytes, int Gen0Collections, int Gen1Collections, int Gen2Collections, IReadOnlyList<int> Projection, int Target, string Role);

internal sealed record WorkCensusSnapshot(
    int SchemaVersion,
    DateTimeOffset RecordedAtUtc,
    string SourceRevision,
    string Runtime,
    string OperatingSystem,
    string Architecture,
    string Processor,
    string ProcessorAffinity,
    int LogicalProcessor,
    bool ServerGarbageCollection,
    string GcLatencyMode,
    string TieredCompilation,
    string TieredPgo,
    string ResolvedOutputPath,
    string OutputFileSystem,
    string RunnerFingerprint,
    string PackageLockHash,
    IReadOnlyList<WorkCensusCase> Cases);


