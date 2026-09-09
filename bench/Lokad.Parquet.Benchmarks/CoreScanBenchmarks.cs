using System.Security.Cryptography;
using BenchmarkDotNet.Attributes;
using BaselineParquetReader = Parquet.ParquetReader;

namespace Lokad.Parquet.Benchmarks;

[MemoryDiagnoser]
public class CoreScanBenchmarks
{
    private byte[] _fixture = [];
    private long _expectedChecksum;
    private long[] _lokadColumnChecksums = [];
    private long[] _lokadNullChains = [];
    private long[] _parquetNetColumnChecksums = [];
    private long[] _parquetNetNullChains = [];
    private Utf8ScanSink? _lokadUtf8Sink;
    private Utf8ScanSink? _parquetNetUtf8Sink;
    private int _utf8PayloadBytes;

    [Params(65_536)]
    public int RowCount { get; set; }

    [Params(
        ScanWorkload.NullableInt32Plain,
        ScanWorkload.RequiredInt32Snappy,
        ScanWorkload.RequiredStringPlain,
        ScanWorkload.RequiredStringSnappy,
        ScanWorkload.RequiredStringDictionary,
        ScanWorkload.RequiredStringDictionarySnappy,
        ScanWorkload.TwoRequiredInt32Plain,
        ScanWorkload.EightRequiredInt32Plain)]
    public ScanWorkload Workload { get; set; }

    [GlobalSetup]
    public async Task Setup()
    {
        BenchmarkHostPolicy.AssertWorkerEnvironment();
        var fixture = await ScanFixture.CreateAsync(Workload, RowCount);
        _fixture = fixture.Bytes;
        _expectedChecksum = fixture.Checksum;
        _lokadColumnChecksums = new long[fixture.ColumnCount];
        _lokadNullChains = new long[fixture.ColumnCount];
        _parquetNetColumnChecksums = new long[fixture.ColumnCount];
        _parquetNetNullChains = new long[fixture.ColumnCount];
        if (!ScanWorkloadCatalog.IsString(Workload))
            await fixture.AssertLokadScanAsync();
        _utf8PayloadBytes = fixture.Utf8PayloadBytes;
        if (ScanWorkloadCatalog.IsString(Workload))
        {
            _lokadUtf8Sink = new Utf8ScanSink(RowCount, fixture.Utf8PayloadBytes);
            _parquetNetUtf8Sink = new Utf8ScanSink(RowCount, fixture.Utf8PayloadBytes);
        }
        var lokad = await ReadLokadAsync();
        var baseline = await ReadParquetNetAsync();
        if (lokad != _expectedChecksum || baseline != _expectedChecksum)
            throw new InvalidOperationException(
                $"Benchmark truth mismatch: expected {_expectedChecksum}, Lokad {lokad}, Parquet.NET {baseline}.");

        var peakPooledBytes = await PeakPoolMeasurement.MeasureAsync(ReadLokadAsync);
        var retained = await RetainedMemoryMeasurement.MeasureAsync(ReadLokadAsync, 16);
        Console.WriteLine($"Fixture SHA-256: {Convert.ToHexStringLower(SHA256.HashData(_fixture))}");
        Console.WriteLine($"Fixture bytes: {_fixture.Length}; rows: {RowCount}; workload: {Workload}.");
        Console.WriteLine($"Lokad peak pooled bytes: {peakPooledBytes}.");
        Console.WriteLine(
            $"Lokad retained bytes after 16 warmed scans: managed={retained.ManagedBytes}; " +
            $"process-private={retained.ProcessPrivateBytes}.");
    }

    [Benchmark(Baseline = true, Description = "Lokad Core projected scan")]
    public Task<long> LokadProjectedScan() => ReadLokadAsync();

    [Benchmark(Description = "Parquet.NET Core projected scan")]
    public Task<long> ParquetNetProjectedScan() => ReadParquetNetAsync();

    private Task<long> ReadLokadAsync() => ReadLokadAsync(
        _fixture, null, null, _lokadUtf8Sink, RowCount, _utf8PayloadBytes, _lokadColumnChecksums, _lokadNullChains);

    // Shared Core consumer: the timed benchmark above and the truth verification
    // run this same loop. A null ordinal list scans the full schema in order with
    // the default target; explicit ordinals scan the projection in the given order
    // with the given target, so adversarial orderings exercise the real path.
    internal static async Task<long> ReadLokadAsync(
        byte[] fixtureBytes,
        IReadOnlyList<int>? ordinals,
        int? targetRowCount,
        Utf8ScanSink? utf8Sink,
        int rowCount,
        int utf8PayloadBytes,
        long[] valueChains,
        long[] nullChains)
    {
        using var stream = new MemoryStream(fixtureBytes, writable: false);
        await using var file = await ParquetFile.OpenAsync(stream);
        utf8Sink?.Reset();
        var schemaColumns = file.Metadata.Schema.Columns;
        IReadOnlyList<ParquetColumn> projection = ordinals is null
            ? schemaColumns
            : ordinals.Select(ordinal => schemaColumns[ordinal]).ToArray();
        // Multi-column lanes accumulate one checksum per column so the result
        // does not depend on how pages batch across columns.
        var multi = projection.Count > 1;
        if (multi)
        {
            Array.Fill(valueChains, ScanChecksum.Seed, 0, projection.Count);
            Array.Fill(nullChains, ScanChecksum.Seed, 0, projection.Count);
        }
        else
        {
            valueChains[0] = ScanChecksum.Seed;
            nullChains[0] = ScanChecksum.Seed;
        }
        var options = targetRowCount is null
            ? new ParquetScanOptions(projection)
            : new ParquetScanOptions(projection, null, null, targetRowCount.Value);
        var consumed = 0;
        await foreach (var batch in file.ScanAsync(options))
        {
            using (batch)
            {
                for (var columnIndex = 0; columnIndex < batch.Columns.Count; columnIndex++)
                {
                    var column = batch.Columns[columnIndex];
                    switch (column)
                    {
                        case ParquetPrimitiveColumnBatch<int> integers:
                            if (multi)
                            {
                                if (!integers.Validity.IsAllValid)
                                    throw new InvalidOperationException("A required multi-column benchmark value decoded as null.");
                                valueChains[columnIndex] = ScanChecksum.ConsumeRequired(valueChains[columnIndex], integers.Values.Span);
                            }
                            else
                            {
                                for (var row = 0; row < integers.RowCount; row++)
                                {
                                    if (integers.Validity.IsValid(row))
                                        valueChains[0] = ScanChecksum.Mix(valueChains[0], integers.Values.Span[row]);
                                    else
                                        nullChains[0] = ScanChecksum.Mix(nullChains[0], consumed + row);
                                }
                            }
                            break;
                        case ParquetBinaryColumnBatch strings:
                            if (!strings.Validity.IsAllValid || utf8Sink is null)
                                throw new InvalidOperationException("The required UTF-8 benchmark column is invalid.");
                            var offsets = strings.Offsets.Span;
                            for (var row = 0; row < strings.RowCount; row++)
                            {
                                var bytes = strings.Payload.Span[offsets[row]..offsets[row + 1]];
                                utf8Sink.AppendUtf8(bytes);
                            }
                            break;
                        default:
                            throw new InvalidOperationException("The benchmark decoded an unexpected batch type.");
                    }
                }
                consumed += batch.RowCount;
            }
        }
        if (utf8Sink is not null)
            return utf8Sink.Complete(rowCount, utf8PayloadBytes);
        if (multi)
            return ScanChecksum.CombineColumns(valueChains.AsSpan(0, projection.Count), nullChains.AsSpan(0, projection.Count));
        return ScanChecksum.CombineColumn(valueChains[0], nullChains[0]);
    }

    private Task<long> ReadParquetNetAsync() => ReadParquetNetAsync(
        _fixture, Workload, null, _parquetNetUtf8Sink, RowCount, _utf8PayloadBytes, _parquetNetColumnChecksums, _parquetNetNullChains);

    // Shared Core baseline consumer, parameterized like the Lokad loop above so
    // truth verification exercises the real baseline path with explicit ordinals.
    internal static async Task<long> ReadParquetNetAsync(
        byte[] fixtureBytes,
        ScanWorkload workload,
        IReadOnlyList<int>? ordinals,
        Utf8ScanSink? utf8Sink,
        int rowCount,
        int utf8PayloadBytes,
        long[] valueChains,
        long[] nullChains)
    {
        using var stream = new MemoryStream(fixtureBytes, writable: false);
        await using var reader = await BaselineParquetReader.CreateAsync(stream);
        utf8Sink?.Reset();
        var allFields = reader.Schema.DataFields.ToArray();
        var fields = ordinals is null
            ? allFields
            : ordinals.Select(ordinal => allFields[ordinal]).ToArray();
        var multi = fields.Length > 1;
        if (multi)
        {
            Array.Fill(valueChains, ScanChecksum.Seed, 0, fields.Length);
            Array.Fill(nullChains, ScanChecksum.Seed, 0, fields.Length);
        }
        else
        {
            valueChains[0] = ScanChecksum.Seed;
            nullChains[0] = ScanChecksum.Seed;
        }
        var consumed = 0;
        for (var groupOrdinal = 0; groupOrdinal < reader.RowGroupCount; groupOrdinal++)
        {
            using var rowGroup = reader.OpenRowGroupReader(groupOrdinal);
            if (workload == ScanWorkload.NullableInt32Plain)
            {
                var values = new int?[checked((int)rowGroup.RowCount)];
                await rowGroup.ReadAsync<int>(fields[0], values);
                for (var row = 0; row < values.Length; row++)
                {
                    if (values[row] is int value)
                        valueChains[0] = ScanChecksum.Mix(valueChains[0], value);
                    else
                        nullChains[0] = ScanChecksum.Mix(nullChains[0], consumed + row);
                }
                consumed += values.Length;
            }
            else if (ScanWorkloadCatalog.IsString(workload))
            {
                var values = new string?[checked((int)rowGroup.RowCount)];
                await rowGroup.ReadAsync(fields[0], values);
                foreach (var value in values)
                {
                    if (value is null || utf8Sink is null)
                        throw new InvalidOperationException("A required benchmark string decoded as null.");
                    utf8Sink.AppendString(value);
                }
            }
            else if (multi)
            {
                for (var columnIndex = 0; columnIndex < fields.Length; columnIndex++)
                {
                    var values = new int[checked((int)rowGroup.RowCount)];
                    await rowGroup.ReadAsync<int>(fields[columnIndex], values);
                    valueChains[columnIndex] = ScanChecksum.ConsumeRequired(valueChains[columnIndex], values);
                }
            }
            else
            {
                var values = new int[checked((int)rowGroup.RowCount)];
                await rowGroup.ReadAsync<int>(fields[0], values);
                valueChains[0] = ScanChecksum.ConsumeRequired(valueChains[0], values);
            }
        }
        if (utf8Sink is not null)
            return utf8Sink.Complete(rowCount, utf8PayloadBytes);
        if (multi)
            return ScanChecksum.CombineColumns(valueChains.AsSpan(0, fields.Length), nullChains.AsSpan(0, fields.Length));
        return ScanChecksum.CombineColumn(valueChains[0], nullChains[0]);
    }

}
