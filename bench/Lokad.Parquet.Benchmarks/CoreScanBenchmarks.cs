using System.Security.Cryptography;
using BenchmarkDotNet.Attributes;
using BaselineParquetReader = Parquet.ParquetReader;

namespace Lokad.Parquet.Benchmarks;

[MemoryDiagnoser]
public class CoreScanBenchmarks
{
    private byte[] _fixture = [];
    private long _expectedChecksum;
    private int _columnCount;
    private long[] _lokadColumnChecksums = [];
    private long[] _parquetNetColumnChecksums = [];
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
        var fixture = await ScanFixture.CreateAsync(Workload, RowCount);
        _fixture = fixture.Bytes;
        _expectedChecksum = fixture.Checksum;
        _columnCount = fixture.ColumnCount;
        _lokadColumnChecksums = new long[fixture.ColumnCount];
        _parquetNetColumnChecksums = new long[fixture.ColumnCount];
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

    private async Task<long> ReadLokadAsync()
    {
        using var stream = new MemoryStream(_fixture, writable: false);
        await using var file = await ParquetFile.OpenAsync(stream);
        long checksum = ScanChecksum.Seed;
        var utf8Sink = _lokadUtf8Sink;
        utf8Sink?.Reset();
        // Multi-column lanes accumulate one checksum per column so the result
        // does not depend on how pages batch across columns.
        var perColumn = _columnCount > 1 ? _lokadColumnChecksums : null;
        if (perColumn is not null)
            Array.Fill(perColumn, ScanChecksum.Seed, 0, _columnCount);
        await foreach (var batch in file.ScanAsync(
            new ParquetScanOptions(file.Metadata.Schema.Columns)))
        {
            using (batch)
            {
                for (var columnIndex = 0; columnIndex < batch.Columns.Count; columnIndex++)
                {
                    var column = batch.Columns[columnIndex];
                    switch (column)
                    {
                        case ParquetPrimitiveColumnBatch<int> integers:
                            if (perColumn is null)
                            {
                                for (var row = 0; row < integers.RowCount; row++)
                                    checksum = ScanChecksum.Mix(
                                        checksum,
                                        integers.Validity.IsValid(row) ? integers.Values.Span[row] : ScanChecksum.NullMarker);
                            }
                            else
                            {
                                if (!integers.Validity.IsAllValid)
                                    throw new InvalidOperationException("A required multi-column benchmark value decoded as null.");
                                perColumn[columnIndex] = ScanChecksum.ConsumeRequired(perColumn[columnIndex], integers.Values.Span);
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
            }
        }
        if (utf8Sink is not null)
            return utf8Sink.Complete(RowCount, _utf8PayloadBytes);
        if (perColumn is not null)
            return ScanChecksum.CombineColumns(perColumn.AsSpan(0, _columnCount));
        return checksum;
    }

    private async Task<long> ReadParquetNetAsync()
    {
        using var stream = new MemoryStream(_fixture, writable: false);
        await using var reader = await BaselineParquetReader.CreateAsync(stream);
        long checksum = ScanChecksum.Seed;
        var utf8Sink = _parquetNetUtf8Sink;
        utf8Sink?.Reset();
        var perColumn = _columnCount > 1 ? _parquetNetColumnChecksums : null;
        if (perColumn is not null)
            Array.Fill(perColumn, ScanChecksum.Seed, 0, _columnCount);
        for (var groupOrdinal = 0; groupOrdinal < reader.RowGroupCount; groupOrdinal++)
        {
            using var rowGroup = reader.OpenRowGroupReader(groupOrdinal);
            if (Workload == ScanWorkload.NullableInt32Plain)
            {
                var values = new int?[checked((int)rowGroup.RowCount)];
                await rowGroup.ReadAsync<int>(reader.Schema.DataFields[0], values);
                foreach (var value in values)
                    checksum = ScanChecksum.Mix(checksum, value ?? ScanChecksum.NullMarker);
            }
            else if (ScanWorkloadCatalog.IsString(Workload))
            {
                var values = new string?[checked((int)rowGroup.RowCount)];
                await rowGroup.ReadAsync(reader.Schema.DataFields[0], values);
                foreach (var value in values)
                {
                    if (value is null || utf8Sink is null)
                        throw new InvalidOperationException("A required benchmark string decoded as null.");
                    utf8Sink.AppendString(value);
                }
            }
            else if (perColumn is not null)
            {
                var columnIndex = 0;
                foreach (var field in reader.Schema.DataFields)
                {
                    var values = new int[checked((int)rowGroup.RowCount)];
                    await rowGroup.ReadAsync<int>(field, values);
                    perColumn[columnIndex] = ScanChecksum.ConsumeRequired(perColumn[columnIndex], values);
                    columnIndex++;
                }
            }
            else
            {
                foreach (var field in reader.Schema.DataFields)
                {
                    var values = new int[checked((int)rowGroup.RowCount)];
                    await rowGroup.ReadAsync<int>(field, values);
                    checksum = ScanChecksum.ConsumeRequired(checksum, values);
                }
            }
        }
        if (utf8Sink is not null)
            return utf8Sink.Complete(RowCount, _utf8PayloadBytes);
        if (perColumn is not null)
            return ScanChecksum.CombineColumns(perColumn.AsSpan(0, _columnCount));
        return checksum;
    }

}
