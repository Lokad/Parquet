using System.Security.Cryptography;
using BenchmarkDotNet.Attributes;
using BaselineParquetReader = Parquet.ParquetReader;

namespace Lokad.Parquet.Benchmarks;

[MemoryDiagnoser]
public class CoreScanBenchmarks
{
    private byte[] _fixture = [];
    private long _expectedChecksum;
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
        await foreach (var batch in file.ScanAsync(
            new ParquetScanOptions(file.Metadata.Schema.Columns)))
        {
            using (batch)
            {
                foreach (var column in batch.Columns)
                {
                    switch (column)
                    {
                        case ParquetPrimitiveColumnBatch<int> integers:
                            for (var row = 0; row < integers.RowCount; row++)
                                checksum = ScanChecksum.Mix(
                                    checksum,
                                    integers.Validity.IsValid(row) ? integers.Values.Span[row] : ScanChecksum.NullMarker);
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
        return utf8Sink?.Complete(
            RowCount,
            _utf8PayloadBytes) ?? checksum;
    }

    private async Task<long> ReadParquetNetAsync()
    {
        using var stream = new MemoryStream(_fixture, writable: false);
        await using var reader = await BaselineParquetReader.CreateAsync(stream);
        long checksum = ScanChecksum.Seed;
        var utf8Sink = _parquetNetUtf8Sink;
        utf8Sink?.Reset();
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
        return utf8Sink?.Complete(
            RowCount,
            _utf8PayloadBytes) ?? checksum;
    }

}
