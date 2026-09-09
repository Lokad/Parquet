using System.Security.Cryptography;
using BenchmarkDotNet.Attributes;
using BaselineDataField = Parquet.Schema.DataField;
using BaselineParquetReader = Parquet.ParquetReader;

namespace Lokad.Parquet.Benchmarks;

[MemoryDiagnoser]
public class PreopenedScanBenchmarks
{
    private ParityScanCase? _scanCase;

    [Params(65_536)]
    public int RowCount { get; set; }

    [Params(
        ScanWorkload.RequiredInt32Plain,
        ScanWorkload.NullableInt32Plain,
        ScanWorkload.RequiredInt32Snappy,
        ScanWorkload.TwoRequiredInt32Plain,
        ScanWorkload.EightRequiredInt32Plain)]
    public ScanWorkload Workload { get; set; }

    [GlobalSetup]
    public async Task Setup()
    {
        BenchmarkHostPolicy.AssertWorkerEnvironment();
        _scanCase = await ParityScanCase.CreateAsync(Workload, RowCount);
        Console.WriteLine($"Fixture SHA-256: {_scanCase.FixtureHash}.");
        Console.WriteLine(
            $"Fixture bytes: {_scanCase.FixtureLength}; rows: {RowCount}; workload: {Workload}; " +
            "readers and Parquet.NET destinations are pre-opened/preallocated.");
    }

    [Benchmark(Baseline = true, Description = "Lokad pre-opened projected scan")]
    public Task<long> LokadProjectedScan() => GetScanCase().ReadLokadAsync();

    [Benchmark(Description = "Parquet.NET pre-opened projected scan")]
    public Task<long> ParquetNetProjectedScan() => GetScanCase().ReadParquetNetAsync();

    [GlobalCleanup]
    public async Task Cleanup()
    {
        if (_scanCase is not null)
            await _scanCase.DisposeAsync();
    }

    private ParityScanCase GetScanCase() =>
        _scanCase ?? throw new InvalidOperationException("The parity scan benchmark is not initialized.");
}

[MemoryDiagnoser]
public class PreopenedUtf8ScanBenchmarks
{
    private ParityScanCase? _scanCase;

    [Params(65_536)]
    public int RowCount { get; set; }

    [Params(
        ScanWorkload.RequiredStringPlain,
        ScanWorkload.RequiredStringSnappy,
        ScanWorkload.RequiredStringDictionary,
        ScanWorkload.RequiredStringDictionarySnappy)]
    public ScanWorkload Workload { get; set; }

    [GlobalSetup]
    public async Task Setup()
    {
        BenchmarkHostPolicy.AssertWorkerEnvironment();
        _scanCase = await ParityScanCase.CreateAsync(Workload, RowCount);
        Console.WriteLine($"Fixture SHA-256: {_scanCase.FixtureHash}.");
        Console.WriteLine(
            $"Fixture bytes: {_scanCase.FixtureLength}; UTF-8 payload bytes: " +
            $"{ScanWorkloadCatalog.GetUtf8PayloadByteCount(Workload, RowCount)}; " +
            $"rows: {RowCount}; workload: {Workload}; " +
            "both readers deliver strictly validated UTF-8 to reusable payload-and-offset sinks.");
    }

    [Benchmark(Baseline = true, Description = "Lokad pre-opened UTF-8 pipeline")]
    public Task<long> LokadUtf8Pipeline() => GetScanCase().ReadLokadAsync();

    [Benchmark(Description = "Parquet.NET pre-opened UTF-8 pipeline")]
    public Task<long> ParquetNetUtf8Pipeline() => GetScanCase().ReadParquetNetAsync();

    [GlobalCleanup]
    public async Task Cleanup()
    {
        if (_scanCase is not null)
            await _scanCase.DisposeAsync();
    }

    private ParityScanCase GetScanCase() =>
        _scanCase ?? throw new InvalidOperationException("The UTF-8 benchmark is not initialized.");
}

[MemoryDiagnoser]
public class PreopenedMaterializationBenchmarks
{
    private ParityScanCase? _scanCase;

    [Params(65_536)]
    public int RowCount { get; set; }

    [Params(
        ScanWorkload.RequiredInt32Plain,
        ScanWorkload.NullableInt32Plain,
        ScanWorkload.TwoRequiredInt32Plain,
        ScanWorkload.EightRequiredInt32Plain)]
    public ScanWorkload Workload { get; set; }

    [GlobalSetup]
    public async Task Setup()
    {
        BenchmarkHostPolicy.AssertWorkerEnvironment();
        _scanCase = await ParityScanCase.CreateAsync(Workload, RowCount);
    }

    [Benchmark(Baseline = true, Description = "Lokad pre-opened materialization")]
    public Task<long> LokadMaterialize() => GetScanCase().MaterializeLokadAsync();

    [Benchmark(Description = "Parquet.NET pre-opened materialization")]
    public Task<long> ParquetNetMaterialize() => GetScanCase().MaterializeParquetNetAsync();

    [GlobalCleanup]
    public async Task Cleanup()
    {
        if (_scanCase is not null)
            await _scanCase.DisposeAsync();
    }

    private ParityScanCase GetScanCase() =>
        _scanCase ?? throw new InvalidOperationException("The materialization benchmark is not initialized.");
}

internal sealed class ParityScanCase : IAsyncDisposable
{
    private readonly MemoryStream _lokadStream;
    private readonly ParquetFile _lokadFile;
    private readonly MemoryStream _parquetNetStream;
    private readonly BaselineParquetReader _parquetNetReader;
    private readonly BaselineDataField[] _parquetNetFields;
    private readonly int[][] _requiredDestinations;
    private readonly int?[]? _nullableDestination;
    private readonly string?[]? _stringDestination;
    private readonly Utf8ScanSink? _lokadUtf8Sink;
    private readonly Utf8ScanSink? _parquetNetUtf8Sink;
    private readonly ParquetScanOptions _scanOptions;
    private readonly int _utf8PayloadBytes;
    private readonly long[] _columnChecksums;
    private readonly long[] _columnNullChains;
    private readonly long[] _parquetNetColumnChecksums;
    private readonly long[] _parquetNetColumnNullChains;

    private ParityScanCase(
        ScanFixture fixture,
        MemoryStream lokadStream,
        ParquetFile lokadFile,
        MemoryStream parquetNetStream,
        BaselineParquetReader parquetNetReader)
    {
        _lokadStream = lokadStream;
        _lokadFile = lokadFile;
        _parquetNetStream = parquetNetStream;
        _parquetNetReader = parquetNetReader;
        _parquetNetFields = parquetNetReader.Schema.DataFields.ToArray();
        _scanOptions = new ParquetScanOptions(
            lokadFile.Metadata.Schema.Columns,
            null,
            null,
            fixture.RowCount);
        Fixture = fixture;
        _columnChecksums = new long[fixture.ColumnCount];
        _columnNullChains = new long[fixture.ColumnCount];
        _parquetNetColumnChecksums = new long[fixture.ColumnCount];
        _parquetNetColumnNullChains = new long[fixture.ColumnCount];
        _utf8PayloadBytes = fixture.Utf8PayloadBytes;
        FixtureLength = fixture.Bytes.Length;
        FixtureHash = Convert.ToHexStringLower(SHA256.HashData(fixture.Bytes));

        if (fixture.Workload == ScanWorkload.NullableInt32Plain)
        {
            _nullableDestination = new int?[fixture.RowCount];
            _stringDestination = null;
            _requiredDestinations = [];
        }
        else if (ScanWorkloadCatalog.IsString(fixture.Workload))
        {
            _nullableDestination = null;
            _stringDestination = new string?[fixture.RowCount];
            _requiredDestinations = [];
            _lokadUtf8Sink = new Utf8ScanSink(fixture.RowCount, fixture.Utf8PayloadBytes);
            _parquetNetUtf8Sink = new Utf8ScanSink(fixture.RowCount, fixture.Utf8PayloadBytes);
        }
        else
        {
            _nullableDestination = null;
            _stringDestination = null;
            _requiredDestinations = Enumerable.Range(0, fixture.ColumnCount)
                .Select(_ => new int[fixture.RowCount])
                .ToArray();
        }
    }

    public int FixtureLength { get; }

    public string FixtureHash { get; }

    internal ScanFixture Fixture { get; }

    internal IReadOnlyList<ParquetColumn> SchemaColumns => _lokadFile.Metadata.Schema.Columns;

    public static async Task<ParityScanCase> CreateAsync(ScanWorkload workload, int rowCount)
    {
        var fixture = await ScanFixture.CreateAsync(workload, rowCount);
        var lokadStream = new MemoryStream(fixture.Bytes, writable: false);
        var parquetNetStream = new MemoryStream(fixture.Bytes, writable: false);
        ParquetFile? lokadFile = null;
        BaselineParquetReader? parquetNetReader = null;
        try
        {
            lokadFile = await ParquetFile.OpenAsync(lokadStream);
            parquetNetReader = await BaselineParquetReader.CreateAsync(parquetNetStream);
            if (lokadFile.Metadata.RowCount != rowCount || parquetNetReader.Metadata?.NumRows != rowCount ||
                lokadFile.Metadata.Schema.Columns.Count != fixture.ColumnCount ||
                parquetNetReader.Schema.DataFields.Count() != fixture.ColumnCount ||
                lokadFile.Metadata.RowGroups.Count != 1 || parquetNetReader.RowGroupCount != 1)
                throw new InvalidOperationException("The parity benchmark metadata truth check failed.");

            var result = new ParityScanCase(
                fixture,
                lokadStream,
                lokadFile,
                parquetNetStream,
                parquetNetReader);
            lokadFile = null;
            parquetNetReader = null;
            try
            {
                if (!ScanWorkloadCatalog.IsString(fixture.Workload))
                    await fixture.AssertLokadScanAsync();
                var lokadChecksum = await result.ReadLokadAsync();
                var parquetNetChecksum = await result.ReadParquetNetAsync();
                if (lokadChecksum != fixture.Checksum || parquetNetChecksum != fixture.Checksum)
                    throw new InvalidOperationException(
                        $"Parity benchmark truth mismatch: expected {fixture.Checksum}, " +
                        $"Lokad {lokadChecksum}, Parquet.NET {parquetNetChecksum}.");
                return result;
            }
            catch
            {
                await result.DisposeAsync();
                throw;
            }
        }
        finally
        {
            if (lokadFile is not null)
                await lokadFile.DisposeAsync();
            if (parquetNetReader is not null)
                await parquetNetReader.DisposeAsync();
            if (lokadFile is not null)
                lokadStream.Dispose();
            if (parquetNetReader is not null)
                parquetNetStream.Dispose();
        }
    }

    public Task<long> ReadLokadAsync() => ReadLokadAsync(_scanOptions);

    // Shared Parity consumer with an explicit projection so truth verification
    // exercises the real pre-opened path with adversarial orderings.
    public async Task<long> ReadLokadAsync(ParquetScanOptions scanOptions)
    {
        _lokadUtf8Sink?.Reset();
        // Multi-column lanes accumulate one checksum per column so the result
        // does not depend on how pages batch across columns.
        var multi = scanOptions.Columns.Count > 1;
        var valueChains = _columnChecksums;
        var nullChains = _columnNullChains;
        if (multi)
        {
            Array.Fill(valueChains, ScanChecksum.Seed, 0, scanOptions.Columns.Count);
            Array.Fill(nullChains, ScanChecksum.Seed, 0, scanOptions.Columns.Count);
        }
        else
        {
            valueChains[0] = ScanChecksum.Seed;
            nullChains[0] = ScanChecksum.Seed;
        }
        var consumed = 0;
        await foreach (var batch in _lokadFile.ScanAsync(scanOptions))
        {
            using (batch)
            {
                for (var columnIndex = 0; columnIndex < batch.Columns.Count; columnIndex++)
                {
                    var untypedColumn = batch.Columns[columnIndex];
                    if (untypedColumn is ParquetBinaryColumnBatch strings)
                    {
                        if (!strings.Validity.IsAllValid || _lokadUtf8Sink is null)
                            throw new InvalidOperationException("The required UTF-8 parity column is invalid.");
                        var offsets = strings.Offsets.Span;
                        for (var row = 0; row < strings.RowCount; row++)
                            _lokadUtf8Sink.AppendUtf8(
                                strings.Payload.Span[offsets[row]..offsets[row + 1]]);
                    }
                    else
                    {
                        var column = (ParquetPrimitiveColumnBatch<int>)untypedColumn;
                        var values = column.Values.Span;
                        var validity = column.Validity;
                        if (multi)
                        {
                            if (!validity.IsAllValid)
                                throw new InvalidOperationException("A required multi-column parity value decoded as null.");
                            valueChains[columnIndex] = ScanChecksum.ConsumeRequired(valueChains[columnIndex], values);
                        }
                        else if (validity.IsAllValid)
                        {
                            valueChains[0] = ScanChecksum.ConsumeRequired(valueChains[0], values);
                        }
                        else
                        {
                            var bits = validity.Bits.Span;
                            for (var row = 0; row < values.Length; row++)
                            {
                                if ((bits[row >> 3] & (1 << (row & 7))) != 0)
                                    valueChains[0] = ScanChecksum.Mix(valueChains[0], values[row]);
                                else
                                    nullChains[0] = ScanChecksum.Mix(nullChains[0], consumed + row);
                            }
                        }
                    }
                }
                consumed += batch.RowCount;
            }
        }
        if (_lokadUtf8Sink is null)
            return multi
                ? ScanChecksum.CombineColumns(valueChains.AsSpan(0, scanOptions.Columns.Count), nullChains.AsSpan(0, scanOptions.Columns.Count))
                : ScanChecksum.CombineColumn(valueChains[0], nullChains[0]);
        if (_stringDestination is null)
            throw new InvalidOperationException("The UTF-8 parity destination is unavailable.");
        return _lokadUtf8Sink.Complete(_stringDestination.Length, _utf8PayloadBytes);
    }

    public Task<long> ReadParquetNetAsync() => ReadParquetNetAsync(null);

    // Shared Parity baseline consumer with explicit ordinals so truth
    // verification exercises the real destination-buffer path adversarially.
    // A null ordinal list reads every field in schema order.
    public async Task<long> ReadParquetNetAsync(IReadOnlyList<int>? ordinals)
    {
        using var rowGroup = _parquetNetReader.OpenRowGroupReader(0);
        if (_stringDestination is not null)
        {
            _parquetNetUtf8Sink?.Reset();
            await rowGroup.ReadAsync(_parquetNetFields[0], _stringDestination);
            foreach (var value in _stringDestination)
            {
                if (value is null || _parquetNetUtf8Sink is null)
                    throw new InvalidOperationException("A required UTF-8 parity value decoded as null.");
                _parquetNetUtf8Sink.AppendString(value);
            }
            if (_parquetNetUtf8Sink is null)
                throw new InvalidOperationException("The Parquet.NET UTF-8 parity sink is unavailable.");
            return _parquetNetUtf8Sink.Complete(_stringDestination.Length, _utf8PayloadBytes);
        }
        var resolved = ordinals ?? AllOrdinals(_parquetNetFields.Length);

        static IReadOnlyList<int> AllOrdinals(int count)
        {
            var all = new int[count];
            for (var ordinal = 0; ordinal < all.Length; ordinal++)
                all[ordinal] = ordinal;
            return all;
        }
        var valueChains = _parquetNetColumnChecksums;
        var nullChains = _parquetNetColumnNullChains;
        if (_nullableDestination is not null)
        {
            // Single row group, like the previous baseline path: a multi-group
            // fixture fails the truth comparison loudly instead of half-reading.
            valueChains[0] = ScanChecksum.Seed;
            nullChains[0] = ScanChecksum.Seed;
            await rowGroup.ReadAsync<int>(_parquetNetFields[0], _nullableDestination);
            for (var row = 0; row < _nullableDestination.Length; row++)
            {
                if (_nullableDestination[row] is int value)
                    valueChains[0] = ScanChecksum.Mix(valueChains[0], value);
                else
                    nullChains[0] = ScanChecksum.Mix(nullChains[0], row);
            }
            return ScanChecksum.CombineColumn(valueChains[0], nullChains[0]);
        }
        var multi = resolved.Count > 1;
        if (multi)
        {
            Array.Fill(valueChains, ScanChecksum.Seed, 0, resolved.Count);
            Array.Fill(nullChains, ScanChecksum.Seed, 0, resolved.Count);
        }
        else
        {
            valueChains[0] = ScanChecksum.Seed;
            nullChains[0] = ScanChecksum.Seed;
        }
        for (var columnIndex = 0; columnIndex < resolved.Count; columnIndex++)
        {
            var values = _requiredDestinations[resolved[columnIndex]];
            await rowGroup.ReadAsync<int>(_parquetNetFields[resolved[columnIndex]], values);
            valueChains[columnIndex] = ScanChecksum.ConsumeRequired(valueChains[columnIndex], values);
        }
        return multi
            ? ScanChecksum.CombineColumns(valueChains.AsSpan(0, resolved.Count), nullChains.AsSpan(0, resolved.Count))
            : ScanChecksum.CombineColumn(valueChains[0], nullChains[0]);
    }


    public async Task<long> MaterializeLokadAsync()
    {
        long sentinel = 0;
        await foreach (var batch in _lokadFile.ScanAsync(_scanOptions))
        {
            using (batch)
            {
                foreach (var untypedColumn in batch.Columns)
                {
                    var column = (ParquetPrimitiveColumnBatch<int>)untypedColumn;
                    var values = column.Values.Span;
                    sentinel = ScanChecksum.Mix(sentinel, values[0]);
                    sentinel = ScanChecksum.Mix(sentinel, values[^1]);
                    sentinel = ScanChecksum.Mix(sentinel, column.Validity.Bits.Length);
                }
            }
        }
        return sentinel;
    }

    public async Task<long> MaterializeParquetNetAsync()
    {
        using var rowGroup = _parquetNetReader.OpenRowGroupReader(0);
        long sentinel = 0;
        if (_nullableDestination is not null)
        {
            await rowGroup.ReadAsync<int>(_parquetNetFields[0], _nullableDestination);
            if (_nullableDestination[0] is int first)
                sentinel = ScanChecksum.Mix(sentinel, first);
            if (_nullableDestination[^1] is int last)
                sentinel = ScanChecksum.Mix(sentinel, last);
        }
        else
        {
            for (var column = 0; column < _parquetNetFields.Length; column++)
            {
                var values = _requiredDestinations[column];
                await rowGroup.ReadAsync<int>(_parquetNetFields[column], values);
                sentinel = ScanChecksum.Mix(sentinel, values[0]);
                sentinel = ScanChecksum.Mix(sentinel, values[^1]);
            }
        }
        return sentinel;
    }

    public async ValueTask DisposeAsync()
    {
        await _lokadFile.DisposeAsync();
        await _parquetNetReader.DisposeAsync();
        _lokadStream.Dispose();
        _parquetNetStream.Dispose();
    }

}
