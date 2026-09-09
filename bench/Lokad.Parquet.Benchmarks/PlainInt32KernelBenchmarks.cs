using System.Buffers.Binary;
using System.Reflection;
using BenchmarkDotNet.Attributes;

namespace Lokad.Parquet.Benchmarks;

public enum PlainInt32KernelMode { Default, ForcedScalar }

[MemoryDiagnoser]
public class PlainInt32KernelBenchmarks
{
    private delegate void PlainDecode(ReadOnlySpan<byte> source, Span<int> destination, CancellationToken cancellationToken);

    private static readonly PlainDecode DecodeInt32;
    private byte[] _source = [];
    private int[] _destination = [];
    private ReadOnlyMemory<int> _memory;
    private ParquetFile? _scanFile;
    private long _scanChecksum;

    static PlainInt32KernelBenchmarks()
    {
        var type = typeof(ParquetFile).Assembly.GetType("Lokad.Parquet.Internal.PlainDecoder") ??
            throw new InvalidOperationException("The internal PLAIN decoder type was not found.");
        var method = type.GetMethod("DecodeInt32", BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Static) ??
            throw new InvalidOperationException("The internal PLAIN INT32 decoder was not found.");
        DecodeInt32 = method.CreateDelegate<PlainDecode>();
    }

    [Params(262_144)]
    public int ValueCount { get; set; }

    [Params(65_536)]
    public int ScanRowCount { get; set; }

    [Params(PlainInt32KernelMode.Default, PlainInt32KernelMode.ForcedScalar)]
    public PlainInt32KernelMode Mode { get; set; }

    [GlobalSetup]
    public async Task Setup()
    {
        BenchmarkHostPolicy.AssertWorkerEnvironment();
        _source = new byte[checked(ValueCount * sizeof(int))];
        _destination = new int[ValueCount];
        for (var i = 0; i < ValueCount; i++)
            BinaryPrimitives.WriteInt32LittleEndian(_source.AsSpan(i * sizeof(int)), i);
        AppContext.SetSwitch("Lokad.Parquet.ForceScalar", Mode == PlainInt32KernelMode.ForcedScalar);
        DecodeInt32(_source, _destination, CancellationToken.None);
        for (var i = 0; i < _destination.Length; i++)
        {
            if (_destination[i] != i)
                throw new InvalidOperationException("The PLAIN INT32 mode benchmark failed its truth check.");
        }
        _memory = _destination;
        var fixture = await ScanFixture.CreateAsync(ScanWorkload.EightRequiredInt32Plain, ScanRowCount);
        _scanFile = await ParquetFile.OpenAsync((ReadOnlyMemory<byte>)fixture.Bytes);
        _scanChecksum = fixture.Checksum;
    }

    [GlobalCleanup]
    public async Task Cleanup()
    {
        if (_scanFile is not null)
            await _scanFile.DisposeAsync();
        AppContext.SetSwitch("Lokad.Parquet.ForceScalar", false);
    }

    [Benchmark(OperationsPerInvoke = 262_144)]
    public int Decode()
    {
        DecodeInt32(_source, _destination, CancellationToken.None);
        return _destination[^1];
    }

    [Benchmark]
    public async Task<long> EightColumnScan()
    {
        var file = _scanFile ?? throw new InvalidOperationException("The scan benchmark is not initialized.");
        var columns = file.Metadata.Schema.Columns;
        var perColumn = new long[columns.Count];
        var nullChains = new long[columns.Count];
        Array.Fill(perColumn, ScanChecksum.Seed);
        Array.Fill(nullChains, ScanChecksum.Seed);
        await foreach (var batch in file.ScanAsync(new(columns)))
        {
            using (batch)
            {
                for (var columnIndex = 0; columnIndex < batch.Columns.Count; columnIndex++)
                {
                    var column = (ParquetPrimitiveColumnBatch<int>)batch.Columns[columnIndex];
                    perColumn[columnIndex] = ScanChecksum.ConsumeRequired(perColumn[columnIndex], column.Values.Span);
                }
            }
        }
        var checksum = ScanChecksum.CombineColumns(perColumn, nullChains);
        if (checksum != _scanChecksum)
            throw new InvalidOperationException("The PLAIN INT32 scan-mode benchmark failed its truth check.");
        return checksum;
    }

    [Benchmark(OperationsPerInvoke = 262_144)]
    public long ArrayChecksum()
    {
        var checksum = ScanChecksum.Seed;
        foreach (var value in _destination)
            checksum = ScanChecksum.Mix(checksum, value);
        return checksum;
    }

    [Benchmark(OperationsPerInvoke = 262_144)]
    public long MemorySpanChecksum()
    {
        var checksum = ScanChecksum.Seed;
        foreach (var value in _memory.Span)
            checksum = ScanChecksum.Mix(checksum, value);
        return checksum;
    }

}
