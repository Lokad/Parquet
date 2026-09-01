using System.Reflection;
using BenchmarkDotNet.Attributes;

namespace Lokad.Parquet.Benchmarks;

[MemoryDiagnoser]
public class SnappyCodecBenchmarks
{
    private delegate void SnappyDecode(
        ReadOnlySpan<byte> source,
        Span<byte> destination,
        CancellationToken cancellationToken);

    private static readonly SnappyDecode DecodeSnappy;
    private byte[] _source = [];
    private byte[] _destination = [];

    static SnappyCodecBenchmarks()
    {
        var type = typeof(ParquetFile).Assembly.GetType("Lokad.Parquet.Internal.SnappyBlockDecoder") ??
            throw new InvalidOperationException("The internal Snappy decoder type was not found.");
        var method = type.GetMethod("Decompress", BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Static) ??
            throw new InvalidOperationException("The internal Snappy decoder was not found.");
        DecodeSnappy = method.CreateDelegate<SnappyDecode>();
    }

    [Params(262_144)]
    public int ByteCount { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        var lengthBytes = new List<byte>();
        var length = (uint)ByteCount;
        while (length >= 0x80)
        {
            lengthBytes.Add((byte)(length | 0x80));
            length >>= 7;
        }
        lengthBytes.Add((byte)length);

        _source = new byte[checked(lengthBytes.Count + 4 + ByteCount)];
        lengthBytes.CopyTo(_source, 0);
        var offset = lengthBytes.Count;
        _source[offset++] = 0xF8;
        _source[offset++] = 0xFF;
        _source[offset++] = 0xFF;
        _source[offset++] = 0x03;
        for (var index = 0; index < ByteCount; index++)
            _source[offset + index] = (byte)index;
        _destination = new byte[ByteCount];
        DecodeSnappy(_source, _destination, CancellationToken.None);
        for (var index = 0; index < _destination.Length; index++)
        {
            if (_destination[index] != (byte)index)
                throw new InvalidOperationException("The Snappy codec benchmark failed its truth check.");
        }
    }

    [Benchmark(OperationsPerInvoke = 262_144, Description = "Snappy literal block decode")]
    public byte Decode()
    {
        DecodeSnappy(_source, _destination, CancellationToken.None);
        return _destination[^1];
    }
}

[MemoryDiagnoser]
public class SteadyStateScanBenchmarks
{
    private MemoryStream? _stream;
    private ParquetFile? _file;
    private long _expectedChecksum;

    [Params(262_144)]
    public int RowCount { get; set; }

    [GlobalSetup]
    public async Task Setup()
    {
        var fixture = await ScanFixture.CreateAsync(ScanWorkload.RequiredInt32Plain, RowCount);
        _expectedChecksum = fixture.Checksum;
        _stream = new MemoryStream(fixture.Bytes, writable: false);
        _file = await ParquetFile.OpenAsync(_stream);
        var checksum = await BenchmarkScan.ReadRequiredInt32Async(_file);
        if (checksum != _expectedChecksum)
            throw new InvalidOperationException("The steady-state scan benchmark failed its truth check.");
    }

    [Benchmark(Description = "Lokad scan on an open file")]
    public async Task<long> Scan()
    {
        var checksum = await BenchmarkScan.ReadRequiredInt32Async(
            _file ?? throw new InvalidOperationException("The benchmark file is not open."));
        if (checksum != _expectedChecksum)
            throw new InvalidOperationException("The steady-state scan benchmark produced an invalid checksum.");
        return checksum;
    }

    [GlobalCleanup]
    public async Task Cleanup()
    {
        if (_file is not null)
            await _file.DisposeAsync();
        _stream?.Dispose();
    }
}

[MemoryDiagnoser]
public class SourceScanBenchmarks
{
    private byte[] _fixture = [];
    private string _temporaryPath = "";
    private long _expectedChecksum;

    [Params(262_144)]
    public int RowCount { get; set; }

    [GlobalSetup]
    public async Task Setup()
    {
        var fixture = await ScanFixture.CreateAsync(ScanWorkload.RequiredInt32Plain, RowCount);
        _fixture = fixture.Bytes;
        _expectedChecksum = fixture.Checksum;
        var repositoryRoot = Environment.GetEnvironmentVariable("LOKAD_PARQUET_REPOSITORY_ROOT") ??
            throw new InvalidOperationException("The benchmark repository root is unavailable.");
        var inputDirectory = Path.Combine(repositoryRoot, "artifacts", "benchmarks", "source-inputs");
        Directory.CreateDirectory(inputDirectory);
        _temporaryPath = Path.Combine(inputDirectory, $"lokad-parquet-benchmark-{Guid.NewGuid():N}.parquet");
        await File.WriteAllBytesAsync(_temporaryPath, _fixture);
        var memory = await BenchmarkScan.ReadMemoryAsync(_fixture);
        var stream = await BenchmarkScan.ReadStreamAsync(_fixture);
        var localFile = await BenchmarkScan.ReadFileAsync(_temporaryPath);
        if (memory != _expectedChecksum || stream != _expectedChecksum || localFile != _expectedChecksum)
            throw new InvalidOperationException("The source comparison benchmark failed its truth check.");
        Console.WriteLine("Source comparison uses direct memory, a MemoryStream wrapper, and a warmed local temporary file.");
    }

    [Benchmark(Baseline = true, Description = "In-memory open and scan")]
    public Task<long> Memory() => BenchmarkScan.ReadMemoryAsync(_fixture);

    [Benchmark(Description = "MemoryStream open and scan")]
    public Task<long> MemoryStream() => BenchmarkScan.ReadStreamAsync(_fixture);

    [Benchmark(Description = "Warmed local-file open and scan")]
    public Task<long> LocalFile() => BenchmarkScan.ReadFileAsync(_temporaryPath);

    [GlobalCleanup]
    public void Cleanup()
    {
        if (_temporaryPath.Length != 0 && File.Exists(_temporaryPath))
            File.Delete(_temporaryPath);
    }
}

internal static class BenchmarkScan
{
    public static async Task<long> ReadMemoryAsync(byte[] bytes)
    {
        await using var file = await ParquetFile.OpenAsync((ReadOnlyMemory<byte>)bytes);
        return await ReadRequiredInt32Async(file);
    }

    public static async Task<long> ReadFileAsync(string path)
    {
        await using var file = await ParquetFile.OpenAsync(path);
        return await ReadRequiredInt32Async(file);
    }

    public static async Task<long> ReadStreamAsync(byte[] bytes)
    {
        using var stream = new MemoryStream(bytes, writable: false);
        await using var file = await ParquetFile.OpenAsync(stream);
        return await ReadRequiredInt32Async(file);
    }

    public static async Task<long> ReadRequiredInt32Async(ParquetFile file)
    {
        long checksum = ScanChecksum.Seed;
        await foreach (var batch in file.ScanAsync(new([file.Metadata.Schema.Columns[0]])))
        {
            using (batch)
            {
                var values = ((ParquetPrimitiveColumnBatch<int>)batch.Columns[0]).Values.Span;
                checksum = ScanChecksum.ConsumeRequired(checksum, values);
            }
        }
        return checksum;
    }
}
