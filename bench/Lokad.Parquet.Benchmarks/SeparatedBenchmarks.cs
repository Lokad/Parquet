using System.Reflection;
using BenchmarkDotNet.Attributes;
using BaselineParquetReader = Parquet.ParquetReader;

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
        BenchmarkHostPolicy.AssertWorkerEnvironment();
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
        BenchmarkHostPolicy.AssertWorkerEnvironment();
        var fixture = await ScanFixture.CreateAsync(ScanWorkload.RequiredInt32Plain, RowCount);
        _expectedChecksum = fixture.Checksum;
        _stream = new MemoryStream(fixture.Bytes, writable: false);
        _file = await ParquetFile.OpenAsync(_stream);
        var checksum = await BenchmarkScan.ReadRequiredInt32Async(_file);
        if (checksum != _expectedChecksum)
            throw new InvalidOperationException("The steady-state scan benchmark failed its truth check.");
    }

    [Benchmark(Description = "Lokad scan on an open file (consumer side; open excluded)")]
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
        BenchmarkHostPolicy.AssertWorkerEnvironment();
        var fixture = await ScanFixture.CreateAsync(ScanWorkload.RequiredInt32Plain, RowCount);
        _fixture = fixture.Bytes;
        _expectedChecksum = fixture.Checksum;
        var repositoryRoot = Environment.GetEnvironmentVariable("LOKAD_PARQUET_REPOSITORY_ROOT") ??
            throw new InvalidOperationException("The benchmark repository root is unavailable.");
        var inputDirectory = Path.Combine(repositoryRoot, "artifacts", "benchmarks", "source-inputs");
        Directory.CreateDirectory(inputDirectory);
        _temporaryPath = Path.Combine(inputDirectory, $"lokad-parquet-benchmark-{Guid.NewGuid():N}.parquet");
        await File.WriteAllBytesAsync(_temporaryPath, _fixture);
        var custom = await BenchmarkScan.ReadCustomSourceAsync(_fixture);
        var memory = await BenchmarkScan.ReadMemoryAsync(_fixture);
        var stream = await BenchmarkScan.ReadStreamAsync(_fixture);
        var localFile = await BenchmarkScan.ReadFileAsync(_temporaryPath);
        Console.WriteLine("Source comparison uses direct memory, a MemoryStream wrapper, a custom random-access source, and a warmed local temporary file.");
        Console.WriteLine("Source checksums: memory=" + memory + "; stream=" + stream + "; custom=" + custom + "; file=" + localFile + "; expected=" + _expectedChecksum + ".");
        if (memory != _expectedChecksum || stream != _expectedChecksum || localFile != _expectedChecksum || custom != _expectedChecksum)
            throw new InvalidOperationException("The source comparison benchmark failed its truth check.");
    }

    [Benchmark(Baseline = true, Description = "In-memory open and scan")]
    public Task<long> Memory() => BenchmarkScan.ReadMemoryAsync(_fixture);

    [Benchmark(Description = "MemoryStream open and scan")]
    public Task<long> MemoryStream() => BenchmarkScan.ReadStreamAsync(_fixture);

    [Benchmark(Description = "Warmed local-file open and scan")]
    public Task<long> LocalFile() => BenchmarkScan.ReadFileAsync(_temporaryPath);

    [Benchmark(Description = "Custom random-access source open and scan")]
    public Task<long> CustomSource() => BenchmarkScan.ReadCustomSourceAsync(_fixture);

    [Benchmark(Description = "Baseline open and scan from a warmed local file")]
    public Task<long> BaselineFile() => BenchmarkScan.ReadBaselineFileAsync(_temporaryPath, _expectedChecksum);

    [Benchmark(Description = "Baseline open and scan from a memory stream")]
    public Task<long> BaselineStream() => BenchmarkScan.ReadBaselineStreamAsync(_fixture, _expectedChecksum);

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

    public static async Task<long> ReadCustomSourceAsync(byte[] bytes)
    {
        await using var source = new MemoryRandomAccessSource(bytes);
        await using var file = await ParquetFile.OpenAsync(source);
        return await ReadRequiredInt32Async(file);
    }

    // Competitor counterparts to the Lokad file/stream lanes above, so source
    // comparisons never rest on a single implementation. The pinned baseline
    // reads whole row groups into reusable destinations; checksums fold the
    // same required values as the Lokad source consumer.
    public static async Task<long> ReadBaselineFileAsync(string path, long expectedChecksum)
    {
        await using var stream = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read, 4096, FileOptions.Asynchronous | FileOptions.SequentialScan);
        return await ReadBaselineStreamAsync(stream, expectedChecksum);
    }

    public static async Task<long> ReadBaselineStreamAsync(byte[] bytes, long expectedChecksum)
    {
        using var stream = new MemoryStream(bytes, writable: false);
        return await ReadBaselineStreamAsync(stream, expectedChecksum);
    }

    private static async Task<long> ReadBaselineStreamAsync(Stream stream, long expectedChecksum)
    {
        await using var reader = await BaselineParquetReader.CreateAsync(stream);
        long checksum = ScanChecksum.Seed;
        for (var groupOrdinal = 0; groupOrdinal < reader.RowGroupCount; groupOrdinal++)
        {
            using var group = reader.OpenRowGroupReader(groupOrdinal);
            var fields = reader.Schema.DataFields.ToArray();
            var values = new int[checked((int)group.RowCount)];
            await group.ReadAsync<int>(fields[0], values);
            checksum = ScanChecksum.ConsumeRequired(checksum, values);
        }

        checksum = ScanChecksum.CombineColumn(checksum, ScanChecksum.Seed);
        if (checksum != expectedChecksum)
            throw new InvalidOperationException("The baseline source benchmark produced an invalid checksum.");
        return checksum;
    }

    // Bench-side custom random-access source: synchronous segment copies over
    // borrowed fixture memory. The source-I/O endpoint distinguishes custom
    // sources from non-exposable streams and files by construction.
    public sealed class MemoryRandomAccessSource : IParquetRandomAccessSource
    {
        private readonly byte[] _bytes;

        public MemoryRandomAccessSource(byte[] bytes) => _bytes = bytes;

        public long Length => _bytes.Length;

        public ValueTask ReadExactlyAsync(long offset, ArraySegment<byte> destination, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (offset < 0 || offset > _bytes.Length - destination.Count)
                throw new ArgumentOutOfRangeException(nameof(offset));
            _bytes.AsSpan((int)offset, destination.Count).CopyTo(destination.AsSpan());
            return ValueTask.CompletedTask;
        }

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    // Canonical single-column fold: every consumer returns the value chain folded
    // with its (here all-valid) null chain, so open-file, source, and steady-state
    // scans compare against the same fixture checksum as the Core consumer.
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
        return ScanChecksum.CombineColumn(checksum, ScanChecksum.Seed);
    }
}
