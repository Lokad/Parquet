using System.Security.Cryptography;
using System.Reflection;
using System.Diagnostics;
using BenchmarkDotNet.Attributes;
using BaselineParquetReader = Parquet.ParquetReader;

namespace Lokad.Parquet.Benchmarks;

[MemoryDiagnoser]
public class RequiredInt32Benchmarks
{
    private byte[] _fixture = [];
    private long _expectedChecksum;

    [Params(262_144)]
    public int RowCount { get; set; }

    [GlobalSetup]
    public async Task Setup()
    {
        BenchmarkHostPolicy.AssertWorkerEnvironment();
        async Task ValidateMetadataAsync()
        {
            using var lokadStream = new MemoryStream(_fixture, writable: false);
            await using var lokad = await ParquetFile.OpenAsync(lokadStream);
            using var baselineStream = new MemoryStream(_fixture, writable: false);
            await using var baseline = await BaselineParquetReader.CreateAsync(baselineStream);
            using var baselineRowGroup = baseline.OpenRowGroupReader(0);

            var column = lokad.Metadata.Schema.Columns.Single();
            if (lokad.Metadata.RowCount != RowCount || baseline.Metadata?.NumRows != RowCount ||
                lokad.Metadata.RowGroups.Count != baseline.RowGroupCount || baseline.RowGroupCount != 1 ||
                lokad.Metadata.RowGroups[0].RowCount != baselineRowGroup.RowCount ||
                column.SchemaElement.PhysicalType != ParquetPhysicalType.Int32 || baseline.Schema.DataFields.Count() != 1 ||
                !string.Equals(column.Name, baseline.Schema.DataFields[0].Name, StringComparison.Ordinal))
                throw new InvalidOperationException("Independent footer metadata comparison failed.");
        }

        var fixture = await ScanFixture.CreateAsync(ScanWorkload.RequiredInt32Plain, RowCount);
        _fixture = fixture.Bytes;
        _expectedChecksum = fixture.Checksum;
        await ValidateMetadataAsync();
        var lokad = await ReadLokadAsync();
        var baseline = await ReadParquetNetAsync();
        if (lokad != _expectedChecksum || baseline != _expectedChecksum)
            throw new InvalidOperationException($"Benchmark truth mismatch: expected {_expectedChecksum}, Lokad {lokad}, Parquet.NET {baseline}.");

        var peakPooledBytes = await PeakPoolMeasurement.MeasureAsync(ReadLokadAsync);
        var retained = await RetainedMemoryMeasurement.MeasureAsync(ReadLokadAsync, 16);

        Console.WriteLine($"Fixture SHA-256: {Convert.ToHexStringLower(SHA256.HashData(_fixture))}");
        Console.WriteLine($"Fixture bytes: {_fixture.Length}; rows: {RowCount}; physical type: INT32; required; PLAIN; uncompressed.");
        Console.WriteLine($"Lokad peak pooled bytes: {peakPooledBytes}.");
        Console.WriteLine(
            $"Lokad retained bytes after 16 warmed scans: managed={retained.ManagedBytes}; " +
            $"process-private={retained.ProcessPrivateBytes}.");
    }

    [Benchmark(Baseline = true, Description = "Lokad projected INT32 scan")]
    public Task<long> LokadProjectedScan() => ReadLokadAsync();

    [Benchmark(Description = "Parquet.NET projected INT32 scan")]
    public Task<long> ParquetNetProjectedScan() => ReadParquetNetAsync();

    private async Task<long> ReadLokadAsync()
    {
        using var stream = new MemoryStream(_fixture, writable: false);
        await using var file = await ParquetFile.OpenAsync(stream);
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

    private async Task<long> ReadParquetNetAsync()
    {
        using var stream = new MemoryStream(_fixture, writable: false);
        await using var reader = await BaselineParquetReader.CreateAsync(stream);
        var field = reader.Schema.DataFields[0];
        long checksum = ScanChecksum.Seed;
        for (var rowGroupOrdinal = 0; rowGroupOrdinal < reader.RowGroupCount; rowGroupOrdinal++)
        {
            using var rowGroup = reader.OpenRowGroupReader(rowGroupOrdinal);
            var values = new int[checked((int)rowGroup.RowCount)];
            await rowGroup.ReadAsync<int>(field, values);
            checksum = ScanChecksum.ConsumeRequired(checksum, values);
        }
        return ScanChecksum.CombineColumn(checksum, ScanChecksum.Seed);
    }
    // Public-accessor diagnostic: the retired per-row Validity/Span consumer on
    // the required lane. It prices the public accessor path against the bulk
    // engine consumer above and never feeds a parity claim.
    [Benchmark(Description = "Lokad public-accessor diagnostic (per-row Validity/Span; not a parity endpoint)")]
    public async Task<long> LokadPublicAccessorDiagnostic()
    {
        using var stream = new MemoryStream(_fixture, writable: false);
        await using var file = await ParquetFile.OpenAsync(stream);
        long valueChain = ScanChecksum.Seed;
        await foreach (var batch in file.ScanAsync(new([file.Metadata.Schema.Columns[0]])))
        {
            using (batch)
            {
                var integers = (ParquetPrimitiveColumnBatch<int>)batch.Columns[0];
                for (var row = 0; row < integers.RowCount; row++)
                {
                    if (integers.Validity.IsValid(row))
                        valueChain = ScanChecksum.Mix(valueChain, integers.Values.Span[row]);
                    else
                        throw new InvalidOperationException("The required diagnostic lane decoded a null.");
                }
            }
        }
        return ScanChecksum.CombineColumn(valueChain, ScanChecksum.Seed);
    }
}


internal static class PeakPoolMeasurement
{
    private static readonly Type PoolType =
        typeof(ParquetFile).Assembly
            .GetType("Lokad.Parquet.Internal.ParquetArrayPool", throwOnError: true, ignoreCase: false)
        ?? throw new InvalidOperationException("The internal pool facade is unavailable.");
    private static readonly PropertyInfo RentObserverProperty =
        PoolType.GetProperty("RentObserver", BindingFlags.Public | BindingFlags.Static)
        ?? throw new InvalidOperationException("The internal pool rent observer is unavailable.");
    private static readonly PropertyInfo ReturnObserverProperty =
        PoolType.GetProperty("ReturnObserver", BindingFlags.Public | BindingFlags.Static)
        ?? throw new InvalidOperationException("The internal pool return observer is unavailable.");

    public static async Task<long> MeasureAsync(Func<Task<long>> operation)
    {
        long retainedBytes = 0;
        long peakBytes = 0;
        void ObserveRent(Array array, int _)
        {
            var bytes = Buffer.ByteLength(array);
            retainedBytes += bytes;
            peakBytes = Math.Max(peakBytes, retainedBytes);
        }

        void ObserveReturn(Array array, int _)
        {
            var bytes = Buffer.ByteLength(array);
            retainedBytes -= bytes;
            if (retainedBytes < 0)
                throw new InvalidOperationException("The benchmark pool measurement is unbalanced.");
        }

        var previousRentObserver = RentObserverProperty.GetValue(null);
        var previousReturnObserver = ReturnObserverProperty.GetValue(null);
        RentObserverProperty.SetValue(null, (Action<Array, int>)ObserveRent);
        ReturnObserverProperty.SetValue(null, (Action<Array, int>)ObserveReturn);
        try
        {
            await operation();
            if (retainedBytes != 0)
                throw new InvalidOperationException("The benchmark operation retained a pooled array.");
            return peakBytes;
        }
        finally
        {
            RentObserverProperty.SetValue(null, previousRentObserver);
            ReturnObserverProperty.SetValue(null, previousReturnObserver);
        }
    }
}

internal static class RetainedMemoryMeasurement
{
    public static async Task<RetainedMemoryResult> MeasureAsync(Func<Task<long>> operation, int repetitions)
    {
        if (repetitions <= 0)
            throw new ArgumentOutOfRangeException(nameof(repetitions));

        await operation();
        var managedBefore = CollectAndMeasureManagedBytes();
        using var process = Process.GetCurrentProcess();
        process.Refresh();
        var privateBefore = process.PrivateMemorySize64;
        for (var repetition = 0; repetition < repetitions; repetition++)
            await operation();
        var managedAfter = CollectAndMeasureManagedBytes();
        process.Refresh();
        var privateAfter = process.PrivateMemorySize64;
        return new RetainedMemoryResult(managedAfter - managedBefore, privateAfter - privateBefore);

        static long CollectAndMeasureManagedBytes()
        {
            GC.Collect(2, GCCollectionMode.Forced, blocking: true, compacting: true);
            GC.WaitForPendingFinalizers();
            GC.Collect(2, GCCollectionMode.Forced, blocking: true, compacting: true);
            return GC.GetTotalMemory(forceFullCollection: false);
        }
    }
}

internal readonly record struct RetainedMemoryResult(long ManagedBytes, long ProcessPrivateBytes);
[MemoryDiagnoser]
public class MetadataOpenBenchmarks
{
    private byte[] _fixture = [];

    [GlobalSetup(Target = nameof(LokadOpen))]
    public async Task SetupLokad()
    {
        BenchmarkHostPolicy.AssertWorkerEnvironment();
        await PrepareFixtureAsync();
        using var stream = new MemoryStream(_fixture, writable: false);
        await using var reference = await BaselineParquetReader.CreateAsync(stream);
        if (reference.Metadata?.NumRows != 262_144)
            throw new InvalidOperationException("The metadata benchmark reference truth check failed.");
        Console.WriteLine($"Fixture SHA-256: {Convert.ToHexStringLower(SHA256.HashData(_fixture))}");
    }

    [GlobalSetup(Target = nameof(ParquetNetOpen))]
    public async Task SetupParquetNet()
    {
        BenchmarkHostPolicy.AssertWorkerEnvironment();
        await PrepareFixtureAsync();
        using var stream = new MemoryStream(_fixture, writable: false);
        await using var reference = await ParquetFile.OpenAsync(stream);
        if (reference.Metadata.RowCount != 262_144)
            throw new InvalidOperationException("The metadata benchmark reference truth check failed.");
        Console.WriteLine($"Fixture SHA-256: {Convert.ToHexStringLower(SHA256.HashData(_fixture))}");
    }

    [Benchmark(Baseline = true, Description = "Lokad metadata open")]
    public async Task<long> LokadOpen()
    {
        using var stream = new MemoryStream(_fixture, writable: false);
        await using var file = await ParquetFile.OpenAsync(stream);
        return file.Metadata.RowCount;
    }

    [Benchmark(Description = "Parquet.NET metadata open")]
    public async Task<long> ParquetNetOpen()
    {
        using var stream = new MemoryStream(_fixture, writable: false);
        await using var reader = await BaselineParquetReader.CreateAsync(stream);
        return reader.Metadata?.NumRows ?? 0;
    }

    private async Task PrepareFixtureAsync() =>
        _fixture = (await ScanFixture.CreateAsync(ScanWorkload.RequiredInt32Plain, 262_144)).Bytes;
}
