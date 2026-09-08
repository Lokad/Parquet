using System.Reflection;
using System.Runtime.InteropServices;
using System.Security.Cryptography;
using System.Text.Json;

namespace Lokad.Parquet.Benchmarks;

internal static class WorkCensusRunner
{
    private const int RowCount = 65_536;
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

    public static async Task<int> RunAsync()
    {
        var results = new List<WorkCensusCase>();
        foreach (var workload in ScanWorkloadCatalog.ParityWorkloads)
            results.Add(await MeasureAsync(workload));

        var snapshot = new WorkCensusSnapshot(
            2,
            DateTimeOffset.UtcNow,
            Environment.GetEnvironmentVariable("LOKAD_PARQUET_SOURCE_REVISION") ?? "unrecorded",
            RuntimeInformation.FrameworkDescription,
            RuntimeInformation.OSDescription,
            RuntimeInformation.ProcessArchitecture.ToString(),
            results);
        var platform = OperatingSystem.IsWindows() ? "windows" : "linux";
        var outputPath = Path.Combine(
            "artifacts",
            "benchmarks",
            $"work-census-{platform}-{DateTimeOffset.UtcNow:yyyyMMdd-HHmmss}.json");
        Directory.CreateDirectory(Path.GetDirectoryName(outputPath) ??
            throw new InvalidOperationException("The work-census output has no directory."));
        await using (var output = File.Create(outputPath))
        {
            await JsonSerializer.SerializeAsync(output, snapshot, new JsonSerializerOptions
            {
                PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
                WriteIndented = true,
            });
        }

        Console.WriteLine($"Work census: {Path.GetFullPath(outputPath)}");
        foreach (var result in results)
        {
            Console.WriteLine(
                $"{result.Name}: reads={result.SourceReadCalls}/{result.SourceBytesRead} B, " +
                $"pool={result.PoolRents} rents/{result.PeakPooledBytes} B peak/" +
                $"{result.PooledBytesCleared} B cleared, moves={result.SynchronousMoves}/" +
                $"{result.TotalMoves} synchronous.");
        }
        return 0;

        static async Task<WorkCensusCase> MeasureAsync(ScanWorkload workload)
        {
            var fixture = await ScanFixture.CreateAsync(workload, RowCount);
            using var stream = new CountingMemoryStream(fixture.Bytes);
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
                pool.OutstandingBytes -= capacityBytes;
                if (pool.OutstandingBytes < 0)
                    throw new InvalidOperationException("The work-census pool accounting is negative.");
            }));
            try
            {
                var publicBatchCount = 0;
                var synchronousMoves = 0;
                var totalMoves = 0;
                var logicalOutputBytes = 0L;
                var decodedBatches = 0;
                var consumerUtf8Bytes = 0L;

                // Pass one scans the full projection at the full-row target. Pass two
                // rotates to the second half of the columns at a small target, so
                // multi-batch slicing and projection changes share one truth-checked,
                // pool-balanced case. Cross-column page misalignment cannot come from
                // the single-page baseline writer; it is covered by the partitioning
                // tests against synthetic uneven fixtures.
                var allColumns = file.Metadata.Schema.Columns;
                var secondHalf = allColumns.Skip(allColumns.Count / 2).ToArray();
                var passes = new (IReadOnlyList<ParquetColumn> Columns, int Target, long ExpectedChecksum, long LogicalBytes)[]
                {
                    (allColumns, RowCount, fixture.Checksum, LogicalOutputBytes(allColumns.Count)),
                    (secondHalf, 4096, ExpectedChecksum(secondHalf), LogicalOutputBytes(secondHalf.Length)),
                };
                foreach (var pass in passes)
                {
                    decodedBatches = checked(decodedBatches + await RunPassAsync(pass.Columns, pass.Target, pass.ExpectedChecksum) * pass.Columns.Count);
                    logicalOutputBytes += pass.LogicalBytes;
                }
                // Retained storage still held by file-owned caches after the scans,
                // sampled separately from the zero-after-disposal check below.
                var endOfScanRetainedBytes = pool.OutstandingBytes;
                await file.DisposeAsync().ConfigureAwait(false);

                async Task<int> RunPassAsync(IReadOnlyList<ParquetColumn> projection, int target, long expectedChecksum)
                {
                    var options = new ParquetScanOptions(projection, null, null, target);
                    var checksum = ScanChecksum.Seed;
                    // Multi-column lanes accumulate one checksum per column so the
                    // result does not depend on how pages batch across columns.
                    var perColumn = projection.Count > 1 ? new long[projection.Count] : null;
                    if (perColumn is not null)
                        Array.Fill(perColumn, ScanChecksum.Seed);
                    var utf8Sink = ScanWorkloadCatalog.IsString(workload)
                        ? new Utf8ScanSink(RowCount, fixture.Utf8PayloadBytes)
                        : null;
                    var passBatches = 0;
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
                            publicBatchCount++;
                            passBatches++;
                            for (var columnIndex = 0; columnIndex < batch.Columns.Count; columnIndex++)
                            {
                                var untypedColumn = batch.Columns[columnIndex];
                                if (untypedColumn is ParquetBinaryColumnBatch strings)
                                {
                                    if (!strings.Validity.IsAllValid || utf8Sink is null)
                                        throw new InvalidOperationException("The required UTF-8 census column is invalid.");
                                    var offsets = strings.Offsets.Span;
                                    for (var row = 0; row < strings.RowCount; row++)
                                        utf8Sink.AppendUtf8(
                                            strings.Payload.Span[offsets[row]..offsets[row + 1]]);
                                }
                                else
                                {
                                    var column = (ParquetPrimitiveColumnBatch<int>)untypedColumn;
                                    var values = column.Values.Span;
                                    if (perColumn is not null)
                                    {
                                        if (!column.Validity.IsAllValid)
                                            throw new InvalidOperationException("A required multi-column census value decoded as null.");
                                        perColumn[columnIndex] = ScanChecksum.ConsumeRequired(perColumn[columnIndex], values);
                                    }
                                    else if (column.Validity.IsAllValid)
                                    {
                                        foreach (var value in values)
                                            checksum = ScanChecksum.Mix(checksum, value);
                                    }
                                    else
                                    {
                                        var bits = column.Validity.Bits.Span;
                                        for (var row = 0; row < values.Length; row++)
                                        {
                                            var value = (bits[row >> 3] & (1 << (row & 7))) != 0
                                                ? values[row]
                                                : ScanChecksum.NullMarker;
                                            checksum = ScanChecksum.Mix(checksum, value);
                                        }
                                    }
                                }
                            }
                        }
                    }
                    finally
                    {
                        await enumerator.DisposeAsync().ConfigureAwait(false);
                    }

                    if (utf8Sink is not null)
                    {
                        checksum = utf8Sink.Complete(RowCount, fixture.Utf8PayloadBytes);
                        consumerUtf8Bytes += fixture.Utf8PayloadBytes;
                    }
                    else if (perColumn is not null)
                    {
                        checksum = ScanChecksum.CombineColumns(perColumn);
                    }

                    if (checksum != expectedChecksum)
                        throw new InvalidOperationException($"The {workload} work-census truth check failed.");
                    return passBatches;
                }

                long ExpectedChecksum(IReadOnlyList<ParquetColumn> projection)
                {
                    if (projection.Count == allColumns.Count)
                        return fixture.Checksum;
                    return ScanChecksum.CombineColumns(projection.Select(column => fixture.ColumnChecksums[column.Ordinal]).ToArray());
                }

                long LogicalOutputBytes(int columnCount) => ScanWorkloadCatalog.IsString(workload)
                    ? checked((long)fixture.Utf8PayloadBytes + ((long)RowCount + 1) * sizeof(int))
                    : checked((long)RowCount * columnCount * sizeof(int));
                if (pool.OutstandingBytes != 0 || pool.RentCount != pool.ReturnCount)
                    throw new InvalidOperationException($"The {workload} work-census pool accounting is unbalanced.");

                return new WorkCensusCase(
                    workload.ToString(),
                    Convert.ToHexStringLower(SHA256.HashData(fixture.Bytes)),
                    fixture.Bytes.Length,
                    RowCount,
                    fixture.ColumnCount,
                    fixture.Utf8PayloadBytes,
                    stream.ReadCount,
                    stream.BytesRead,
                    stream.MaximumConcurrentReads,
                    publicBatchCount,
                    decodedBatches,
                    totalMoves,
                    synchronousMoves,
                    pool.RentCount,
                    pool.ReturnCount,
                    pool.RequestedBytes,
                    pool.RentedCapacityBytes,
                    pool.PeakBytes,
                    pool.ReturnedCapacityBytes,
                    logicalOutputBytes,
                    stream.BytesRead,
                    endOfScanRetainedBytes,
                    consumerUtf8Bytes,
                    pool.ReturnedCapacityBytes,
                    pool.OutstandingBytes);
            }
            finally
            {
                PoolRentObserverProperty.SetValue(null, previousRentObserver);
                PoolReturnObserverProperty.SetValue(null, previousReturnObserver);
            }
        }
    }

    private sealed class CountingMemoryStream : MemoryStream
    {
        private int _activeReads;

        public CountingMemoryStream(byte[] bytes) : base(bytes, writable: false) { }

        public int ReadCount { get; private set; }
        public long BytesRead { get; private set; }
        public int MaximumConcurrentReads { get; private set; }

        public override int Read(Span<byte> buffer)
        {
            var active = Interlocked.Increment(ref _activeReads);
            MaximumConcurrentReads = Math.Max(MaximumConcurrentReads, active);
            try
            {
                var read = base.Read(buffer);
                ReadCount++;
                BytesRead += read;
                return read;
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
            MaximumConcurrentReads = 0;
        }
    }

    private sealed class PoolCensus
    {
        public int RentCount { get; set; }
        public int ReturnCount { get; set; }
        public long RequestedBytes { get; set; }
        public long RentedCapacityBytes { get; set; }
        public long ReturnedCapacityBytes { get; set; }
        public long OutstandingBytes { get; set; }
        public long PeakBytes { get; set; }
    }
}

/// <summary>Measured per-scan work for one benchmark workload.</summary>
/// <param name="DecodedColumnBatches">Derived estimate of internal batches (public batches times projected columns); projected or realigned scans consume fewer, larger source batches.</param>
/// <param name="EndOfScanRetainedPoolBytes">Pooled bytes still retained by file-owned caches after the scans, before file disposal.</param>
/// <param name="RetainedPoolBytes">Pooled bytes outstanding after file disposal; always zero.</param>
internal sealed record WorkCensusCase(
    string Name,
    string FixtureHash,
    int FixtureBytes,
    int RowCount,
    int ColumnCount,
    int Utf8PayloadBytes,
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
    long RetainedPoolBytes);

internal sealed record WorkCensusSnapshot(
    int SchemaVersion,
    DateTimeOffset RecordedAtUtc,
    string SourceRevision,
    string Runtime,
    string OperatingSystem,
    string Architecture,
    IReadOnlyList<WorkCensusCase> Cases);
