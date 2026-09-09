using System.Reflection;
using System.Runtime.InteropServices;
using System.Security.Cryptography;
using System.Text.Json;

namespace Lokad.Parquet.Benchmarks;

internal static class WorkCensusRunner
{
    private const int RowCount = 65_536;
    internal const int SnapshotSchemaVersion = 3;
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
        {
            // Uneven multi-row-group case: the catalog writer emits one page per
            // column chunk, so uneven batch partitioning is covered here with a
            // fully known oracle instead.
            const int firstGroupRows = 2048;
            const int secondGroupRows = 6144;
            var (unevenBytes, unevenFolded, unevenExpected) = await ScanTruthVerification.WriteUnevenTwoColumnFixtureAsync(firstGroupRows, secondGroupRows);
            results.Add(await MeasureCustomAsync(
                "UnevenInt32Plain",
                unevenBytes,
                firstGroupRows + secondGroupRows,
                2,
                0,
                unevenExpected,
                unevenFolded,
                [0, 0],
                ScanWorkload.TwoRequiredInt32Plain));
        }

        var platform = OperatingSystem.IsWindows() ? "windows" : "linux";
        var outputPath = Path.Combine(
            "artifacts",
            "benchmarks",
            "work-census-" + platform + "-" + DateTimeOffset.UtcNow.ToString("yyyyMMdd-HHmmss") + ".json");
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
            results);
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
            return await MeasureCustomAsync(
                workload.ToString(),
                fixture.Bytes,
                RowCount,
                fixture.ColumnCount,
                fixture.Utf8PayloadBytes,
                fixture.Checksum,
                fixture.ColumnChecksums,
                fixture.NullCounts,
                workload);
        }

        static async Task<WorkCensusCase> MeasureCustomAsync(
            string name,
            byte[] fixtureBytes,
            int rowCount,
            int columnCount,
            int utf8PayloadBytes,
            long checksum,
            long[] columnHashes,
            int[] nullCounts,
            ScanWorkload workload)
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
            try
            {

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
                    (allColumns, rowCount, checksum, LogicalOutputBytes(allColumns.Count)),
                    (secondHalf, 4096, CensusExpectedChecksum(checksum, columnHashes, allColumns.Count, secondHalf), LogicalOutputBytes(secondHalf.Length)),
                };
                foreach (var pass in passes)
                {
                    // File-owned caches carry retained buffers across passes without
                    // touching the shared pool, so a pass that reuses them would record
                    // a zero peak. The carried outstanding bytes are the storage the
                    // pass actually occupies, and therefore the honest peak floor.
                    var carryBytes = pool.OutstandingBytes;
                    pool.PeakBytes = 0;
                    var outcome = await RunCensusPassAsync(
                        file, workload, rowCount, utf8PayloadBytes, pass.Columns, pass.Target,
                        pass.ExpectedChecksum, columnHashes, nullCounts);
                    publicBatchCount += outcome.PassBatches;
                    totalMoves += outcome.TotalMoves;
                    synchronousMoves += outcome.SynchronousMoves;
                    consumerUtf8Bytes += outcome.ConsumerUtf8Bytes;
                    decodedBatches = checked(decodedBatches + outcome.PassBatches * pass.Columns.Count);
                    logicalOutputBytes += pass.LogicalBytes;
                    var passPeakBytes = Math.Max(pool.PeakBytes, carryBytes);
                    peakPooledBytes = Math.Max(peakPooledBytes, passPeakBytes);
                    passPeaks.Add(new CensusPassMeasurement(passPeakBytes, pass.LogicalBytes));
                }
                // Retained storage still held by file-owned caches after the scans,
                // sampled separately from the zero-after-disposal check below.
                endOfScanRetainedBytes = pool.OutstandingBytes;
                await file.DisposeAsync().ConfigureAwait(false);


                // Decoded layout counts value bytes plus validity bitmap bytes where the
                // lane can produce nulls; required lanes carry implicit validity only.
                long LogicalOutputBytes(int columnCount) => ScanWorkloadCatalog.IsString(workload)
                    ? checked((long)utf8PayloadBytes + ((long)rowCount + 1) * sizeof(int))
                    : checked((long)rowCount * columnCount * sizeof(int)) +
                        (workload == ScanWorkload.NullableInt32Plain ? checked((long)columnCount * ((rowCount + 7) / 8)) : 0);
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
            // Retained-memory probes run after the pool observers are restored so
            // their rents never pollute the pass accounting above. Both readers scan
            // the same fixture bytes with the same yardstick, so the snapshot compares
            // Lokad pool retention against the competitor reusable buffers.
            async Task<long> ScanLokadOnceAsync()
            {
                var valueChains = new long[columnCount];
                var nullChains = new long[columnCount];
                Utf8ScanSink? sink = ScanWorkloadCatalog.IsString(workload)
                    ? new Utf8ScanSink(rowCount, utf8PayloadBytes)
                    : null;
                return await CoreScanBenchmarks.ReadLokadAsync(
                    fixtureBytes, null, null, sink, rowCount, utf8PayloadBytes, valueChains, nullChains);
            }

            async Task<long> ScanBaselineOnceAsync()
            {
                var valueChains = new long[columnCount];
                var nullChains = new long[columnCount];
                Utf8ScanSink? sink = ScanWorkloadCatalog.IsString(workload)
                    ? new Utf8ScanSink(rowCount, utf8PayloadBytes)
                    : null;
                return await CoreScanBenchmarks.ReadParquetNetAsync(
                    fixtureBytes, workload, null, sink, rowCount, utf8PayloadBytes, valueChains, nullChains);
            }

            var lokadRetained = await RetainedMemoryMeasurement.MeasureAsync(ScanLokadOnceAsync, 16);
            var baselineRetained = await RetainedMemoryMeasurement.MeasureAsync(ScanBaselineOnceAsync, 16);
            return new WorkCensusCase(
                    name,
                    Convert.ToHexStringLower(SHA256.HashData(fixtureBytes)),
                    fixtureBytes.Length,
                    rowCount,
                    columnCount,
                    utf8PayloadBytes,
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
                    lokadRetained.ManagedBytes,
                    lokadRetained.ProcessPrivateBytes,
                    baselineRetained.ManagedBytes,
                    baselineRetained.ProcessPrivateBytes);
        }
    }

    internal sealed record CensusPassOutcome(int PassBatches, int TotalMoves, int SynchronousMoves, long ConsumerUtf8Bytes);

    // Shared Census consumer: the work census and the truth verification run this
    // same pass. Besides the checksum it asserts row counts, per-column folded
    // hashes, and null counts against the caller-supplied expectations.
    internal static async Task<CensusPassOutcome> RunCensusPassAsync(
        ParquetFile file,
        ScanWorkload workload,
        int rowCount,
        int utf8PayloadBytes,
        IReadOnlyList<ParquetColumn> projection,
        int target,
        long expectedChecksum,
        long[] expectedColumnHashes,
        int[] expectedNullCounts)
    {
        var options = new ParquetScanOptions(projection, null, null, target);
        var valueChains = new long[projection.Count];
        var nullChains = new long[projection.Count];
        var nullCounts = new int[projection.Count];
        var consumed = new int[projection.Count];
        Array.Fill(valueChains, ScanChecksum.Seed);
        Array.Fill(nullChains, ScanChecksum.Seed);
        var utf8Sink = ScanWorkloadCatalog.IsString(workload)
            ? new Utf8ScanSink(rowCount, utf8PayloadBytes)
            : null;
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
            checksum = utf8Sink.Complete(rowCount, utf8PayloadBytes);
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
        if (rows != rowCount)
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
        return new CensusPassOutcome(passBatches, totalMoves, synchronousMoves, consumerUtf8Bytes);
    }

    internal static long CensusExpectedChecksum(
        long fullChecksum,
        long[] columnHashes,
        int allColumnCount,
        IReadOnlyList<ParquetColumn> projection)
    {
        if (projection.Count == allColumnCount)
            return fullChecksum;
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
/// <param name="LokadRetainedManagedBytes">Measured managed-heap growth after warmed Lokad scans with the same yardstick as the baseline pair.</param>
/// <param name="LokadRetainedProcessPrivateBytes">Measured process-private growth after warmed Lokad scans.</param>
/// <param name="BaselineRetainedManagedBytes">Measured managed-heap growth after warmed competitor scans: the reusable-buffer retention to compare against.</param>
/// <param name="BaselineRetainedProcessPrivateBytes">Measured process-private growth after warmed competitor scans.</param>
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
    long RetainedPoolBytes,
    IReadOnlyList<CensusPassMeasurement> PassPeaks,
    long LokadRetainedManagedBytes,
    long LokadRetainedProcessPrivateBytes,
    long BaselineRetainedManagedBytes,
    long BaselineRetainedProcessPrivateBytes);

/// <summary>Measured peak pooled bytes with the decoded-layout size of one census pass.</summary>
/// <param name="PeakPooledBytes">Measured maximum outstanding pooled bytes during the pass.</param>
/// <param name="LogicalOutputBytes">Derived decoded-layout size of the pass.</param>
internal sealed record CensusPassMeasurement(long PeakPooledBytes, long LogicalOutputBytes);

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
    IReadOnlyList<WorkCensusCase> Cases);

