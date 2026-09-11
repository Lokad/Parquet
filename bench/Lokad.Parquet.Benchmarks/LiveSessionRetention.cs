using System.Diagnostics;
using BaselineDataField = Parquet.Schema.DataField;
using BaselineParquetReader = Parquet.ParquetReader;

namespace Lokad.Parquet.Benchmarks;

/// <summary>Heap retained by one warmed live equivalent session.</summary>
/// <param name="LiveOwnedBytes">Managed-heap growth with the pre-opened reader, reused destinations and sink held alive through the observation.</param>
/// <param name="LiveOwnedProcessPrivateBytes">Process-private growth with the session held alive.</param>
/// <param name="PostDisposalBytes">Managed-heap growth after tearing the session down.</param>
/// <param name="PostDisposalProcessPrivateBytes">Process-private growth after tearing the session down.</param>
/// <param name="Checksum">The truth checksum verified on every repetition.</param>
public sealed record LiveSessionMeasurement(long LiveOwnedBytes, long LiveOwnedProcessPrivateBytes, long PostDisposalBytes, long PostDisposalProcessPrivateBytes, long Checksum);

/// <summary>Retention probes over live equivalent sessions.</summary>
/// <remarks>Each probe holds one pre-opened reader with reused destinations and
/// sink alive through the observation window and verifies the truth checksum on
/// every repetition, so both sides scan identical emitted rows, columns and
/// ranges. Lokad batching follows the census target; the baseline reads whole
/// row groups and checksums the same emitted rows while holding group-sized
/// reusable destinations, which the snapshot records honestly as its owned
/// storage.</remarks>
public static class LiveSessionRetention
{
    /// <summary>Measures retained storage of a live Lokad session.</summary>
    /// <param name="fixtureBytes">The fixture bytes.</param>
    /// <param name="projectionOrdinals">The projected column ordinals.</param>
    /// <param name="targetRowCount">The preferred output rows per batch.</param>
    /// <param name="rowRangeStart">The first selected row, or null for a full scan.</param>
    /// <param name="rowRangeCount">The selected row count, or null for a full scan.</param>
    /// <param name="emittedRowCount">The number of emitted rows.</param>
    /// <param name="utf8PayloadBytes">The UTF-8 payload bytes.</param>
    /// <param name="workload">The workload token selecting the consumer.</param>
    /// <param name="expectedChecksum">The truth checksum verified on every repetition.</param>
    /// <param name="expectedColumnHashes">The per-column truth hashes indexed by ordinal.</param>
    /// <param name="expectedNullCounts">The per-column truth null counts indexed by ordinal.</param>
    /// <param name="repetitions">The number of truth-checked scans.</param>
    /// <param name="caseName">The census case naming probe diagnostics.</param>
    /// <returns>Live owned storage, post-disposal growth and the verified checksum.</returns>
    public static async Task<LiveSessionMeasurement> MeasureLokadAsync(
        byte[] fixtureBytes,
        int[] projectionOrdinals,
        int targetRowCount,
        long? rowRangeStart,
        long? rowRangeCount,
        int emittedRowCount,
        int utf8PayloadBytes,
        ScanWorkload workload,
        long expectedChecksum,
        long[] expectedColumnHashes,
        int[] expectedNullCounts,
        int repetitions,
        string caseName)
    {
        ArgumentNullException.ThrowIfNull(fixtureBytes);
        ArgumentNullException.ThrowIfNull(projectionOrdinals);
        ArgumentNullException.ThrowIfNull(expectedColumnHashes);
        ArgumentNullException.ThrowIfNull(expectedNullCounts);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(repetitions);
        ArgumentException.ThrowIfNullOrWhiteSpace(caseName);
        if ((rowRangeStart.HasValue || rowRangeCount.HasValue) && (!rowRangeStart.HasValue || !rowRangeCount.HasValue))
            throw new ArgumentException("A live-session range needs both its start and its count.", nameof(rowRangeCount));
        var heapBefore = CollectHeapBytes();
        using var process = Process.GetCurrentProcess();
        process.Refresh();
        var privateBefore = process.PrivateMemorySize64;
        var stream = new MemoryStream(fixtureBytes, writable: false);
        var file = await ParquetFile.OpenAsync(stream);
        try
        {
            var columns = projectionOrdinals.Select(ordinal => file.Metadata.Schema.Columns[ordinal]).ToArray();
            ParquetRowRange? rowRange = rowRangeStart.HasValue && rowRangeCount.HasValue
                ? new ParquetRowRange(rowRangeStart.Value, rowRangeCount.Value)
                : null;
            var valueChains = new long[columns.Length];
            var nullChains = new long[columns.Length];
            Utf8ScanSink? sink = ScanWorkloadCatalog.IsString(workload)
                ? new Utf8ScanSink(emittedRowCount, utf8PayloadBytes)
                : null;
            var checksum = ScanChecksum.Seed;
            for (var repetition = 0; repetition < repetitions; repetition++)
            {
                try
                {
                    var outcome = await WorkCensusRunner.RunCensusPassAsync(
                        file, workload, emittedRowCount, utf8PayloadBytes, columns, targetRowCount,
                        expectedChecksum, expectedColumnHashes, expectedNullCounts, rowRange,
                        valueChains, nullChains, sink);
                    checksum = outcome.Checksum;
                }
                catch (InvalidOperationException exception)
                {
                    throw new InvalidOperationException($"The Lokad live-session probe failed for {workload} in census case {caseName}.", exception);
                }
            }

            var heapLive = CollectHeapBytes();
            process.Refresh();
            var privateLive = process.PrivateMemorySize64;
            GC.KeepAlive(file);
            GC.KeepAlive(stream);
            GC.KeepAlive(valueChains);
            GC.KeepAlive(nullChains);
            if (sink is not null)
                GC.KeepAlive(sink);
            var liveOwnedBytes = heapLive - heapBefore;
            await file.DisposeAsync().ConfigureAwait(false);
            stream.Dispose();
            var heapPost = CollectHeapBytes();
            process.Refresh();
            var privatePost = process.PrivateMemorySize64;
            return new LiveSessionMeasurement(liveOwnedBytes, privateLive - privateBefore, heapPost - heapBefore, privatePost - privateBefore, checksum);
        }
        catch
        {
            await file.DisposeAsync().ConfigureAwait(false);
            stream.Dispose();
            throw;
        }
    }


    /// <summary>Measures retained storage of a live baseline session.</summary>
    /// <param name="fixtureBytes">The fixture bytes.</param>
    /// <param name="projectionOrdinals">The projected column ordinals.</param>
    /// <param name="rowRangeStart">The first selected row, or null for a full scan.</param>
    /// <param name="rowRangeCount">The selected row count, or null for a full scan.</param>
    /// <param name="emittedRowCount">The number of emitted rows.</param>
    /// <param name="utf8PayloadBytes">The UTF-8 payload bytes.</param>
    /// <param name="workload">The workload token selecting the consumer.</param>
    /// <param name="expectedChecksum">The truth checksum verified on every repetition.</param>
    /// <param name="repetitions">The number of truth-checked scans.</param>
    /// <param name="caseName">The census case naming probe diagnostics.</param>
    /// <returns>Live owned storage, post-disposal growth and the verified checksum.</returns>
    public static async Task<LiveSessionMeasurement> MeasureBaselineAsync(
        byte[] fixtureBytes,
        int[] projectionOrdinals,
        long? rowRangeStart,
        long? rowRangeCount,
        int emittedRowCount,
        int utf8PayloadBytes,
        ScanWorkload workload,
        long expectedChecksum,
        int repetitions,
        string caseName)
    {
        ArgumentNullException.ThrowIfNull(fixtureBytes);
        ArgumentNullException.ThrowIfNull(projectionOrdinals);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(repetitions);
        ArgumentException.ThrowIfNullOrWhiteSpace(caseName);
        if ((rowRangeStart.HasValue || rowRangeCount.HasValue) && (!rowRangeStart.HasValue || !rowRangeCount.HasValue))
            throw new ArgumentException("A live-session range needs both its start and its count.", nameof(rowRangeCount));
        var heapBefore = CollectHeapBytes();
        using var process = Process.GetCurrentProcess();
        process.Refresh();
        var privateBefore = process.PrivateMemorySize64;
        var stream = new MemoryStream(fixtureBytes, writable: false);
        BaselineParquetReader? reader = null;
        try
        {
            reader = await BaselineParquetReader.CreateAsync(stream);
            var allFields = reader.Schema.DataFields.ToArray();
            var fields = projectionOrdinals.Select(ordinal => allFields[ordinal]).ToArray();
            var maximumGroupRows = 0;
            for (var groupOrdinal = 0; groupOrdinal < reader.RowGroupCount; groupOrdinal++)
            {
                using var group = reader.OpenRowGroupReader(groupOrdinal);
                maximumGroupRows = Math.Max(maximumGroupRows, checked((int)group.RowCount));
            }

            var valueChains = new long[fields.Length];
            var nullChains = new long[fields.Length];
            var destinations = BaselineDestinations.Create(fields, workload, maximumGroupRows);

            Utf8ScanSink? sink = ScanWorkloadCatalog.IsString(workload)
                ? new Utf8ScanSink(emittedRowCount, utf8PayloadBytes)
                : null;
            var rangeStart = rowRangeStart ?? 0;
            var rangeCount = rowRangeCount ?? emittedRowCount;
            var checksum = ScanChecksum.Seed;
            for (var repetition = 0; repetition < repetitions; repetition++)
            {
                sink?.Reset();
                checksum = await ScanBaselineSessionAsync(
                    reader, fields, workload, emittedRowCount, utf8PayloadBytes, rangeStart, rangeCount,
                    valueChains, nullChains, destinations, sink);
                if (checksum != expectedChecksum)
                    throw new InvalidOperationException($"The baseline live-session truth check failed for {workload} in census case {caseName}.");
            }

            var heapLive = CollectHeapBytes();
            process.Refresh();
            var privateLive = process.PrivateMemorySize64;
            GC.KeepAlive(reader);
            GC.KeepAlive(stream);
            GC.KeepAlive(valueChains);
            GC.KeepAlive(nullChains);
            GC.KeepAlive(destinations);
            if (sink is not null)
                GC.KeepAlive(sink);
            var liveOwnedBytes = heapLive - heapBefore;
            await reader.DisposeAsync().ConfigureAwait(false);
            stream.Dispose();
            var heapPost = CollectHeapBytes();
            process.Refresh();
            var privatePost = process.PrivateMemorySize64;
            return new LiveSessionMeasurement(liveOwnedBytes, privateLive - privateBefore, heapPost - heapBefore, privatePost - privateBefore, checksum);
        }
        catch
        {
            if (reader is not null)
                await reader.DisposeAsync().ConfigureAwait(false);
            stream.Dispose();
            throw;
        }
    }


    /// <summary>Reusable baseline destination buffer for one projected column, sized to the largest row group.</summary>
    /// <remarks>Each projected column retains exactly one layout buffer, selected from its
    /// baseline field by the same predicate the session consumer reads with, so unused layouts
    /// cannot inflate live retention. The session holds the table alive through its window.</remarks>
    internal sealed record BaselineColumnDestinations(Array Values);

    /// <summary>Reusable baseline destinations sized to the largest row group.</summary>
    internal sealed record BaselineDestinations(BaselineColumnDestinations[] Columns)
    {
        internal BaselineColumnDestinations this[int column] => Columns[column];

        internal static BaselineDestinations Create(BaselineDataField[] fields, ScanWorkload workload, int maximumGroupRows)
        {
            ArgumentNullException.ThrowIfNull(fields);
            ArgumentOutOfRangeException.ThrowIfNegative(maximumGroupRows);
            var multi = fields.Length > 1;
            var columns = new BaselineColumnDestinations[fields.Length];
            for (var column = 0; column < fields.Length; column++)
                columns[column] = new BaselineColumnDestinations(SelectBuffer(multi, workload, fields[column], maximumGroupRows));
            return new BaselineDestinations(columns);

            static Array SelectBuffer(bool multi, ScanWorkload workload, BaselineDataField field, int maximumGroupRows)
            {
                if (!multi && field.ClrType == typeof(bool))
                    return new bool?[maximumGroupRows];
                if (!multi && field.ClrType == typeof(long))
                    return field.IsNullable ? new long?[maximumGroupRows] : new long[maximumGroupRows];
                if (!multi && field.ClrType == typeof(float))
                    return field.IsNullable ? new float?[maximumGroupRows] : new float[maximumGroupRows];
                if (!multi && field.ClrType == typeof(double))
                    return field.IsNullable ? new double?[maximumGroupRows] : new double[maximumGroupRows];
                if (!multi && field.ClrType == typeof(ReadOnlyMemory<byte>))
                    return field.IsNullable ? new ReadOnlyMemory<byte>?[maximumGroupRows] : new ReadOnlyMemory<byte>[maximumGroupRows];
                if (!multi && field.IsNullable)
                    return new int?[maximumGroupRows];
                if (ScanWorkloadCatalog.IsString(workload))
                    return new string?[maximumGroupRows];
                return new int[maximumGroupRows];
            }
        }
    }

    internal static async Task<long> ScanBaselineSessionAsync(
        BaselineParquetReader reader,
        BaselineDataField[] fields,
        ScanWorkload workload,
        int emittedRowCount,
        int utf8PayloadBytes,
        long rangeStart,
        long rangeCount,
        long[] valueChains,
        long[] nullChains,
        BaselineDestinations destinations,
        Utf8ScanSink? sink)
    {
        Array.Fill(valueChains, ScanChecksum.Seed, 0, fields.Length);
        Array.Fill(nullChains, ScanChecksum.Seed, 0, fields.Length);
        var rangeEnd = checked(rangeStart + rangeCount);
        var consumedGlobal = 0L;
        var rows = 0;
        var multi = fields.Length > 1;
        for (var groupOrdinal = 0; groupOrdinal < reader.RowGroupCount; groupOrdinal++)
        {
            using var group = reader.OpenRowGroupReader(groupOrdinal);
            var groupRowCount = checked((int)group.RowCount);
            for (var column = 0; column < fields.Length; column++)
            {
                if (!multi && fields[column].ClrType == typeof(bool))
                {
                    var booleans = (bool?[])destinations[column].Values;
                    await group.ReadAsync<bool>(fields[column], booleans);
                    for (var row = 0; row < groupRowCount; row++)
                    {
                        var global = consumedGlobal + row;
                        if (global < rangeStart || global >= rangeEnd)
                            continue;
                        if (booleans[row] is bool value)
                            valueChains[column] = ScanChecksum.Mix(valueChains[column], value ? 1 : 0);
                        else
                            nullChains[column] = ScanChecksum.Mix(nullChains[column], checked((int)global));
                        if (column == 0)
                            rows++;
                    }
                }
                else if (!multi && fields[column].ClrType == typeof(long))
                {
                    var nullableLong = fields[column].IsNullable;
                    var longBuffer = destinations[column].Values;
                    if (nullableLong)
                        await group.ReadAsync<long>(fields[column], (long?[])longBuffer);
                    else
                        await group.ReadAsync<long>(fields[column], (long[])longBuffer);
                    for (var row = 0; row < groupRowCount; row++)
                    {
                        var global = consumedGlobal + row;
                        if (global < rangeStart || global >= rangeEnd)
                            continue;
                        long? actual = nullableLong ? ((long?[])longBuffer)[row] : ((long[])longBuffer)[row];
                        if (actual is long longValue)
                            valueChains[column] = ScanChecksum.MixInt64(valueChains[column], longValue);
                        else
                            nullChains[column] = ScanChecksum.Mix(nullChains[column], checked((int)global));
                        if (column == 0)
                            rows++;
                    }
                }
                else if (!multi && fields[column].ClrType == typeof(float))
                {
                    var nullableFloat = fields[column].IsNullable;
                    var floatBuffer = destinations[column].Values;
                    if (nullableFloat)
                        await group.ReadAsync<float>(fields[column], (float?[])floatBuffer);
                    else
                        await group.ReadAsync<float>(fields[column], (float[])floatBuffer);
                    for (var row = 0; row < groupRowCount; row++)
                    {
                        var global = consumedGlobal + row;
                        if (global < rangeStart || global >= rangeEnd)
                            continue;
                        float? actual = nullableFloat ? ((float?[])floatBuffer)[row] : ((float[])floatBuffer)[row];
                        if (actual is float floatValue)
                            valueChains[column] = ScanChecksum.MixFloat(valueChains[column], floatValue);
                        else
                            nullChains[column] = ScanChecksum.Mix(nullChains[column], checked((int)global));
                        if (column == 0)
                            rows++;
                    }
                }
                else if (!multi && fields[column].ClrType == typeof(double))
                {
                    var nullableDouble = fields[column].IsNullable;
                    var doubleBuffer = destinations[column].Values;
                    if (nullableDouble)
                        await group.ReadAsync<double>(fields[column], (double?[])doubleBuffer);
                    else
                        await group.ReadAsync<double>(fields[column], (double[])doubleBuffer);
                    for (var row = 0; row < groupRowCount; row++)
                    {
                        var global = consumedGlobal + row;
                        if (global < rangeStart || global >= rangeEnd)
                            continue;
                        double? actual = nullableDouble ? ((double?[])doubleBuffer)[row] : ((double[])doubleBuffer)[row];
                        if (actual is double doubleValue)
                            valueChains[column] = ScanChecksum.MixDouble(valueChains[column], doubleValue);
                        else
                            nullChains[column] = ScanChecksum.Mix(nullChains[column], checked((int)global));
                        if (column == 0)
                            rows++;
                    }
                }
                else if (!multi && fields[column].ClrType == typeof(ReadOnlyMemory<byte>))
                {
                    var nullableRom = fields[column].IsNullable;
                    var romBuffer = destinations[column].Values;
                    if (nullableRom)
                        await group.ReadAsync<ReadOnlyMemory<byte>>(fields[column], (ReadOnlyMemory<byte>?[])romBuffer);
                    else
                        await group.ReadAsync<ReadOnlyMemory<byte>>(fields[column], (ReadOnlyMemory<byte>[])romBuffer);
                    for (var row = 0; row < groupRowCount; row++)
                    {
                        var global = consumedGlobal + row;
                        if (global < rangeStart || global >= rangeEnd)
                            continue;
                        ReadOnlyMemory<byte>? actual = nullableRom ? ((ReadOnlyMemory<byte>?[])romBuffer)[row] : ((ReadOnlyMemory<byte>[])romBuffer)[row];
                        if (actual is ReadOnlyMemory<byte> bytes)
                            valueChains[column] = ScanChecksum.MixBytes(valueChains[column], bytes.ToArray());
                        else
                            nullChains[column] = ScanChecksum.Mix(nullChains[column], checked((int)global));
                        if (column == 0)
                            rows++;
                    }
                }
                else if (!multi && fields[column].IsNullable)
                {
                    var nullableIntegers = (int?[])destinations[column].Values;
                    await group.ReadAsync<int>(fields[column], nullableIntegers);
                    for (var row = 0; row < groupRowCount; row++)
                    {
                        var global = consumedGlobal + row;
                        if (global < rangeStart || global >= rangeEnd)
                            continue;
                        if (nullableIntegers[row] is int value)
                            valueChains[column] = ScanChecksum.Mix(valueChains[column], value);
                        else
                            nullChains[column] = ScanChecksum.Mix(nullChains[column], checked((int)global));
                        if (column == 0)
                            rows++;
                    }
                }
                else if (ScanWorkloadCatalog.IsString(workload))
                {
                    if (sink is null)
                        throw new InvalidOperationException("The baseline live session has no UTF-8 sink.");
                    var strings = (string?[])destinations[column].Values;
                    await group.ReadAsync(fields[column], strings);
                    for (var row = 0; row < groupRowCount; row++)
                    {
                        var global = consumedGlobal + row;
                        if (global < rangeStart || global >= rangeEnd)
                            continue;
                        sink.AppendString(strings[row] ??
                            throw new InvalidOperationException("A required baseline string decoded as null."));
                        if (column == 0)
                            rows++;
                    }
                }
                else
                {
                    var integers = (int[])destinations[column].Values;
                    await group.ReadAsync<int>(fields[column], integers);
                    for (var row = 0; row < groupRowCount; row++)
                    {
                        var global = consumedGlobal + row;
                        if (global < rangeStart || global >= rangeEnd)
                            continue;
                        valueChains[column] = ScanChecksum.Mix(valueChains[column], integers[row]);
                        if (column == 0)
                            rows++;
                    }
                }
            }

            consumedGlobal = checked(consumedGlobal + groupRowCount);
        }

        if (rows != emittedRowCount)
            throw new InvalidOperationException($"The baseline live session saw {rows} rows instead of {emittedRowCount}.");
        if (sink is not null)
            return sink.Complete(emittedRowCount, utf8PayloadBytes);
        return multi
            ? ScanChecksum.CombineColumns(valueChains.AsSpan(0, fields.Length), nullChains.AsSpan(0, fields.Length))
            : ScanChecksum.CombineColumn(valueChains[0], nullChains[0]);
    }

    private static long CollectHeapBytes()
    {
        GC.Collect(2, GCCollectionMode.Forced, blocking: true, compacting: true);
        GC.WaitForPendingFinalizers();
        GC.Collect(2, GCCollectionMode.Forced, blocking: true, compacting: true);
        return GC.GetTotalMemory(forceFullCollection: false);
    }
}

