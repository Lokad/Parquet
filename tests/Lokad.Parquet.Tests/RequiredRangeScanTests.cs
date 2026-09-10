using Xunit.Abstractions;

namespace Lokad.Parquet.Tests;

// Row-range scan coverage: peak pooled bytes follow the selection (not the
// page), source reads stay bounded by the selection, boundaries round-trip,
// and malformed, truncated or cancelled range scans fail pool-balanced.
// Shares the non-parallel allocation collection so pool-observer peaks stay
// free of cross-test interference.
[Collection("Scan batch allocation")]
public sealed class RequiredRangeScanTests
{
    private readonly ITestOutputHelper _output;

    public RequiredRangeScanTests(ITestOutputHelper output) => _output = output;

    [Fact]
    public async Task RangePeakBytesBoundedBySelection()
    {
        var values = new int[65536];
        for (var row = 0; row < values.Length; row++)
            values[row] = row * 3 + 1;
        var bytes = ParquetFixtureBuilder.CreateInt32(new() { Values = values });
        await AssertRangePeak(bytes, values, 0, 1, 1, 4_096, "one-row");
        await AssertRangePeak(bytes, values, 4096, 4096, 4096, 65_536, "short-range");
        await AssertRangePeak(bytes, values, 0, 65536, 65536, 2_000_000, "full-range");

        async Task AssertRangePeak(byte[] fixture, int[] oracle, long start, long count, int target, long ceiling, string label)
        {
            using var tracker = new PoolTracker();
            var source = new CountingSource(fixture);
            var collected = new List<int>();
            await using (var file = await ParquetFile.OpenAsync(source, ParquetSourceOwnership.Caller, new ParquetReaderOptions(), CancellationToken.None))
            {
                await foreach (var batch in file.ScanAsync(new([file.Metadata.Schema.Columns[0]], null, new ParquetRowRange(start, count), target)))
                {
                    collected.AddRange(Assert.IsType<ParquetPrimitiveColumnBatch<int>>(batch.Columns[0]).Values.ToArray());
                    batch.Dispose();
                }
            }

            Assert.Equal(oracle.Skip((int)start).Take((int)count), collected);
            _output.WriteLine($"{label}: peak pooled {tracker.PeakOutstandingBytes} bytes for {count} selected rows.");
            Assert.True(tracker.PeakOutstandingBytes <= ceiling);
        }
    }

    [Fact]
    public async Task RangeReadsBoundedSourceBytes()
    {
        var values = new int[65536];
        for (var row = 0; row < values.Length; row++)
            values[row] = row * 3 + 1;
        var bytes = ParquetFixtureBuilder.CreateInt32(new() { Values = values });
        var fullBytes = await ScanRangeWithCounter(bytes, 0, 65536, 65536);
        var shortBytes = await ScanRangeWithCounter(bytes, 4096, 4096, 4096);
        var singleBytes = await ScanRangeWithCounter(bytes, 0, 1, 1);
        _output.WriteLine($"source bytes full={fullBytes} short={shortBytes} single={singleBytes}.");
        Assert.True(shortBytes < fullBytes);
        Assert.True(singleBytes < fullBytes);
        Assert.True(singleBytes <= shortBytes);

        async Task<long> ScanRangeWithCounter(byte[] fixture, long start, long count, int target)
        {
            var source = new CountingSource(fixture);
            var collected = new List<int>();
            await using (var file = await ParquetFile.OpenAsync(source, ParquetSourceOwnership.Caller, new ParquetReaderOptions(), CancellationToken.None))
            {
                await foreach (var batch in file.ScanAsync(new([file.Metadata.Schema.Columns[0]], null, new ParquetRowRange(start, count), target)))
                {
                    collected.AddRange(Assert.IsType<ParquetPrimitiveColumnBatch<int>>(batch.Columns[0]).Values.ToArray());
                    batch.Dispose();
                }
            }

            Assert.Equal(values.Skip((int)start).Take((int)count), collected);
            return source.BytesRead;
        }
    }

    [Fact]
    public async Task RangeBoundariesRoundTrip()
    {
        var values = new int[65536];
        for (var row = 0; row < values.Length; row++)
            values[row] = row * 3 + 1;
        var bytes = ParquetFixtureBuilder.CreateInt32(new() { Values = values });
        await AssertRangeValues(bytes, values, 0, 1, false);
        await AssertRangeValues(bytes, values, 65535, 1, false);
        await AssertRangeValues(bytes, values, 4096, 4096, false);
        await AssertRangeValues(bytes, values, 0, 65536, false);
        await AssertRangeValues(bytes, values, 5, 0, false);
        var grouped = ParquetFixtureBuilder.CreateRequiredInt32RowGroups([[10, 20, 30], [40, 50]]);
        await AssertRangeValues(grouped, [10, 20, 30, 40, 50], 2, 2, false);
        await AssertRangeValues(bytes, values, 0, 1, true);
        await AssertRangeValues(bytes, values, 65535, 1, true);
        await AssertRangeValues(bytes, values, 4096, 4096, true);

        async Task AssertRangeValues(byte[] fixture, int[] oracle, long start, long count, bool opaque)
        {
            var collected = new List<int>();
            if (opaque)
            {
                var source = new CountingSource(fixture);
                await using var file = await ParquetFile.OpenAsync(source, ParquetSourceOwnership.Caller, new ParquetReaderOptions(), CancellationToken.None);
                await CollectAsync(file);
            }
            else
            {
                await using var file = await ParquetFile.OpenAsync(fixture, new ParquetReaderOptions(), CancellationToken.None);
                await CollectAsync(file);
            }

            Assert.Equal(oracle.Skip((int)start).Take((int)count), collected);

            async Task CollectAsync(ParquetFile file)
            {
                await foreach (var batch in file.ScanAsync(new([file.Metadata.Schema.Columns[0]], null, new ParquetRowRange(start, count), 64)))
                {
                    collected.AddRange(Assert.IsType<ParquetPrimitiveColumnBatch<int>>(batch.Columns[0]).Values.ToArray());
                    batch.Dispose();
                }
            }
        }
    }

    [Fact]
    public async Task RangeSelectedSliceStillEnforcesChunkTotals()
    {
        var values = new int[4096];
        for (var row = 0; row < values.Length; row++)
            values[row] = row * 3 + 1;
        var bytes = ParquetFixtureBuilder.CreateInt32(new() { Values = values, ChunkTotalUncompressedSize = 0 });
        using var tracker = new PoolTracker();
        await using var file = await ParquetFile.OpenAsync(bytes, new ParquetReaderOptions(), CancellationToken.None);
        await Assert.ThrowsAsync<ParquetFormatException>(async () =>
        {
            await foreach (var batch in file.ScanAsync(new([file.Metadata.Schema.Columns[0]], null, new ParquetRowRange(100, 50), 50)))
                batch.Dispose();
        });
    }

    [Fact]
    public async Task RangeTruncatedSourceFailsBalanced()
    {
        var values = new int[4096];
        for (var row = 0; row < values.Length; row++)
            values[row] = row * 3 + 1;
        var bytes = ParquetFixtureBuilder.CreateInt32(new() { Values = values });
        using var tracker = new PoolTracker();
        long dataPageStart;
        long footerStart;
        await using (var probe = await ParquetFile.OpenAsync(bytes, new ParquetReaderOptions(), CancellationToken.None))
        {
            var chunk = probe.Metadata.RowGroups[0].Columns[0];
            dataPageStart = chunk.DataPageOffset;
            var footerLength = System.Buffers.Binary.BinaryPrimitives.ReadInt32LittleEndian(bytes.AsSpan(bytes.Length - 8));
            footerStart = bytes.Length - 8 - footerLength;
        }

        var source = new TruncatingSource(bytes, dataPageStart, footerStart);
        await using var file = await ParquetFile.OpenAsync(source, ParquetSourceOwnership.Caller, new ParquetReaderOptions(), CancellationToken.None);
        await Assert.ThrowsAsync<ParquetFormatException>(async () =>
        {
            await foreach (var batch in file.ScanAsync(new([file.Metadata.Schema.Columns[0]], null, new ParquetRowRange(100, 50), 50)))
                batch.Dispose();
        });
    }

    [Fact]
    public async Task RangeCancellationBalancesPools()
    {
        var values = new int[4096];
        for (var row = 0; row < values.Length; row++)
            values[row] = row * 3 + 1;
        var bytes = ParquetFixtureBuilder.CreateInt32(new() { Values = values });
        using var tracker = new PoolTracker();
        await using var file = await ParquetFile.OpenAsync(bytes, new ParquetReaderOptions(), CancellationToken.None);
        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
        {
            await foreach (var batch in file.ScanAsync(new([file.Metadata.Schema.Columns[0]], null, new ParquetRowRange(100, 50), 50), cancellation.Token))
                batch.Dispose();
        });
    }

    private sealed class CountingSource(byte[] bytes) : IParquetRandomAccessSource
    {
        public long Length => bytes.Length;

        public long Reads { get; private set; }

        public long BytesRead { get; private set; }

        public ValueTask ReadExactlyAsync(long offset, ArraySegment<byte> destination, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            bytes.AsMemory(checked((int)offset), destination.Count).CopyTo(destination.AsMemory());
            Reads++;
            BytesRead += destination.Count;
            return ValueTask.CompletedTask;
        }

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    private sealed class TruncatingSource(byte[] bytes, long payloadStart, long footerStart) : IParquetRandomAccessSource
    {
        public long Length => bytes.Length;

        public ValueTask ReadExactlyAsync(long offset, ArraySegment<byte> destination, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (offset < 0 || destination.Count < 0 || (offset < footerStart && offset + destination.Count > payloadStart))
                throw new EndOfStreamException("test truncation");
            bytes.AsMemory(checked((int)offset), destination.Count).CopyTo(destination.AsMemory());
            return ValueTask.CompletedTask;
        }

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }
}

