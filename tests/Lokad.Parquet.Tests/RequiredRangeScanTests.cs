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

    [Fact]
    public async Task FixedBinaryRangeValuesRoundTrip()
    {
        // R01: partial required FIXED_LEN_BYTE_ARRAY pages decode their slice.
        var values = new byte[][] { [1, 2], [3, 4], [5, 6], [7, 8] };
        foreach (var version in Enum.GetValues<FixturePageVersion>())
            foreach (var crc in new[] { FixtureCrcMode.Absent, FixtureCrcMode.Valid })
            {
                var bytes = ParquetFixtureBuilder.CreateInt32(new()
                {
                    PhysicalTypeCode = (int)ParquetPhysicalType.FixedLengthByteArray,
                    TypeLength = 2,
                    PhysicalValues = values,
                    PageVersion = version,
                    CrcMode = crc,
                });
                var label = version.ToString() + " crc=" + crc.ToString();
                using var tracker = new PoolTracker();
                Assert.Equal([3, 4, 5, 6], await ScanFixedBinaryRange(bytes, 1, 2, 64, 0));
                Assert.Equal([3, 4, 5, 6], await ScanFixedBinaryRange(bytes, 1, 2, 64, 1));
                Assert.Equal([3, 4, 5, 6], await ScanFixedBinaryRange(bytes, 1, 2, 64, 2));
                Assert.Equal([3, 4, 5, 6], await ScanFixedBinaryRange(bytes, 1, 2, 64, 3));
                Assert.Equal([1, 2, 3, 4, 5, 6, 7, 8], await ScanFixedBinaryRange(bytes, 0, 4, 64, 2));
                Assert.Equal([7, 8], await ScanFixedBinaryRange(bytes, 3, 1, 64, 1));
                Assert.Equal([3, 4, 5, 6], await ScanFixedBinaryRange(bytes, 1, 2, 1, 3));
                _output.WriteLine(label + ": fixed range selections round-tripped.");
            }
    }
    [Fact]
    public async Task FixedBinaryRangeReadsBoundedSourceBytes()
    {
        var bytes = ParquetFixtureBuilder.CreateInt32(new()
        {
            PhysicalTypeCode = (int)ParquetPhysicalType.FixedLengthByteArray,
            TypeLength = 2,
            PhysicalValues = new byte[][] { [1, 2], [3, 4], [5, 6], [7, 8] },
        });
        var fullBytes = await ScanFixedRangeBytes(bytes, 0, 4, 64);
        var shortBytes = await ScanFixedRangeBytes(bytes, 1, 2, 64);
        _output.WriteLine("fixed source bytes full=" + fullBytes + " short=" + shortBytes + ".");
        Assert.True(shortBytes < fullBytes);

        async Task<long> ScanFixedRangeBytes(byte[] fixture, long start, long count, int target)
        {
            using var tracker = new PoolTracker();
            var source = new CountingSource(fixture);
            await using var file = await ParquetFile.OpenAsync(source, ParquetSourceOwnership.Caller, new ParquetReaderOptions(), CancellationToken.None);
            await foreach (var batch in file.ScanAsync(new([file.Metadata.Schema.Columns[0]], null, new ParquetRowRange(start, count), target)))
                batch.Dispose();
            return source.BytesRead;
        }
    }
    [Fact]
    public async Task FixedBinaryRangeSnappyControlRoundTrips()
    {
        // Compressed pages bypass slicing and decode the whole page.
        var bytes = ParquetFixtureBuilder.CreateInt32(new()
        {
            PhysicalTypeCode = (int)ParquetPhysicalType.FixedLengthByteArray,
            TypeLength = 2,
            PhysicalValues = new byte[][] { [1, 2], [3, 4], [5, 6], [7, 8] },
            CompressionCodec = ParquetCompressionCodec.Snappy,
        });
        using var tracker = new PoolTracker();
        Assert.Equal([3, 4, 5, 6], await ScanFixedBinaryRange(bytes, 1, 2, 64, 0));
        Assert.Equal([3, 4, 5, 6], await ScanFixedBinaryRange(bytes, 1, 2, 64, 3));
    }

    [Fact]
    public async Task FixedBinaryRangeTruncatedDeclarationFailsBalanced()
    {
        // The bounded slice read never observes the whole page, so a truncated
        // declared size must fail at validation with balanced pools.
        foreach (var version in Enum.GetValues<FixturePageVersion>())
        {
            var bytes = ParquetFixtureBuilder.CreateInt32(new()
            {
                PhysicalTypeCode = (int)ParquetPhysicalType.FixedLengthByteArray,
                TypeLength = 2,
                PhysicalValues = new byte[][] { [1, 2], [3, 4], [5, 6], [7, 8] },
                PageVersion = version,
                PageHeaderOverrides = new ParquetPageHeaderOverrides { UncompressedSize = 4, CompressedSize = 4 },
            });
            using var tracker = new PoolTracker();
            await using var file = await ParquetFile.OpenAsync(bytes, new ParquetReaderOptions(), CancellationToken.None);
            await Assert.ThrowsAsync<ParquetFormatException>(async () =>
            {
                await foreach (var batch in file.ScanAsync(new([file.Metadata.Schema.Columns[0]], null, new ParquetRowRange(1, 2), 64)))
                    batch.Dispose();
            });
        }
    }
    [Fact]
    public async Task FixedBinaryRangeTruncatedSourceFailsBalanced()
    {
        var bytes = ParquetFixtureBuilder.CreateInt32(new()
        {
            PhysicalTypeCode = (int)ParquetPhysicalType.FixedLengthByteArray,
            TypeLength = 2,
            PhysicalValues = new byte[][] { [1, 2], [3, 4], [5, 6], [7, 8] },
        });
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
            await foreach (var batch in file.ScanAsync(new([file.Metadata.Schema.Columns[0]], null, new ParquetRowRange(1, 2), 64)))
                batch.Dispose();
        });
    }

    [Fact]
    public async Task FixedBinaryRangeCancellationBalancesPools()
    {
        var bytes = ParquetFixtureBuilder.CreateInt32(new()
        {
            PhysicalTypeCode = (int)ParquetPhysicalType.FixedLengthByteArray,
            TypeLength = 2,
            PhysicalValues = new byte[][] { [1, 2], [3, 4], [5, 6], [7, 8] },
        });
        using var tracker = new PoolTracker();
        await using var file = await ParquetFile.OpenAsync(bytes, new ParquetReaderOptions(), CancellationToken.None);
        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
        {
            await foreach (var batch in file.ScanAsync(new([file.Metadata.Schema.Columns[0]], null, new ParquetRowRange(1, 2), 64), cancellation.Token))
                batch.Dispose();
        });
    }

    private static async Task<byte[]> ScanFixedBinaryRange(byte[] fixture, long start, long count, int target, int sourceKind)
    {
        // Source kinds: 0 direct memory, 1 exposed MemoryStream, 2 non-exposed
        // MemoryStream subclass, 3 opaque custom source.
        var collected = new List<byte>();
        if (sourceKind == 3)
        {
            var source = new CountingSource(fixture);
            await using var file = await ParquetFile.OpenAsync(source, ParquetSourceOwnership.Caller, new ParquetReaderOptions(), CancellationToken.None);
            await CollectAsync(file);
        }
        else if (sourceKind == 0)
        {
            await using var file = await ParquetFile.OpenAsync(fixture, new ParquetReaderOptions(), CancellationToken.None);
            await CollectAsync(file);
        }
        else
        {
            using MemoryStream stream = sourceKind == 1
                ? new MemoryStream(fixture, 0, fixture.Length, false, true)
                : new NonExposingStream(fixture);
            await using var file = await ParquetFile.OpenAsync(stream, ParquetSourceOwnership.Caller, new ParquetReaderOptions(), CancellationToken.None);
            await CollectAsync(file);
        }

        return [.. collected];

        async Task CollectAsync(ParquetFile file)
        {
            await foreach (var batch in file.ScanAsync(new([file.Metadata.Schema.Columns[0]], null, new ParquetRowRange(start, count), target)))
            {
                var fixedBatch = Assert.IsType<ParquetFixedLengthByteArrayColumnBatch>(batch.Columns[0]);
                Assert.Equal(2, fixedBatch.TypeWidth);
                collected.AddRange(fixedBatch.Payload.ToArray());
                batch.Dispose();
            }
        }
    }

    private sealed class NonExposingStream(byte[] bytes) : MemoryStream(bytes, false)
    {
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

