namespace Lokad.Parquet.Tests;

public sealed class V2PayloadReuseTests
{
    [Fact]
    public async Task UncompressedV2UnderSnappyChunkRentsNoPayloadBuffer()
    {
        using var tracker = new PoolTracker();
        var bytes = ParquetFixtureBuilder.CreateInt32(new()
        {
            Values = [1, 2, 3],
            PageVersion = FixturePageVersion.DataPageV2,
            CompressionCodec = ParquetCompressionCodec.Snappy,
            PageHeaderOverrides = new() { V2IsCompressed = false },
        });
        await using (var file = await ParquetFile.OpenAsync(new MemoryStream(bytes, writable: false)))
        {
            var rentsAfterOpen = tracker.RentCount;
            await using var enumerator = file.ScanAsync(new([file.Metadata.Schema.Columns[0]], null, null, 3)).GetAsyncEnumerator();
            Assert.True(await enumerator.MoveNextAsync());
            using (var batch = enumerator.Current)
                Assert.Equal([1, 2, 3], Assert.IsType<ParquetPrimitiveColumnBatch<int>>(batch.Columns[0]).Values.ToArray());
            Assert.False(await enumerator.MoveNextAsync());
            // Header read plus column decode only; the uncompressed value section is
            // reused without renting or copying a payload buffer.
            Assert.Equal(2, tracker.RentCount - rentsAfterOpen);
        }
    }

    [Fact]
    public async Task UncompressedV2RentedPayloadMovesWithoutCopy()
    {
        using var tracker = new PoolTracker();
        var bytes = ParquetFixtureBuilder.CreateInt32(new()
        {
            Values = [10, -2, 42],
            PageVersion = FixturePageVersion.DataPageV2,
        });
        var source = new UnbufferedSource(bytes);
        await using (var file = await ParquetFile.OpenAsync(source))
        {
            var rentsAfterOpen = tracker.RentCount;
            await using var enumerator = file.ScanAsync(new([file.Metadata.Schema.Columns[0]], null, null, 3)).GetAsyncEnumerator();
            Assert.True(await enumerator.MoveNextAsync());
            using (var batch = enumerator.Current)
                Assert.Equal([10, -2, 42], Assert.IsType<ParquetPrimitiveColumnBatch<int>>(batch.Columns[0]).Values.ToArray());
            Assert.False(await enumerator.MoveNextAsync());
            // Header read and column decode only: the payload read reuses the
            // retained header buffer, which then moves into place with no copy.
            Assert.Equal(2, tracker.RentCount - rentsAfterOpen);
        }
    }

    [Fact]
    public async Task RequiredV2WithZeroLevelLengthsScans()
    {
        using var tracker = new PoolTracker();
        var bytes = ParquetFixtureBuilder.CreateInt32(new()
        {
            Values = [5, 6],
            PageVersion = FixturePageVersion.DataPageV2,
        });
        await using var file = await ParquetFile.OpenAsync(new MemoryStream(bytes, writable: false));
        await using var enumerator = file.ScanAsync(new([file.Metadata.Schema.Columns[0]])).GetAsyncEnumerator();
        Assert.True(await enumerator.MoveNextAsync());
        using var batch = enumerator.Current;
        Assert.Equal([5, 6], Assert.IsType<ParquetPrimitiveColumnBatch<int>>(batch.Columns[0]).Values.ToArray());
    }

    [Fact]
    public async Task SkipsAnEmptyV2PageBeforeADataPage()
    {
        var bytes = ParquetFixtureBuilder.CreateInt32(new()
        {
            Values = [],
            TrailingPlainValues = new int[] { 7 },
            PageVersion = FixturePageVersion.DataPageV2,
        });
        await using var file = await ParquetFile.OpenAsync(new MemoryStream(bytes, writable: false));
        await using var enumerator = file.ScanAsync(new([file.Metadata.Schema.Columns[0]])).GetAsyncEnumerator();
        Assert.True(await enumerator.MoveNextAsync());
        var batch = enumerator.Current;
        Assert.Equal([7], Assert.IsType<ParquetPrimitiveColumnBatch<int>>(batch.Columns[0]).Values.ToArray());
        batch.Dispose();
        Assert.False(await enumerator.MoveNextAsync());
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task V1AndV2MatchAcrossSources(bool useV2)
    {
        var bytes = ParquetFixtureBuilder.CreateInt32(new()
        {
            Values = [10, -2, 42],
            PageVersion = useV2 ? FixturePageVersion.DataPageV2 : FixturePageVersion.DataPageV1,
        });
        var expected = new[] { 10, -2, 42 };
        // Direct memory with a non-zero origin exercises borrowed caller memory.
        var container = new byte[checked(bytes.Length + 23)];
        bytes.AsSpan().CopyTo(container.AsSpan(17));
        await using (var file = await ParquetFile.OpenAsync(container.AsMemory(17, bytes.Length)))
        {
            await using var enumerator = file.ScanAsync(new([file.Metadata.Schema.Columns[0]])).GetAsyncEnumerator();
            Assert.True(await enumerator.MoveNextAsync());
            using var batch = enumerator.Current;
            Assert.Equal(expected, Assert.IsType<ParquetPrimitiveColumnBatch<int>>(batch.Columns[0]).Values.ToArray());
        }
        // Streams with a visible buffer and unbuffered custom sources cover the
        // remaining payload paths; file sources share the rented path.
        await using (var file = await ParquetFile.OpenAsync(new MemoryStream(bytes, writable: false)))
        {
            await using var enumerator = file.ScanAsync(new([file.Metadata.Schema.Columns[0]])).GetAsyncEnumerator();
            Assert.True(await enumerator.MoveNextAsync());
            using var batch = enumerator.Current;
            Assert.Equal(expected, Assert.IsType<ParquetPrimitiveColumnBatch<int>>(batch.Columns[0]).Values.ToArray());
        }
        await using (var file = await ParquetFile.OpenAsync(new UnbufferedSource(bytes)))
        {
            await using var enumerator = file.ScanAsync(new([file.Metadata.Schema.Columns[0]])).GetAsyncEnumerator();
            Assert.True(await enumerator.MoveNextAsync());
            using var batch = enumerator.Current;
            Assert.Equal(expected, Assert.IsType<ParquetPrimitiveColumnBatch<int>>(batch.Columns[0]).Values.ToArray());
        }
    }

    private sealed class UnbufferedSource : IParquetRandomAccessSource
    {
        private readonly byte[] _bytes;
        public UnbufferedSource(byte[] bytes) => _bytes = bytes;
        public long Length => _bytes.Length;
        public ValueTask ReadExactlyAsync(long offset, ArraySegment<byte> destination, CancellationToken cancellationToken)
        {
            _bytes.AsMemory((int)offset, destination.Count).CopyTo(destination.AsMemory());
            return ValueTask.CompletedTask;
        }

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }
}
