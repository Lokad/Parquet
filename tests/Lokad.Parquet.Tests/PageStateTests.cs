namespace Lokad.Parquet.Tests;

public sealed class PageStateTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task OptionalAllValidInt32Slicing(bool isV2)
    {
        using var tracker = new PoolTracker();
        var bytes = ParquetFixtureBuilder.CreateInt32(new()
        {
            Values = [1, 2, 3],
            Repetition = ParquetRepetition.Optional,
            Validity = null,
            PageVersion = isV2 ? FixturePageVersion.DataPageV2 : FixturePageVersion.DataPageV1,
        });
        await using var file = await ParquetFile.OpenAsync(new MemoryStream(bytes, writable: false));
        var values = new List<int>();
        var batchSizes = new List<int>();
        await foreach (var batch in file.ScanAsync(new([file.Metadata.Schema.Columns[0]], null, null, 2)))
        {
            batchSizes.Add(batch.RowCount);
            using (batch)
            {
                var column = Assert.IsType<ParquetPrimitiveColumnBatch<int>>(batch.Columns[0]);
                Assert.True(column.Validity.IsAllValid);
                values.AddRange(column.Values.ToArray());
            }
        }

        Assert.Equal([1, 2, 3], values);
        Assert.Equal([2, 1], batchSizes);
    }

    [Fact]
    public async Task RequiredBorrowedAndOwnedPayloadsDecodeIdentically()
    {
        using var tracker = new PoolTracker();
        var bytes = ParquetFixtureBuilder.CreateInt32(new() { Values = [10, -2, 42] });

        await using (var borrowedFile = await ParquetFile.OpenAsync(new ReadOnlyMemory<byte>(bytes)))
        {
            var borrowed = new List<int>();
            await foreach (var batch in borrowedFile.ScanAsync(new([borrowedFile.Metadata.Schema.Columns[0]])))
            {
                using (batch)
                    borrowed.AddRange(Assert.IsType<ParquetPrimitiveColumnBatch<int>>(batch.Columns[0]).Values.ToArray());
            }
            Assert.Equal([10, -2, 42], borrowed);
        }

        var ownedSource = new CopyingSource(bytes);
        await using (var ownedFile = await ParquetFile.OpenAsync(ownedSource))
        {
            var owned = new List<int>();
            await foreach (var batch in ownedFile.ScanAsync(new([ownedFile.Metadata.Schema.Columns[0]])))
            {
                using (batch)
                    owned.AddRange(Assert.IsType<ParquetPrimitiveColumnBatch<int>>(batch.Columns[0]).Values.ToArray());
            }
            Assert.Equal([10, -2, 42], owned);
        }
    }

    [Fact]
    public async Task UnknownPageTypeIsUnsupported()
    {
        using var tracker = new PoolTracker();
        var bytes = ParquetFixtureBuilder.CreateInt32(new()
        {
            Values = [1, 2, 3],
            PageHeaderOverrides = new() { TypeCode = 1 },
        });
        await using var file = await ParquetFile.OpenAsync(new MemoryStream(bytes, writable: false));
        await Assert.ThrowsAsync<ParquetUnsupportedFeatureException>(async () =>
        {
            await foreach (var batch in file.ScanAsync(new([file.Metadata.Schema.Columns[0]])))
                batch.Dispose();
        });
    }

    private sealed class CopyingSource : IParquetRandomAccessSource
    {
        private readonly byte[] _bytes;
        public CopyingSource(byte[] bytes) => _bytes = bytes;
        public long Length => _bytes.Length;
        public ValueTask ReadExactlyAsync(long offset, ArraySegment<byte> destination, CancellationToken cancellationToken)
        {
            _bytes.AsMemory(checked((int)offset), destination.Count).CopyTo(destination.AsMemory());
            return ValueTask.CompletedTask;
        }
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }
}
