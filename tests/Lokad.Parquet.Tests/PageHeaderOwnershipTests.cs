namespace Lokad.Parquet.Tests;

public sealed class PageHeaderOwnershipTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task MalformedPageHeaderReturnsRentedBuffer(bool delayed)
    {
        using var tracker = new PoolTracker();
        var bytes = ParquetFixtureBuilder.CreateInt32(new() { Values = [1, 2, 3], PageHeaderOverrides = new() { CompressedSize = -1 } });
        if (!delayed)
        {
            await using (var file = await ParquetFile.OpenAsync(new MemoryStream(bytes, writable: false)))
            {
                await Assert.ThrowsAsync<ParquetFormatException>(async () =>
                {
                    await foreach (var batch in file.ScanAsync(new([file.Metadata.Schema.Columns[0]])))
                    {
                        batch.Dispose();
                    }
                });
            }
        }
        else
        {
            var source = new DelayedSource(bytes);
            await using (var file = await ParquetFile.OpenAsync(source))
            {
                await Assert.ThrowsAsync<ParquetFormatException>(async () =>
                {
                    await foreach (var batch in file.ScanAsync(new([file.Metadata.Schema.Columns[0]])))
                    {
                        batch.Dispose();
                    }
                });
            }
        }
    }

    private sealed class DelayedSource : IParquetRandomAccessSource
    {
        private readonly byte[] _bytes;
        public DelayedSource(byte[] bytes) => _bytes = bytes;
        public long Length => _bytes.Length;
        public async ValueTask ReadExactlyAsync(long offset, ArraySegment<byte> destination, CancellationToken cancellationToken)
        {
            await Task.Yield();
            _bytes.AsMemory((int)offset, destination.Count).CopyTo(destination.AsMemory());
        }

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }
}
