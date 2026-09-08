namespace Lokad.Parquet.Tests;

public sealed class ExhaustiveTeardownTests
{
    [Fact]
    public async Task FileDisposalDisposesOwnedSourceEvenWhenCancellationCallbackThrows()
    {
        var bytes = ParquetFixtureBuilder.CreateInt32(new() { Values = [1, 2, 3] });
        var source = new ThrowingCancellationSource(bytes);
        var file = await ParquetFile.OpenAsync(source, ParquetSourceOwnership.ParquetFile, ParquetReaderOptions.Default, CancellationToken.None);
        var scan = file.ScanAsync(new([file.Metadata.Schema.Columns[0]])).GetAsyncEnumerator();
        Assert.True(await scan.MoveNextAsync());
        scan.Current.Dispose();
        Exception? disposalFailure = null;
        try
        {
            await file.DisposeAsync();
        }
        catch (Exception exception)
        {
            disposalFailure = exception;
        }

        Assert.True(source.IsDisposed);
        Assert.NotNull(disposalFailure);
        await scan.DisposeAsync();
        await source.DisposeAsync();
    }

    [Fact]
    public async Task BatchDisposalReleasesAllOwnersEvenWhenOneThrows()
    {
        var bytes = ParquetFixtureBuilder.CreateInt32(new() { Values = [1] });
        await using var file = await ParquetFile.OpenAsync(new MemoryStream(bytes, writable: false));
        await using var enumerator = file.ScanAsync(new([file.Metadata.Schema.Columns[0]])).GetAsyncEnumerator();
        Assert.True(await enumerator.MoveNextAsync());
        var batch = enumerator.Current;
        batch.Dispose();
        Assert.True(batch.IsDisposed);
    }

    private sealed class ThrowingCancellationSource : IParquetRandomAccessSource
    {
        private readonly byte[] _bytes;
        private CancellationTokenRegistration _registration;
        private bool _registered;
        public ThrowingCancellationSource(byte[] bytes) => _bytes = bytes;
        public bool IsDisposed { get; private set; }
        public long Length => _bytes.Length;
        public ValueTask ReadExactlyAsync(long offset, ArraySegment<byte> destination, CancellationToken cancellationToken)
        {
            _bytes.AsMemory((int)offset, destination.Count).CopyTo(destination.AsMemory());
            if (cancellationToken.CanBeCanceled && !_registered)
            {
                _registered = true;
                _registration = cancellationToken.Register(() => throw new InvalidOperationException("test cancellation callback"));
            }

            return ValueTask.CompletedTask;
        }

        public ValueTask DisposeAsync()
        {
            IsDisposed = true;
            _registration.Dispose();
            return ValueTask.CompletedTask;
        }
    }
}
