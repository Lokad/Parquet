namespace Lokad.Parquet.Tests;

public sealed class OpenFailureTests
{
    [Fact]
    public async Task SynchronousReadFailurePreservesOriginalErrorAndDisposesOnce()
    {
        using var tracker = new PoolTracker();
        var source = new SyncFailingSource();
        var exception = await Assert.ThrowsAsync<IOException>(async () =>
            await ParquetFile.OpenAsync(source, ParquetSourceOwnership.ParquetFile, ParquetReaderOptions.Default, CancellationToken.None));
        Assert.Equal("primary source read failure", exception.Message);
        Assert.Equal(1, source.Disposals);
    }

    [Fact]
    public async Task PendingReadFailurePreservesOriginalErrorAndDisposesOnce()
    {
        using var tracker = new PoolTracker();
        var source = new PendingFailingSource();
        var opening = ParquetFile.OpenAsync(source, ParquetSourceOwnership.ParquetFile, ParquetReaderOptions.Default, CancellationToken.None).AsTask();
        source.Fail(new IOException("primary source read failure"));
        var exception = await Assert.ThrowsAsync<IOException>(() => opening);
        Assert.Equal("primary source read failure", exception.Message);
        Assert.Equal(1, source.Disposals);
    }

    [Fact]
    public async Task CancellationWithThrowingDisposalPreservesCancellation()
    {
        using var tracker = new PoolTracker();
        var bytes = ParquetFixtureBuilder.CreateInt32(new() { Values = [1, 2, 3] });
        var source = new ThrowingDisposalSource(bytes);
        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
            await ParquetFile.OpenAsync(source, ParquetSourceOwnership.ParquetFile, ParquetReaderOptions.Default, cancellation.Token));
        Assert.Equal(1, source.Disposals);
    }

    [Fact]
    public async Task CallerOwnedSourceIsNotDisposedOnOpenFailure()
    {
        using var tracker = new PoolTracker();
        var source = new SyncFailingSource();
        await Assert.ThrowsAsync<IOException>(async () =>
            await ParquetFile.OpenAsync(source, ParquetSourceOwnership.Caller, ParquetReaderOptions.Default, CancellationToken.None));
        Assert.Equal(0, source.Disposals);
    }

    [Fact]
    public async Task OwnedFailingStreamPreservesReadError()
    {
        using var tracker = new PoolTracker();
        var stream = new FailingStream(ParquetFixtureBuilder.CreateInt32(new() { Values = [1, 2, 3] }));
        var exception = await Assert.ThrowsAsync<IOException>(async () =>
            await ParquetFile.OpenAsync(stream, ParquetSourceOwnership.ParquetFile, ParquetReaderOptions.Default, CancellationToken.None));
        Assert.Equal("primary stream read failure", exception.Message);
        Assert.Equal(1, stream.Disposals);
    }

    [Fact]
    public async Task CallerOwnedFailingStreamIsNotDisposedOnOpenFailure()
    {
        using var tracker = new PoolTracker();
        var stream = new FailingStream(ParquetFixtureBuilder.CreateInt32(new() { Values = [1, 2, 3] }));
        await Assert.ThrowsAsync<IOException>(async () =>
            await ParquetFile.OpenAsync(stream, ParquetSourceOwnership.Caller, ParquetReaderOptions.Default, CancellationToken.None));
        Assert.Equal(0, stream.Disposals);
        Assert.True(stream.CanRead);
    }

    [Fact]
    public async Task OwnedDisposalFailureIsReportedWithoutEarlierError()
    {
        using var tracker = new PoolTracker();
        var bytes = ParquetFixtureBuilder.CreateInt32(new() { Values = [1, 2, 3] });
        var source = new ThrowingDisposalSource(bytes);
        var file = await ParquetFile.OpenAsync(source, ParquetSourceOwnership.ParquetFile, ParquetReaderOptions.Default, CancellationToken.None);
        var exception = await Assert.ThrowsAsync<InvalidOperationException>(async () => await file.DisposeAsync());
        Assert.Equal("secondary source disposal failure", exception.Message);
        Assert.Equal(1, source.Disposals);
        await Assert.ThrowsAsync<InvalidOperationException>(async () => await file.DisposeAsync());
        Assert.Equal(1, source.Disposals);
    }

    [Fact]
    public async Task MissingPathFailsWithoutCreatingSource()
    {
        var missing = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString("N") + ".parquet");
        await Assert.ThrowsAsync<FileNotFoundException>(async () =>
            await ParquetFile.OpenAsync(missing, ParquetReaderOptions.Default, CancellationToken.None));
    }

    private sealed class SyncFailingSource : IParquetRandomAccessSource
    {
        public int Disposals { get; private set; }
        public long Length => 100;
        public ValueTask ReadExactlyAsync(long offset, ArraySegment<byte> destination, CancellationToken cancellationToken) =>
            throw new IOException("primary source read failure");
        public ValueTask DisposeAsync()
        {
            Disposals++;
            throw new InvalidOperationException("secondary source disposal failure");
        }
    }

    private sealed class PendingFailingSource : IParquetRandomAccessSource
    {
        private readonly TaskCompletionSource _gate = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public int Disposals { get; private set; }
        public long Length => 100;
        public void Fail(Exception error) => _gate.TrySetException(error);
        public ValueTask ReadExactlyAsync(long offset, ArraySegment<byte> destination, CancellationToken cancellationToken) =>
            new(_gate.Task);
        public ValueTask DisposeAsync()
        {
            Disposals++;
            throw new InvalidOperationException("secondary source disposal failure");
        }
    }

    private sealed class ThrowingDisposalSource(byte[] bytes) : IParquetRandomAccessSource
    {
        public int Disposals { get; private set; }
        public long Length => bytes.Length;
        public ValueTask ReadExactlyAsync(long offset, ArraySegment<byte> destination, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            bytes.AsMemory(checked((int)offset), destination.Count).CopyTo(destination.AsMemory());
            return ValueTask.CompletedTask;
        }

        public ValueTask DisposeAsync()
        {
            Disposals++;
            throw new InvalidOperationException("secondary source disposal failure");
        }
    }


    private sealed class FailingStream : MemoryStream
    {
        public FailingStream(byte[] bytes)
            : base(bytes, writable: false)
        {
        }

        public int Disposals { get; private set; }
        public override int Read(byte[] buffer, int offset, int count) =>
            throw new IOException("primary stream read failure");
        public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken) =>
            throw new IOException("primary stream read failure");
        protected override void Dispose(bool disposing)
        {
            Disposals++;
            throw new InvalidOperationException("secondary stream disposal failure");
        }
    }
}


