namespace Lokad.Parquet.Tests;

public sealed class OwnedStreamDisposalTests
{
    [Fact]
    public async Task OwnedMemoryStreamSubclassDisposalCompletesAsynchronously()
    {
        var bytes = ParquetFixtureBuilder.CreateInt32(new() { Values = [1, 2, 3] });
        var stream = new AsyncDisposalMemoryStream(bytes);
        var file = await ParquetFile.OpenAsync(stream, ParquetSourceOwnership.ParquetFile, ParquetReaderOptions.Default, CancellationToken.None);
        var disposal = file.DisposeAsync();
        Assert.False(disposal.IsCompletedSuccessfully);
        stream.CompleteDisposal();
        await disposal;
        Assert.True(stream.IsBaseDisposed);
    }

    [Fact]
    public async Task OwnedFaultedStreamDisposalSurfacesFailureWithoutLeaking()
    {
        // A subclass whose DisposeAsync faults must still let file teardown
        // finish: the stream error surfaces, but pooled file storage returns.
        using var tracker = new PoolTracker();
        var bytes = ParquetFixtureBuilder.CreateInt32(new() { Values = [1, 2, 3] });
        var stream = new FaultedDisposalMemoryStream(bytes);
        var file = await ParquetFile.OpenAsync(stream, ParquetSourceOwnership.ParquetFile, ParquetReaderOptions.Default, CancellationToken.None);
        var exception = await Assert.ThrowsAsync<InvalidOperationException>(async () => await file.DisposeAsync());
        Assert.Equal("Test stream disposal failure.", exception.Message);
    }

    [Fact]
    public async Task OwnedPlainMemoryStreamDisposesSynchronously()
    {
        var bytes = ParquetFixtureBuilder.CreateInt32(new() { Values = [1] });
        using var stream = new MemoryStream(bytes, writable: false);
        var file = await ParquetFile.OpenAsync(stream, ParquetSourceOwnership.ParquetFile, ParquetReaderOptions.Default, CancellationToken.None);
        var disposal = file.DisposeAsync();
        Assert.True(disposal.IsCompletedSuccessfully);
        await disposal;
        Assert.False(stream.CanRead);
    }
    public sealed class FaultedDisposalMemoryStream : MemoryStream
    {
        public FaultedDisposalMemoryStream(byte[] bytes)
            : base(bytes, writable: false)
        {
        }

        public override ValueTask DisposeAsync() =>
            ValueTask.FromException(new InvalidOperationException("Test stream disposal failure."));
    }
    public sealed class AsyncDisposalMemoryStream : MemoryStream
    {
        private readonly TaskCompletionSource _disposal = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public AsyncDisposalMemoryStream(byte[] bytes)
            : base(bytes, writable: false)
        {
        }

        public bool IsBaseDisposed { get; private set; }

        public override ValueTask DisposeAsync() => new(_disposal.Task);
        public void CompleteDisposal()
        {
            base.Dispose();
            IsBaseDisposed = true;
            _disposal.SetResult();
        }
    }
}
