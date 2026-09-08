namespace Lokad.Parquet.Tests;

using System.Threading.Tasks.Sources;

public sealed class ValueTaskConsumptionTests
{
    [Fact]
    public async Task SuccessfulSourceValueTasksAreConsumedExactlyOnce()
    {
        using var tracker = new PoolTracker();
        var bytes = ParquetFixtureBuilder.CreateInt32(new() { Values = [1] });
        var source = new CountingValueTaskSource(bytes);
        await using (var file = await ParquetFile.OpenAsync(source))
        {
            await foreach (var batch in file.ScanAsync(new([file.Metadata.Schema.Columns[0]])))
            {
                batch.Dispose();
            }
        }

        Assert.True(source.Reads > 0);
        Assert.Equal(source.Reads, source.GetResultCalls);
    }

    [Fact]
    public async Task FailedSourceReadsStillTranslateTruncation()
    {
        using var tracker = new PoolTracker();
        var source = new FailingSource();
        await Assert.ThrowsAsync<ParquetFormatException>(async () =>
        {
            await ParquetFile.OpenAsync(source);
        });
        Assert.Equal(1, source.GetResultCalls);
    }

    private sealed class CountingValueTaskSource : IParquetRandomAccessSource, IValueTaskSource
    {
        private readonly byte[] _bytes;
        public CountingValueTaskSource(byte[] bytes) => _bytes = bytes;
        public int Reads { get; private set; }
        public int GetResultCalls { get; private set; }
        public long Length => _bytes.Length;
        public ValueTask ReadExactlyAsync(long offset, ArraySegment<byte> destination, CancellationToken cancellationToken)
        {
            _bytes.AsMemory((int)offset, destination.Count).CopyTo(destination.AsMemory());
            Reads++;
            return new ValueTask(this, 0);
        }

        public void GetResult(short token) => GetResultCalls++;
        public ValueTaskSourceStatus GetStatus(short token) => ValueTaskSourceStatus.Succeeded;
        public void OnCompleted(Action<object?> continuation, object? state, short token, ValueTaskSourceOnCompletedFlags flags) => throw new InvalidOperationException();
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    private sealed class FailingSource : IParquetRandomAccessSource, IValueTaskSource
    {
        private readonly TaskCompletionSource _completion = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public int GetResultCalls { get; private set; }
        public long Length => 100;
        public ValueTask ReadExactlyAsync(long offset, ArraySegment<byte> destination, CancellationToken cancellationToken)
        {
            return new ValueTask(this, 0);
        }

        public void GetResult(short token)
        {
            GetResultCalls++;
            throw new EndOfStreamException("test truncation");
        }

        public ValueTaskSourceStatus GetStatus(short token) => ValueTaskSourceStatus.Succeeded;
        public void OnCompleted(Action<object?> continuation, object? state, short token, ValueTaskSourceOnCompletedFlags flags) => throw new InvalidOperationException();
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }
}
