namespace Lokad.Parquet.Tests;

using System.Reflection;

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
        using var tracker = new PoolTracker();
        byte[] bytes = ParquetFixtureBuilder.CreateInt32(new()
        {
            PhysicalTypeCode = (int)ParquetPhysicalType.ByteArray,
            PhysicalValues = new byte[][] { [1, 2], [3, 4], [1, 2] },
            DictionaryValues = new byte[][] { [1, 2], [3, 4] },
            DictionaryIndices = [0, 1, 0],
        });
        await using var file = await ParquetFile.OpenAsync(new MemoryStream(bytes, writable: false));
        await using var enumerator = file.ScanAsync(new([file.Metadata.Schema.Columns[0]])).GetAsyncEnumerator();
        Assert.True(await enumerator.MoveNextAsync());
        ParquetBatch batch = enumerator.Current;
        object? ownersValue = batch.GetType().GetField("_owners", BindingFlags.NonPublic | BindingFlags.Instance)?.GetValue(batch);
        IDisposable[] owners = Assert.IsAssignableFrom<IDisposable[]>(ownersValue);
        Assert.True(owners.Length >= 2);
        owners[0] = new ThrowAfterDispose(owners[0]);
        Assert.Throws<IOException>(batch.Dispose);
        Assert.True(batch.IsDisposed);
        batch.Dispose();
    }

    [Fact]
    public async Task FileDisposalReleasesDictionaryWhenPageCleanupThrows()
    {
        // A throwing pool-return observer during file disposal must not skip the
        // retained dictionary: every array is still released and a repeated disposal
        // reports the same failure without further side effects.
        var outstanding = new PoolOutstandingArrays();
        var throwingEnabled = false;
        var injected = false;
        PoolTracker.SetObservers(
            (array, _) => outstanding.NoteRent(array),
            (array, _) =>
            {
                outstanding.NoteReturn(array);
                if (throwingEnabled && !injected)
                {
                    injected = true;
                    throw new IOException("Injected pool return failure.");
                }
            });
        try
        {
            byte[] bytes = ParquetFixtureBuilder.CreateInt32(new()
            {
                PhysicalTypeCode = (int)ParquetPhysicalType.ByteArray,
                PhysicalValues = new byte[][] { [1, 2], [3, 4], [1, 2] },
                DictionaryValues = new byte[][] { [1, 2], [3, 4] },
                DictionaryIndices = [0, 1, 0],
            });
            ParquetFile file = await ParquetFile.OpenAsync((ReadOnlyMemory<byte>)bytes);
            var scan = file.ScanAsync(new(file.Metadata.Schema.Columns, null, null, 1)).GetAsyncEnumerator();
            Assert.True(await scan.MoveNextAsync());
            scan.Current.Dispose();
            throwingEnabled = true;
            IOException first = await Assert.ThrowsAsync<IOException>(async () => await file.DisposeAsync());
            Assert.True(injected);
            throwingEnabled = false;
            await scan.DisposeAsync();
            Assert.True(outstanding.IsEmpty);
            IOException second = await Assert.ThrowsAsync<IOException>(async () => await file.DisposeAsync());
            Assert.Same(first, second);
            Assert.True(outstanding.IsEmpty);
        }
        finally
        {
            PoolTracker.ClearObservers();
        }
    }

    [Fact]
    public async Task ProjectedDisposalUnregistersScanWhenRetainedOwnerThrows()
    {
        // A throwing retained batch owner must not prevent scan unregistration:
        // a replacement scan on the same file still registers and reads.
        using var tracker = new PoolTracker();
        byte[] bytes = ParquetFixtureBuilder.CreateRequiredInt32Columns(
        [
            new() { Name = "a", Pages = [[1, 2, 3, 4, 5, 6]] },
            new() { Name = "b", Pages = [[1, 2], [3, 4, 5, 6]] },
        ]);
        await using var file = await ParquetFile.OpenAsync((ReadOnlyMemory<byte>)bytes);
        var scan = file.ScanAsync(new(file.Metadata.Schema.Columns)).GetAsyncEnumerator();
        Assert.True(await scan.MoveNextAsync());
        scan.Current.Dispose();
        object? batchesValue = scan.GetType().GetField("_sourceBatches", BindingFlags.NonPublic | BindingFlags.Instance)?.GetValue(scan);
        Array batches = Assert.IsAssignableFrom<Array>(batchesValue);
        object retained = Assert.IsAssignableFrom<object>(batches.GetValue(0));
        object? ownersValue = retained.GetType().GetField("Owners", BindingFlags.NonPublic | BindingFlags.Instance)?.GetValue(retained);
        IDisposable[] owners = Assert.IsAssignableFrom<IDisposable[]>(ownersValue);
        Assert.True(owners.Length > 0);
        owners[0] = new ThrowAfterDispose(owners[0]);
        await Assert.ThrowsAsync<IOException>(async () => await scan.DisposeAsync());
        await using (var replacement = file.ScanAsync(new(file.Metadata.Schema.Columns)).GetAsyncEnumerator())
        {
            Assert.True(await replacement.MoveNextAsync());
            replacement.Current.Dispose();
        }
    }

    [Fact]
    public async Task CancelledDecodePreservesCancellationWhenCleanupThrows()
    {
        // Cancellation injected on the definition-level rent unwinds through page,
        // emission, and scan teardown while every pool return also throws. The
        // original cancellation must still surface instead of a teardown error.
        var outstanding = new PoolOutstandingArrays();
        using var cancellation = new CancellationTokenSource();
        var throwingEnabled = true;
        var fired = false;
        PoolTracker.SetObservers(
            (array, requested) =>
            {
                outstanding.NoteRent(array);
                if (array is int[] && requested == 3)
                {
                    fired = true;
                    cancellation.Cancel();
                }
            },
            (array, _) =>
            {
                outstanding.NoteReturn(array);
                if (throwingEnabled && cancellation.IsCancellationRequested)
                {
                    throw new IOException("Injected teardown failure.");
                }
            });
        try
        {
            byte[] bytes = ParquetFixtureBuilder.CreateInt32(new()
            {
                PhysicalTypeCode = (int)ParquetPhysicalType.ByteArray,
                PhysicalValues = new byte[][] { [1, 2], [3, 4], [5, 6] },
                Repetition = ParquetRepetition.Optional,
                Validity = [true, false, true],
            });
            await using var file = await ParquetFile.OpenAsync((ReadOnlyMemory<byte>)bytes);
            await using var scan = file.ScanAsync(new([file.Metadata.Schema.Columns[0]]), cancellation.Token).GetAsyncEnumerator();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await scan.MoveNextAsync());
            Assert.True(fired);
            throwingEnabled = false;
        }
        finally
        {
            PoolTracker.ClearObservers();
        }

        Assert.True(outstanding.IsEmpty);
    }

    private sealed class ThrowAfterDispose(IDisposable inner) : IDisposable
    {
        public void Dispose()
        {
            inner.Dispose();
            throw new IOException("Injected owner failure.");
        }
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
