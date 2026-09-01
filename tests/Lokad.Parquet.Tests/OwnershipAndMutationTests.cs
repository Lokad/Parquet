using System.Diagnostics;

namespace Lokad.Parquet.Tests;

public sealed class OwnershipAndMutationTests
{
    [Fact]
    public async Task MultiColumnEarlyDisposalReturnsEveryPooledBuffer()
    {
        using var tracker = new PoolTracker();
        var bytes = ParquetFixtureBuilder.CreateRequiredInt32Columns(
        [
            new RequiredInt32FixtureColumn { Name = "left", Pages = [[1], [2, 3]] },
            new RequiredInt32FixtureColumn { Name = "right", Pages = [[4, 5], [6]] },
        ]);
        await using var file = await ParquetFile.OpenAsync(new MemoryStream(bytes, writable: false));
        await using var enumerator = file.ScanAsync(new ParquetScanOptions([file.Metadata.Schema.Columns[0], file.Metadata.Schema.Columns[1]])).GetAsyncEnumerator();

        Assert.True(await enumerator.MoveNextAsync());
        using var batch = enumerator.Current;
        Assert.Equal(2, batch.Columns.Count);
    }

    [Fact]
    public async Task ScanMemoryBudgetRejectsActualPoolCapacityAndReturnsTheRent()
    {
        using var tracker = new PoolTracker();
        var bytes = ParquetFixtureBuilder.CreateInt32(new() { Values = [1] });
        await using var file = await ParquetFile.OpenAsync(
            new MemoryStream(bytes, writable: false),
            ParquetSourceOwnership.Caller,
            new ParquetReaderOptions { MaximumScanPooledBytes = 1 },
            CancellationToken.None);

        await Assert.ThrowsAsync<ParquetLimitExceededException>(async () =>
        {
            await foreach (var batch in file.ScanAsync(new([file.Metadata.Schema.Columns[0]])))
                batch.Dispose();
        });
    }

    [Fact]
    public async Task ScanShortensBatchesToFitRemainingPoolBudget()
    {
        using var tracker = new PoolTracker();
        var bytes = ParquetFixtureBuilder.CreateInt32(new()
        {
            Values = Enumerable.Range(0, 64).ToArray(),
        });
        await using var file = await ParquetFile.OpenAsync(
            new MemoryStream(bytes, writable: false),
            ParquetSourceOwnership.Caller,
            new ParquetReaderOptions { MaximumScanPooledBytes = 400 },
            CancellationToken.None);
        var batchSizes = new List<int>();

        await foreach (var batch in file.ScanAsync(new([file.Metadata.Schema.Columns[0]])))
        {
            using (batch)
                batchSizes.Add(batch.RowCount);
        }

        Assert.Equal([32, 32], batchSizes);
    }

    [Fact]
    public async Task MultiColumnReadsAreSequential()
    {
        var bytes = ParquetFixtureBuilder.CreateRequiredInt32Columns(
        [
            new RequiredInt32FixtureColumn { Name = "left", Pages = [[1, 2]] },
            new RequiredInt32FixtureColumn { Name = "right", Pages = [[3, 4]] },
        ]);
        var source = new ConcurrentRecordingSource(bytes);
        await using var file = await ParquetFile.OpenAsync(source);

        source.ResetPeak();
        await foreach (var batch in file.ScanAsync(new ParquetScanOptions([file.Metadata.Schema.Columns[0], file.Metadata.Schema.Columns[1]])))
            batch.Dispose();
        Assert.Equal(1, source.PeakConcurrentReads);
    }

    [Fact]
    public async Task OpenFileRejectsOverlappingScans()
    {
        var bytes = ParquetFixtureBuilder.CreateInt32(new() { Values = [1, 2, 3] });
        await using var file = await ParquetFile.OpenAsync(new MemoryStream(bytes, writable: false));
        await using var first = file.ScanAsync(new([file.Metadata.Schema.Columns[0]])).GetAsyncEnumerator();
        Assert.True(await first.MoveNextAsync());

        var exception = Assert.Throws<InvalidOperationException>(
            () => file.ScanAsync(new([file.Metadata.Schema.Columns[0]])).GetAsyncEnumerator());
        Assert.Contains("Only one scan", exception.Message, StringComparison.Ordinal);

        first.Current.Dispose();
    }

    [Fact]
    public async Task CompleteScanReturnsEveryObservedPoolArrayExactlyOnce()
    {
        using var tracker = new PoolTracker();
        var bytes = ParquetFixtureBuilder.CreateInt32(new() { Values = [1, 2, 3] });
        await using (var file = await ParquetFile.OpenAsync(new MemoryStream(bytes, writable: false)))
        {
            await foreach (var batch in file.ScanAsync(new([file.Metadata.Schema.Columns[0]])))
                batch.Dispose();
        }

        Assert.True(tracker.RentCount >= 3);
    }

    [Fact]
    public async Task ConcurrentBatchAndFileDisposalReturnEveryPoolArrayExactlyOnce()
    {
        using var tracker = new PoolTracker();
        var bytes = ParquetFixtureBuilder.CreateInt32(new() { Values = [1, 2, 3] });

        for (var iteration = 0; iteration < 64; iteration++)
        {
            var file = await ParquetFile.OpenAsync(bytes);
            var enumerator = file.ScanAsync(new([file.Metadata.Schema.Columns[0]])).GetAsyncEnumerator();
            Assert.True(await enumerator.MoveNextAsync());
            var batch = enumerator.Current;
            using var gate = new Barrier(2);

            var disposeFile = Task.Run(async () =>
            {
                gate.SignalAndWait();
                await file.DisposeAsync();
            });
            var disposeBatch = Task.Run(() =>
            {
                gate.SignalAndWait();
                batch.Dispose();
            });

            await Task.WhenAll(disposeFile, disposeBatch);
            await enumerator.DisposeAsync();
        }
    }

    [Fact]
    public async Task GrowingPagesReplaceSmallerCachedArraysWithoutLeakingTheirBudget()
    {
        using var tracker = new PoolTracker();
        var bytes = ParquetFixtureBuilder.CreateRequiredInt32Columns(
        [
            new RequiredInt32FixtureColumn
            {
                Name = "value",
                Pages = [[1], Enumerable.Range(0, 256).ToArray()],
            },
        ]);
        await using var file = await ParquetFile.OpenAsync(bytes);

        await foreach (var batch in file.ScanAsync(new([file.Metadata.Schema.Columns[0]])))
            batch.Dispose();

        Assert.True(tracker.ReturnCount > 0);
    }

    [Fact]
    public async Task LateReturnReleasesBudgetWhenPoolInstrumentationThrows()
    {
        var bytes = ParquetFixtureBuilder.CreateInt32(new() { Values = [1] });
        var file = await ParquetFile.OpenAsync(bytes);
        var enumerator = file.ScanAsync(new([file.Metadata.Schema.Columns[0]])).GetAsyncEnumerator();
        Assert.True(await enumerator.MoveNextAsync());
        var batch = enumerator.Current;
        await file.DisposeAsync();

        var observer = PoolTracker.ReturnObserver;
        Assert.Null(observer.GetValue(null));
        observer.SetValue(null, (Action<Array, int>)((_, _) =>
            throw new InvalidOperationException("Injected pool observer failure.")));
        try
        {
            Assert.Throws<InvalidOperationException>(batch.Dispose);
            batch.Dispose();
        }
        finally
        {
            observer.SetValue(null, null);
            await enumerator.DisposeAsync();
        }
    }

    [Fact]
    public async Task FailureAndEarlyEnumeratorDisposalReturnEveryPoolArray()
    {
        using var tracker = new PoolTracker();
        var invalid = ParquetFixtureBuilder.CreateInt32(new()
        {
            Values = [1],
            CrcMode = FixtureCrcMode.Corrupt,
        });
        await using (var file = await ParquetFile.OpenAsync(new MemoryStream(invalid, writable: false)))
        {
            await Assert.ThrowsAsync<ParquetFormatException>(async () =>
            {
                await foreach (var batch in file.ScanAsync(new([file.Metadata.Schema.Columns[0]])))
                    batch.Dispose();
            });
        }

        var valid = ParquetFixtureBuilder.CreateInt32(new() { Values = [1, 2, 3] });
        await using (var file = await ParquetFile.OpenAsync(new MemoryStream(valid, writable: false)))
        {
            var enumerator = file.ScanAsync(new(
                [file.Metadata.Schema.Columns[0]],
                null,
                null,
                1)).GetAsyncEnumerator();
            Assert.True(await enumerator.MoveNextAsync());
            var batch = enumerator.Current;
            await enumerator.DisposeAsync();
            Assert.Equal([1], Assert.IsType<ParquetPrimitiveColumnBatch<int>>(batch.Columns[0]).Values.ToArray());
            batch.Dispose();
        }
    }

    [Fact]
    public async Task CallerCancellationReturnsEveryPoolArray()
    {
        using var tracker = new PoolTracker();
        var source = new BlockingSource(ParquetFixtureBuilder.CreateInt32(new() { Values = [1] }));
        await using var file = await ParquetFile.OpenAsync(source);
        using var cancellation = new CancellationTokenSource();
        await using var enumerator = file.ScanAsync(new([file.Metadata.Schema.Columns[0]]), cancellation.Token).GetAsyncEnumerator();
        source.BlockReads = true;

        var move = enumerator.MoveNextAsync().AsTask();
        await source.ReadStarted.Task.WaitAsync(TimeSpan.FromSeconds(5));
        cancellation.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => move);
        Assert.False(source.IsDisposed);
    }

    [Fact]
    public async Task MultiColumnCancellationReturnsEveryPoolArray()
    {
        using var tracker = new PoolTracker();
        var bytes = ParquetFixtureBuilder.CreateRequiredInt32Columns(
        [
            new RequiredInt32FixtureColumn { Name = "left", Pages = [[1, 2]] },
            new RequiredInt32FixtureColumn { Name = "right", Pages = [[3, 4]] },
        ]);
        var source = new BlockingSource(bytes);
        await using var file = await ParquetFile.OpenAsync(source);
        using var cancellation = new CancellationTokenSource();
        await using var enumerator = file.ScanAsync(
            new ParquetScanOptions([file.Metadata.Schema.Columns[0], file.Metadata.Schema.Columns[1]]),
            cancellation.Token).GetAsyncEnumerator();
        source.BlockReads = true;

        var move = enumerator.MoveNextAsync().AsTask();
        await source.ReadStarted.Task.WaitAsync(TimeSpan.FromSeconds(5));
        cancellation.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => move);
    }

    [Fact]
    public async Task MultiColumnSourceFailureDisposesSuccessfulSiblingBatch()
    {
        using var tracker = new PoolTracker();
        var bytes = ParquetFixtureBuilder.CreateRequiredInt32Columns(
        [
            new RequiredInt32FixtureColumn { Name = "left", Pages = [[1, 2]] },
            new RequiredInt32FixtureColumn { Name = "right", Pages = [[3, 4]] },
        ]);
        var source = new SelectiveFailureSource(bytes);
        await using var file = await ParquetFile.OpenAsync(source);
        source.FailingOffset = file.Metadata.RowGroups[0].Columns[1].DataPageOffset;

        await Assert.ThrowsAsync<IOException>(async () =>
        {
            await foreach (var batch in file.ScanAsync(new ParquetScanOptions([file.Metadata.Schema.Columns[0], file.Metadata.Schema.Columns[1]])))
                batch.Dispose();
        });
    }

    [Fact]
    public async Task ShortPageReadIsClassifiedAndReturnsEveryPoolArray()
    {
        using var tracker = new PoolTracker();
        var source = new SelectiveFailureSource(
            ParquetFixtureBuilder.CreateInt32(new() { Values = [1, 2] }));
        await using var file = await ParquetFile.OpenAsync(source);
        source.FailingOffset = file.Metadata.RowGroups[0].Columns[0].DataPageOffset;
        source.ThrowEndOfStream = true;

        await Assert.ThrowsAsync<ParquetFormatException>(async () =>
        {
            await foreach (var batch in file.ScanAsync(new([file.Metadata.Schema.Columns[0]])))
                batch.Dispose();
        });
    }

    [Fact]
    public async Task UnsupportedProjectionIsRejectedBeforePayloadIo()
    {
        var source = new RecordingSource(ParquetFixtureBuilder.CreateInt32(new()
        {
            Values = [1],
            PhysicalTypeCode = (int)ParquetPhysicalType.Int96,
        }));
        await using var file = await ParquetFile.OpenAsync(source);
        source.ReadCount = 0;

        Assert.Throws<ParquetUnsupportedFeatureException>(() =>
            file.ScanAsync(new([file.Metadata.Schema.Columns[0]])).GetAsyncEnumerator());
        Assert.Equal(0, source.ReadCount);
    }

    [Fact]
    public async Task FileDisposalCancelsActiveReadReturnsBuffersAndThenDisposesOwnedSource()
    {
        using var tracker = new PoolTracker();
        var source = new BlockingSource(ParquetFixtureBuilder.CreateInt32(new() { Values = [1] }));
        var file = await ParquetFile.OpenAsync(
            source,
            ParquetSourceOwnership.ParquetFile,
            ParquetReaderOptions.Default,
            CancellationToken.None);
        await using var enumerator = file.ScanAsync(new([file.Metadata.Schema.Columns[0]])).GetAsyncEnumerator();
        source.BlockReads = true;

        var move = enumerator.MoveNextAsync().AsTask();
        await source.ReadStarted.Task.WaitAsync(TimeSpan.FromSeconds(5));
        var dispose = file.DisposeAsync().AsTask();

        await Assert.ThrowsAsync<ObjectDisposedException>(() => move);
        await dispose;
        Assert.True(source.IsDisposed);
    }

    [Fact]
    public async Task OwnedSourceIsDisposedWhenOpenFailsOnShortRead()
    {
        var source = new AlwaysShortSource(128);

        await Assert.ThrowsAsync<ParquetFormatException>(async () =>
            await ParquetFile.OpenAsync(
                source,
                ParquetSourceOwnership.ParquetFile,
                ParquetReaderOptions.Default,
                CancellationToken.None));

        Assert.True(source.IsDisposed);
    }

    [Fact]
    public async Task StreamDefaultsToCallerOwnershipAndCanBeTransferred()
    {
        var bytes = ParquetFixtureBuilder.CreateInt32(new() { RowGroupMode = FixtureRowGroupMode.Omitted });
        var borrowed = new MemoryStream(bytes, writable: false);
        await using (var file = await ParquetFile.OpenAsync(borrowed)) { }
        Assert.True(borrowed.CanRead);

        var owned = new MemoryStream(bytes, writable: false);
        await using (var file = await ParquetFile.OpenAsync(
            owned,
            ParquetSourceOwnership.ParquetFile,
            ParquetReaderOptions.Default,
            CancellationToken.None)) { }
        Assert.False(owned.CanRead);
    }

    [Fact]
    public async Task IdleBuiltInAndCallerOwnedSourcesDisposeSynchronously()
    {
        var bytes = ParquetFixtureBuilder.CreateInt32(new() { RowGroupMode = FixtureRowGroupMode.Omitted });

        var memoryFile = await ParquetFile.OpenAsync(bytes);
        var memoryDisposal = memoryFile.DisposeAsync();
        Assert.True(memoryDisposal.IsCompletedSuccessfully);
        await memoryDisposal;

        var stream = new MemoryStream(bytes, writable: false);
        var streamFile = await ParquetFile.OpenAsync(
            stream,
            ParquetSourceOwnership.ParquetFile,
            ParquetReaderOptions.Default,
            CancellationToken.None);
        var streamDisposal = streamFile.DisposeAsync();
        Assert.True(streamDisposal.IsCompletedSuccessfully);
        await streamDisposal;
        Assert.False(stream.CanRead);

        var source = new RecordingSource(bytes);
        var sourceFile = await ParquetFile.OpenAsync(source);
        var sourceDisposal = sourceFile.DisposeAsync();
        Assert.True(sourceDisposal.IsCompletedSuccessfully);
        await sourceDisposal;
    }

    [Fact]
    public async Task CustomSourceReceivesArraySegmentsAndRejectsInvalidOwnershipBeforeIo()
    {
        var source = new RecordingSource(ParquetFixtureBuilder.CreateInt32(new()
        {
            RowGroupMode = FixtureRowGroupMode.Omitted,
        }));

        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(async () =>
            await ParquetFile.OpenAsync(
                source,
                (ParquetSourceOwnership)42,
                ParquetReaderOptions.Default,
                CancellationToken.None));
        Assert.Equal(0, source.ReadCount);

        await using var file = await ParquetFile.OpenAsync(source);
        Assert.True(source.MaximumDestinationOffset > 0);
    }

    [Fact]
    public async Task DeterministicBoundedMutationsHaveOnlyClassifiedOutcomes()
    {
        const int seed = 0x51A7E;
        var random = new Random(seed);
        var baseline = ParquetFixtureBuilder.CreateInt32(new() { Values = [1, 2, 3] });
        var options = new ParquetReaderOptions
        {
            MaximumFooterBytes = 4096,
            MaximumMetadataStringBytes = 4096,
            MaximumSchemaElements = 32,
            MaximumLeafColumns = 16,
            MaximumRowGroups = 16,
            MaximumThriftContainerElements = 64,
        };
        var stopwatch = Stopwatch.StartNew();

        for (var iteration = 0; iteration < 256; iteration++)
        {
            var mutation = baseline.ToArray();
            var changes = random.Next(1, 4);
            for (var change = 0; change < changes; change++)
                mutation[random.Next(mutation.Length)] ^= (byte)(1 << random.Next(8));

            try
            {
                await using var file = await ParquetFile.OpenAsync(
                    new MemoryStream(mutation, writable: false),
                    ParquetSourceOwnership.Caller,
                    options,
                    CancellationToken.None);
            }
            catch (ParquetException)
            {
            }
            catch (Exception exception)
            {
                Assert.Fail($"Mutation seed {seed}, iteration {iteration} produced {exception.GetType().FullName}: {exception.Message}");
            }
        }

        Assert.True(stopwatch.Elapsed < TimeSpan.FromSeconds(10),
            $"Bounded mutation run exceeded its 10-second cap: {stopwatch.Elapsed}.");
    }

    [Fact]
    public async Task DeterministicPageAndScanMutationsHaveOnlyClassifiedOutcomes()
    {
        const int seed = 0xC0DEC;
        var random = new Random(seed);
        var baselines = new[]
        {
            ParquetFixtureBuilder.CreateInt32(new()
            {
                Values = [1, 2, 3],
                CompressionCodec = ParquetCompressionCodec.Snappy,
                CrcMode = FixtureCrcMode.Valid,
            }),
            ParquetFixtureBuilder.CreateInt32(new()
            {
                PhysicalTypeCode = (int)ParquetPhysicalType.ByteArray,
                PhysicalValues = new byte[][] { [], [1, 2], [3] },
                Repetition = ParquetRepetition.Optional,
                Validity = [true, false, true],
                PageVersion = FixturePageVersion.DataPageV2,
                CompressionCodec = ParquetCompressionCodec.Snappy,
            }),
            ParquetFixtureBuilder.CreateInt32(new()
            {
                Values = [10, 20, 10],
                DictionaryValues = new int[] { 10, 20 },
                DictionaryIndices = [0, 1, 0],
                TrailingPlainValues = new int[] { 30 },
                CrcMode = FixtureCrcMode.Valid,
            }),
            ParquetFixtureBuilder.CreateInt32(new()
            {
                PhysicalTypeCode = (int)ParquetPhysicalType.FixedLengthByteArray,
                TypeLength = 2,
                PhysicalValues = new byte[][] { [1, 2], [3, 4] },
            }),
            ParquetFixtureBuilder.CreateRequiredInt32Columns(
            [
                new RequiredInt32FixtureColumn { Name = "left", Pages = [[1], [2, 3]] },
                new RequiredInt32FixtureColumn { Name = "right", Pages = [[4, 5], [6]] },
            ]),
            ParquetFixtureBuilder.CreateRequiredInt32RowGroups([[1, 2], [3, 4, 5]]),
        };
        var options = new ParquetReaderOptions
        {
            MaximumFooterBytes = 4096,
            MaximumMetadataStringBytes = 4096,
            MaximumSchemaElements = 32,
            MaximumLeafColumns = 16,
            MaximumRowGroups = 16,
            MaximumThriftContainerElements = 64,
            MaximumPageHeaderBytes = 4096,
            MaximumCompressedPageBytes = 4096,
            MaximumUncompressedPageBytes = 4096,
            MaximumPagesPerColumnChunk = 64,
            MaximumDictionaryEntries = 256,
            MaximumDictionaryBytes = 4096,
            MaximumValuesPerPage = 4096,
            MaximumBinaryValueBytes = 4096,
            MaximumRowsPerBatch = 4096,
            MaximumBinaryBatchBytes = 4096,
            MaximumScanPooledBytes = 1024 * 1024,
        };
        var stopwatch = Stopwatch.StartNew();

        for (var baselineOrdinal = 0; baselineOrdinal < baselines.Length; baselineOrdinal++)
        {
            for (var iteration = 0; iteration < 96; iteration++)
            {
                var mutation = baselines[baselineOrdinal].ToArray();
                var changes = random.Next(1, 4);
                for (var change = 0; change < changes; change++)
                    mutation[random.Next(mutation.Length)] ^= (byte)(1 << random.Next(8));

                try
                {
                    await using var file = await ParquetFile.OpenAsync(
                        new MemoryStream(mutation, writable: false),
                        ParquetSourceOwnership.Caller,
                        options,
                        CancellationToken.None);
                    var ordinals = file.Metadata.Schema.Columns
                        .Where(static column => column.IsReadable)
                        .Select(static column => column.Ordinal)
                        .ToArray();
                    if (ordinals.Length == 0)
                        continue;
                    var columns = ordinals
                        .Select(ordinal => file.Metadata.Schema.Columns[ordinal])
                        .ToArray();
                    await foreach (var batch in file.ScanAsync(
                        new ParquetScanOptions(columns, null, null, 4096)))
                        batch.Dispose();
                }
                catch (ParquetException)
                {
                }
                catch (Exception exception)
                {
                    Assert.Fail(
                        $"Mutation seed {seed}, baseline {baselineOrdinal}, iteration {iteration} " +
                        $"produced {exception.GetType().FullName}: {exception.Message}");
                }
            }
        }

        Assert.True(stopwatch.Elapsed < TimeSpan.FromSeconds(15),
            $"Bounded page/scan mutation run exceeded its 15-second cap: {stopwatch.Elapsed}.");
    }

    private sealed class BlockingSource(byte[] bytes) : IParquetRandomAccessSource
    {
        public bool BlockReads { get; set; }
        public bool IsDisposed { get; private set; }
        public long Length => bytes.LongLength;
        public TaskCompletionSource ReadStarted { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public async ValueTask ReadExactlyAsync(long offset, ArraySegment<byte> destination, CancellationToken cancellationToken)
        {
            ObjectDisposedException.ThrowIf(IsDisposed, this);
            if (BlockReads)
            {
                ReadStarted.TrySetResult();
                await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken);
            }
            bytes.AsMemory(checked((int)offset), destination.Count).CopyTo(destination.AsMemory());
        }

        public ValueTask DisposeAsync()
        {
            IsDisposed = true;
            return ValueTask.CompletedTask;
        }
    }

    private sealed class AlwaysShortSource(long length) : IParquetRandomAccessSource
    {
        public long Length { get; } = length;
        public bool IsDisposed { get; private set; }

        public ValueTask ReadExactlyAsync(long offset, ArraySegment<byte> destination, CancellationToken cancellationToken) =>
            ValueTask.FromException(new EndOfStreamException());

        public ValueTask DisposeAsync()
        {
            IsDisposed = true;
            return ValueTask.CompletedTask;
        }
    }

    private sealed class RecordingSource(byte[] bytes) : IParquetRandomAccessSource
    {
        public int ReadCount { get; set; }
        public int MaximumDestinationOffset { get; private set; }
        public long Length => bytes.LongLength;

        public ValueTask ReadExactlyAsync(long offset, ArraySegment<byte> destination, CancellationToken cancellationToken)
        {
            ReadCount++;
            MaximumDestinationOffset = Math.Max(MaximumDestinationOffset, destination.Offset);
            bytes.AsMemory(checked((int)offset), destination.Count).CopyTo(destination.AsMemory());
            return ValueTask.CompletedTask;
        }

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    private sealed class ConcurrentRecordingSource(byte[] bytes) : IParquetRandomAccessSource
    {
        private int _activeReads;
        private int _peakConcurrentReads;

        public long Length => bytes.LongLength;
        public int PeakConcurrentReads => Volatile.Read(ref _peakConcurrentReads);

        public void ResetPeak() => Volatile.Write(ref _peakConcurrentReads, 0);

        public async ValueTask ReadExactlyAsync(
            long offset,
            ArraySegment<byte> destination,
            CancellationToken cancellationToken)
        {
            if (offset < 0 || offset > bytes.LongLength || destination.Count > bytes.LongLength - offset)
                throw new EndOfStreamException();
            var active = Interlocked.Increment(ref _activeReads);
            while (true)
            {
                var peak = Volatile.Read(ref _peakConcurrentReads);
                if (active <= peak || Interlocked.CompareExchange(ref _peakConcurrentReads, active, peak) == peak)
                    break;
            }
            try
            {
                await Task.Delay(10, cancellationToken);
                bytes.AsMemory(checked((int)offset), destination.Count).CopyTo(destination.AsMemory());
            }
            finally
            {
                Interlocked.Decrement(ref _activeReads);
            }
        }

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    private sealed class SelectiveFailureSource(byte[] bytes) : IParquetRandomAccessSource
    {
        public long FailingOffset { get; set; } = -1;
        public bool ThrowEndOfStream { get; set; }
        public long Length => bytes.LongLength;

        public async ValueTask ReadExactlyAsync(
            long offset,
            ArraySegment<byte> destination,
            CancellationToken cancellationToken)
        {
            await Task.Yield();
            cancellationToken.ThrowIfCancellationRequested();
            if (offset == FailingOffset)
            {
                if (ThrowEndOfStream)
                    throw new EndOfStreamException("Injected short read.");
                throw new IOException("Injected source failure.");
            }
            if (offset < 0 || offset > bytes.LongLength || destination.Count > bytes.LongLength - offset)
                throw new EndOfStreamException();
            bytes.AsMemory(checked((int)offset), destination.Count).CopyTo(destination.AsMemory());
        }

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }
}
