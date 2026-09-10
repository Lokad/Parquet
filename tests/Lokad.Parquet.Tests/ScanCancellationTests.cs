namespace Lokad.Parquet.Tests;

using System.Buffers.Binary;
using System.Reflection;

public sealed class ScanCancellationTests
{
    [Theory]
    [InlineData((int)ParquetPhysicalType.Int32)]
    [InlineData((int)ParquetPhysicalType.ByteArray)]
    public async Task CancelledScanDoesNotYieldBatchAfterPayloadRead(int physicalTypeCode)
    {
        using var cancellation = new CancellationTokenSource();
        var physicalType = (ParquetPhysicalType)physicalTypeCode;
        byte[] bytes = physicalType == ParquetPhysicalType.ByteArray
            ? ParquetFixtureBuilder.CreateInt32(new() { PhysicalTypeCode = (int)ParquetPhysicalType.ByteArray, PhysicalValues = new byte[][] { [1, 2, 3] } })
            : ParquetFixtureBuilder.CreateInt32(new() { Values = [1, 2, 3] });
        var source = new CancelAfterPayloadSource(bytes, cancellation, 5);
        await using var file = await ParquetFile.OpenAsync(source);
        await using var scan = file.ScanAsync(new([file.Metadata.Schema.Columns[0]]), cancellation.Token).GetAsyncEnumerator();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await scan.MoveNextAsync());
    }

    [Fact]
    public async Task CancelledProjectedScanDoesNotYieldBatch()
    {
        using var cancellation = new CancellationTokenSource();
        var bytes = ParquetFixtureBuilder.CreateRequiredInt32Columns(
        [
            new RequiredInt32FixtureColumn { Name = "a", Pages = [[1, 2, 3]] },
            new RequiredInt32FixtureColumn { Name = "b", Pages = [[4, 5, 6]] },
        ]);
        var source = new CancelAfterPayloadSource(bytes, cancellation, 5);
        await using var file = await ParquetFile.OpenAsync(source);
        var options = new ParquetScanOptions([file.Metadata.Schema.Columns[0], file.Metadata.Schema.Columns[1]]);
        await using var scan = file.ScanAsync(options, cancellation.Token).GetAsyncEnumerator();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await scan.MoveNextAsync());
    }

    [Fact]
    public async Task CancelledFooterOpenThrows()
    {
        var bytes = ParquetFixtureBuilder.CreateInt32(new() { Values = [1] });
        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
        {
            await ParquetFile.OpenAsync(new MemoryStream(bytes, writable: false), ParquetSourceOwnership.Caller, ParquetReaderOptions.Default, cancellation.Token);
        });
    }

    [Fact]
    public void CancelledCrcComputationThrowsBeforeScanningPayload()
    {
        // The CRC loop observes cancellation every 4096 bytes, so a
        // pre-cancelled token must surface immediately even for a megabyte
        // input instead of running unbounded bit-by-bit work after cancel.
        var input = new byte[1024 * 1024];
        new Random(42).NextBytes(input);
        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();
        Assert.ThrowsAny<OperationCanceledException>(() => ComputeCrc32(input, cancellation.Token));
    }

    [Fact]
    public void Crc32MatchesStandardCheckValue()
    {
        // IEEE CRC-32 check value for "123456789".
        var input = System.Text.Encoding.ASCII.GetBytes("123456789");
        Assert.Equal(0xCBF43926u, ComputeCrc32(input, CancellationToken.None));
    }

    [Fact]
    public async Task CancelledLevelsReturnDoesNotYieldBatch()
    {
        // Cancelling when the decoded definition-level buffer is returned must surface
        // at the pre-publication boundary instead of yielding an already cancelled batch.
        var outstanding = new PoolOutstandingArrays();
        using var cancellation = new CancellationTokenSource();
        Array? levelArray = null;
        PoolTracker.SetObservers(
            (array, requested) =>
        {
            outstanding.NoteRent(array);

            if (array is int[] && requested == 3)
            {
                levelArray = array;
            }
        },
        (array, _) =>
        {
            outstanding.NoteReturn(array);

            if (ReferenceEquals(array, levelArray))
            {
                cancellation.Cancel();
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
            await using var scan = file.ScanAsync(new(file.Metadata.Schema.Columns), cancellation.Token).GetAsyncEnumerator();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await scan.MoveNextAsync());
            Assert.True(cancellation.IsCancellationRequested);
        }
        finally
        {
            PoolTracker.ClearObservers();
        }

        Assert.True(outstanding.IsEmpty);
    }

    [Fact]
    public async Task CancelledProjectedCopyDoesNotYieldBatch()
    {
        // Uneven pages force projected copies (never transfers) of two rows. The int[2]
        // rents on this path are the second cursor emission and then the two projected
        // copies, so cancelling on the third lands inside the last copy, after every
        // earlier check on that path, and only the pre-publication boundary observes it.
        var outstanding = new PoolOutstandingArrays();
        using var cancellation = new CancellationTokenSource();
        var copyRents = 0;
        PoolTracker.SetObservers(
            (array, requested) =>
        {
            outstanding.NoteRent(array);

            if (array is int[] && requested == 2 && ++copyRents == 3)
            {
                cancellation.Cancel();
            }
        },
        (array, _) =>
        {
            outstanding.NoteReturn(array);
        });
        try
        {
            byte[] bytes = ParquetFixtureBuilder.CreateRequiredInt32Columns(
            [
                new RequiredInt32FixtureColumn { Name = "a", Pages = [[1, 2, 3, 4, 5, 6]] },
                new RequiredInt32FixtureColumn { Name = "b", Pages = [[1, 2], [3, 4, 5, 6]] },
            ]);
            await using var file = await ParquetFile.OpenAsync((ReadOnlyMemory<byte>)bytes);
            var options = new ParquetScanOptions(file.Metadata.Schema.Columns);
            await using var scan = file.ScanAsync(options, cancellation.Token).GetAsyncEnumerator();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await scan.MoveNextAsync());
            Assert.True(cancellation.IsCancellationRequested);
            Assert.Equal(3, copyRents);
        }
        finally
        {
            PoolTracker.ClearObservers();
        }

        Assert.True(outstanding.IsEmpty);
    }

    [Fact]
    public async Task FileDisposalDuringScanSurfacesObjectDisposed()
    {
        // The first scan read blocks until the test releases it after disposal starts,
        // so disposal-triggered cancellation is guaranteed to land mid-scan. It must
        // surface as a file-disposal error rather than a user cancellation.
        using var tracker = new PoolTracker();
        byte[] bytes = ParquetFixtureBuilder.CreateInt32(new() { Values = [1, 2, 3] });
        var source = new GatedSource(bytes);
        await using var file = await ParquetFile.OpenAsync(source);
        await using var enumerator = file.ScanAsync(new([file.Metadata.Schema.Columns[0]])).GetAsyncEnumerator();
        source.BlockReads = true;
        Task<bool> move = enumerator.MoveNextAsync().AsTask();
        await file.DisposeAsync();
        ObjectDisposedException failure = await Assert.ThrowsAsync<ObjectDisposedException>(async () => await move);
        Assert.Contains("ParquetFile", failure.ObjectName ?? string.Empty, StringComparison.Ordinal);
    }

    [Fact]
    public async Task FileDisposalDuringProjectedCopySurfacesObjectDisposed()
    {
        // Disposing the file from inside the last projected copy rent means the
        // resulting cancellation surfaces after every earlier check on that path.
        // It must surface as a file-disposal error rather than a user cancellation.
        var outstanding = new PoolOutstandingArrays();
        var copyRents = 0;
        ParquetFile? file = null;
        Task disposeTask = Task.CompletedTask;
        PoolTracker.SetObservers(
            (array, requested) =>
        {
            outstanding.NoteRent(array);

            if (array is int[] && requested == 2 && ++copyRents == 3 && file is not null)
            {
                disposeTask = file.DisposeAsync().AsTask();
            }
        },
        (array, _) =>
        {
            outstanding.NoteReturn(array);
        });
        try
        {
            byte[] bytes = ParquetFixtureBuilder.CreateRequiredInt32Columns(
            [
                new RequiredInt32FixtureColumn { Name = "a", Pages = [[1, 2, 3, 4, 5, 6]] },
                new RequiredInt32FixtureColumn { Name = "b", Pages = [[1, 2], [3, 4, 5, 6]] },
            ]);
            file = await ParquetFile.OpenAsync((ReadOnlyMemory<byte>)bytes);
            var options = new ParquetScanOptions(file.Metadata.Schema.Columns);
            await using var scan = file.ScanAsync(options).GetAsyncEnumerator();
            ObjectDisposedException failure = await Assert.ThrowsAsync<ObjectDisposedException>(async () => await scan.MoveNextAsync());
            Assert.Contains("ParquetFile", failure.ObjectName ?? string.Empty, StringComparison.Ordinal);
            Assert.Equal(3, copyRents);
            await disposeTask;
            await file.DisposeAsync();
        }
        finally
        {
            PoolTracker.ClearObservers();
        }

        Assert.True(outstanding.IsEmpty);
    }

    [Fact]
    public async Task LargeSchemaOnlyFooterOpensWithoutSpuriousCancellation()
    {
        // 2,000 schema leaves exercise the footer element loops with a live token;
        // the scan of the rowless table then yields no batches.
        static byte[] BuildLargeSchemaOnlyFile(int leafCount)
        {
            var footer = new CompactTestWriter();
            short previous = 0;
            footer.Int32Field(ref previous, 1, 1);
            footer.ListField(ref previous, 2, CompactTestType.Struct, leafCount + 1, () =>
            {
                short root = 0;
                footer.StringField(ref root, 4, "schema");
                footer.Int32Field(ref root, 5, leafCount);
                footer.Stop();
                for (var i = 0; i < leafCount; i++)
                {
                    short leaf = 0;
                    footer.Int32Field(ref leaf, 1, 1);
                    footer.Int32Field(ref leaf, 3, 0);
                    footer.StringField(ref leaf, 4, "c" + i);
                    footer.Stop();
                }
            });
            footer.Int64Field(ref previous, 3, 0);
            footer.ListField(ref previous, 4, CompactTestType.Struct, 0, () => { });
            footer.Stop();
            var footerBytes = footer.ToArray();
            var file = new MemoryStream();
            file.Write("PAR1"u8);
            file.Write(footerBytes);
            Span<byte> tail = stackalloc byte[8];
            BinaryPrimitives.WriteInt32LittleEndian(tail, footerBytes.Length);
            "PAR1"u8.CopyTo(tail[4..]);
            file.Write(tail);
            return file.ToArray();
        }

        using var tracker = new PoolTracker();
        byte[] bytes = BuildLargeSchemaOnlyFile(2000);
        await using var file = await ParquetFile.OpenAsync(new MemoryStream(bytes, writable: false));
        Assert.Equal(2000, file.Metadata.Schema.Columns.Count);
        var yielded = false;
        await foreach (var batch in file.ScanAsync(new([file.Metadata.Schema.Columns[0]])))
        {
            using (batch)
                yielded = true;
        }

        Assert.False(yielded);
    }

    [Fact]
    public async Task CancelledBinaryDictionaryDecodeThrowsAndBalances()
    {
        // Cancellation injected while binary-dictionary loading rents must surface
        // through the dictionary, data, emission, or publication checks, and every
        // rented array is still released. The exact firing point is an implementation
        // detail; this guards mid-decode injection for dictionary work.
        var outstanding = new PoolOutstandingArrays();
        using var cancellation = new CancellationTokenSource();
        var fired = false;
        PoolTracker.SetObservers(
            (array, requested) =>
        {
            outstanding.NoteRent(array);

            if (array is int[] && requested == 2001 && !fired)
            {
                fired = true;
                cancellation.Cancel();
            }
        },
        (array, _) =>
        {
            outstanding.NoteReturn(array);
        });
        try
        {
            var entries = new byte[2000][];
            for (var i = 0; i < entries.Length; i++)
                entries[i] = BitConverter.GetBytes(i);
            var physical = new byte[20000][];
            var indices = new int[20000];
            for (var i = 0; i < physical.Length; i++)
            {
                physical[i] = entries[i % entries.Length];
                indices[i] = i % entries.Length;
            }
            byte[] bytes = ParquetFixtureBuilder.CreateInt32(new()
            {
                PhysicalTypeCode = (int)ParquetPhysicalType.ByteArray,
                PhysicalValues = physical,
                DictionaryValues = entries,
                DictionaryIndices = indices,
            });
            await using var file = await ParquetFile.OpenAsync((ReadOnlyMemory<byte>)bytes);
            await using var scan = file.ScanAsync(new([file.Metadata.Schema.Columns[0]]), cancellation.Token).GetAsyncEnumerator();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await scan.MoveNextAsync());
            Assert.True(fired);
        }
        finally
        {
            PoolTracker.ClearObservers();
        }

        Assert.True(outstanding.IsEmpty);
    }

    [Fact]
    public async Task CancelledLargeBinaryDecodeThrowsAndBalances()
    {
        // Cancellation injected on the definition-level rent must surface through the
        // level, value, bitmap, emission, or publication checks with every rented
        // array released. The exact firing point is an implementation detail; this
        // guards mid-decode injection for binary work.
        var outstanding = new PoolOutstandingArrays();
        using var cancellation = new CancellationTokenSource();
        var fired = false;
        PoolTracker.SetObservers(
            (array, requested) =>
        {
            outstanding.NoteRent(array);

            if (array is int[] && requested == 20000 && !fired)
            {
                fired = true;
                cancellation.Cancel();
            }
        },
        (array, _) =>
        {
            outstanding.NoteReturn(array);
        });
        try
        {
            var physical = new byte[20000][];
            for (var i = 0; i < physical.Length; i++)
                physical[i] = BitConverter.GetBytes(i);
            byte[] bytes = ParquetFixtureBuilder.CreateInt32(new()
            {
                PhysicalTypeCode = (int)ParquetPhysicalType.ByteArray,
                PhysicalValues = physical,
                Repetition = ParquetRepetition.Optional,
                Validity = Enumerable.Repeat(true, physical.Length).ToArray(),
            });
            await using var file = await ParquetFile.OpenAsync((ReadOnlyMemory<byte>)bytes);
            await using var scan = file.ScanAsync(new([file.Metadata.Schema.Columns[0]]), cancellation.Token).GetAsyncEnumerator();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await scan.MoveNextAsync());
            Assert.True(fired);
        }
        finally
        {
            PoolTracker.ClearObservers();
        }

        Assert.True(outstanding.IsEmpty);
    }

    private static readonly Func<ReadOnlySpan<byte>, CancellationToken, uint> ComputeCrc32 = GetCrc32Method();

    private static Func<ReadOnlySpan<byte>, CancellationToken, uint> GetCrc32Method()
    {
        var type = typeof(ParquetFile).Assembly.GetType("Lokad.Parquet.Internal.ScanPageReader") ??
            throw new InvalidOperationException("Internal type ScanPageReader was not found.");
        var method = type.GetMethod("ComputeCrc32", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static) ??
            throw new InvalidOperationException("Internal method ScanPageReader.ComputeCrc32 was not found.");
        return (Func<ReadOnlySpan<byte>, CancellationToken, uint>)method.CreateDelegate(typeof(Func<ReadOnlySpan<byte>, CancellationToken, uint>));
    }

    private sealed class GatedSource : IParquetRandomAccessSource
    {
        private readonly byte[] _bytes;
        public GatedSource(byte[] bytes) => _bytes = bytes;
        public long Length => _bytes.Length;
        public bool BlockReads { get; set; }
        public async ValueTask ReadExactlyAsync(long offset, ArraySegment<byte> destination, CancellationToken cancellationToken)
        {
            if (BlockReads)
            {
                await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken);
            }

            _bytes.AsMemory((int)offset, destination.Count).CopyTo(destination.AsMemory());
        }

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    private sealed class CancelAfterPayloadSource : IParquetRandomAccessSource
    {
        private readonly byte[] _bytes;
        private readonly CancellationTokenSource _cancellation;
        private readonly int _cancelAfter;
        private int _reads;
        public CancelAfterPayloadSource(byte[] bytes, CancellationTokenSource cancellation, int cancelAfter)
        {
            _bytes = bytes;
            _cancellation = cancellation;
            _cancelAfter = cancelAfter;
        }

        public long Length => _bytes.Length;
        public ValueTask ReadExactlyAsync(long offset, ArraySegment<byte> destination, CancellationToken cancellationToken)
        {
            _bytes.AsMemory((int)offset, destination.Count).CopyTo(destination.AsMemory());
            if (++_reads == _cancelAfter)
            {
                _cancellation.Cancel();
            }

            return ValueTask.CompletedTask;
        }

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }
}
