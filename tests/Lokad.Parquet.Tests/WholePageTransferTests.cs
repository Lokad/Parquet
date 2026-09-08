namespace Lokad.Parquet.Tests;

public sealed class WholePageTransferTests
{
    [Fact]
    public async Task BinaryWholePageBatchTransfersDecodedStorage()
    {
        using var tracker = new PoolTracker();
        var bytes = ParquetFixtureBuilder.CreateInt32(new()
        {
            PhysicalTypeCode = (int)ParquetPhysicalType.ByteArray,
            PhysicalValues = new byte[][] { [1, 2], [3], [4, 5, 6] },
        });
        await using (var file = await ParquetFile.OpenAsync(new MemoryStream(bytes, writable: false)))
        {
            await using var enumerator = file.ScanAsync(new([file.Metadata.Schema.Columns[0]], null, null, 3)).GetAsyncEnumerator();
            Assert.True(await enumerator.MoveNextAsync());
            using (var batch = enumerator.Current)
            {
                Assert.Equal(3, batch.RowCount);
                var column = Assert.IsType<ParquetBinaryColumnBatch>(batch.Columns[0]);
                Assert.True(column.Validity.IsAllValid);
                Assert.Equal([0, 2, 3, 6], column.Offsets.ToArray());
                Assert.Equal([1, 2, 3, 4, 5, 6], column.Payload.ToArray());
            }
            Assert.False(await enumerator.MoveNextAsync());
        }
    }

    [Fact]
    public async Task BinaryWholePageBatchRentsNoBatchStageBuffers()
    {
        // The memory-stream source serves payload bytes without renting, so a
        // whole-page scan rents the header plus the decode offsets and payload
        // only; the copy path would rent offsets and payload again.
        using var tracker = new PoolTracker();
        var bytes = ParquetFixtureBuilder.CreateInt32(new()
        {
            PhysicalTypeCode = (int)ParquetPhysicalType.ByteArray,
            PhysicalValues = new byte[][] { [1, 2], [3], [4, 5, 6] },
        });
        await using (var file = await ParquetFile.OpenAsync(new MemoryStream(bytes, writable: false)))
        {
            var rentsAfterOpen = tracker.RentCount;
            await foreach (var batch in file.ScanAsync(new([file.Metadata.Schema.Columns[0]], null, null, 3)))
                batch.Dispose();
            Assert.Equal(3, tracker.RentCount - rentsAfterOpen);
        }
    }

    [Fact]
    public async Task FixedWholePageBatchTransfersDecodedStorage()
    {
        using var tracker = new PoolTracker();
        var bytes = ParquetFixtureBuilder.CreateInt32(new()
        {
            PhysicalTypeCode = (int)ParquetPhysicalType.FixedLengthByteArray,
            TypeLength = 2,
            PhysicalValues = new byte[][] { [1, 2], [3, 4], [5, 6] },
        });
        await using (var file = await ParquetFile.OpenAsync(new MemoryStream(bytes, writable: false)))
        {
            var rentsAfterOpen = tracker.RentCount;
            await using var enumerator = file.ScanAsync(new([file.Metadata.Schema.Columns[0]], null, null, 3)).GetAsyncEnumerator();
            Assert.True(await enumerator.MoveNextAsync());
            using (var batch = enumerator.Current)
            {
                var column = Assert.IsType<ParquetFixedLengthByteArrayColumnBatch>(batch.Columns[0]);
                Assert.Equal(2, column.TypeWidth);
                Assert.Equal([1, 2, 3, 4, 5, 6], column.Payload.ToArray());
            }
            Assert.False(await enumerator.MoveNextAsync());
            Assert.Equal(2, tracker.RentCount - rentsAfterOpen);
        }
    }

    [Fact]
    public async Task BinaryWholePageTransferHandlesEmptyValues()
    {
        using var tracker = new PoolTracker();
        var bytes = ParquetFixtureBuilder.CreateInt32(new()
        {
            PhysicalTypeCode = (int)ParquetPhysicalType.ByteArray,
            PhysicalValues = new byte[][] { [], [], [] },
        });
        await using (var file = await ParquetFile.OpenAsync(new MemoryStream(bytes, writable: false)))
        {
            await using var enumerator = file.ScanAsync(new([file.Metadata.Schema.Columns[0]], null, null, 3)).GetAsyncEnumerator();
            Assert.True(await enumerator.MoveNextAsync());
            using (var batch = enumerator.Current)
            {
                var column = Assert.IsType<ParquetBinaryColumnBatch>(batch.Columns[0]);
                Assert.Equal([0, 0, 0, 0], column.Offsets.ToArray());
                Assert.Empty(column.Payload.ToArray());
            }
            Assert.False(await enumerator.MoveNextAsync());
        }
    }

    [Fact]
    public async Task BinaryWholePageTransferHandlesNulls()
    {
        using var tracker = new PoolTracker();
        var bytes = ParquetFixtureBuilder.CreateInt32(new()
        {
            PhysicalTypeCode = (int)ParquetPhysicalType.ByteArray,
            PhysicalValues = new byte[][] { [1], [9], [2, 3] },
            Repetition = ParquetRepetition.Optional,
            Validity = [true, false, true],
        });
        await using (var file = await ParquetFile.OpenAsync(new MemoryStream(bytes, writable: false)))
        {
            await using var enumerator = file.ScanAsync(new([file.Metadata.Schema.Columns[0]], null, null, 3)).GetAsyncEnumerator();
            Assert.True(await enumerator.MoveNextAsync());
            using (var batch = enumerator.Current)
            {
                var column = Assert.IsType<ParquetBinaryColumnBatch>(batch.Columns[0]);
                Assert.Equal([true, false, true], Enumerable.Range(0, 3).Select(column.Validity.IsValid));
                Assert.Equal([0, 1, 1, 3], column.Offsets.ToArray());
                Assert.Equal([1, 2, 3], column.Payload.ToArray());
            }
            Assert.False(await enumerator.MoveNextAsync());
        }
    }

    [Fact]
    public async Task BinaryPayloadLimitFallsBackToCopies()
    {
        using var tracker = new PoolTracker();
        var bytes = ParquetFixtureBuilder.CreateInt32(new()
        {
            PhysicalTypeCode = (int)ParquetPhysicalType.ByteArray,
            PhysicalValues = new byte[][] { [1, 2], [3, 4], [5, 6] },
        });
        await using var file = await ParquetFile.OpenAsync(
            new MemoryStream(bytes, writable: false),
            ParquetSourceOwnership.Caller,
            new ParquetReaderOptions { MaximumBinaryBatchBytes = 3 },
            CancellationToken.None);
        var payloads = new List<byte[]>();
        await foreach (var batch in file.ScanAsync(new([file.Metadata.Schema.Columns[0]])))
        {
            using (batch)
                payloads.Add(Assert.IsType<ParquetBinaryColumnBatch>(batch.Columns[0]).Payload.ToArray());
        }

        Assert.Equal([[1, 2], [3, 4], [5, 6]], payloads);
    }

    [Fact]
    public async Task MisalignedProjectedPagesFallBackToCopies()
    {
        using var tracker = new PoolTracker();
        var bytes = ParquetFixtureBuilder.CreateRequiredInt32Columns(
        [
            new RequiredInt32FixtureColumn { Name = "left", Pages = [[1, 2], [3, 4, 5]] },
            new RequiredInt32FixtureColumn { Name = "right", Pages = [[10], [20, 30, 40, 50]] },
        ]);
        await using var file = await ParquetFile.OpenAsync(new MemoryStream(bytes, writable: false));
        var left = new List<int>();
        var right = new List<int>();
        await foreach (var batch in file.ScanAsync(new ParquetScanOptions([file.Metadata.Schema.Columns[0], file.Metadata.Schema.Columns[1]], null, null, 2)))
        {
            using (batch)
            {
                left.AddRange(Assert.IsType<ParquetPrimitiveColumnBatch<int>>(batch.Columns[0]).Values.ToArray());
                right.AddRange(Assert.IsType<ParquetPrimitiveColumnBatch<int>>(batch.Columns[1]).Values.ToArray());
            }
        }

        Assert.Equal([1, 2, 3, 4, 5], left);
        Assert.Equal([10, 20, 30, 40, 50], right);
    }

    [Fact]
    public async Task TransferredBatchSurvivesEnumeratorDisposal()
    {
        using var tracker = new PoolTracker();
        var bytes = ParquetFixtureBuilder.CreateInt32(new()
        {
            PhysicalTypeCode = (int)ParquetPhysicalType.ByteArray,
            PhysicalValues = new byte[][] { [1, 2], [3] },
        });
        await using var file = await ParquetFile.OpenAsync(new MemoryStream(bytes, writable: false));
        var enumerator = file.ScanAsync(new([file.Metadata.Schema.Columns[0]], null, null, 2)).GetAsyncEnumerator();
        Assert.True(await enumerator.MoveNextAsync());
        var batch = enumerator.Current;
        await enumerator.DisposeAsync();
        Assert.Equal([1, 2, 3], Assert.IsType<ParquetBinaryColumnBatch>(batch.Columns[0]).Payload.ToArray());
        batch.Dispose();
    }
}
