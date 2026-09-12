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
    public async Task MisalignedProjectedPagesMixTransfersAndCopies()
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

    [Fact]
    public async Task MixedProjectedBatchesCopyOnlyMisalignedColumn()
    {
        // Alternating page boundaries force mixed projected batches of two rows:
        // the six-row column copies each batch while every two-row page transfers.
        // After open, the int[2] rents are one cursor emission plus three projected
        // copies; the retired all-copy path rented six copies instead of three.
        var outstanding = new PoolOutstandingArrays();
        var bytes = ParquetFixtureBuilder.CreateRequiredInt32Columns(
        [
            new RequiredInt32FixtureColumn { Name = "left", Pages = [[1, 2, 3, 4, 5, 6]] },
            new RequiredInt32FixtureColumn { Name = "right", Pages = [[1, 2], [3, 4], [5, 6]] },
        ]);
        await using var file = await ParquetFile.OpenAsync(new MemoryStream(bytes, writable: false));
        var int2Rents = 0;
        PoolTracker.SetObservers(
            (array, requested) =>
        {
            outstanding.NoteRent(array);
            if (array is int[] && requested == 2)
            {
                int2Rents++;
            }
        },
        (array, _) =>
        {
            outstanding.NoteReturn(array);
        });
        try
        {
            var left = new List<int>();
            var right = new List<int>();
            var batches = 0;
            await foreach (var batch in file.ScanAsync(new ParquetScanOptions(file.Metadata.Schema.Columns)))
            {
                using (batch)
                {
                    batches++;
                    Assert.Equal(2, batch.RowCount);
                    left.AddRange(Assert.IsType<ParquetPrimitiveColumnBatch<int>>(batch.Columns[0]).Values.ToArray());
                    right.AddRange(Assert.IsType<ParquetPrimitiveColumnBatch<int>>(batch.Columns[1]).Values.ToArray());
                }
            }

            Assert.Equal(3, batches);
            Assert.Equal([1, 2, 3, 4, 5, 6], left);
            Assert.Equal([1, 2, 3, 4, 5, 6], right);
            Assert.Equal(4, int2Rents);
            await file.DisposeAsync();
        }
        finally
        {
            PoolTracker.ClearObservers();
        }

        Assert.True(outstanding.IsEmpty);
    }

    [Fact]
    public async Task MixedNullableProjectedBatchesPreserveValidity()
    {
        // Partial validity pages force mixed projected batches: the four-row
        // nullable column copies each two-row slice with a validity slice while
        // every two-row page transfers its bitmap into the public batch.
        static int? ReadNullable(ParquetPrimitiveColumnBatch<int> column, int row)
        {
            if (column.Validity.IsAllValid)
            {
                return column.Values.Span[row];
            }

            var bits = column.Validity.Bits.Span;
            return ((bits[row >> 3] & (1 << (row & 7))) != 0) ? column.Values.Span[row] : null;
        }

        using var tracker = new PoolTracker();
        var bytes = ParquetFixtureBuilder.CreateNullableInt32Columns(
        [
            new NullableInt32FixtureColumn
            {
                Name = "left",
                Pages = [[1, 2, 3, 4]],
                ValidityPages = [[true, false, true, true]],
            },
            new NullableInt32FixtureColumn
            {
                Name = "right",
                Pages = [[10, 11], [12, 13]],
                ValidityPages = [[true, true], [false, true]],
            },
        ]);
        var left = new List<int?>();
        var right = new List<int?>();
        await using var file = await ParquetFile.OpenAsync(new MemoryStream(bytes, writable: false));
        await foreach (var batch in file.ScanAsync(new ParquetScanOptions(file.Metadata.Schema.Columns)))
        {
            using (batch)
            {
                Assert.Equal(2, batch.RowCount);
                var leftColumn = Assert.IsType<ParquetPrimitiveColumnBatch<int>>(batch.Columns[0]);
                var rightColumn = Assert.IsType<ParquetPrimitiveColumnBatch<int>>(batch.Columns[1]);
                for (var row = 0; row < batch.RowCount; row++)
                {
                    left.Add(ReadNullable(leftColumn, row));
                    right.Add(ReadNullable(rightColumn, row));
                }
            }
        }

        Assert.Equal((System.Collections.Generic.IEnumerable<int?>)[1, null, 3, 4], (System.Collections.Generic.IEnumerable<int?>)left);
        Assert.Equal((System.Collections.Generic.IEnumerable<int?>)[10, 11, null, 13], (System.Collections.Generic.IEnumerable<int?>)right);
    }

    [Fact]
    public async Task MixedNullableBatchSurvivesEnumeratorDisposal()
    {
        // A live mixed nullable batch keeps its copied values and its transferred
        // bitmap after the coordinator is gone: the batch owns both outright.
        using var tracker = new PoolTracker();
        var bytes = ParquetFixtureBuilder.CreateNullableInt32Columns(
        [
            new NullableInt32FixtureColumn
            {
                Name = "left",
                Pages = [[1, 2, 3, 4]],
                ValidityPages = [[true, true, false, true]],
            },
            new NullableInt32FixtureColumn
            {
                Name = "right",
                Pages = [[10, 11], [12, 13]],
                ValidityPages = [[true, false], [true, true]],
            },
        ]);
        await using var file = await ParquetFile.OpenAsync(new MemoryStream(bytes, writable: false));
        var enumerator = file.ScanAsync(new ParquetScanOptions(file.Metadata.Schema.Columns)).GetAsyncEnumerator();
        Assert.True(await enumerator.MoveNextAsync());
        var batch = enumerator.Current;
        await enumerator.DisposeAsync();
        Assert.Equal(2, batch.RowCount);
        var leftColumn = Assert.IsType<ParquetPrimitiveColumnBatch<int>>(batch.Columns[0]);
        Assert.True(leftColumn.Validity.IsAllValid);
        Assert.Equal([1, 2], leftColumn.Values.ToArray());
        var rightColumn = Assert.IsType<ParquetPrimitiveColumnBatch<int>>(batch.Columns[1]);
        Assert.Equal(10, rightColumn.Values.Span[0]);
        var rightBits = rightColumn.Validity.Bits.Span;
        Assert.Equal(1, rightBits[0] & 1);
        Assert.Equal(0, rightBits[0] & 2);
        batch.Dispose();
    }

    [Fact]
    public async Task MixedBinaryProjectedBatchesNormalizeOffsets()
    {
        // Alternating binary page boundaries force mixed projected batches: the
        // four-row column copies each two-row slice with rebased offsets while
        // every two-row page transfers its offsets and payload into the batch.
        using var tracker = new PoolTracker();
        var bytes = ParquetFixtureBuilder.CreateBinaryColumns(
        [
            new FixtureBinaryColumn
            {
                Name = "left",
                PhysicalTypeCode = (int)ParquetPhysicalType.ByteArray,
                Pages = [[[10], [20], [30], [40]]],
            },
            new FixtureBinaryColumn
            {
                Name = "right",
                PhysicalTypeCode = (int)ParquetPhysicalType.ByteArray,
                Pages = [[[1], [2]], [[3], [4]]],
            },
        ]);
        var leftOffsets = new List<int[]>();
        var leftPayloads = new List<byte[]>();
        var rightOffsets = new List<int[]>();
        var rightPayloads = new List<byte[]>();
        await using var file = await ParquetFile.OpenAsync(new MemoryStream(bytes, writable: false));
        await foreach (var batch in file.ScanAsync(new ParquetScanOptions(file.Metadata.Schema.Columns)))
        {
            using (batch)
            {
                var leftColumn = Assert.IsType<ParquetBinaryColumnBatch>(batch.Columns[0]);
                leftOffsets.Add(leftColumn.Offsets.ToArray());
                leftPayloads.Add(leftColumn.Payload.ToArray());
                var rightColumn = Assert.IsType<ParquetBinaryColumnBatch>(batch.Columns[1]);
                rightOffsets.Add(rightColumn.Offsets.ToArray());
                rightPayloads.Add(rightColumn.Payload.ToArray());
            }
        }

        Assert.Equal(2, leftOffsets.Count);
        Assert.Equal([0, 1, 2], leftOffsets[0]);
        Assert.Equal([10, 20], leftPayloads[0]);
        Assert.Equal([0, 1, 2], leftOffsets[1]);
        Assert.Equal([30, 40], leftPayloads[1]);
        Assert.Equal([0, 1, 2], rightOffsets[0]);
        Assert.Equal([1, 2], rightPayloads[0]);
        Assert.Equal([0, 1, 2], rightOffsets[1]);
        Assert.Equal([3, 4], rightPayloads[1]);
    }

    [Fact]
    public async Task MixedBatchSurvivesEnumeratorDisposal()
    {
        // A live mixed batch keeps its copied column and its transferred column
        // after the coordinator is gone: the batch owns both outright.
        using var tracker = new PoolTracker();
        var bytes = ParquetFixtureBuilder.CreateRequiredInt32Columns(
        [
            new RequiredInt32FixtureColumn { Name = "left", Pages = [[1, 2, 3, 4]] },
            new RequiredInt32FixtureColumn { Name = "right", Pages = [[10], [20, 30, 40]] },
        ]);
        await using var file = await ParquetFile.OpenAsync(new MemoryStream(bytes, writable: false));
        var enumerator = file.ScanAsync(new ParquetScanOptions(file.Metadata.Schema.Columns)).GetAsyncEnumerator();
        Assert.True(await enumerator.MoveNextAsync());
        var batch = enumerator.Current;
        await enumerator.DisposeAsync();
        Assert.Equal(1, batch.RowCount);
        Assert.Equal([1], Assert.IsType<ParquetPrimitiveColumnBatch<int>>(batch.Columns[0]).Values.ToArray());
        Assert.Equal([10], Assert.IsType<ParquetPrimitiveColumnBatch<int>>(batch.Columns[1]).Values.ToArray());
        batch.Dispose();
    }

    [Fact]
    public async Task SmallTargetSlicesFeedAlignedTransfers()
    {
        // A small target slices the four-row pages into aligned two-row source
        // batches, so every projected batch transfers both columns instead of
        // copying either one.
        using var tracker = new PoolTracker();
        var bytes = ParquetFixtureBuilder.CreateRequiredInt32Columns(
        [
            new RequiredInt32FixtureColumn { Name = "left", Pages = [[1, 2, 3, 4]] },
            new RequiredInt32FixtureColumn { Name = "right", Pages = [[10, 20], [30, 40]] },
        ]);
        await using var file = await ParquetFile.OpenAsync(new MemoryStream(bytes, writable: false));
        var left = new List<int>();
        var right = new List<int>();
        var batches = 0;
        await foreach (var batch in file.ScanAsync(new ParquetScanOptions(
            [file.Metadata.Schema.Columns[0], file.Metadata.Schema.Columns[1]], null, null, 2)))
        {
            using (batch)
            {
                batches++;
                Assert.Equal(2, batch.RowCount);
                left.AddRange(Assert.IsType<ParquetPrimitiveColumnBatch<int>>(batch.Columns[0]).Values.ToArray());
                right.AddRange(Assert.IsType<ParquetPrimitiveColumnBatch<int>>(batch.Columns[1]).Values.ToArray());
            }
        }

        Assert.Equal(2, batches);
        Assert.Equal([1, 2, 3, 4], left);
        Assert.Equal([10, 20, 30, 40], right);
    }
}
