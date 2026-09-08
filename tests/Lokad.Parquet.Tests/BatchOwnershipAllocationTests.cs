namespace Lokad.Parquet.Tests;

public sealed class BatchOwnershipAllocationTests
{
    [Fact]
    public async Task BinaryCopyPathWithSmallTargetPreservesValues()
    {
        using var tracker = new PoolTracker();
        var bytes = ParquetFixtureBuilder.CreateInt32(new()
        {
            PhysicalTypeCode = (int)ParquetPhysicalType.ByteArray,
            PhysicalValues = new byte[][] { [1, 2], [3, 4], [5, 6] },
        });
        await using var file = await ParquetFile.OpenAsync(new MemoryStream(bytes, writable: false));
        var payloads = new List<byte[]>();
        var batchCount = 0;
        await foreach (var batch in file.ScanAsync(new([file.Metadata.Schema.Columns[0]], null, null, 1)))
        {
            batchCount++;
            using (batch)
            {
                Assert.Equal(1, batch.RowCount);
                payloads.Add(Assert.IsType<ParquetBinaryColumnBatch>(batch.Columns[0]).Payload.ToArray());
            }
        }

        Assert.Equal(3, batchCount);
        Assert.Equal([[1, 2], [3, 4], [5, 6]], payloads);
    }

    [Fact]
    public async Task BinaryCopyPathWithEmptyValuesUsesSingleOwner()
    {
        using var tracker = new PoolTracker();
        var bytes = ParquetFixtureBuilder.CreateInt32(new()
        {
            PhysicalTypeCode = (int)ParquetPhysicalType.ByteArray,
            PhysicalValues = new byte[][] { [], [], [] },
        });
        await using var file = await ParquetFile.OpenAsync(new MemoryStream(bytes, writable: false));
        var batchCount = 0;
        await foreach (var batch in file.ScanAsync(new([file.Metadata.Schema.Columns[0]], null, null, 1)))
        {
            batchCount++;
            using (batch)
            {
                var column = Assert.IsType<ParquetBinaryColumnBatch>(batch.Columns[0]);
                Assert.Equal(1, batch.RowCount);
                Assert.Empty(column.Payload.ToArray());
                Assert.Equal([0, 0], column.Offsets.ToArray());
            }
        }

        Assert.Equal(3, batchCount);
    }

    [Fact]
    public async Task BinaryCopyPathWithMixedValidityCoversThreeOwnerCase()
    {
        using var tracker = new PoolTracker();
        var bytes = ParquetFixtureBuilder.CreateInt32(new()
        {
            PhysicalTypeCode = (int)ParquetPhysicalType.ByteArray,
            PhysicalValues = new byte[][] { [1], [9], [2, 3] },
            Repetition = ParquetRepetition.Optional,
            Validity = [true, false, true],
        });
        await using var file = await ParquetFile.OpenAsync(new MemoryStream(bytes, writable: false));
        var seen = new List<(byte[] Payload, bool[] Validity)>();
        await foreach (var batch in file.ScanAsync(new([file.Metadata.Schema.Columns[0]], null, null, 2)))
        {
            using (batch)
            {
                var column = Assert.IsType<ParquetBinaryColumnBatch>(batch.Columns[0]);
                seen.Add((column.Payload.ToArray(), Enumerable.Range(0, batch.RowCount).Select(column.Validity.IsValid).ToArray()));
            }
        }

        Assert.Equal(2, seen.Count);
        Assert.Equal([1], seen[0].Payload);
        Assert.Equal([true, false], seen[0].Validity);
        Assert.Equal([2, 3], seen[1].Payload);
        Assert.Equal([true], seen[1].Validity);
    }

    [Fact]
    public async Task ManySmallPagesWithSmallTargetStayBounded()
    {
        using var tracker = new PoolTracker();
        var bytes = ParquetFixtureBuilder.CreateRequiredInt32Columns(
        [
            new RequiredInt32FixtureColumn { Name = "value", Pages = [[1], [2], [3], [4], [5], [6], [7], [8]] },
        ]);
        await using var file = await ParquetFile.OpenAsync(new MemoryStream(bytes, writable: false));
        await foreach (var batch in file.ScanAsync(new([file.Metadata.Schema.Columns[0]], null, null, 1)))
            batch.Dispose();

        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();
        var before = GC.GetAllocatedBytesForCurrentThread();
        var values = new List<int>();
        var batchCount = 0;
        await foreach (var batch in file.ScanAsync(new([file.Metadata.Schema.Columns[0]], null, null, 1)))
        {
            batchCount++;
            using (batch)
                values.AddRange(Assert.IsType<ParquetPrimitiveColumnBatch<int>>(batch.Columns[0]).Values.ToArray());
        }

        var allocated = GC.GetAllocatedBytesForCurrentThread() - before;
        Assert.Equal([1, 2, 3, 4, 5, 6, 7, 8], values);
        Assert.Equal(8, batchCount);
        var perBatch = (double)allocated / batchCount;
        Assert.True(perBatch < 2048, $"Small-page scan allocated {allocated} bytes for {batchCount} batches ({perBatch:F1} B/batch).");
    }

    [Fact]
    public async Task BinarySmallTargetScanStaysBounded()
    {
        using var tracker = new PoolTracker();
        var bytes = ParquetFixtureBuilder.CreateInt32(new()
        {
            PhysicalTypeCode = (int)ParquetPhysicalType.ByteArray,
            PhysicalValues = new byte[][] { [1, 2], [3, 4], [5, 6], [7, 8], [9, 10], [11, 12], [13, 14], [15, 16] },
        });
        await using var file = await ParquetFile.OpenAsync(new MemoryStream(bytes, writable: false));
        await foreach (var batch in file.ScanAsync(new([file.Metadata.Schema.Columns[0]], null, null, 1)))
            batch.Dispose();

        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();
        var before = GC.GetAllocatedBytesForCurrentThread();
        var batchCount = 0;
        var totalPayload = 0;
        await foreach (var batch in file.ScanAsync(new([file.Metadata.Schema.Columns[0]], null, null, 1)))
        {
            batchCount++;
            using (batch)
                totalPayload += Assert.IsType<ParquetBinaryColumnBatch>(batch.Columns[0]).Payload.Length;
        }

        var allocated = GC.GetAllocatedBytesForCurrentThread() - before;
        Assert.Equal(8, batchCount);
        Assert.Equal(16, totalPayload);
        var perBatch = (double)allocated / batchCount;
        Assert.True(perBatch < 4096, $"Binary small-target scan allocated {allocated} bytes for {batchCount} batches ({perBatch:F1} B/batch).");
    }
}
