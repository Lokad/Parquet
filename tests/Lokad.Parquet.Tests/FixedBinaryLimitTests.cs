namespace Lokad.Parquet.Tests;

public sealed class FixedBinaryLimitTests
{
    [Fact]
    public async Task FixedValueAtExactLimitSucceeds()
    {
        using var tracker = new PoolTracker();
        var bytes = ParquetFixtureBuilder.CreateInt32(new()
        {
            PhysicalTypeCode = (int)ParquetPhysicalType.FixedLengthByteArray,
            TypeLength = 4,
            PhysicalValues = new byte[][] { [1, 2, 3, 4], [5, 6, 7, 8], [9, 10, 11, 12] },
        });
        await using var file = await ParquetFile.OpenAsync(
            new MemoryStream(bytes, writable: false),
            ParquetSourceOwnership.Caller,
            new ParquetReaderOptions { MaximumBinaryValueBytes = 4 },
            CancellationToken.None);
        var rowCount = 0;
        await foreach (var batch in file.ScanAsync(new([file.Metadata.Schema.Columns[0]])))
        {
            using (batch)
            {
                var column = Assert.IsType<ParquetFixedLengthByteArrayColumnBatch>(batch.Columns[0]);
                Assert.Equal(4, column.TypeWidth);
                rowCount += batch.RowCount;
            }
        }

        Assert.Equal(3, rowCount);
    }

    [Fact]
    public async Task FixedPlainValueOverLimitFails()
    {
        using var tracker = new PoolTracker();
        var bytes = ParquetFixtureBuilder.CreateInt32(new()
        {
            PhysicalTypeCode = (int)ParquetPhysicalType.FixedLengthByteArray,
            TypeLength = 4,
            PhysicalValues = new byte[][] { [1, 2, 3, 4], [5, 6, 7, 8], [9, 10, 11, 12] },
        });
        await using var file = await ParquetFile.OpenAsync(
            new MemoryStream(bytes, writable: false),
            ParquetSourceOwnership.Caller,
            new ParquetReaderOptions { MaximumBinaryValueBytes = 2 },
            CancellationToken.None);
        await Assert.ThrowsAsync<ParquetLimitExceededException>(async () =>
        {
            await foreach (var batch in file.ScanAsync(new([file.Metadata.Schema.Columns[0]])))
                batch.Dispose();
        });
    }

    [Fact]
    public async Task FixedDictionaryValueOverLimitFails()
    {
        using var tracker = new PoolTracker();
        var bytes = ParquetFixtureBuilder.CreateInt32(new()
        {
            PhysicalTypeCode = (int)ParquetPhysicalType.FixedLengthByteArray,
            TypeLength = 4,
            PhysicalValues = new byte[][] { [1, 2, 3, 4], [5, 6, 7, 8] },
            DictionaryValues = new byte[][] { [1, 2, 3, 4], [5, 6, 7, 8] },
            DictionaryIndices = [0, 1],
        });
        await using var file = await ParquetFile.OpenAsync(
            new MemoryStream(bytes, writable: false),
            ParquetSourceOwnership.Caller,
            new ParquetReaderOptions { MaximumBinaryValueBytes = 2 },
            CancellationToken.None);
        await Assert.ThrowsAsync<ParquetLimitExceededException>(async () =>
        {
            await foreach (var batch in file.ScanAsync(new([file.Metadata.Schema.Columns[0]])))
                batch.Dispose();
        });
    }

    [Fact]
    public async Task FixedDictionaryValueAtExactLimitSucceeds()
    {
        using var tracker = new PoolTracker();
        var bytes = ParquetFixtureBuilder.CreateInt32(new()
        {
            PhysicalTypeCode = (int)ParquetPhysicalType.FixedLengthByteArray,
            TypeLength = 2,
            PhysicalValues = new byte[][] { [3, 4], [1, 2] },
            DictionaryValues = new byte[][] { [1, 2], [3, 4] },
            DictionaryIndices = [1, 0],
        });
        await using var file = await ParquetFile.OpenAsync(
            new MemoryStream(bytes, writable: false),
            ParquetSourceOwnership.Caller,
            new ParquetReaderOptions { MaximumBinaryValueBytes = 2 },
            CancellationToken.None);
        var values = new List<byte[]>();
        await foreach (var batch in file.ScanAsync(new([file.Metadata.Schema.Columns[0]])))
        {
            using (batch)
            {
                var column = Assert.IsType<ParquetFixedLengthByteArrayColumnBatch>(batch.Columns[0]);
                for (var row = 0; row < batch.RowCount; row++)
                    values.Add(column.Payload.Span.Slice(row * column.TypeWidth, column.TypeWidth).ToArray());
            }
        }

        Assert.Equal([[3, 4], [1, 2]], values);
    }

    [Fact]
    public async Task FixedBatchAtExactLimitSucceedsInOneBatch()
    {
        using var tracker = new PoolTracker();
        var bytes = ParquetFixtureBuilder.CreateInt32(new()
        {
            PhysicalTypeCode = (int)ParquetPhysicalType.FixedLengthByteArray,
            TypeLength = 2,
            PhysicalValues = new byte[][] { [1, 2], [3, 4], [5, 6] },
        });
        await using var file = await ParquetFile.OpenAsync(
            new MemoryStream(bytes, writable: false),
            ParquetSourceOwnership.Caller,
            new ParquetReaderOptions { MaximumBinaryBatchBytes = 6 },
            CancellationToken.None);
        var batches = 0;
        await foreach (var batch in file.ScanAsync(new([file.Metadata.Schema.Columns[0]])))
        {
            using (batch)
            {
                batches++;
                Assert.Equal(3, batch.RowCount);
                Assert.Equal(6, Assert.IsType<ParquetFixedLengthByteArrayColumnBatch>(batch.Columns[0]).Payload.Length);
            }
        }

        Assert.Equal(1, batches);
    }

    [Fact]
    public async Task FixedBatchShortensWhenFeasible()
    {
        using var tracker = new PoolTracker();
        var bytes = ParquetFixtureBuilder.CreateInt32(new()
        {
            PhysicalTypeCode = (int)ParquetPhysicalType.FixedLengthByteArray,
            TypeLength = 4,
            PhysicalValues = new byte[][] { [1, 2, 3, 4], [5, 6, 7, 8], [9, 10, 11, 12] },
        });
        await using var file = await ParquetFile.OpenAsync(
            new MemoryStream(bytes, writable: false),
            ParquetSourceOwnership.Caller,
            new ParquetReaderOptions { MaximumBinaryBatchBytes = 5 },
            CancellationToken.None);
        var payloads = new List<byte[]>();
        await foreach (var batch in file.ScanAsync(new([file.Metadata.Schema.Columns[0]])))
        {
            using (batch)
            {
                Assert.Equal(1, batch.RowCount);
                payloads.Add(Assert.IsType<ParquetFixedLengthByteArrayColumnBatch>(batch.Columns[0]).Payload.ToArray());
            }
        }

        Assert.Equal([[1, 2, 3, 4], [5, 6, 7, 8], [9, 10, 11, 12]], payloads);
    }

    [Fact]
    public async Task FixedSingleValueOverBatchLimitFails()
    {
        using var tracker = new PoolTracker();
        var bytes = ParquetFixtureBuilder.CreateInt32(new()
        {
            PhysicalTypeCode = (int)ParquetPhysicalType.FixedLengthByteArray,
            TypeLength = 4,
            PhysicalValues = new byte[][] { [1, 2, 3, 4] },
        });
        await using var file = await ParquetFile.OpenAsync(
            new MemoryStream(bytes, writable: false),
            ParquetSourceOwnership.Caller,
            new ParquetReaderOptions { MaximumBinaryBatchBytes = 3 },
            CancellationToken.None);
        var exception = await Assert.ThrowsAsync<ParquetLimitExceededException>(async () =>
        {
            await foreach (var batch in file.ScanAsync(new([file.Metadata.Schema.Columns[0]], null, null, 1)))
                batch.Dispose();
        });
        Assert.Null(exception.ByteOffset);
        Assert.Equal(0, exception.RowGroupOrdinal);
        Assert.Equal(0, exception.ColumnOrdinal);
        Assert.Equal(0, exception.PageOrdinal);
    }

    [Fact]
    public async Task MissingFixedWidthIsMalformed()
    {
        var bytes = ParquetFixtureBuilder.CreateInt32(new()
        {
            PhysicalTypeCode = (int)ParquetPhysicalType.FixedLengthByteArray,
            TypeLength = null,
            RowGroupMode = FixtureRowGroupMode.Omitted,
        });
        var exception = await Assert.ThrowsAsync<ParquetFormatException>(async () =>
            await ParquetFile.OpenAsync(new MemoryStream(bytes, writable: false)));
        Assert.Contains("type length", exception.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task InvalidFixedWidthZeroIsMalformed()
    {
        var bytes = ParquetFixtureBuilder.CreateInt32(new()
        {
            PhysicalTypeCode = (int)ParquetPhysicalType.FixedLengthByteArray,
            TypeLength = 0,
            RowGroupMode = FixtureRowGroupMode.Omitted,
        });
        await Assert.ThrowsAsync<ParquetFormatException>(async () =>
            await ParquetFile.OpenAsync(new MemoryStream(bytes, writable: false)));
    }

    [Fact]
    public async Task ProjectedFixedBatchesObeyBatchLimit()
    {
        using var tracker = new PoolTracker();
        var bytes = ParquetFixtureBuilder.CreateBinaryColumns(
        [
            new FixtureBinaryColumn
            {
                Name = "left",
                PhysicalTypeCode = (int)ParquetPhysicalType.FixedLengthByteArray,
                TypeLength = 2,
                PhysicalValues = new byte[][] { [1, 2], [3, 4], [5, 6] },
            },
            new FixtureBinaryColumn
            {
                Name = "right",
                PhysicalTypeCode = (int)ParquetPhysicalType.FixedLengthByteArray,
                TypeLength = 2,
                PhysicalValues = new byte[][] { [7, 8], [9, 10], [11, 12] },
            },
        ]);
        await using var file = await ParquetFile.OpenAsync(
            new MemoryStream(bytes, writable: false),
            ParquetSourceOwnership.Caller,
            new ParquetReaderOptions { MaximumBinaryBatchBytes = 5 },
            CancellationToken.None);
        var rowCounts = new List<int>();
        var leftPayloads = new List<byte[]>();
        var rightPayloads = new List<byte[]>();
        await foreach (var batch in file.ScanAsync(new ParquetScanOptions([file.Metadata.Schema.Columns[0], file.Metadata.Schema.Columns[1]])))
        {
            using (batch)
            {
                rowCounts.Add(batch.RowCount);
                leftPayloads.Add(Assert.IsType<ParquetFixedLengthByteArrayColumnBatch>(batch.Columns[0]).Payload.ToArray());
                rightPayloads.Add(Assert.IsType<ParquetFixedLengthByteArrayColumnBatch>(batch.Columns[1]).Payload.ToArray());
                var aggregate = Assert.IsType<ParquetFixedLengthByteArrayColumnBatch>(batch.Columns[0]).Payload.Length +
                    Assert.IsType<ParquetFixedLengthByteArrayColumnBatch>(batch.Columns[1]).Payload.Length;
                Assert.True(aggregate <= 5, $"An aligned batch retains {aggregate} binary bytes above the limit of 5.");
            }
        }

        Assert.Equal([1, 1, 1], rowCounts);
        Assert.Equal([[1, 2], [3, 4], [5, 6]], leftPayloads);
        Assert.Equal([[7, 8], [9, 10], [11, 12]], rightPayloads);
    }

    [Fact]
    public async Task MixedBinaryAndFixedProjectionObeysAggregateCap()
    {
        using var tracker = new PoolTracker();
        var bytes = ParquetFixtureBuilder.CreateBinaryColumns(
        [
            new FixtureBinaryColumn
            {
                Name = "binary",
                PhysicalTypeCode = (int)ParquetPhysicalType.ByteArray,
                PhysicalValues = new byte[][] { [1, 2], [3, 4], [5, 6] },
            },
            new FixtureBinaryColumn
            {
                Name = "fixed",
                PhysicalTypeCode = (int)ParquetPhysicalType.FixedLengthByteArray,
                TypeLength = 2,
                PhysicalValues = new byte[][] { [7, 8], [9, 10], [11, 12] },
            },
        ]);
        await using var file = await ParquetFile.OpenAsync(
            new MemoryStream(bytes, writable: false),
            ParquetSourceOwnership.Caller,
            new ParquetReaderOptions { MaximumBinaryBatchBytes = 5 },
            CancellationToken.None);
        var rowCounts = new List<int>();
        await foreach (var batch in file.ScanAsync(new ParquetScanOptions([file.Metadata.Schema.Columns[0], file.Metadata.Schema.Columns[1]])))
        {
            using (batch)
            {
                rowCounts.Add(batch.RowCount);
                var binary = Assert.IsType<ParquetBinaryColumnBatch>(batch.Columns[0]);
                var fixedBytes = Assert.IsType<ParquetFixedLengthByteArrayColumnBatch>(batch.Columns[1]);
                var aggregate = binary.Payload.Length + fixedBytes.Payload.Length;
                Assert.True(aggregate <= 5, $"An aligned batch retains {aggregate} binary bytes above the limit of 5.");
            }
        }

        Assert.Equal([1, 1, 1], rowCounts);
    }

    [Fact]
    public async Task CancelledFixedScanBalancesPools()
    {
        using var tracker = new PoolTracker();
        var bytes = ParquetFixtureBuilder.CreateInt32(new()
        {
            PhysicalTypeCode = (int)ParquetPhysicalType.FixedLengthByteArray,
            TypeLength = 2,
            PhysicalValues = new byte[][] { [1, 2], [3, 4], [5, 6] },
        });
        await using var file = await ParquetFile.OpenAsync(new MemoryStream(bytes, writable: false));
        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
        {
            await foreach (var batch in file.ScanAsync(new([file.Metadata.Schema.Columns[0]]), cancellation.Token))
                batch.Dispose();
        });
    }
}
