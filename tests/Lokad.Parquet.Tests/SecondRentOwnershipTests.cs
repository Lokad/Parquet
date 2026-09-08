namespace Lokad.Parquet.Tests;

public sealed class SecondRentOwnershipTests
{
    [Fact]
    public async Task OptionalInt32SecondRentFailureReturnsFirstRent()
    {
        using var tracker = new PoolTracker();
        var bytes = ParquetFixtureBuilder.CreateInt32(new() { Values = [1, 2, 3], Repetition = ParquetRepetition.Optional });
        await using (var file = await ParquetFile.OpenAsync(
            new MemoryStream(bytes, writable: false),
            ParquetSourceOwnership.Caller,
            new ParquetReaderOptions { MaximumScanPooledBytes = 130 },
            CancellationToken.None))
        {
            await Assert.ThrowsAsync<ParquetLimitExceededException>(async () =>
            {
                await foreach (var batch in file.ScanAsync(new([file.Metadata.Schema.Columns[0]], null, null, 3)))
                {
                    batch.Dispose();
                }
            });
        }
    }

    [Fact]
    public async Task ByteArraySecondRentFailureReturnsFirstRent()
    {
        // Whole-page binary batches transfer decoded storage without renting
        // (R22), so the copy path is exercised with a partial batch here.
        using var tracker = new PoolTracker();
        var bytes = ParquetFixtureBuilder.CreateInt32(new() { PhysicalTypeCode = (int)ParquetPhysicalType.ByteArray, PhysicalValues = new byte[][] { new byte[17], new byte[17], new byte[17] } });
        await using (var file = await ParquetFile.OpenAsync(
            new MemoryStream(bytes, writable: false),
            ParquetSourceOwnership.Caller,
            new ParquetReaderOptions { MaximumScanPooledBytes = 336 },
            CancellationToken.None))
        {
            await Assert.ThrowsAsync<ParquetLimitExceededException>(async () =>
            {
                await foreach (var batch in file.ScanAsync(new([file.Metadata.Schema.Columns[0]], null, null, 1)))
                {
                    batch.Dispose();
                }
            });
        }
    }
}
