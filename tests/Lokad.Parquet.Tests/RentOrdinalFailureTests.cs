namespace Lokad.Parquet.Tests;

public sealed class RentOrdinalFailureTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task EveryRentOrdinalFailureReturnsPriorRents(bool useDictionary)
    {
        byte[] bytes = useDictionary
            ? ParquetFixtureBuilder.CreateInt32(new() { Values = [20, 10, 20], DictionaryValues = new int[] { 10, 20 }, DictionaryIndices = [1, 0, 1] })
            : ParquetFixtureBuilder.CreateInt32(new() { Values = [1, 2, 3], Repetition = ParquetRepetition.Optional });
        var totalRents = await CountRentsAsync(bytes, false);
        Assert.True(totalRents > 2);
        var capped = Math.Min(totalRents, 30);
        for (var ordinal = 1; ordinal <= capped; ordinal++)
        {
            await AssertOrdinalBalancedAsync(bytes, ordinal, false);
        }
    }

    [Fact]
    public async Task MisalignedProjectedRentOrdinalFailuresReturnPriorRents()
    {
        // Misaligned pages mix per-column transfers with projected copies, so each
        // rent ordinal fails at a different point of that interleaving: every prior
        // rent must still be returned, including staged transfer ownership.
        var bytes = ParquetFixtureBuilder.CreateRequiredInt32Columns(
        [
            new RequiredInt32FixtureColumn { Name = "a", Pages = [[1, 2, 3, 4, 5, 6]] },
            new RequiredInt32FixtureColumn { Name = "b", Pages = [[1, 2], [3, 4, 5, 6]] },
        ]);
        var totalRents = await CountRentsAsync(bytes, true);
        Assert.True(totalRents > 2);
        var capped = Math.Min(totalRents, 20);
        for (var ordinal = 1; ordinal <= capped; ordinal++)
        {
            await AssertOrdinalBalancedAsync(bytes, ordinal, true);
        }
    }

    [Fact]
    public async Task ProjectedRentOrdinalFailuresReturnPriorRents()
    {
        var bytes = ParquetFixtureBuilder.CreateRequiredInt32Columns(
        [
            new RequiredInt32FixtureColumn { Name = "a", Pages = [[1, 2, 3]] },
            new RequiredInt32FixtureColumn { Name = "b", Pages = [[4, 5, 6]] },
        ]);
        var totalRents = await CountRentsAsync(bytes, true);
        Assert.True(totalRents > 2);
        var capped = Math.Min(totalRents, 20);
        for (var ordinal = 1; ordinal <= capped; ordinal++)
        {
            await AssertOrdinalBalancedAsync(bytes, ordinal, true);
        }
    }

    private static async Task<int> CountRentsAsync(byte[] bytes, bool projected)
    {
        var rents = 0;
        PoolTracker.SetObservers((_, _) => rents++, null);
        try
        {
            await using var file = await ParquetFile.OpenAsync(new MemoryStream(bytes, writable: false));
            if (projected)
            {
                var options = new ParquetScanOptions([file.Metadata.Schema.Columns[0], file.Metadata.Schema.Columns[1]]);
                await foreach (var batch in file.ScanAsync(options))
                {
                    batch.Dispose();
                }
            }
            else
            {
                await foreach (var batch in file.ScanAsync(new([file.Metadata.Schema.Columns[0]], null, null, 3)))
                {
                    batch.Dispose();
                }
            }

            return rents;
        }
        finally
        {
            PoolTracker.ClearObservers();
        }
    }

    private static async Task AssertOrdinalBalancedAsync(byte[] bytes, int failAtOrdinal, bool projected)
    {
        var outstanding = new PoolOutstandingArrays();
        var rents = 0;
        var triggered = false;
        PoolTracker.SetObservers(
            (array, _) =>
            {
                rents++;
                if (rents == failAtOrdinal)
                {
                    triggered = true;
                    throw new InvalidOperationException("injected rent failure");
                }

                outstanding.NoteRent(array);
            },
            (array, _) => outstanding.NoteReturn(array));
        try
        {
            try
            {
                await using var file = await ParquetFile.OpenAsync(new MemoryStream(bytes, writable: false));
                if (projected)
                {
                    var options = new ParquetScanOptions([file.Metadata.Schema.Columns[0], file.Metadata.Schema.Columns[1]]);
                    await foreach (var batch in file.ScanAsync(options))
                    {
                        batch.Dispose();
                    }
                }
                else
                {
                    await foreach (var batch in file.ScanAsync(new([file.Metadata.Schema.Columns[0]], null, null, 3)))
                    {
                        batch.Dispose();
                    }
                }
            }
            catch (InvalidOperationException exception) when (exception.Message == "injected rent failure")
            {
            }
            Assert.True(triggered);
            Assert.True(outstanding.IsEmpty);
        }
        finally
        {
            PoolTracker.ClearObservers();
        }
    }
}
