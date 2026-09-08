namespace Lokad.Parquet.Tests;

using System.Reflection;

public sealed class RentOrdinalFailureTests
{
    private static readonly Type PoolType =
        (typeof(ParquetFile).Assembly.GetType("Lokad.Parquet.Internal.ParquetArrayPool") ??
            throw new InvalidOperationException("Pool facade was not found."));
    private static readonly PropertyInfo RentObserverProperty =
        PoolType.GetProperty("RentObserver", BindingFlags.Public | BindingFlags.Static) ??
            throw new InvalidOperationException("Rent observer was not found.");
    private static readonly PropertyInfo ReturnObserverProperty =
        PoolType.GetProperty("ReturnObserver", BindingFlags.Public | BindingFlags.Static) ??
            throw new InvalidOperationException("Return observer was not found.");

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
        RentObserverProperty.SetValue(null, (Action<Array, int>)((_, _) => rents++));
        ReturnObserverProperty.SetValue(null, null);
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
            RentObserverProperty.SetValue(null, null);
            ReturnObserverProperty.SetValue(null, null);
        }
    }

    private static async Task AssertOrdinalBalancedAsync(byte[] bytes, int failAtOrdinal, bool projected)
    {
        var outstanding = new Dictionary<Array, int>(ReferenceEqualityComparer.Instance);
        var rents = 0;
        var triggered = false;
        RentObserverProperty.SetValue(null, (Action<Array, int>)((array, requested) =>
        {
            rents++;
            if (rents == failAtOrdinal)
            {
                triggered = true;
                throw new InvalidOperationException("injected rent failure");
            }

            lock (outstanding)
            {
                outstanding.Add(array, array.Length);
            }
        }));
        ReturnObserverProperty.SetValue(null, (Action<Array, int>)((array, _) =>
        {
            lock (outstanding)
            {
                outstanding.Remove(array);
            }
        }));
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
            Assert.Empty(outstanding);
        }
        finally
        {
            RentObserverProperty.SetValue(null, null);
            ReturnObserverProperty.SetValue(null, null);
        }
    }
}
