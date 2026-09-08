namespace Lokad.Parquet.Tests;

using System.Reflection;

public sealed class CacheEvictionTests
{
    [Fact]
    public async Task SecondProjectionSucceedsAfterFirstColumnCacheEvicted()
    {
        using var tracker = new PoolTracker();
        var bytes = ParquetFixtureBuilder.CreateRequiredInt32Columns(
        [
            new RequiredInt32FixtureColumn { Name = "a", Pages = [[1, 2, 3]] },
            new RequiredInt32FixtureColumn { Name = "b", Pages = [[4, 5, 6]] },
        ]);
        var options = new ParquetReaderOptions { MaximumScanPooledBytes = 128 };
        await using (var file = await ParquetFile.OpenAsync(new MemoryStream(bytes, writable: false), ParquetSourceOwnership.Caller, options, CancellationToken.None))
        {
            await using var first = file.ScanAsync(new([file.Metadata.Schema.Columns[0]], null, null, 3)).GetAsyncEnumerator();
            Assert.True(await first.MoveNextAsync());
            using (var batch = first.Current)
                Assert.Equal([1, 2, 3], Assert.IsType<ParquetPrimitiveColumnBatch<int>>(batch.Columns[0]).Values.ToArray());
            Assert.False(await first.MoveNextAsync());

            await using var second = file.ScanAsync(new([file.Metadata.Schema.Columns[1]], null, null, 3)).GetAsyncEnumerator();
            Assert.True(await second.MoveNextAsync());
            using (var batch = second.Current)
                Assert.Equal([4, 5, 6], Assert.IsType<ParquetPrimitiveColumnBatch<int>>(batch.Columns[0]).Values.ToArray());
            Assert.False(await second.MoveNextAsync());
        }
    }

    [Fact]
    public async Task RescanningSameColumnPreservesWarmedReuse()
    {
        using var tracker = new PoolTracker();
        var bytes = ParquetFixtureBuilder.CreateRequiredInt32Columns(
        [
            new RequiredInt32FixtureColumn { Name = "a", Pages = [[1, 2, 3]] },
            new RequiredInt32FixtureColumn { Name = "b", Pages = [[4, 5, 6]] },
        ]);
        var options = new ParquetReaderOptions { MaximumScanPooledBytes = 128 };
        await using (var file = await ParquetFile.OpenAsync(new MemoryStream(bytes, writable: false), ParquetSourceOwnership.Caller, options, CancellationToken.None))
        {
            for (var scan = 0; scan < 2; scan++)
            {
                await using var enumerator = file.ScanAsync(new([file.Metadata.Schema.Columns[0]], null, null, 3)).GetAsyncEnumerator();
                Assert.True(await enumerator.MoveNextAsync());
                using (var batch = enumerator.Current)
                    Assert.Equal([1, 2, 3], Assert.IsType<ParquetPrimitiveColumnBatch<int>>(batch.Columns[0]).Values.ToArray());
                Assert.False(await enumerator.MoveNextAsync());
            }
        }
    }

    [Fact]
    public async Task ChangingProjectionsAcrossTightBudget()
    {
        using var tracker = new PoolTracker();
        var bytes = ParquetFixtureBuilder.CreateRequiredInt32Columns(
        [
            new RequiredInt32FixtureColumn { Name = "a", Pages = [[1, 2, 3]] },
            new RequiredInt32FixtureColumn { Name = "b", Pages = [[4, 5, 6]] },
        ]);
        var options = new ParquetReaderOptions { MaximumScanPooledBytes = 128 };
        await using (var file = await ParquetFile.OpenAsync(new MemoryStream(bytes, writable: false), ParquetSourceOwnership.Caller, options, CancellationToken.None))
        {
            var expected = new[] { new[] { 1, 2, 3 }, new[] { 4, 5, 6 }, new[] { 1, 2, 3 } };
            for (var scan = 0; scan < 3; scan++)
            {
                var ordinal = scan == 1 ? 1 : 0;
                await using var enumerator = file.ScanAsync(new([file.Metadata.Schema.Columns[ordinal]], null, null, 3)).GetAsyncEnumerator();
                Assert.True(await enumerator.MoveNextAsync());
                using (var batch = enumerator.Current)
                    Assert.Equal(expected[scan], Assert.IsType<ParquetPrimitiveColumnBatch<int>>(batch.Columns[0]).Values.ToArray());
                Assert.False(await enumerator.MoveNextAsync());
            }
        }
    }

    [Fact]
    public async Task GrowingAndShrinkingPagesAcrossRowGroups()
    {
        using var tracker = new PoolTracker();
        var bytes = ParquetFixtureBuilder.CreateRequiredInt32RowGroups([[1, 2, 3, 4, 5, 6, 7, 8], [9]]);
        var options = new ParquetReaderOptions { MaximumScanPooledBytes = 128 };
        var offsets = new List<long>();
        var values = new List<int>();
        await using (var file = await ParquetFile.OpenAsync(new MemoryStream(bytes, writable: false), ParquetSourceOwnership.Caller, options, CancellationToken.None))
        {
            await foreach (var batch in file.ScanAsync(new([file.Metadata.Schema.Columns[0]], null, null, 8)))
            {
                using (batch)
                {
                    offsets.Add(batch.RowOffset);
                    values.AddRange(Assert.IsType<ParquetPrimitiveColumnBatch<int>>(batch.Columns[0]).Values.ToArray());
                }
            }
        }

        Assert.Equal([0L, 8L], offsets);
        Assert.Equal([1, 2, 3, 4, 5, 6, 7, 8, 9], values);
    }

    [Fact]
    public async Task ExhaustedBudgetStillFailsAfterEviction()
    {
        using var tracker = new PoolTracker();
        var bytes = ParquetFixtureBuilder.CreateRequiredInt32Columns(
        [
            new RequiredInt32FixtureColumn { Name = "a", Pages = [[1, 2, 3]] },
            new RequiredInt32FixtureColumn { Name = "b", Pages = [[4, 5, 6]] },
        ]);
        await using (var tiny = await ParquetFile.OpenAsync(
            new MemoryStream(bytes, writable: false),
            ParquetSourceOwnership.Caller,
            new ParquetReaderOptions { MaximumScanPooledBytes = 64 },
            CancellationToken.None))
        {
            await Assert.ThrowsAsync<ParquetLimitExceededException>(async () =>
            {
                await foreach (var batch in tiny.ScanAsync(new([tiny.Metadata.Schema.Columns[0]], null, null, 3)))
                    batch.Dispose();
            });
            var assembly = typeof(ParquetFile).Assembly;
            var budgetType = assembly.GetType("Lokad.Parquet.Internal.ParquetScanMemoryBudget") ??
                throw new InvalidOperationException("The scan memory budget was not found.");
            var budgetProperty = typeof(ParquetFile).GetProperty("ScanMemoryBudget", BindingFlags.NonPublic | BindingFlags.Instance) ??
                throw new InvalidOperationException("The scan memory budget accessor was not found.");
            var budget = budgetProperty.GetValue(tiny) ?? throw new InvalidOperationException("The scan memory budget was not available.");
            var transient = (long)(budgetType.GetProperty("PeakTransientBytes")?.GetValue(budget) ?? throw new InvalidOperationException("The transient peak was not available."));
            Assert.True(transient > 0);
        }
        await using (var minimal = await ParquetFile.OpenAsync(
            new MemoryStream(bytes, writable: false),
            ParquetSourceOwnership.Caller,
            new ParquetReaderOptions { MaximumScanPooledBytes = 1 },
            CancellationToken.None))
        {
            await Assert.ThrowsAsync<ParquetLimitExceededException>(async () =>
            {
                await foreach (var batch in minimal.ScanAsync(new([minimal.Metadata.Schema.Columns[0]], null, null, 3)))
                    batch.Dispose();
            });
        }
    }
}
