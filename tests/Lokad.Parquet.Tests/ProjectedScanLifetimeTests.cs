namespace Lokad.Parquet.Tests;

public sealed class ProjectedScanLifetimeTests
{
    [Fact]
    public async Task CompletedProjectedScanDisposalDoesNotUnregisterNewScan()
    {
        var bytes = ParquetFixtureBuilder.CreateRequiredInt32Columns(
        [
            new RequiredInt32FixtureColumn { Name = "a", Pages = [[1, 2, 3]] },
            new RequiredInt32FixtureColumn { Name = "b", Pages = [[4, 5, 6]] },
        ]);
        await using var file = await ParquetFile.OpenAsync(new MemoryStream(bytes, writable: false));
        var options = new ParquetScanOptions([file.Metadata.Schema.Columns[0], file.Metadata.Schema.Columns[1]]);
        var old = file.ScanAsync(options).GetAsyncEnumerator();
        while (await old.MoveNextAsync())
        {
            old.Current.Dispose();
        }

        await using var current = file.ScanAsync(options).GetAsyncEnumerator();
        await old.DisposeAsync();
        Assert.True(await current.MoveNextAsync());
        current.Current.Dispose();
        Assert.False(await current.MoveNextAsync());
    }

    [Fact]
    public async Task RepeatedProjectedScanDisposalDoesNotUnregisterNewScan()
    {
        var bytes = ParquetFixtureBuilder.CreateRequiredInt32Columns(
        [
            new RequiredInt32FixtureColumn { Name = "a", Pages = [[1, 2, 3]] },
            new RequiredInt32FixtureColumn { Name = "b", Pages = [[4, 5, 6]] },
        ]);
        await using var file = await ParquetFile.OpenAsync(new MemoryStream(bytes, writable: false));
        var options = new ParquetScanOptions([file.Metadata.Schema.Columns[0], file.Metadata.Schema.Columns[1]]);
        var old = file.ScanAsync(options).GetAsyncEnumerator();
        while (await old.MoveNextAsync())
        {
            old.Current.Dispose();
        }

        await using var current = file.ScanAsync(options).GetAsyncEnumerator();
        await old.DisposeAsync();
        await old.DisposeAsync();
        Assert.True(await current.MoveNextAsync());
        current.Current.Dispose();
    }
}
