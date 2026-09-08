namespace Lokad.Parquet.Tests;

public sealed class ThriftContainerLimitTests
{
    [Fact]
    public async Task InlineCollectionCountRespectsContainerLimit()
    {
        var bytes = ParquetFixtureBuilder.CreateInt32(new() { Values = [1] });
        await Assert.ThrowsAsync<ParquetLimitExceededException>(async () =>
        {
            await ParquetFile.OpenAsync(bytes, new() { MaximumThriftContainerElements = 1 }, CancellationToken.None);
        });
    }

    [Fact]
    public async Task ContainerLimitAtExactCountOpens()
    {
        var bytes = ParquetFixtureBuilder.CreateInt32(new() { Values = [1] });
        await using var file = await ParquetFile.OpenAsync(bytes, new() { MaximumThriftContainerElements = 2 }, CancellationToken.None);
        Assert.True(file.Metadata.Schema.Elements.Count >= 2);
    }

    [Fact]
    public async Task ExtendedCollectionCountRespectsContainerLimit()
    {
        // Fourteen columns make a fifteen-entry schema list, crossing from inline
        // counts into extended varint counts: limit 14 must reject the parsed list.
        var columns = new RequiredInt32FixtureColumn[14];
        for (var i = 0; i < columns.Length; i++)
        {
            columns[i] = new RequiredInt32FixtureColumn { Name = "c" + i, Pages = [[i]] };
        }

        var bytes = ParquetFixtureBuilder.CreateRequiredInt32Columns(columns);
        await Assert.ThrowsAsync<ParquetLimitExceededException>(async () =>
        {
            await ParquetFile.OpenAsync(bytes, new() { MaximumThriftContainerElements = 14 }, CancellationToken.None);
        });
    }

    [Fact]
    public async Task ExtendedCollectionCountAtExactCountOpens()
    {
        var columns = new RequiredInt32FixtureColumn[14];
        for (var i = 0; i < columns.Length; i++)
        {
            columns[i] = new RequiredInt32FixtureColumn { Name = "c" + i, Pages = [[i]] };
        }

        var bytes = ParquetFixtureBuilder.CreateRequiredInt32Columns(columns);
        await using var file = await ParquetFile.OpenAsync(bytes, new() { MaximumThriftContainerElements = 15 }, CancellationToken.None);
        Assert.Equal(15, file.Metadata.Schema.Elements.Count);
    }
}
