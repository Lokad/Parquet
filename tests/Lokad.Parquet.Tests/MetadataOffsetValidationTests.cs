namespace Lokad.Parquet.Tests;

public sealed class MetadataOffsetValidationTests
{
    [Fact]
    public async Task IndexAndBloomOffsetsOutsideInputAreRejected()
    {
        var bytes = ParquetFixtureBuilder.CreateInt32(new() { Values = [1], AuxiliaryOffset = 1_000_000, AuxiliaryLength = 1 });
        await Assert.ThrowsAsync<ParquetFormatException>(async () =>
        {
            await ParquetFile.OpenAsync(bytes, ParquetReaderOptions.Default, CancellationToken.None);
        });
    }

    [Fact]
    public async Task AbsentIndexAndBloomOffsetsOpen()
    {
        var bytes = ParquetFixtureBuilder.CreateInt32(new() { Values = [1] });
        await using var file = await ParquetFile.OpenAsync(bytes, ParquetReaderOptions.Default, CancellationToken.None);
        Assert.NotNull(file.Metadata);
    }

    [Fact]
    public async Task BloomOffsetWithoutLengthOpens()
    {
        // A present in-range bloom offset with no length stays legitimate: the
        // reader records the location without consuming it.
        var bytes = ParquetFixtureBuilder.CreateInt32(new() { Values = [1], AuxiliaryOffset = 4, AuxiliaryLength = 1, OmitBloomLength = true });
        await using var file = await ParquetFile.OpenAsync(bytes, ParquetReaderOptions.Default, CancellationToken.None);
        var chunk = file.Metadata.RowGroups[0].Columns[0];
        Assert.Equal(4, chunk.BloomFilterOffset);
        Assert.Null(chunk.BloomFilterLength);
    }

    [Fact]
    public async Task BloomLengthWithoutOffsetIsRejected()
    {
        var bytes = ParquetFixtureBuilder.CreateInt32(new() { Values = [1], AuxiliaryOffset = 4, AuxiliaryLength = 1, BloomLengthWithoutOffset = true });
        await Assert.ThrowsAsync<ParquetFormatException>(async () =>
        {
            await ParquetFile.OpenAsync(bytes, ParquetReaderOptions.Default, CancellationToken.None);
        });
    }
}
