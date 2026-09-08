namespace Lokad.Parquet.Tests;

public sealed class EnumAccessorAllocationTests
{
    [Fact]
    public async Task PhysicalTypeGetterDoesNotAllocate()
    {
        var bytes = ParquetFixtureBuilder.CreateInt32(new() { Values = [1] });
        await using var file = await ParquetFile.OpenAsync(bytes, ParquetReaderOptions.Default, CancellationToken.None);
        var element = file.Metadata.Schema.Columns[0].SchemaElement;
        for (var i = 0; i < 100_000; i++)
        {
            _ = element.PhysicalType;
        }

        var before = GC.GetAllocatedBytesForCurrentThread();
        var sum = 0;
        for (var i = 0; i < 100_000; i++)
        {
            sum += (int)(element.PhysicalType ?? 0);
        }

        var allocated = GC.GetAllocatedBytesForCurrentThread() - before;
        Assert.Equal(100000 * (int)ParquetPhysicalType.Int32, sum);
        Assert.Equal(0, allocated);
    }

    [Fact]
    public async Task UnknownRawValuesArePreserved()
    {
        var bytes = ParquetFixtureBuilder.CreateInt32(new() { Values = [1] });
        await using var file = await ParquetFile.OpenAsync(bytes, ParquetReaderOptions.Default, CancellationToken.None);
        var element = file.Metadata.Schema.Columns[0].SchemaElement;
        Assert.NotNull(element.PhysicalType);
        Assert.Equal((int)ParquetPhysicalType.Int32, element.PhysicalTypeCode);
    }
}
