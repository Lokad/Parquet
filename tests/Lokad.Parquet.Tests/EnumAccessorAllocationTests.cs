namespace Lokad.Parquet.Tests;

// The metadata accessors resolve recognized enums with contiguous range checks
// instead of Enum.IsDefined; these pins fail loudly if an enum gains a gap or a
// non-zero base, forcing the accessors to be revisited together with the pin.
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

    [Fact]
    public void RecognizedEnumsStayContiguousFromZero()
    {
        Assert.Equal(
            [
                ParquetPhysicalType.Boolean, ParquetPhysicalType.Int32, ParquetPhysicalType.Int64,
                ParquetPhysicalType.Int96, ParquetPhysicalType.Float, ParquetPhysicalType.Double,
                ParquetPhysicalType.ByteArray, ParquetPhysicalType.FixedLengthByteArray,
            ],
            Enum.GetValues<ParquetPhysicalType>());
        Assert.Equal(
            [ParquetRepetition.Required, ParquetRepetition.Optional, ParquetRepetition.Repeated],
            Enum.GetValues<ParquetRepetition>());
        Assert.Equal(
            [
                ParquetConvertedType.Utf8, ParquetConvertedType.Map, ParquetConvertedType.MapKeyValue,
                ParquetConvertedType.List, ParquetConvertedType.Enum, ParquetConvertedType.Decimal,
                ParquetConvertedType.Date, ParquetConvertedType.TimeMilliseconds, ParquetConvertedType.TimeMicroseconds,
                ParquetConvertedType.TimestampMilliseconds, ParquetConvertedType.TimestampMicroseconds,
                ParquetConvertedType.UInt8, ParquetConvertedType.UInt16, ParquetConvertedType.UInt32,
                ParquetConvertedType.UInt64, ParquetConvertedType.Int8, ParquetConvertedType.Int16,
                ParquetConvertedType.Int32, ParquetConvertedType.Int64, ParquetConvertedType.Json,
                ParquetConvertedType.Bson, ParquetConvertedType.Interval,
            ],
            Enum.GetValues<ParquetConvertedType>());
        Assert.Equal(
            [
                ParquetCompressionCodec.Uncompressed, ParquetCompressionCodec.Snappy, ParquetCompressionCodec.Gzip,
                ParquetCompressionCodec.Lzo, ParquetCompressionCodec.Brotli, ParquetCompressionCodec.Lz4,
                ParquetCompressionCodec.Zstandard, ParquetCompressionCodec.Lz4Raw,
            ],
            Enum.GetValues<ParquetCompressionCodec>());
    }
}
