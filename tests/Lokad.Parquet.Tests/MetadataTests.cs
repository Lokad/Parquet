using System.Buffers.Binary;

namespace Lokad.Parquet.Tests;

public sealed class MetadataTests
{
    [Fact]
    public async Task OpensGeneratedRequiredInt32Metadata()
    {
        var bytes = ParquetFixtureBuilder.CreateInt32(new() { Values = [10, -2, 42] });
        await using var stream = new MemoryStream(bytes, writable: false);
        await using var file = await ParquetFile.OpenAsync(stream);

        Assert.Equal(bytes.Length, file.Length);
        Assert.Equal(3, file.Metadata.RowCount);
        Assert.Single(file.Metadata.Schema.Columns);
        Assert.Single(file.Metadata.RowGroups);
        Assert.Equal(ParquetPhysicalType.Int32, file.Metadata.Schema.Columns[0].SchemaElement.PhysicalType);
        Assert.Equal(ParquetRepetition.Required, file.Metadata.Schema.Columns[0].SchemaElement.Repetition);
        Assert.True(file.Metadata.Schema.Columns[0].IsReadable);
        Assert.Equal(3, file.Metadata.RowGroups[0].Columns[0].ValueCount);
    }

    [Fact]
    public async Task OpensSchemaOnlyFileWithNoRowGroups()
    {
        var bytes = ParquetFixtureBuilder.CreateInt32(new() { RowGroupMode = FixtureRowGroupMode.Omitted });
        await using var file = await ParquetFile.OpenAsync(new MemoryStream(bytes, writable: false));

        Assert.Equal(0, file.Metadata.RowCount);
        Assert.Empty(file.Metadata.RowGroups);
        Assert.Single(file.Metadata.Schema.Columns);
    }

    [Fact]
    public async Task AcceptsRequiredRootMarkerWithoutChangingLevels()
    {
        var bytes = ParquetFixtureBuilder.CreateInt32(new()
        {
            RowGroupMode = FixtureRowGroupMode.Omitted,
            RootRepetition = FixtureRootRepetition.Required,
        });
        await using var file = await ParquetFile.OpenAsync(new MemoryStream(bytes, writable: false));

        Assert.Equal(ParquetRepetition.Required, file.Metadata.Schema.Elements[0].Repetition);
        Assert.Equal(0, file.Metadata.Schema.Columns[0].SchemaElement.MaximumDefinitionLevel);
        Assert.Equal(0, file.Metadata.Schema.Columns[0].SchemaElement.MaximumRepetitionLevel);
    }

    [Fact]
    public async Task PreservesLegacyAnnotationWithoutModernDuplicate()
    {
        var bytes = ParquetFixtureBuilder.CreateInt32(new()
        {
            RowGroupMode = FixtureRowGroupMode.Omitted,
            ConvertedType = (int)ParquetConvertedType.Date,
        });
        await using var file = await ParquetFile.OpenAsync(new MemoryStream(bytes, writable: false));
        var element = file.Metadata.Schema.Columns[0].SchemaElement;

        Assert.Equal(ParquetConvertedType.Date, element.ConvertedType);
        Assert.Null(element.LogicalAnnotation);
        Assert.Equal(ParquetAnnotationStatus.LegacyOnly, element.AnnotationStatus);
    }

    [Fact]
    public async Task RetainsUnknownTimeUnitWithoutBlockingPhysicalRead()
    {
        var bytes = ParquetFixtureBuilder.CreateInt32(new()
        {
            RowGroupMode = FixtureRowGroupMode.Omitted,
            LogicalTypeDiscriminator = (int)ParquetLogicalTypeKind.Time,
            TimeUnitDiscriminator = 9,
        });
        await using var file = await ParquetFile.OpenAsync(new MemoryStream(bytes, writable: false));
        var column = file.Metadata.Schema.Columns[0];

        var annotation = Assert.IsType<ParquetLogicalAnnotation>(column.SchemaElement.LogicalAnnotation);
        Assert.Equal(ParquetLogicalTypeKind.Time, annotation.Kind);
        Assert.Equal(9, column.SchemaElement.LogicalAnnotation.TimeUnitDiscriminator);
        Assert.Null(column.SchemaElement.LogicalAnnotation.TimeUnit);
        Assert.True(column.IsReadable);
    }

    [Fact]
    public async Task ReportsConflictingModernAndLegacyAnnotationsWithoutBlockingPhysicalRead()
    {
        var bytes = ParquetFixtureBuilder.CreateInt32(new()
        {
            RowGroupMode = FixtureRowGroupMode.Omitted,
            ConvertedType = (int)ParquetConvertedType.Date,
            LogicalTypeDiscriminator = (int)ParquetLogicalTypeKind.String,
        });
        await using var file = await ParquetFile.OpenAsync(new MemoryStream(bytes, writable: false));
        var column = file.Metadata.Schema.Columns[0];

        Assert.Equal(ParquetAnnotationStatus.Conflict, column.SchemaElement.AnnotationStatus);
        Assert.True(column.IsReadable);
    }

    [Fact]
    public async Task TreatsAdvertisedEncodingsAndRowGroupTotalAsAdvisory()
    {
        var bytes = ParquetFixtureBuilder.CreateInt32(new()
        {
            Values = [1],
            AdvertisedEncodings = [(int)ParquetEncoding.Plain],
            RowGroupTotalByteSize = 1,
        });
        await using var file = await ParquetFile.OpenAsync(new MemoryStream(bytes, writable: false));

        Assert.Equal(1, file.Metadata.RowGroups[0].TotalByteSize);
        Assert.Equal([(int)ParquetEncoding.Plain], file.Metadata.RowGroups[0].Columns[0].EncodingCodes);
    }

    [Fact]
    public async Task RetainsButDoesNotConsumeBoundedIndexAndBloomLocations()
    {
        var bytes = ParquetFixtureBuilder.CreateInt32(new()
        {
            Values = [1],
            AuxiliaryOffset = 4,
            AuxiliaryLength = 1,
        });
        await using var file = await ParquetFile.OpenAsync(new MemoryStream(bytes, writable: false));
        var chunk = file.Metadata.RowGroups[0].Columns[0];

        Assert.Equal(4, chunk.IndexPageOffset);
        Assert.Equal(4, chunk.OffsetIndexOffset);
        Assert.Equal(1, chunk.OffsetIndexLength);
        Assert.Equal(4, chunk.ColumnIndexOffset);
        Assert.Equal(1, chunk.ColumnIndexLength);
        Assert.Equal(4, chunk.BloomFilterOffset);
        Assert.Equal(1, chunk.BloomFilterLength);
    }

    [Fact]
    public async Task RejectsIndexAndBloomLocationsOutsideTheInput()
    {
        var bytes = ParquetFixtureBuilder.CreateInt32(new()
        {
            Values = [1],
            AuxiliaryOffset = long.MaxValue,
            AuxiliaryLength = 1,
        });

        await Assert.ThrowsAsync<ParquetFormatException>(async () =>
            await ParquetFile.OpenAsync(new MemoryStream(bytes, writable: false)));
    }

    [Fact]
    public async Task PreservesColumnOrder()
    {
        var bytes = ParquetFixtureBuilder.CreateInt32(new()
        {
            RowGroupMode = FixtureRowGroupMode.Omitted,
            ColumnOrder = FixtureColumnOrder.TypeDefined,
        });
        await using var file = await ParquetFile.OpenAsync(new MemoryStream(bytes, writable: false));

        Assert.Equal([ParquetColumnOrderKind.TypeDefined], file.Metadata.ColumnOrders);
    }

    [Fact]
    public async Task RejectsWrongTrailingMagic()
    {
        var bytes = ParquetFixtureBuilder.CreateInt32(new() { RowGroupMode = FixtureRowGroupMode.Omitted });
        bytes[^1] ^= 1;

        await Assert.ThrowsAsync<ParquetFormatException>(async () =>
            await ParquetFile.OpenAsync(new MemoryStream(bytes, writable: false)));
    }

    [Fact]
    public async Task RejectsFooterOutsideInput()
    {
        var bytes = ParquetFixtureBuilder.CreateInt32(new() { RowGroupMode = FixtureRowGroupMode.Omitted });
        BinaryPrimitives.WriteInt32LittleEndian(bytes.AsSpan(bytes.Length - 8), bytes.Length);

        await Assert.ThrowsAsync<ParquetFormatException>(async () =>
            await ParquetFile.OpenAsync(new MemoryStream(bytes, writable: false)));
    }

    [Fact]
    public async Task EnforcesFooterLimitBeforeFooterRent()
    {
        var bytes = ParquetFixtureBuilder.CreateInt32(new() { RowGroupMode = FixtureRowGroupMode.Omitted });
        var options = new ParquetReaderOptions { MaximumFooterBytes = 8 };

        await Assert.ThrowsAsync<ParquetLimitExceededException>(async () =>
            await ParquetFile.OpenAsync(
                new MemoryStream(bytes, writable: false),
                ParquetSourceOwnership.Caller,
                options,
                CancellationToken.None));
    }
}
