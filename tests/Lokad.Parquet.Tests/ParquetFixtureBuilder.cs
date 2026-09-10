using System.Buffers.Binary;
using System.Text;

namespace Lokad.Parquet.Tests;

internal enum FixturePageVersion
{
    DataPageV1,
    DataPageV2,
}

internal enum FixtureDictionaryMode
{
    BeforeDataPage,
    DuplicateBeforeDataPage,
    AfterDataPage,
    LegacyEncodingBeforeDataPage,
    UnadvertisedBeforeDataPage,
}

internal enum FixtureRowGroupMode
{
    Included,
    Omitted,
}

internal enum FixtureRootRepetition
{
    Absent,
    Required,
}

internal enum FixtureColumnOrder
{
    Absent,
    TypeDefined,
}

internal enum FixtureCrcMode
{
    Absent,
    Valid,
    Corrupt,
}

internal sealed class ParquetFixtureOptions
{
    public int[] Values { get; init; } = [];
    public Array? PhysicalValues { get; init; }
    public string[] ColumnNames { get; init; } = ["value"];
    public int PhysicalTypeCode { get; init; } = (int)ParquetPhysicalType.Int32;
    public int? TypeLength { get; init; }
    public ParquetCompressionCodec CompressionCodec { get; init; } = ParquetCompressionCodec.Uncompressed;
    public FixturePageVersion PageVersion { get; init; }
    public Array? DictionaryValues { get; init; }
    public int[]? DictionaryIndices { get; init; }
    public bool CoalesceIndexRuns { get; init; }
    public bool BitPackedIndices { get; init; }
    public bool AppendTrailingIndexRun { get; init; }
    public bool SnappyCopyEncoding { get; init; }
    public FixtureDictionaryMode DictionaryMode { get; init; }
    public Array? TrailingPlainValues { get; init; }
    public FixtureRowGroupMode RowGroupMode { get; init; }
    public ParquetRepetition Repetition { get; init; }
    public bool[]? Validity { get; init; }
    public FixtureRootRepetition RootRepetition { get; init; }
    public int? ConvertedType { get; init; }
    public int? LogicalTypeDiscriminator { get; init; }
    public int? TimeUnitDiscriminator { get; init; }
    public bool TimeAdjustedToUtc { get; init; }
    public int[] AdvertisedEncodings { get; init; } = [0];
    public FixtureColumnOrder ColumnOrder { get; init; }
    public FixtureCrcMode CrcMode { get; init; }
    public ParquetPageHeaderOverrides? PageHeaderOverrides { get; init; }
    public long? AuxiliaryOffset { get; init; }
    public long? ChunkTotalCompressedSize { get; init; }
    public long? ChunkTotalUncompressedSize { get; init; }
    public bool OmitBloomLength { get; init; }
    public bool BloomLengthWithoutOffset { get; init; }
    public int AuxiliaryLength { get; init; } = 1;
    public long? RowGroupTotalByteSize { get; init; }
    public Action<CompactTestWriter>? ExtraFileMetadataFields { get; init; }
}

internal sealed class ParquetPageHeaderOverrides
{
    public int? TypeCode { get; init; }
    public int? UncompressedSize { get; init; }
    public int? CompressedSize { get; init; }
    public int? ValueCount { get; init; }
    public int? ValueEncodingCode { get; init; }
    public int? DefinitionEncodingCode { get; init; }
    public int? RepetitionEncodingCode { get; init; }
    public int? V1DefinitionLevelByteLength { get; init; }
    public bool? V2IsCompressed { get; init; }
    public int? HeaderPaddingBytes { get; init; }
}

internal sealed class RequiredInt32FixtureColumn
{
    public required string Name { get; init; }
    public required int[][] Pages { get; init; }
    public int? ConvertedType { get; init; }
    public int? LogicalTypeDiscriminator { get; init; }
}

internal sealed class FixtureBinaryColumn
{
    public required string Name { get; init; }
    public required int PhysicalTypeCode { get; init; }
    public int? TypeLength { get; init; }
    public required Array PhysicalValues { get; init; }
    public ParquetCompressionCodec FooterCodec { get; init; } = ParquetCompressionCodec.Uncompressed;
}

internal static class ParquetFixtureBuilder
{
    private static ReadOnlySpan<byte> Magic => "PAR1"u8;

    public static byte[] CreateInt32(ParquetFixtureOptions options)
    {
        var hasRowGroup = options.RowGroupMode == FixtureRowGroupMode.Included;
        var isOptional = options.Repetition == ParquetRepetition.Optional;
        if (options.ColumnNames.Length == 0)
            throw new ArgumentException("A generated fixture needs at least one column name.", nameof(options));
        if (hasRowGroup && options.ColumnNames.Length != 1)
            throw new ArgumentException("Data-bearing generated fixtures currently support one column.", nameof(options));
        if (options.Repetition is not (ParquetRepetition.Required or ParquetRepetition.Optional))
            throw new ArgumentException("Generated fixtures support only required or optional leaves.", nameof(options));
        var physicalValues = options.PhysicalValues ?? options.Values;
        if (options.Validity is not null && (!isOptional || options.Validity.Length != physicalValues.Length))
            throw new ArgumentException("Validity requires one bit per optional fixture value.", nameof(options));
        var pages = hasRowGroup ? CreateInt32Page(
            physicalValues,
            options.PhysicalTypeCode,
            options.TypeLength,
            options.Repetition,
            options.Validity,
            options.PageVersion,
            options.DictionaryValues,
            options.DictionaryIndices,
            options.DictionaryMode,
            options.CoalesceIndexRuns,
            options.BitPackedIndices,
            options.AppendTrailingIndexRun,
            options.SnappyCopyEncoding,
            options.CompressionCodec,
            options.CrcMode,
            options.PageHeaderOverrides) : new GeneratedPages([], 0, null, 0);
        if (options.TrailingPlainValues is { } trailingValues)
        {
            if (!hasRowGroup || isOptional || options.DictionaryMode == FixtureDictionaryMode.AfterDataPage)
                throw new ArgumentException("Trailing generated PLAIN values require an ordinary required row group.", nameof(options));
            var trailingPage = CreateInt32Page(
                trailingValues,
                options.PhysicalTypeCode,
                options.TypeLength,
                ParquetRepetition.Required,
                null,
                options.PageVersion,
                null,
                null,
                FixtureDictionaryMode.BeforeDataPage,
                false,
                false,
                false,
                false,
                options.CompressionCodec,
                options.CrcMode,
                null);
            pages = new GeneratedPages(
                Combine(pages.Bytes, trailingPage.Bytes),
                pages.DataPageOffset,
                pages.DictionaryPageOffset,
                checked(pages.UncompressedLength + trailingPage.UncompressedLength));
        }
        var page = pages.Bytes;
        var rowCount = checked(physicalValues.LongLength + (options.TrailingPlainValues?.LongLength ?? 0));
        using var file = new MemoryStream();
        file.Write(Magic);
        file.Write(page);

        var footer = new CompactTestWriter();
        short previous = 0;
        footer.Int32Field(ref previous, 1, 1);
        footer.ListField(ref previous, 2, CompactTestType.Struct, options.ColumnNames.Length + 1, () =>
        {
            short root = 0;
            if (options.RootRepetition == FixtureRootRepetition.Required)
                footer.Int32Field(ref root, 3, 0);
            footer.StringField(ref root, 4, "schema");
            footer.Int32Field(ref root, 5, options.ColumnNames.Length);
            footer.Stop();

            foreach (var columnName in options.ColumnNames)
            {
                short leaf = 0;
                footer.Int32Field(ref leaf, 1, options.PhysicalTypeCode);
                if (options.TypeLength is int typeLength)
                    footer.Int32Field(ref leaf, 2, typeLength);
                footer.Int32Field(ref leaf, 3, isOptional ? 1 : 0);
                footer.StringField(ref leaf, 4, columnName);
                AddAnnotations(
                    footer,
                    ref leaf,
                    options.ConvertedType,
                    options.LogicalTypeDiscriminator,
                    options.TimeUnitDiscriminator,
                    options.TimeAdjustedToUtc);
                footer.Stop();
            }
        });
        footer.Int64Field(ref previous, 3, rowCount);

        if (hasRowGroup)
        {
            footer.ListField(ref previous, 4, CompactTestType.Struct, 1, () =>
            {
                short rowGroup = 0;
                footer.ListField(ref rowGroup, 1, CompactTestType.Struct, 1, () =>
                {
                    short chunk = 0;
                    footer.Int64Field(ref chunk, 2, 0);
                    footer.StructField(ref chunk, 3, () =>
                    {
                        short metadata = 0;
                        footer.Int32Field(ref metadata, 1, options.PhysicalTypeCode);
                        footer.Int32ListField(ref metadata, 2, options.AdvertisedEncodings);
                        footer.StringListField(ref metadata, 3, [options.ColumnNames[0]]);
                        footer.Int32Field(ref metadata, 4, (int)options.CompressionCodec);
                        footer.Int64Field(ref metadata, 5, rowCount);
                        footer.Int64Field(ref metadata, 6, options.ChunkTotalUncompressedSize ?? pages.UncompressedLength);
                        footer.Int64Field(ref metadata, 7, options.ChunkTotalCompressedSize ?? page.Length);
                        footer.Int64Field(ref metadata, 9, 4 + pages.DataPageOffset);
                        if (options.AuxiliaryOffset is long auxiliaryOffset)
                            footer.Int64Field(ref metadata, 10, auxiliaryOffset);
                        if (pages.DictionaryPageOffset is int dictionaryPageOffset)
                            footer.Int64Field(ref metadata, 11, 4 + dictionaryPageOffset);
                        if (options.AuxiliaryOffset is long bloomOffset)
                        {
                            if (!options.BloomLengthWithoutOffset)
                                footer.Int64Field(ref metadata, 14, bloomOffset);
                            if (!options.OmitBloomLength)
                                footer.Int32Field(ref metadata, 15, options.AuxiliaryLength);
                        }
                        footer.Stop();
                    });
                    if (options.AuxiliaryOffset is long indexOffset)
                    {
                        footer.Int64Field(ref chunk, 4, indexOffset);
                        footer.Int32Field(ref chunk, 5, options.AuxiliaryLength);
                        footer.Int64Field(ref chunk, 6, indexOffset);
                        footer.Int32Field(ref chunk, 7, options.AuxiliaryLength);
                    }
                    footer.Stop();
                });
                footer.Int64Field(ref rowGroup, 2, options.RowGroupTotalByteSize ?? page.Length);
                footer.Int64Field(ref rowGroup, 3, rowCount);
                footer.Int64Field(ref rowGroup, 6, page.Length);
                footer.Stop();
            });
        }
        else
        {
            footer.ListField(ref previous, 4, CompactTestType.Struct, 0, static () => { });
        }

        if (options.ColumnOrder == FixtureColumnOrder.TypeDefined)
        {
            footer.ListField(ref previous, 7, CompactTestType.Struct, 1, () =>
            {
                short order = 0;
                footer.StructField(ref order, 1, footer.Stop);
                footer.Stop();
            });
        }

        options.ExtraFileMetadataFields?.Invoke(footer);
        footer.Stop();
        return CompleteFile(file, footer);
    }

    public static byte[] CreateRequiredInt32Columns(RequiredInt32FixtureColumn[] columns)
    {
        if (columns.Length == 0)
            throw new ArgumentException("A generated fixture needs at least one column.", nameof(columns));
        var rowCount = columns[0].Pages.Sum(static page => page.Length);
        if (columns.Any(column => column.Pages.Length == 0 ||
                column.Pages.Sum(static page => page.Length) != rowCount))
            throw new ArgumentException("Every generated column needs pages with the same aggregate row count.", nameof(columns));

        var chunks = new byte[columns.Length][];
        for (var columnIndex = 0; columnIndex < columns.Length; columnIndex++)
        {
            using var chunk = new MemoryStream();
            foreach (var values in columns[columnIndex].Pages)
            {
                var page = CreateInt32Page(
                    values,
                    (int)ParquetPhysicalType.Int32,
                    null,
                    ParquetRepetition.Required,
                    null,
                    FixturePageVersion.DataPageV1,
                    null,
                    null,
                    FixtureDictionaryMode.BeforeDataPage,
                    false,
                    false,
                    false,
                    false,
                    ParquetCompressionCodec.Uncompressed,
                    FixtureCrcMode.Valid,
                    null);
                chunk.Write(page.Bytes);
            }
            chunks[columnIndex] = chunk.ToArray();
        }

        using var file = new MemoryStream();
        file.Write(Magic);
        var chunkOffsets = new long[chunks.Length];
        for (var columnIndex = 0; columnIndex < chunks.Length; columnIndex++)
        {
            chunkOffsets[columnIndex] = file.Position;
            file.Write(chunks[columnIndex]);
        }

        var footer = new CompactTestWriter();
        short previous = 0;
        footer.Int32Field(ref previous, 1, 1);
        footer.ListField(ref previous, 2, CompactTestType.Struct, columns.Length + 1, () =>
        {
            short root = 0;
            footer.StringField(ref root, 4, "schema");
            footer.Int32Field(ref root, 5, columns.Length);
            footer.Stop();
            foreach (var column in columns)
            {
                short leaf = 0;
                footer.Int32Field(ref leaf, 1, (int)ParquetPhysicalType.Int32);
                footer.Int32Field(ref leaf, 3, (int)ParquetRepetition.Required);
                footer.StringField(ref leaf, 4, column.Name);
                AddAnnotations(
                    footer,
                    ref leaf,
                    column.ConvertedType,
                    column.LogicalTypeDiscriminator,
                    timeUnitDiscriminator: null,
                    timeAdjustedToUtc: false);
                footer.Stop();
            }
        });
        footer.Int64Field(ref previous, 3, rowCount);
        footer.ListField(ref previous, 4, CompactTestType.Struct, 1, () =>
        {
            short rowGroup = 0;
            footer.ListField(ref rowGroup, 1, CompactTestType.Struct, columns.Length, () =>
            {
                for (var columnIndex = 0; columnIndex < columns.Length; columnIndex++)
                {
                    short chunk = 0;
                    footer.Int64Field(ref chunk, 2, chunkOffsets[columnIndex]);
                    footer.StructField(ref chunk, 3, () =>
                    {
                        short metadata = 0;
                        footer.Int32Field(ref metadata, 1, (int)ParquetPhysicalType.Int32);
                        footer.Int32ListField(ref metadata, 2, [(int)ParquetEncoding.Plain]);
                        footer.StringListField(ref metadata, 3, [columns[columnIndex].Name]);
                        footer.Int32Field(ref metadata, 4, (int)ParquetCompressionCodec.Uncompressed);
                        footer.Int64Field(ref metadata, 5, rowCount);
                        footer.Int64Field(ref metadata, 6, chunks[columnIndex].Length);
                        footer.Int64Field(ref metadata, 7, chunks[columnIndex].Length);
                        footer.Int64Field(ref metadata, 9, chunkOffsets[columnIndex]);
                        footer.Stop();
                    });
                    footer.Stop();
                }
            });
            footer.Int64Field(ref rowGroup, 2, chunks.Sum(static chunk => (long)chunk.Length));
            footer.Int64Field(ref rowGroup, 3, rowCount);
            footer.Int64Field(ref rowGroup, 6, chunks.Sum(static chunk => (long)chunk.Length));
            footer.Stop();
        });
        footer.Stop();
        return CompleteFile(file, footer);
    }

    public static byte[] CreateBinaryColumns(FixtureBinaryColumn[] columns)
    {
        if (columns.Length == 0)
            throw new ArgumentException("A generated fixture needs at least one column.", nameof(columns));
        var rowCount = columns[0].PhysicalValues.Length;
        if (columns.Any(column => column.PhysicalValues.Length != rowCount))
            throw new ArgumentException("Every generated column needs the same row count.", nameof(columns));

        var chunks = new byte[columns.Length][];
        for (var columnIndex = 0; columnIndex < columns.Length; columnIndex++)
        {
            var column = columns[columnIndex];
            var page = CreateInt32Page(
                column.PhysicalValues,
                column.PhysicalTypeCode,
                column.TypeLength,
                ParquetRepetition.Required,
                null,
                FixturePageVersion.DataPageV1,
                null,
                null,
                FixtureDictionaryMode.BeforeDataPage,
                false,
                false,
                false,
                false,
                ParquetCompressionCodec.Uncompressed,
                FixtureCrcMode.Absent,
                null);
            chunks[columnIndex] = page.Bytes;
        }

        using var file = new MemoryStream();
        file.Write(Magic);
        var chunkOffsets = new long[chunks.Length];
        for (var columnIndex = 0; columnIndex < chunks.Length; columnIndex++)
        {
            chunkOffsets[columnIndex] = file.Position;
            file.Write(chunks[columnIndex]);
        }

        var footer = new CompactTestWriter();
        short previous = 0;
        footer.Int32Field(ref previous, 1, 1);
        footer.ListField(ref previous, 2, CompactTestType.Struct, columns.Length + 1, () =>
        {
            short root = 0;
            footer.StringField(ref root, 4, "schema");
            footer.Int32Field(ref root, 5, columns.Length);
            footer.Stop();
            foreach (var column in columns)
            {
                short leaf = 0;
                footer.Int32Field(ref leaf, 1, column.PhysicalTypeCode);
                if (column.TypeLength is int typeLength)
                    footer.Int32Field(ref leaf, 2, typeLength);
                footer.Int32Field(ref leaf, 3, (int)ParquetRepetition.Required);
                footer.StringField(ref leaf, 4, column.Name);
                footer.Stop();
            }
        });
        footer.Int64Field(ref previous, 3, rowCount);
        footer.ListField(ref previous, 4, CompactTestType.Struct, 1, () =>
        {
            short rowGroup = 0;
            footer.ListField(ref rowGroup, 1, CompactTestType.Struct, columns.Length, () =>
            {
                for (var columnIndex = 0; columnIndex < columns.Length; columnIndex++)
                {
                    short chunk = 0;
                    footer.Int64Field(ref chunk, 2, chunkOffsets[columnIndex]);
                    footer.StructField(ref chunk, 3, () =>
                    {
                        short metadata = 0;
                        footer.Int32Field(ref metadata, 1, columns[columnIndex].PhysicalTypeCode);
                        footer.Int32ListField(ref metadata, 2, [(int)ParquetEncoding.Plain]);
                        footer.StringListField(ref metadata, 3, [columns[columnIndex].Name]);
                        footer.Int32Field(ref metadata, 4, (int)columns[columnIndex].FooterCodec);
                        footer.Int64Field(ref metadata, 5, rowCount);
                        footer.Int64Field(ref metadata, 6, chunks[columnIndex].Length);
                        footer.Int64Field(ref metadata, 7, chunks[columnIndex].Length);
                        footer.Int64Field(ref metadata, 9, chunkOffsets[columnIndex]);
                        footer.Stop();
                    });
                    footer.Stop();
                }
            });
            footer.Int64Field(ref rowGroup, 2, chunks.Sum(static chunk => (long)chunk.Length));
            footer.Int64Field(ref rowGroup, 3, rowCount);
            footer.Int64Field(ref rowGroup, 6, chunks.Sum(static chunk => (long)chunk.Length));
            footer.Stop();
        });
        footer.Stop();
        return CompleteFile(file, footer);
    }

    private static void AddAnnotations(
        CompactTestWriter footer,
        ref short previous,
        int? convertedType,
        int? logicalTypeDiscriminator,
        int? timeUnitDiscriminator,
        bool timeAdjustedToUtc)
    {
        if (convertedType is int converted)
            footer.Int32Field(ref previous, 6, converted);
        if (logicalTypeDiscriminator is not int logical)
            return;

        footer.StructField(ref previous, 10, () =>
        {
            short union = 0;
            footer.StructField(ref union, checked((short)logical), () =>
            {
                if (logical is (int)ParquetLogicalTypeKind.Time or
                    (int)ParquetLogicalTypeKind.Timestamp && timeUnitDiscriminator is int timeUnit)
                {
                    short time = 0;
                    footer.BooleanField(ref time, 1, timeAdjustedToUtc);
                    footer.StructField(ref time, 2, () =>
                    {
                        short unit = 0;
                        footer.StructField(ref unit, checked((short)timeUnit), footer.Stop);
                        footer.Stop();
                    });
                    footer.Stop();
                }
                else
                {
                    footer.Stop();
                }
            });
            footer.Stop();
        });
    }

    public static byte[] CreateRequiredInt32RowGroups(int[][] rowGroups)
    {
        if (rowGroups.Length == 0)
            throw new ArgumentException("A generated fixture needs at least one row group.", nameof(rowGroups));

        var chunks = new byte[rowGroups.Length][];
        for (var rowGroupOrdinal = 0; rowGroupOrdinal < rowGroups.Length; rowGroupOrdinal++)
        {
            chunks[rowGroupOrdinal] = CreateInt32Page(
                rowGroups[rowGroupOrdinal],
                (int)ParquetPhysicalType.Int32,
                null,
                ParquetRepetition.Required,
                null,
                FixturePageVersion.DataPageV1,
                null,
                null,
                FixtureDictionaryMode.BeforeDataPage,
                false,
                false,
                false,
                false,
                ParquetCompressionCodec.Uncompressed,
                FixtureCrcMode.Valid,
                null).Bytes;
        }

        using var file = new MemoryStream();
        file.Write(Magic);
        var chunkOffsets = new long[chunks.Length];
        for (var rowGroupOrdinal = 0; rowGroupOrdinal < chunks.Length; rowGroupOrdinal++)
        {
            chunkOffsets[rowGroupOrdinal] = file.Position;
            file.Write(chunks[rowGroupOrdinal]);
        }

        var footer = new CompactTestWriter();
        short previous = 0;
        footer.Int32Field(ref previous, 1, 1);
        footer.ListField(ref previous, 2, CompactTestType.Struct, 2, () =>
        {
            short root = 0;
            footer.StringField(ref root, 4, "schema");
            footer.Int32Field(ref root, 5, 1);
            footer.Stop();

            short leaf = 0;
            footer.Int32Field(ref leaf, 1, (int)ParquetPhysicalType.Int32);
            footer.Int32Field(ref leaf, 3, (int)ParquetRepetition.Required);
            footer.StringField(ref leaf, 4, "value");
            footer.Stop();
        });
        footer.Int64Field(ref previous, 3, rowGroups.Sum(static values => (long)values.Length));
        footer.ListField(ref previous, 4, CompactTestType.Struct, rowGroups.Length, () =>
        {
            for (var rowGroupOrdinal = 0; rowGroupOrdinal < rowGroups.Length; rowGroupOrdinal++)
            {
                var capturedOrdinal = rowGroupOrdinal;
                short rowGroup = 0;
                footer.ListField(ref rowGroup, 1, CompactTestType.Struct, 1, () =>
                {
                    short chunk = 0;
                    footer.Int64Field(ref chunk, 2, chunkOffsets[capturedOrdinal]);
                    footer.StructField(ref chunk, 3, () =>
                    {
                        short metadata = 0;
                        footer.Int32Field(ref metadata, 1, (int)ParquetPhysicalType.Int32);
                        footer.Int32ListField(ref metadata, 2, [(int)ParquetEncoding.Plain]);
                        footer.StringListField(ref metadata, 3, ["value"]);
                        footer.Int32Field(ref metadata, 4, (int)ParquetCompressionCodec.Uncompressed);
                        footer.Int64Field(ref metadata, 5, rowGroups[capturedOrdinal].Length);
                        footer.Int64Field(ref metadata, 6, chunks[capturedOrdinal].Length);
                        footer.Int64Field(ref metadata, 7, chunks[capturedOrdinal].Length);
                        footer.Int64Field(ref metadata, 9, chunkOffsets[capturedOrdinal]);
                        footer.Stop();
                    });
                    footer.Stop();
                });
                footer.Int64Field(ref rowGroup, 2, chunks[capturedOrdinal].Length);
                footer.Int64Field(ref rowGroup, 3, rowGroups[capturedOrdinal].Length);
                footer.Int64Field(ref rowGroup, 6, chunks[capturedOrdinal].Length);
                footer.Stop();
            }
        });
        footer.Stop();
        return CompleteFile(file, footer);
    }

    private static GeneratedPages CreateInt32Page(
        Array values,
        int physicalTypeCode,
        int? typeLength,
        ParquetRepetition repetition,
        bool[]? validity,
        FixturePageVersion pageVersion,
        Array? dictionaryValues,
        int[]? dictionaryIndices,
        FixtureDictionaryMode dictionaryMode,
        bool coalesceIndexRuns,
        bool bitPackedIndices,
        bool appendTrailingIndexRun,
        bool snappyCopyEncoding,
        ParquetCompressionCodec codec,
        FixtureCrcMode crcMode,
        ParquetPageHeaderOverrides? overrides)
    {
        static byte[] EncodeDefinitionLevels(bool[] validity)
        {
            static void WriteVarUInt32(List<byte> output, uint value)
            {
                while (value >= 0x80)
                {
                    output.Add((byte)(value | 0x80));
                    value >>= 7;
                }
                output.Add((byte)value);
            }

            var result = new List<byte>();
            for (var start = 0; start < validity.Length;)
            {
                var end = start + 1;
                while (end < validity.Length && validity[end] == validity[start])
                    end++;
                WriteVarUInt32(result, checked((uint)((end - start) << 1)));
                result.Add(validity[start] ? (byte)1 : (byte)0);
                start = end;
            }
            return result.ToArray();
        }

        var optional = repetition == ParquetRepetition.Optional;
        var dataPageV2 = pageVersion == FixturePageVersion.DataPageV2;
        var includeCrc = crcMode != FixtureCrcMode.Absent;
        var corruptCrc = crcMode == FixtureCrcMode.Corrupt;
        var effectiveValidity = validity ?? Enumerable.Repeat(true, values.Length).ToArray();
        var physicalCount = optional ? effectiveValidity.Count(static valid => valid) : values.Length;
        var levels = optional ? EncodeDefinitionLevels(effectiveValidity) : [];
        var physicalType = Enum.IsDefined(typeof(ParquetPhysicalType), physicalTypeCode)
            ? (ParquetPhysicalType)physicalTypeCode
            : ParquetPhysicalType.Int32;
        var encodedPhysical = dictionaryValues is null
            ? EncodePlainValues(values, effectiveValidity, repetition, physicalType, physicalCount, typeLength)
            : EncodeDictionaryIndices(dictionaryValues, dictionaryIndices, physicalCount, coalesceIndexRuns, bitPackedIndices, appendTrailingIndexRun);
        byte[] uncompressed;
        byte[] payload;
        if (dataPageV2)
        {
            uncompressed = new byte[checked(levels.Length + encodedPhysical.Length)];
            levels.CopyTo(uncompressed, 0);
            encodedPhysical.CopyTo(uncompressed, levels.Length);
            var v2IsCompressed = overrides?.V2IsCompressed ?? (codec != ParquetCompressionCodec.Uncompressed);
            payload = codec switch
            {
                ParquetCompressionCodec.Uncompressed => uncompressed,
                ParquetCompressionCodec.Snappy when v2IsCompressed => Combine(levels, EncodeSnappyPage(encodedPhysical, snappyCopyEncoding)),
                ParquetCompressionCodec.Snappy => uncompressed,
                _ => throw new ArgumentException("The generated fixture supports only uncompressed and Snappy pages.", nameof(codec)),
            };
        }
        else
        {
            var levelPrefixBytes = optional ? sizeof(int) : 0;
            uncompressed = new byte[checked(levelPrefixBytes + levels.Length + encodedPhysical.Length)];
            var outputOffset = 0;
            if (optional)
            {
                BinaryPrimitives.WriteInt32LittleEndian(
                    uncompressed,
                    overrides?.V1DefinitionLevelByteLength ?? levels.Length);
                levels.CopyTo(uncompressed, sizeof(int));
                outputOffset = sizeof(int) + levels.Length;
            }
            encodedPhysical.CopyTo(uncompressed, outputOffset);
            payload = codec switch
            {
                ParquetCompressionCodec.Uncompressed => uncompressed,
                ParquetCompressionCodec.Snappy => EncodeSnappyPage(uncompressed, snappyCopyEncoding),
                _ => throw new ArgumentException("The generated fixture supports only uncompressed and Snappy pages.", nameof(codec)),
            };
        }

        var header = new CompactTestWriter();
        short page = 0;
        header.Int32Field(ref page, 1, overrides?.TypeCode ?? (dataPageV2 ? 3 : 0));
        header.Int32Field(ref page, 2, overrides?.UncompressedSize ?? uncompressed.Length);
        header.Int32Field(ref page, 3, overrides?.CompressedSize ?? payload.Length);
        if (includeCrc)
            header.Int32Field(ref page, 4, unchecked((int)(ComputeCrc32(payload) ^ (corruptCrc ? 1u : 0u))));
        if (dataPageV2)
        {
            header.StructField(ref page, 8, () =>
            {
                short data = 0;
                header.Int32Field(ref data, 1, overrides?.ValueCount ?? values.Length);
                header.Int32Field(ref data, 2, optional ? values.Length - physicalCount : 0);
                header.Int32Field(ref data, 3, values.Length);
                header.Int32Field(
                    ref data,
                    4,
                    overrides?.ValueEncodingCode ??
                        (dictionaryValues is null ? 0 : (int)ParquetEncoding.RunLengthDictionary));
                header.Int32Field(ref data, 5, levels.Length);
                header.Int32Field(ref data, 6, 0);
                header.BooleanField(ref data, 7, overrides?.V2IsCompressed ?? (codec != ParquetCompressionCodec.Uncompressed));
                header.Stop();
            });
        }
        else
        {
            header.StructField(ref page, 5, () =>
            {
                short data = 0;
                header.Int32Field(ref data, 1, overrides?.ValueCount ?? values.Length);
                header.Int32Field(
                    ref data,
                    2,
                    overrides?.ValueEncodingCode ??
                        (dictionaryValues is null ? 0 : (int)ParquetEncoding.RunLengthDictionary));
                header.Int32Field(ref data, 3, overrides?.DefinitionEncodingCode ?? 3);
                header.Int32Field(ref data, 4, overrides?.RepetitionEncodingCode ?? 3);
                header.Stop();
            });
        }
        if (overrides?.HeaderPaddingBytes is int paddingBytes)
        {
            if (paddingBytes < 0)
                throw new ArgumentOutOfRangeException(nameof(overrides));
            header.StringField(ref page, 9, new string('p', paddingBytes));
        }
        header.Stop();

        var dataPage = new byte[header.Length + payload.Length];
        header.ToArray().CopyTo(dataPage, 0);
        payload.CopyTo(dataPage, header.Length);
        var dataUncompressedLength = checked((long)header.Length + uncompressed.Length);
        if (dictionaryValues is null)
            return new GeneratedPages(dataPage, 0, null, dataUncompressedLength);

        var dictionaryPage = CreateDictionaryPage(
            dictionaryValues,
            physicalType,
            typeLength,
            dictionaryMode,
            snappyCopyEncoding,
            codec,
            crcMode);
        if (dictionaryMode == FixtureDictionaryMode.DuplicateBeforeDataPage)
            return new GeneratedPages(
                Combine(Combine(dictionaryPage.Bytes, dictionaryPage.Bytes), dataPage),
                checked(dictionaryPage.Bytes.Length * 2),
                0,
                checked(dictionaryPage.UncompressedLength * 2 + dataUncompressedLength));
        if (dictionaryMode == FixtureDictionaryMode.AfterDataPage)
            return new GeneratedPages(Combine(dataPage, dictionaryPage.Bytes), 0, dataPage.Length, checked(dataUncompressedLength + dictionaryPage.UncompressedLength));
        if (dictionaryMode == FixtureDictionaryMode.UnadvertisedBeforeDataPage)
            return new GeneratedPages(Combine(dictionaryPage.Bytes, dataPage), 0, null, checked(dataUncompressedLength + dictionaryPage.UncompressedLength));
        return new GeneratedPages(Combine(dictionaryPage.Bytes, dataPage), dictionaryPage.Bytes.Length, 0, checked(dictionaryPage.UncompressedLength + dataUncompressedLength));

        static (byte[] Bytes, long UncompressedLength) CreateDictionaryPage(
        Array values,
        ParquetPhysicalType physicalType,
        int? typeLength,
        FixtureDictionaryMode dictionaryMode,
        bool snappyCopyEncoding,
        ParquetCompressionCodec codec,
        FixtureCrcMode crcMode)
        {
            var validity = Enumerable.Repeat(true, values.Length).ToArray();
            var uncompressed = EncodePlainValues(
                values,
                validity,
                ParquetRepetition.Required,
                physicalType,
                values.Length,
                typeLength);
            var payload = codec switch
            {
                ParquetCompressionCodec.Uncompressed => uncompressed,
                ParquetCompressionCodec.Snappy => EncodeSnappyPage(uncompressed, snappyCopyEncoding),
                _ => throw new ArgumentException("The generated fixture supports only uncompressed and Snappy pages.", nameof(codec)),
            };
            var header = new CompactTestWriter();
            short page = 0;
            header.Int32Field(ref page, 1, 2);
            header.Int32Field(ref page, 2, uncompressed.Length);
            header.Int32Field(ref page, 3, payload.Length);
            if (crcMode != FixtureCrcMode.Absent)
                header.Int32Field(ref page, 4, unchecked((int)ComputeCrc32(payload)));
            header.StructField(ref page, 7, () =>
            {
                short dictionary = 0;
                header.Int32Field(ref dictionary, 1, values.Length);
                header.Int32Field(
                    ref dictionary,
                    2,
                    dictionaryMode == FixtureDictionaryMode.LegacyEncodingBeforeDataPage
                        ? (int)ParquetEncoding.PlainDictionary
                        : (int)ParquetEncoding.Plain);
                header.Stop();
            });
            header.Stop();
            var result = new byte[header.Length + payload.Length];
            header.ToArray().CopyTo(result, 0);
            payload.CopyTo(result, header.Length);
            return (result, checked((long)header.Length + uncompressed.Length));
        }

        static byte[] EncodeDictionaryIndices(
        Array dictionary,
        int[]? indices,
        int physicalCount,
        bool coalesceRuns,
        bool bitPacked,
        bool trailingRun)
        {
            if (indices is null || indices.Length != physicalCount)
                throw new ArgumentException("Dictionary fixtures require one index per physical value.", nameof(indices));
            if (coalesceRuns && bitPacked)
                throw new ArgumentException("Dictionary fixtures support only one index encoding shape.", nameof(bitPacked));
            var bitWidth = 0;
            for (var maximum = dictionary.Length - 1; maximum > 0; maximum >>= 1)
                bitWidth++;
            var result = new List<byte> { checked((byte)bitWidth) };
            if (bitPacked)
            {
                if (indices.Length != 0)
                    EncodeBitPackedIndices(result, indices, bitWidth);
            }
            else
            {
                EncodeRleIndices(result, indices, bitWidth, coalesceRuns);
            }
            if (trailingRun)
            {
                // One unconsumed run exercises the trailing-index rejection.
                result.Add(2);
                var trailingWidth = (bitWidth + 7) / 8;
                for (var i = 0; i < trailingWidth; i++)
                    result.Add(0);
            }
            return result.ToArray();
        }

        // Dictionary indices as RLE runs: singletons by default, coalesced
        // repeats when the repeated-run shape is requested.
        static void EncodeRleIndices(List<byte> result, int[] indices, int bitWidth, bool coalesceRuns)
        {
            var byteWidth = (bitWidth + 7) / 8;
            var position = 0;
            while (position < indices.Length)
            {
                var runLength = 1;
                while (coalesceRuns &&
                    position + runLength < indices.Length &&
                    indices[position + runLength] == indices[position])
                    runLength++;
                WriteIndexVarUInt32(result, checked((uint)runLength << 1));
                for (var i = 0; i < byteWidth; i++)
                    result.Add((byte)(indices[position] >> (8 * i)));
                position += runLength;
            }
        }

        // Dictionary indices as one bit-packed run with a minimal zero-padded
        // final group, matching the decoder padding contract.
        static void EncodeBitPackedIndices(List<byte> result, int[] indices, int bitWidth)
        {
            var groupCount = checked((indices.Length + 7) / 8);
            WriteIndexVarUInt32(result, checked(((uint)groupCount << 1) | 1u));
            var dataStart = result.Count;
            ulong buffer = 0;
            var bufferedBits = 0;
            foreach (var index in indices)
            {
                buffer |= (ulong)(uint)index << bufferedBits;
                bufferedBits += bitWidth;
                while (bufferedBits >= 8)
                {
                    result.Add((byte)buffer);
                    buffer >>= 8;
                    bufferedBits -= 8;
                }
            }
            if (bufferedBits > 0)
                result.Add((byte)buffer);
            while (result.Count - dataStart < groupCount * bitWidth)
                result.Add(0);
        }

        static void WriteIndexVarUInt32(List<byte> output, uint value)
        {
            while (value >= 0x80)
            {
                output.Add((byte)(value | 0x80));
                value >>= 7;
            }
            output.Add((byte)value);
        }

        static byte[] EncodeSnappyPage(byte[] input, bool copyEncoding) =>
            copyEncoding ? EncodeSnappyMixed(input) : EncodeSnappyLiteral(input);

        // Test Snappy encoder emitting literals and back-reference copies: a
        // greedy bigram-indexed matcher finds runs and repeats, so page
        // payloads exercise the copy tags end to end. The first copy uses the
        // four-byte tag whenever it fits, pinning that path as well.
        static byte[] EncodeSnappyMixed(byte[] input)
        {
            var result = new List<byte>();
            var lengthPrefix = (uint)input.Length;
            while (lengthPrefix >= 0x80)
            {
                result.Add((byte)(lengthPrefix | 0x80));
                lengthPrefix >>= 7;
            }
            result.Add((byte)lengthPrefix);
            var lastOccurrence = new Dictionary<int, int>();
            var literalStart = 0;
            var position = 0;
            var firstCopy = true;
            while (position < input.Length)
            {
                var match = FindSnappyMatch(input, position, lastOccurrence);
                if (match.Length < 4)
                {
                    if (position - literalStart == 60)
                    {
                        EmitSnappyLiterals(result, input, literalStart, position);
                        literalStart = position;
                    }
                    IndexSnappyBigrams(input, position, position + 1, lastOccurrence);
                    position++;
                    continue;
                }
                EmitSnappyLiterals(result, input, literalStart, position);
                var matchStart = position;
                position = EmitSnappyCopy(result, position, match.Distance, match.Length, ref firstCopy);
                IndexSnappyBigrams(input, matchStart, position, lastOccurrence);
                literalStart = position;
            }
            EmitSnappyLiterals(result, input, literalStart, position);
            return result.ToArray();
        }

        static (int Distance, int Length) FindSnappyMatch(byte[] input, int position, Dictionary<int, int> lastOccurrence)
        {
            if (position + 4 > input.Length)
                return (0, 0);
            if (!lastOccurrence.TryGetValue((input[position] << 8) | input[position + 1], out var candidate))
                return (0, 0);
            var distance = position - candidate;
            if (distance <= 0 || distance > 65535)
                return (0, 0);
            var length = 0;
            while (position + length < input.Length && input[candidate + length] == input[position + length])
                length++;
            return length >= 4 ? (distance, length) : (0, 0);
        }

        static void IndexSnappyBigrams(byte[] input, int start, int end, Dictionary<int, int> lastOccurrence)
        {
            for (var index = start; index + 1 < input.Length && index < end; index++)
                lastOccurrence[(input[index] << 8) | input[index + 1]] = index;
        }

        static void EmitSnappyLiterals(List<byte> result, byte[] input, int start, int end)
        {
            while (start < end)
            {
                var chunk = Math.Min(60, end - start);
                result.Add((byte)((chunk - 1) << 2));
                for (var index = 0; index < chunk; index++)
                    result.Add(input[start + index]);
                start += chunk;
            }
        }

        static int EmitSnappyCopy(List<byte> result, int position, int distance, int length, ref bool firstCopy)
        {
            while (length > 0)
            {
                var chunk = Math.Min(64, length);
                if (length - chunk is > 0 and < 4)
                    chunk = length - 4;
                if (firstCopy)
                {
                    result.Add((byte)(((chunk - 1) << 2) | 3));
                    result.Add((byte)distance);
                    result.Add((byte)(distance >> 8));
                    result.Add(0);
                    result.Add(0);
                    firstCopy = false;
                }
                else if (chunk <= 11 && distance <= 2047)
                {
                    result.Add((byte)(((chunk - 4) << 2) | ((distance >> 8) << 5) | 1));
                    result.Add((byte)distance);
                }
                else
                {
                    result.Add((byte)(((chunk - 1) << 2) | 2));
                    result.Add((byte)distance);
                    result.Add((byte)(distance >> 8));
                }
                length -= chunk;
                position += chunk;
            }
            return position;
        }
    }

    private static byte[] EncodePlainValues(
        Array values,
        bool[] validity,
        ParquetRepetition repetition,
        ParquetPhysicalType physicalType,
        int physicalCount,
        int? typeLength)
    {
        return physicalType switch
        {
            ParquetPhysicalType.Boolean when values is bool[] typed => EncodeBooleans(typed, validity, repetition, physicalCount),
            ParquetPhysicalType.Int32 when values is int[] typed => EncodeInt32s(typed, validity, repetition, physicalCount),
            ParquetPhysicalType.Int64 when values is long[] typed => EncodeInt64s(typed, validity, repetition, physicalCount),
            ParquetPhysicalType.Float when values is float[] typed => EncodeFloats(typed, validity, repetition, physicalCount),
            ParquetPhysicalType.Double when values is double[] typed => EncodeDoubles(typed, validity, repetition, physicalCount),
            ParquetPhysicalType.ByteArray when values is byte[][] typed => EncodeByteArrays(typed, validity, repetition, physicalCount),
            ParquetPhysicalType.FixedLengthByteArray when values is byte[][] typed => EncodeFixedByteArrays(typed, validity, repetition, physicalCount, typeLength),
            _ when values is int[] typed => EncodeInt32s(typed, validity, repetition, physicalCount),
            _ => throw new ArgumentException("The generated physical values do not match the physical type.", nameof(values)),
        };

        static bool IsOmitted(ParquetRepetition repetition, bool[] validity, int index) =>
            repetition == ParquetRepetition.Optional && !validity[index];

        static byte[] EncodeBooleans(bool[] values, bool[] validity, ParquetRepetition repetition, int physicalCount)
        {
            var result = new byte[checked((physicalCount + 7) / 8)];
            var output = 0;
            for (var i = 0; i < values.Length; i++)
            {
                if (IsOmitted(repetition, validity, i))
                    continue;
                if (values[i])
                    result[output >> 3] |= (byte)(1 << (output & 7));
                output++;
            }
            return result;
        }

        static byte[] EncodeInt32s(int[] values, bool[] validity, ParquetRepetition repetition, int physicalCount)
        {
            var result = new byte[checked(physicalCount * sizeof(int))];
            var output = 0;
            for (var i = 0; i < values.Length; i++)
            {
                if (IsOmitted(repetition, validity, i))
                    continue;
                BinaryPrimitives.WriteInt32LittleEndian(result.AsSpan(output), values[i]);
                output += sizeof(int);
            }
            return result;
        }

        static byte[] EncodeInt64s(long[] values, bool[] validity, ParquetRepetition repetition, int physicalCount)
        {
            var result = new byte[checked(physicalCount * sizeof(long))];
            var output = 0;
            for (var i = 0; i < values.Length; i++)
            {
                if (IsOmitted(repetition, validity, i))
                    continue;
                BinaryPrimitives.WriteInt64LittleEndian(result.AsSpan(output), values[i]);
                output += sizeof(long);
            }
            return result;
        }

        static byte[] EncodeFloats(float[] values, bool[] validity, ParquetRepetition repetition, int physicalCount) =>
        EncodeInt32s(values.Select(BitConverter.SingleToInt32Bits).ToArray(), validity, repetition, physicalCount);

        static byte[] EncodeDoubles(double[] values, bool[] validity, ParquetRepetition repetition, int physicalCount) =>
        EncodeInt64s(values.Select(BitConverter.DoubleToInt64Bits).ToArray(), validity, repetition, physicalCount);

        static byte[] EncodeByteArrays(byte[][] values, bool[] validity, ParquetRepetition repetition, int physicalCount)
        {
            var length = 0;
            for (var index = 0; index < values.Length; index++)
            {
                if (!IsOmitted(repetition, validity, index))
                    length = checked(length + sizeof(int) + values[index].Length);
            }
            var result = new byte[length];
            var output = 0;
            for (var i = 0; i < values.Length; i++)
            {
                if (IsOmitted(repetition, validity, i))
                    continue;
                BinaryPrimitives.WriteInt32LittleEndian(result.AsSpan(output), values[i].Length);
                output += sizeof(int);
                values[i].CopyTo(result, output);
                output += values[i].Length;
            }
            return result;
        }

        static byte[] EncodeFixedByteArrays(
        byte[][] values,
        bool[] validity,
        ParquetRepetition repetition,
        int physicalCount,
        int? typeLength)
        {
            if (typeLength is null or <= 0)
                throw new ArgumentException("A generated fixed byte array requires a positive type length.", nameof(typeLength));
            var result = new byte[checked(physicalCount * typeLength.Value)];
            var output = 0;
            for (var i = 0; i < values.Length; i++)
            {
                if (IsOmitted(repetition, validity, i))
                    continue;
                if (values[i].Length != typeLength)
                    throw new ArgumentException("A generated fixed byte array value has the wrong width.", nameof(values));
                values[i].CopyTo(result, output);
                output += typeLength.Value;
            }
            return result;
        }
    }

    private static byte[] CompleteFile(MemoryStream file, CompactTestWriter footer)
    {
        var footerBytes = footer.ToArray();
        file.Write(footerBytes);
        Span<byte> tail = stackalloc byte[8];
        BinaryPrimitives.WriteInt32LittleEndian(tail, footerBytes.Length);
        Magic.CopyTo(tail[4..]);
        file.Write(tail);
        return file.ToArray();
    }

    private static byte[] EncodeSnappyLiteral(byte[] input)
    {
        if (input.Length == 0)
            return [0];
        if (input.Length > 60)
            throw new ArgumentOutOfRangeException(nameof(input), "The minimal test Snappy encoder accepts up to 60 bytes.");
        var result = new byte[checked(input.Length + 2)];
        result[0] = checked((byte)input.Length);
        result[1] = checked((byte)((input.Length - 1) << 2));
        input.CopyTo(result, 2);
        return result;
    }

    private static byte[] Combine(byte[] first, byte[] second)
    {
        var result = new byte[checked(first.Length + second.Length)];
        first.CopyTo(result, 0);
        second.CopyTo(result, first.Length);
        return result;
    }

    private static uint ComputeCrc32(ReadOnlySpan<byte> input)
    {
        var crc = uint.MaxValue;
        foreach (var value in input)
        {
            crc ^= value;
            for (var bit = 0; bit < 8; bit++)
                crc = (crc >> 1) ^ (0xEDB88320u & (uint)-(int)(crc & 1));
        }
        return ~crc;
    }

    private readonly record struct GeneratedPages(byte[] Bytes, int DataPageOffset, int? DictionaryPageOffset, long UncompressedLength);
}

internal enum CompactTestType : byte
{
    Stop = 0,
    BooleanTrue = 1,
    BooleanFalse = 2,
    Byte = 3,
    Int16 = 4,
    Int32 = 5,
    Int64 = 6,
    Double = 7,
    Binary = 8,
    List = 9,
    Set = 10,
    Map = 11,
    Struct = 12,
}

internal sealed class CompactTestWriter
{
    private readonly MemoryStream _stream = new();

    public int Length => checked((int)_stream.Length);

    public void Stop() => _stream.WriteByte(0);

    public void Int32Field(ref short previous, short id, int value)
    {
        FieldHeader(ref previous, id, CompactTestType.Int32);
        WriteVarUInt32(ZigZag(value));
    }

    public void Int64Field(ref short previous, short id, long value)
    {
        void WriteVarUInt64(ulong encoded)
        {
            while (encoded >= 0x80)
            {
                _stream.WriteByte((byte)(encoded | 0x80));
                encoded >>= 7;
            }
            _stream.WriteByte((byte)encoded);
        }

        FieldHeader(ref previous, id, CompactTestType.Int64);
        WriteVarUInt64(unchecked((ulong)((value << 1) ^ (value >> 63))));
    }

    public void BooleanField(ref short previous, short id, bool value) =>
        FieldHeader(ref previous, id, value ? CompactTestType.BooleanTrue : CompactTestType.BooleanFalse);

    public void StringField(ref short previous, short id, string value)
    {
        FieldHeader(ref previous, id, CompactTestType.Binary);
        var bytes = Encoding.UTF8.GetBytes(value);
        WriteVarUInt32((uint)bytes.Length);
        _stream.Write(bytes);
    }

    public void StructField(ref short previous, short id, Action body)
    {
        FieldHeader(ref previous, id, CompactTestType.Struct);
        body();
    }

    public void ListField(
        ref short previous,
        short id,
        CompactTestType elementType,
        int count,
        Action body)
    {
        void CollectionHeader(CompactTestType type, int elementCount)
        {
            if (elementCount < 15)
            {
                _stream.WriteByte((byte)((elementCount << 4) | (byte)type));
            }
            else
            {
                _stream.WriteByte((byte)(0xF0 | (byte)type));
                WriteVarUInt32((uint)elementCount);
            }
        }

        FieldHeader(ref previous, id, CompactTestType.List);
        CollectionHeader(elementType, count);
        body();
    }

    public void Int32ListField(ref short previous, short id, IReadOnlyList<int> values) =>
        ListField(ref previous, id, CompactTestType.Int32, values.Count, () =>
        {
            foreach (var value in values)
                WriteVarUInt32(ZigZag(value));
        });

    public void StringListField(ref short previous, short id, IReadOnlyList<string> values) =>
        ListField(ref previous, id, CompactTestType.Binary, values.Count, () =>
        {
            foreach (var value in values)
            {
                var bytes = Encoding.UTF8.GetBytes(value);
                WriteVarUInt32((uint)bytes.Length);
                _stream.Write(bytes);
            }
        });

    public byte[] ToArray() => _stream.ToArray();

    private void FieldHeader(ref short previous, short id, CompactTestType type)
    {
        var delta = id - previous;
        if (delta is > 0 and <= 15)
        {
            _stream.WriteByte((byte)((delta << 4) | (byte)type));
        }
        else
        {
            _stream.WriteByte((byte)type);
            WriteVarUInt32(ZigZag((int)id));
        }
        previous = id;
    }

    private void WriteVarUInt32(uint value)
    {
        while (value >= 0x80)
        {
            _stream.WriteByte((byte)(value | 0x80));
            value >>= 7;
        }
        _stream.WriteByte((byte)value);
    }

    private static uint ZigZag(int value) => unchecked((uint)((value << 1) ^ (value >> 31)));
}
