namespace Lokad.Parquet.Internal;

internal sealed class PageHeaderWire
{
    public required int TypeCode { get; init; }
    public required int UncompressedSize { get; init; }
    public required int CompressedSize { get; init; }
    public int? Crc { get; init; }
    public DataPageHeaderWire? DataV1 { get; init; }
    public DictionaryPageHeaderWire? Dictionary { get; init; }
    public DataPageHeaderV2Wire? DataV2 { get; init; }
}

internal sealed class DataPageHeaderWire
{
    public required int ValueCount { get; init; }
    public required int EncodingCode { get; init; }
    public required int DefinitionEncodingCode { get; init; }
    public required int RepetitionEncodingCode { get; init; }
}

internal sealed class DictionaryPageHeaderWire
{
    public required int ValueCount { get; init; }
    public required int EncodingCode { get; init; }
    public bool? IsSorted { get; init; }
}

internal sealed class DataPageHeaderV2Wire
{
    public required int ValueCount { get; init; }
    public required int NullCount { get; init; }
    public required int RowCount { get; init; }
    public required int EncodingCode { get; init; }
    public required int DefinitionLevelsByteLength { get; init; }
    public required int RepetitionLevelsByteLength { get; init; }
    public bool IsCompressed { get; init; } = true;
}

internal readonly record struct ParsedPageHeader(PageHeaderWire Header, int HeaderByteCount);

internal static class PageHeaderParser
{
    public static bool TryParse(
        ReadOnlySpan<byte> input,
        long offset,
        ParquetReaderOptions options,
        out ParsedPageHeader result)
    {
        var reader = new ThriftCompactReader(input, offset, options);
        try
        {
            result = new ParsedPageHeader(ParsePageHeader(ref reader, options), reader.Position);
            return true;
        }
        catch (ThriftTruncatedException)
        {
            result = default;
            return false;
        }

        static PageHeaderWire ParsePageHeader(ref ThriftCompactReader reader, ParquetReaderOptions options)
        {
            int? typeCode = null;
            int? uncompressedSize = null;
            int? compressedSize = null;
            int? crc = null;
            DataPageHeaderWire? dataV1 = null;
            DictionaryPageHeaderWire? dictionary = null;
            DataPageHeaderV2Wire? dataV2 = null;
            short previous = 0;
            ulong seen = 0;
            while (true)
            {
                var field = reader.ReadField(ref previous);
                if (field.Type == CompactType.Stop)
                    break;
                Mark(ref seen, field.Id, ref reader);
                switch (field.Id)
                {
                    case 1:
                        reader.RequireType(field, CompactType.Int32);
                        typeCode = reader.ReadInt32();
                        break;
                    case 2:
                        reader.RequireType(field, CompactType.Int32);
                        uncompressedSize = reader.ReadInt32();
                        break;
                    case 3:
                        reader.RequireType(field, CompactType.Int32);
                        compressedSize = reader.ReadInt32();
                        break;
                    case 4:
                        reader.RequireType(field, CompactType.Int32);
                        crc = reader.ReadInt32();
                        break;
                    case 5:
                        reader.RequireType(field, CompactType.Struct);
                        dataV1 = ParseDataV1(ref reader);
                        break;
                    case 6:
                        reader.RequireType(field, CompactType.Struct);
                        reader.SkipValue(CompactType.Struct, 2, CompactBooleanEncoding.CollectionValue);
                        break;
                    case 7:
                        reader.RequireType(field, CompactType.Struct);
                        dictionary = ParseDictionary(ref reader);
                        break;
                    case 8:
                        reader.RequireType(field, CompactType.Struct);
                        dataV2 = ParseDataV2(ref reader);
                        break;
                    default:
                        reader.SkipField(field, 2);
                        break;
                }
            }

            if (typeCode is null || uncompressedSize is null || compressedSize is null)
                throw reader.Format("A page header is missing a required field.");
            if (uncompressedSize < 0 || compressedSize < 0)
                throw reader.Format("A page header contains a negative payload size.");
            if (uncompressedSize > options.MaximumUncompressedPageBytes)
                throw new ParquetLimitExceededException("A page exceeds the configured uncompressed-size limit.", reader.AbsoluteOffset);
            if (compressedSize > options.MaximumCompressedPageBytes)
                throw new ParquetLimitExceededException("A page exceeds the configured compressed-size limit.", reader.AbsoluteOffset);

            var matchingHeaders = (dataV1 is null ? 0 : 1) + (dictionary is null ? 0 : 1) + (dataV2 is null ? 0 : 1);
            if (typeCode switch { 0 => dataV1 is null, 2 => dictionary is null, 3 => dataV2 is null, _ => false })
                throw reader.Format("The page-specific header does not match the page type.");
            if (matchingHeaders > 1)
                throw reader.Format("A page header contains multiple page-specific headers.");

            return new PageHeaderWire
            {
                TypeCode = typeCode.Value,
                UncompressedSize = uncompressedSize.Value,
                CompressedSize = compressedSize.Value,
                Crc = crc,
                DataV1 = dataV1,
                Dictionary = dictionary,
                DataV2 = dataV2,
            };
        }

        static DataPageHeaderWire ParseDataV1(ref ThriftCompactReader reader)
        {
            int? valueCount = null;
            int? encoding = null;
            int? definition = null;
            int? repetition = null;
            short previous = 0;
            ulong seen = 0;
            while (true)
            {
                var field = reader.ReadField(ref previous);
                if (field.Type == CompactType.Stop)
                    break;
                Mark(ref seen, field.Id, ref reader);
                switch (field.Id)
                {
                    case 1:
                        reader.RequireType(field, CompactType.Int32);
                        valueCount = reader.ReadInt32();
                        break;
                    case 2:
                        reader.RequireType(field, CompactType.Int32);
                        encoding = reader.ReadInt32();
                        break;
                    case 3:
                        reader.RequireType(field, CompactType.Int32);
                        definition = reader.ReadInt32();
                        break;
                    case 4:
                        reader.RequireType(field, CompactType.Int32);
                        repetition = reader.ReadInt32();
                        break;
                    default:
                        reader.SkipField(field, 3);
                        break;
                }
            }
            if (valueCount is null || encoding is null || definition is null || repetition is null)
                throw reader.Format("A V1 data-page header is missing a required field.");
            if (valueCount < 0)
                throw reader.Format("A V1 data-page value count is negative.");
            return new DataPageHeaderWire
            {
                ValueCount = valueCount.Value,
                EncodingCode = encoding.Value,
                DefinitionEncodingCode = definition.Value,
                RepetitionEncodingCode = repetition.Value,
            };
        }

        static DictionaryPageHeaderWire ParseDictionary(ref ThriftCompactReader reader)
        {
            int? valueCount = null;
            int? encoding = null;
            bool? sorted = null;
            short previous = 0;
            ulong seen = 0;
            while (true)
            {
                var field = reader.ReadField(ref previous);
                if (field.Type == CompactType.Stop)
                    break;
                Mark(ref seen, field.Id, ref reader);
                switch (field.Id)
                {
                    case 1:
                        reader.RequireType(field, CompactType.Int32);
                        valueCount = reader.ReadInt32();
                        break;
                    case 2:
                        reader.RequireType(field, CompactType.Int32);
                        encoding = reader.ReadInt32();
                        break;
                    case 3:
                        sorted = reader.ReadBoolean(field.Type);
                        break;
                    default:
                        reader.SkipField(field, 3);
                        break;
                }
            }
            if (valueCount is null || encoding is null)
                throw reader.Format("A dictionary-page header is missing a required field.");
            if (valueCount < 0)
                throw reader.Format("A dictionary-page value count is negative.");
            return new DictionaryPageHeaderWire
            {
                ValueCount = valueCount.Value,
                EncodingCode = encoding.Value,
                IsSorted = sorted,
            };
        }

        static DataPageHeaderV2Wire ParseDataV2(ref ThriftCompactReader reader)
        {
            int? valueCount = null;
            int? nullCount = null;
            int? rowCount = null;
            int? encoding = null;
            int? definitionBytes = null;
            int? repetitionBytes = null;
            var compressed = true;
            short previous = 0;
            ulong seen = 0;
            while (true)
            {
                var field = reader.ReadField(ref previous);
                if (field.Type == CompactType.Stop)
                    break;
                Mark(ref seen, field.Id, ref reader);
                switch (field.Id)
                {
                    case 1:
                        reader.RequireType(field, CompactType.Int32);
                        valueCount = reader.ReadInt32();
                        break;
                    case 2:
                        reader.RequireType(field, CompactType.Int32);
                        nullCount = reader.ReadInt32();
                        break;
                    case 3:
                        reader.RequireType(field, CompactType.Int32);
                        rowCount = reader.ReadInt32();
                        break;
                    case 4:
                        reader.RequireType(field, CompactType.Int32);
                        encoding = reader.ReadInt32();
                        break;
                    case 5:
                        reader.RequireType(field, CompactType.Int32);
                        definitionBytes = reader.ReadInt32();
                        break;
                    case 6:
                        reader.RequireType(field, CompactType.Int32);
                        repetitionBytes = reader.ReadInt32();
                        break;
                    case 7:
                        compressed = reader.ReadBoolean(field.Type);
                        break;
                    default:
                        reader.SkipField(field, 3);
                        break;
                }
            }
            if (valueCount is null || nullCount is null || rowCount is null || encoding is null ||
                definitionBytes is null || repetitionBytes is null)
                throw reader.Format("A V2 data-page header is missing a required field.");
            if (valueCount < 0 || nullCount < 0 || nullCount > valueCount || rowCount < 0 || definitionBytes < 0 || repetitionBytes < 0)
                throw reader.Format("A V2 data-page header contains an invalid count or length.");
            return new DataPageHeaderV2Wire
            {
                ValueCount = valueCount.Value,
                NullCount = nullCount.Value,
                RowCount = rowCount.Value,
                EncodingCode = encoding.Value,
                DefinitionLevelsByteLength = definitionBytes.Value,
                RepetitionLevelsByteLength = repetitionBytes.Value,
                IsCompressed = compressed,
            };
        }
    }

    private static void Mark(ref ulong seen, int fieldId, ref ThriftCompactReader reader)
        => reader.MarkKnownField(ref seen, fieldId, CompactStructContext.PageHeader);
}
