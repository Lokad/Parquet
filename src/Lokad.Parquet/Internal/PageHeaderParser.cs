namespace Lokad.Parquet.Internal;

// Validated page type. The parser guarantees the corresponding specific
// header below is meaningful; specific headers for any other type, including
// Unknown, must be ignored. Unknown preserves the raw TypeCode so the scan
// path can report an unsupported page type rather than malformed input.
internal enum ValidatedPageType
{
    DataV1,
    Dictionary,
    DataV2,
    Unknown,
}

internal readonly record struct ValidatedDataPageV1(
    int ValueCount,
    int EncodingCode,
    int DefinitionEncodingCode,
    int RepetitionEncodingCode);

internal readonly record struct ValidatedDictionaryPage(
    int ValueCount,
    int EncodingCode,
    bool? IsSorted);

internal readonly record struct ValidatedDataPageV2(
    int ValueCount,
    int NullCount,
    int RowCount,
    int EncodingCode,
    int DefinitionLevelsByteLength,
    int RepetitionLevelsByteLength,
    bool IsCompressed);

// Validated page header. Only the specific header selected by PageType is
// meaningful; the others are default and must not be read. Crc absence means
// no CRC was present, which is genuine absence.
internal readonly record struct ValidatedPageHeader(
    int TypeCode,
    int UncompressedSize,
    int CompressedSize,
    int? Crc,
    ValidatedPageType PageType,
    ValidatedDataPageV1 DataV1,
    ValidatedDictionaryPage Dictionary,
    ValidatedDataPageV2 DataV2);

internal readonly record struct ParsedPageHeader(ValidatedPageHeader Header, int HeaderByteCount);

internal static class PageHeaderParser
{
    public static bool TryParse(
        ReadOnlySpan<byte> input,
        long offset,
        ParquetReaderOptions options,
        CancellationToken cancellationToken,
        out ParsedPageHeader result)
    {
        var reader = new ThriftCompactReader(input, offset, options, cancellationToken);
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

        static ValidatedPageHeader ParsePageHeader(ref ThriftCompactReader reader, ParquetReaderOptions options)
        {
            reader.RequireDepth(1);
            int? typeCode = null;
            int? uncompressedSize = null;
            int? compressedSize = null;
            int? crc = null;
            ValidatedDataPageV1 dataV1 = default;
            var hasDataV1 = false;
            ValidatedDictionaryPage dictionary = default;
            var hasDictionary = false;
            ValidatedDataPageV2 dataV2 = default;
            var hasDataV2 = false;
            short previous = 0;
            ulong seen = 0;
            var fieldCount = 0;
            while (true)
            {
                var field = reader.ReadField(ref previous);
                reader.ObserveCancellation(fieldCount);
                fieldCount++;
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
                        hasDataV1 = true;
                        break;
                    case 6:
                        reader.RequireType(field, CompactType.Struct);
                        reader.SkipValue(CompactType.Struct, 1, CompactBooleanEncoding.CollectionValue);
                        break;
                    case 7:
                        reader.RequireType(field, CompactType.Struct);
                        dictionary = ParseDictionary(ref reader);
                        hasDictionary = true;
                        break;
                    case 8:
                        reader.RequireType(field, CompactType.Struct);
                        dataV2 = ParseDataV2(ref reader);
                        hasDataV2 = true;
                        break;
                    default:
                        reader.SkipField(field, 1);
                        break;
                }
            }

            if (typeCode is null || uncompressedSize is null || compressedSize is null)
                throw reader.Format("A page header is missing a required field.");
            if (uncompressedSize < 0 || compressedSize < 0)
                throw reader.Format("A page header contains a negative payload size.");
            if (uncompressedSize > options.MaximumUncompressedPageBytes)
                throw new ParquetLimitExceededException("A page exceeds the configured uncompressed-size limit.", ParquetErrorLocation.AtOffset(reader.AbsoluteOffset));
            if (compressedSize > options.MaximumCompressedPageBytes)
                throw new ParquetLimitExceededException("A page exceeds the configured compressed-size limit.", ParquetErrorLocation.AtOffset(reader.AbsoluteOffset));

            var matchingHeaders = (hasDataV1 ? 1 : 0) + (hasDictionary ? 1 : 0) + (hasDataV2 ? 1 : 0);
            if (typeCode switch { 0 => !hasDataV1, 2 => !hasDictionary, 3 => !hasDataV2, _ => false })
                throw reader.Format("The page-specific header does not match the page type.");
            if (matchingHeaders > 1)
                throw reader.Format("A page header contains multiple page-specific headers.");

            var pageType = typeCode.Value switch
            {
                0 => ValidatedPageType.DataV1,
                2 => ValidatedPageType.Dictionary,
                3 => ValidatedPageType.DataV2,
                _ => ValidatedPageType.Unknown,
            };
            return new ValidatedPageHeader(
                typeCode.Value,
                uncompressedSize.Value,
                compressedSize.Value,
                crc,
                pageType,
                dataV1,
                dictionary,
                dataV2);
        }

        static ValidatedDataPageV1 ParseDataV1(ref ThriftCompactReader reader)
        {
            reader.RequireDepth(2);
            int? valueCount = null;
            int? encoding = null;
            int? definition = null;
            int? repetition = null;
            short previous = 0;
            ulong seen = 0;
            var fieldCount = 0;
            while (true)
            {
                var field = reader.ReadField(ref previous);
                reader.ObserveCancellation(fieldCount);
                fieldCount++;
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
                        reader.SkipField(field, 2);
                        break;
                }
            }
            if (valueCount is null || encoding is null || definition is null || repetition is null)
                throw reader.Format("A V1 data-page header is missing a required field.");
            if (valueCount < 0)
                throw reader.Format("A V1 data-page value count is negative.");
            return new ValidatedDataPageV1(
                valueCount.Value,
                encoding.Value,
                definition.Value,
                repetition.Value);
        }

        static ValidatedDictionaryPage ParseDictionary(ref ThriftCompactReader reader)
        {
            reader.RequireDepth(2);
            int? valueCount = null;
            int? encoding = null;
            bool? sorted = null;
            short previous = 0;
            ulong seen = 0;
            var fieldCount = 0;
            while (true)
            {
                var field = reader.ReadField(ref previous);
                reader.ObserveCancellation(fieldCount);
                fieldCount++;
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
                        reader.SkipField(field, 2);
                        break;
                }
            }
            if (valueCount is null || encoding is null)
                throw reader.Format("A dictionary-page header is missing a required field.");
            if (valueCount < 0)
                throw reader.Format("A dictionary-page value count is negative.");
            return new ValidatedDictionaryPage(
                valueCount.Value,
                encoding.Value,
                sorted);
        }

        static ValidatedDataPageV2 ParseDataV2(ref ThriftCompactReader reader)
        {
            reader.RequireDepth(2);
            int? valueCount = null;
            int? nullCount = null;
            int? rowCount = null;
            int? encoding = null;
            int? definitionBytes = null;
            int? repetitionBytes = null;
            var compressed = true;
            short previous = 0;
            ulong seen = 0;
            var fieldCount = 0;
            while (true)
            {
                var field = reader.ReadField(ref previous);
                reader.ObserveCancellation(fieldCount);
                fieldCount++;
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
                        reader.SkipField(field, 2);
                        break;
                }
            }
            if (valueCount is null || nullCount is null || rowCount is null || encoding is null ||
                definitionBytes is null || repetitionBytes is null)
                throw reader.Format("A V2 data-page header is missing a required field.");
            if (valueCount < 0 || nullCount < 0 || nullCount > valueCount || rowCount < 0 || definitionBytes < 0 || repetitionBytes < 0)
                throw reader.Format("A V2 data-page header contains an invalid count or length.");
            return new ValidatedDataPageV2(
                valueCount.Value,
                nullCount.Value,
                rowCount.Value,
                encoding.Value,
                definitionBytes.Value,
                repetitionBytes.Value,
                compressed);
        }
    }

    private static void Mark(ref ulong seen, int fieldId, ref ThriftCompactReader reader)
        => reader.MarkKnownField(ref seen, fieldId, CompactStructContext.PageHeader);
}
