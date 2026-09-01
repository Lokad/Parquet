namespace Lokad.Parquet.Internal;

internal static class ParquetFooterParser
{
    public static ParquetFileMetadata Parse(
        ReadOnlySpan<byte> footer,
        long footerOffset,
        long sourceLength,
        ParquetReaderOptions options)
    {
        var reader = new ThriftCompactReader(footer, footerOffset, options);
        try
        {
            var wire = ParseFileMetadata(ref reader, options);
            if (!reader.IsAtEnd)
                throw reader.Format("Trailing bytes follow the Parquet file metadata.");
            return BuildMetadata(wire, sourceLength, footerOffset, options);
        }
        catch (ThriftTruncatedException exception)
        {
            throw new ParquetFormatException("The Parquet footer contains truncated Thrift metadata.", byteOffset: exception.ByteOffset);
        }

        static FileMetadataWire ParseFileMetadata(ref ThriftCompactReader reader, ParquetReaderOptions options)
        {
            var result = new FileMetadataWire();
            short previous = 0;
            ulong seen = 0;
            while (true)
            {
                var field = reader.ReadField(ref previous);
                if (field.Type == CompactType.Stop)
                    break;

                switch (field.Id)
                {
                    case 1:
                        Mark(ref seen, field.Id, ref reader);
                        reader.RequireType(field, CompactType.Int32);
                        result.Version = reader.ReadInt32();
                        break;
                    case 2:
                        Mark(ref seen, field.Id, ref reader);
                        result.Schema = ParseSchemaList(ref reader, field, options);
                        break;
                    case 3:
                        Mark(ref seen, field.Id, ref reader);
                        reader.RequireType(field, CompactType.Int64);
                        result.RowCount = reader.ReadInt64();
                        break;
                    case 4:
                        Mark(ref seen, field.Id, ref reader);
                        result.RowGroups = ParseRowGroupList(ref reader, field, options);
                        break;
                    case 5:
                        Mark(ref seen, field.Id, ref reader);
                        result.CustomMetadata = ParseKeyValueList(ref reader, field, options);
                        break;
                    case 6:
                        Mark(ref seen, field.Id, ref reader);
                        reader.RequireType(field, CompactType.Binary);
                        result.CreatedBy = reader.ReadString();
                        break;
                    case 7:
                        Mark(ref seen, field.Id, ref reader);
                        result.ColumnOrders = ParseColumnOrders(ref reader, field, options);
                        break;
                    case 8:
                        Mark(ref seen, field.Id, ref reader);
                        reader.RequireType(field, CompactType.Struct);
                        result.HasEncryptionAlgorithm = true;
                        reader.SkipValue(CompactType.Struct, 1, CompactBooleanEncoding.CollectionValue);
                        break;
                    case 9:
                        Mark(ref seen, field.Id, ref reader);
                        reader.RequireType(field, CompactType.Binary);
                        result.HasFooterSigningKeyMetadata = true;
                        reader.SkipField(field, 1);
                        break;
                    default:
                        reader.SkipField(field, 1);
                        break;
                }
            }

            if (result.Version is null || result.Schema is null || result.RowCount is null || result.RowGroups is null)
                throw reader.Format("Parquet file metadata is missing a required field.");
            return result;
        }

        static SchemaElementWire[] ParseSchemaList(
        ref ThriftCompactReader reader,
        CompactField field,
        ParquetReaderOptions options)
        {
            reader.RequireType(field, CompactType.List);
            var list = reader.ReadCollection();
            if (list.ElementType != CompactType.Struct)
                throw reader.Format("The schema list has an unexpected element type.");
            if (list.Count == 0)
                throw reader.Format("The Parquet schema is empty.");
            if (list.Count > options.MaximumSchemaElements)
                throw new ParquetLimitExceededException("The schema exceeds the configured element limit.", reader.AbsoluteOffset);

            var result = new SchemaElementWire[list.Count];
            for (var i = 0; i < result.Length; i++)
                result[i] = ParseSchemaElement(ref reader);
            return result;
        }

        static SchemaElementWire ParseSchemaElement(ref ThriftCompactReader reader)
        {
            var result = new SchemaElementWire();
            short previous = 0;
            ulong seen = 0;
            while (true)
            {
                var field = reader.ReadField(ref previous);
                if (field.Type == CompactType.Stop)
                    break;
                switch (field.Id)
                {
                    case 1:
                        Mark(ref seen, field.Id, ref reader);
                        reader.RequireType(field, CompactType.Int32);
                        result.TypeCode = reader.ReadInt32();
                        break;
                    case 2:
                        Mark(ref seen, field.Id, ref reader);
                        reader.RequireType(field, CompactType.Int32);
                        result.TypeLength = reader.ReadInt32();
                        break;
                    case 3:
                        Mark(ref seen, field.Id, ref reader);
                        reader.RequireType(field, CompactType.Int32);
                        result.RepetitionCode = reader.ReadInt32();
                        break;
                    case 4:
                        Mark(ref seen, field.Id, ref reader);
                        reader.RequireType(field, CompactType.Binary);
                        result.Name = reader.ReadString();
                        break;
                    case 5:
                        Mark(ref seen, field.Id, ref reader);
                        reader.RequireType(field, CompactType.Int32);
                        result.ChildCount = reader.ReadInt32();
                        break;
                    case 6:
                        Mark(ref seen, field.Id, ref reader);
                        reader.RequireType(field, CompactType.Int32);
                        result.ConvertedTypeCode = reader.ReadInt32();
                        break;
                    case 7:
                        Mark(ref seen, field.Id, ref reader);
                        reader.RequireType(field, CompactType.Int32);
                        result.Scale = reader.ReadInt32();
                        break;
                    case 8:
                        Mark(ref seen, field.Id, ref reader);
                        reader.RequireType(field, CompactType.Int32);
                        result.Precision = reader.ReadInt32();
                        break;
                    case 9:
                        Mark(ref seen, field.Id, ref reader);
                        reader.RequireType(field, CompactType.Int32);
                        result.FieldId = reader.ReadInt32();
                        break;
                    case 10:
                        Mark(ref seen, field.Id, ref reader);
                        reader.RequireType(field, CompactType.Struct);
                        result.LogicalAnnotation = ParseLogicalAnnotation(ref reader);
                        break;
                    default:
                        reader.SkipField(field, 2);
                        break;
                }
            }

            if (result.Name is null)
                throw reader.Format("A schema element is missing its required name.");
            return result;
        }

        static ParquetLogicalAnnotation ParseLogicalAnnotation(ref ThriftCompactReader reader)
        {
            short previous = 0;
            var field = reader.ReadField(ref previous);
            if (field.Type == CompactType.Stop)
                throw reader.Format("A logical-type union is empty.");
            if (field.Type != CompactType.Struct)
                throw reader.Format("A logical-type union member is not a struct.");

            var discriminator = field.Id;
            ParquetLogicalAnnotation annotation;
            switch (discriminator)
            {
                case 5:
                    annotation = ParseDecimalAnnotation(ref reader, discriminator);
                    break;
                case 7:
                    annotation = ParseTimeAnnotation(ref reader, discriminator, ParquetLogicalTypeKind.Time);
                    break;
                case 8:
                    annotation = ParseTimeAnnotation(ref reader, discriminator, ParquetLogicalTypeKind.Timestamp);
                    break;
                case 10:
                    annotation = ParseIntegerAnnotation(ref reader, discriminator);
                    break;
                default:
                    reader.SkipValue(CompactType.Struct, 3, CompactBooleanEncoding.CollectionValue);
                    annotation = new ParquetLogicalAnnotation(
                        discriminator,
                        Enum.IsDefined(typeof(ParquetLogicalTypeKind), (int)discriminator)
                            ? (ParquetLogicalTypeKind)discriminator : null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null);
                    break;
            }

            var extra = reader.ReadField(ref previous);
            if (extra.Type != CompactType.Stop)
                throw reader.Format("A logical-type union contains multiple members.");
            return annotation;
        }

        static ParquetLogicalAnnotation ParseDecimalAnnotation(ref ThriftCompactReader reader, int discriminator)
        {
            int? scale = null;
            int? precision = null;
            short previous = 0;
            ulong seen = 0;
            while (true)
            {
                var field = reader.ReadField(ref previous);
                if (field.Type == CompactType.Stop)
                    break;
                switch (field.Id)
                {
                    case 1:
                        Mark(ref seen, field.Id, ref reader);
                        reader.RequireType(field, CompactType.Int32);
                        scale = reader.ReadInt32();
                        break;
                    case 2:
                        Mark(ref seen, field.Id, ref reader);
                        reader.RequireType(field, CompactType.Int32);
                        precision = reader.ReadInt32();
                        break;
                    default:
                        reader.SkipField(field, 4);
                        break;
                }
            }
            if (scale is null || precision is null)
                throw reader.Format("A decimal logical annotation is missing a required field.");
            return new ParquetLogicalAnnotation(
                discriminator,
                ParquetLogicalTypeKind.Decimal,
                scale,
                precision,
                null,
                null,
                null,
                null,
                null);
        }

        static ParquetLogicalAnnotation ParseTimeAnnotation(
        ref ThriftCompactReader reader,
        int discriminator,
        ParquetLogicalTypeKind kind)
        {
            bool? adjusted = null;
            int? unitDiscriminator = null;
            ParquetTimeUnit? unit = null;
            short previous = 0;
            ulong seen = 0;
            while (true)
            {
                var field = reader.ReadField(ref previous);
                if (field.Type == CompactType.Stop)
                    break;
                switch (field.Id)
                {
                    case 1:
                        Mark(ref seen, field.Id, ref reader);
                        adjusted = reader.ReadBoolean(field.Type);
                        break;
                    case 2:
                        Mark(ref seen, field.Id, ref reader);
                        reader.RequireType(field, CompactType.Struct);
                        var parsedUnit = ParseTimeUnit(ref reader);
                        unitDiscriminator = parsedUnit.Discriminator;
                        unit = parsedUnit.Unit;
                        break;
                    default:
                        reader.SkipField(field, 4);
                        break;
                }
            }
            if (adjusted is null || unitDiscriminator is null)
                throw reader.Format("A time logical annotation is missing a required field.");
            return new ParquetLogicalAnnotation(
                discriminator,
                kind,
                null,
                null,
                unitDiscriminator,
                unit,
                adjusted,
                null,
                null);
        }

        static ParsedTimeUnit ParseTimeUnit(ref ThriftCompactReader reader)
        {
            short previous = 0;
            var field = reader.ReadField(ref previous);
            if (field.Type == CompactType.Stop || field.Type != CompactType.Struct)
                throw reader.Format("A time-unit union is empty or malformed.");
            reader.SkipValue(CompactType.Struct, 5, CompactBooleanEncoding.CollectionValue);
            ParquetTimeUnit? unit = field.Id switch
            {
                1 => ParquetTimeUnit.Milliseconds,
                2 => ParquetTimeUnit.Microseconds,
                3 => ParquetTimeUnit.Nanoseconds,
                _ => null,
            };
            var extra = reader.ReadField(ref previous);
            if (extra.Type != CompactType.Stop)
                throw reader.Format("A time-unit union contains multiple members.");
            return new ParsedTimeUnit(field.Id, unit);
        }

        static ParquetLogicalAnnotation ParseIntegerAnnotation(ref ThriftCompactReader reader, int discriminator)
        {
            int? bitWidth = null;
            bool? signed = null;
            short previous = 0;
            ulong seen = 0;
            while (true)
            {
                var field = reader.ReadField(ref previous);
                if (field.Type == CompactType.Stop)
                    break;
                switch (field.Id)
                {
                    case 1:
                        Mark(ref seen, field.Id, ref reader);
                        reader.RequireType(field, CompactType.Byte);
                        bitWidth = reader.ReadSByte();
                        break;
                    case 2:
                        Mark(ref seen, field.Id, ref reader);
                        signed = reader.ReadBoolean(field.Type);
                        break;
                    default:
                        reader.SkipField(field, 4);
                        break;
                }
            }
            if (bitWidth is null || signed is null)
                throw reader.Format("An integer logical annotation is missing a required field.");
            return new ParquetLogicalAnnotation(
                discriminator,
                ParquetLogicalTypeKind.Integer,
                null,
                null,
                null,
                null,
                null,
                bitWidth,
                signed);
        }

        static RowGroupWire[] ParseRowGroupList(
        ref ThriftCompactReader reader,
        CompactField field,
        ParquetReaderOptions options)
        {
            reader.RequireType(field, CompactType.List);
            var list = reader.ReadCollection();
            if (list.ElementType != CompactType.Struct)
                throw reader.Format("The row-group list has an unexpected element type.");
            if (list.Count > options.MaximumRowGroups)
                throw new ParquetLimitExceededException("The file exceeds the configured row-group limit.", reader.AbsoluteOffset);
            var result = new RowGroupWire[list.Count];
            for (var i = 0; i < result.Length; i++)
                result[i] = ParseRowGroup(ref reader, options);
            return result;
        }

        static RowGroupWire ParseRowGroup(ref ThriftCompactReader reader, ParquetReaderOptions options)
        {
            var result = new RowGroupWire();
            short previous = 0;
            ulong seen = 0;
            while (true)
            {
                var field = reader.ReadField(ref previous);
                if (field.Type == CompactType.Stop)
                    break;
                switch (field.Id)
                {
                    case 1:
                        Mark(ref seen, field.Id, ref reader);
                        result.Columns = ParseColumnChunkList(ref reader, field, options);
                        break;
                    case 2:
                        Mark(ref seen, field.Id, ref reader);
                        reader.RequireType(field, CompactType.Int64);
                        result.TotalByteSize = reader.ReadInt64();
                        break;
                    case 3:
                        Mark(ref seen, field.Id, ref reader);
                        reader.RequireType(field, CompactType.Int64);
                        result.RowCount = reader.ReadInt64();
                        break;
                    case 5:
                        Mark(ref seen, field.Id, ref reader);
                        reader.RequireType(field, CompactType.Int64);
                        result.FileOffset = reader.ReadInt64();
                        break;
                    case 6:
                        Mark(ref seen, field.Id, ref reader);
                        reader.RequireType(field, CompactType.Int64);
                        result.TotalCompressedSize = reader.ReadInt64();
                        break;
                    default:
                        reader.SkipField(field, 2);
                        break;
                }
            }
            if (result.Columns is null || result.TotalByteSize is null || result.RowCount is null)
                throw reader.Format("A row group is missing a required field.");
            return result;
        }

        static ColumnChunkWire[] ParseColumnChunkList(
        ref ThriftCompactReader reader,
        CompactField field,
        ParquetReaderOptions options)
        {
            reader.RequireType(field, CompactType.List);
            var list = reader.ReadCollection();
            if (list.ElementType != CompactType.Struct)
                throw reader.Format("The column-chunk list has an unexpected element type.");
            if (list.Count > options.MaximumLeafColumns)
                throw new ParquetLimitExceededException("A row group exceeds the configured leaf-column limit.", reader.AbsoluteOffset);
            var result = new ColumnChunkWire[list.Count];
            for (var i = 0; i < result.Length; i++)
                result[i] = ParseColumnChunk(ref reader, options);
            return result;
        }

        static ColumnChunkWire ParseColumnChunk(ref ThriftCompactReader reader, ParquetReaderOptions options)
        {
            var result = new ColumnChunkWire();
            short previous = 0;
            ulong seen = 0;
            while (true)
            {
                var field = reader.ReadField(ref previous);
                if (field.Type == CompactType.Stop)
                    break;
                switch (field.Id)
                {
                    case 1:
                        Mark(ref seen, field.Id, ref reader);
                        reader.RequireType(field, CompactType.Binary);
                        result.FilePath = reader.ReadString();
                        break;
                    case 2:
                        Mark(ref seen, field.Id, ref reader);
                        reader.RequireType(field, CompactType.Int64);
                        result.FileOffset = reader.ReadInt64();
                        break;
                    case 3:
                        Mark(ref seen, field.Id, ref reader);
                        reader.RequireType(field, CompactType.Struct);
                        result.Metadata = ParseColumnMetadata(ref reader, options);
                        break;
                    case 4:
                        Mark(ref seen, field.Id, ref reader);
                        reader.RequireType(field, CompactType.Int64);
                        result.OffsetIndexOffset = reader.ReadInt64();
                        break;
                    case 5:
                        Mark(ref seen, field.Id, ref reader);
                        reader.RequireType(field, CompactType.Int32);
                        result.OffsetIndexLength = reader.ReadInt32();
                        break;
                    case 6:
                        Mark(ref seen, field.Id, ref reader);
                        reader.RequireType(field, CompactType.Int64);
                        result.ColumnIndexOffset = reader.ReadInt64();
                        break;
                    case 7:
                        Mark(ref seen, field.Id, ref reader);
                        reader.RequireType(field, CompactType.Int32);
                        result.ColumnIndexLength = reader.ReadInt32();
                        break;
                    case 8:
                        Mark(ref seen, field.Id, ref reader);
                        reader.RequireType(field, CompactType.Struct);
                        result.HasCryptoMetadata = true;
                        reader.SkipValue(CompactType.Struct, 3, CompactBooleanEncoding.CollectionValue);
                        break;
                    case 9:
                        Mark(ref seen, field.Id, ref reader);
                        reader.RequireType(field, CompactType.Binary);
                        result.HasEncryptedMetadata = true;
                        reader.SkipField(field, 3);
                        break;
                    default:
                        reader.SkipField(field, 3);
                        break;
                }
            }
            if (result.FileOffset is null || result.Metadata is null)
                throw reader.Format("A column chunk is missing required metadata.");
            return result;
        }

        static ColumnMetadataWire ParseColumnMetadata(ref ThriftCompactReader reader, ParquetReaderOptions options)
        {
            var result = new ColumnMetadataWire();
            short previous = 0;
            ulong seen = 0;
            while (true)
            {
                var field = reader.ReadField(ref previous);
                if (field.Type == CompactType.Stop)
                    break;
                switch (field.Id)
                {
                    case 1:
                        Mark(ref seen, field.Id, ref reader);
                        reader.RequireType(field, CompactType.Int32);
                        result.TypeCode = reader.ReadInt32();
                        break;
                    case 2:
                        Mark(ref seen, field.Id, ref reader);
                        result.EncodingCodes = ParseInt32List(ref reader, field, "encoding");
                        break;
                    case 3:
                        Mark(ref seen, field.Id, ref reader);
                        result.Path = ParseStringList(ref reader, field);
                        break;
                    case 4:
                        Mark(ref seen, field.Id, ref reader);
                        reader.RequireType(field, CompactType.Int32);
                        result.CodecCode = reader.ReadInt32();
                        break;
                    case 5:
                        Mark(ref seen, field.Id, ref reader);
                        reader.RequireType(field, CompactType.Int64);
                        result.ValueCount = reader.ReadInt64();
                        break;
                    case 6:
                        Mark(ref seen, field.Id, ref reader);
                        reader.RequireType(field, CompactType.Int64);
                        result.TotalUncompressedSize = reader.ReadInt64();
                        break;
                    case 7:
                        Mark(ref seen, field.Id, ref reader);
                        reader.RequireType(field, CompactType.Int64);
                        result.TotalCompressedSize = reader.ReadInt64();
                        break;
                    case 8:
                        Mark(ref seen, field.Id, ref reader);
                        result.CustomMetadata = ParseKeyValueList(ref reader, field, options);
                        break;
                    case 9:
                        Mark(ref seen, field.Id, ref reader);
                        reader.RequireType(field, CompactType.Int64);
                        result.DataPageOffset = reader.ReadInt64();
                        break;
                    case 10:
                        Mark(ref seen, field.Id, ref reader);
                        reader.RequireType(field, CompactType.Int64);
                        result.IndexPageOffset = reader.ReadInt64();
                        break;
                    case 11:
                        Mark(ref seen, field.Id, ref reader);
                        reader.RequireType(field, CompactType.Int64);
                        result.DictionaryPageOffset = reader.ReadInt64();
                        break;
                    case 12:
                        Mark(ref seen, field.Id, ref reader);
                        reader.RequireType(field, CompactType.Struct);
                        result.Statistics = ParseStatistics(ref reader);
                        break;
                    case 14:
                        Mark(ref seen, field.Id, ref reader);
                        reader.RequireType(field, CompactType.Int64);
                        result.BloomFilterOffset = reader.ReadInt64();
                        break;
                    case 15:
                        Mark(ref seen, field.Id, ref reader);
                        reader.RequireType(field, CompactType.Int32);
                        result.BloomFilterLength = reader.ReadInt32();
                        break;
                    default:
                        reader.SkipField(field, 4);
                        break;
                }
            }
            if (result.TypeCode is null || result.EncodingCodes is null || result.Path is null || result.CodecCode is null ||
                result.ValueCount is null || result.TotalUncompressedSize is null || result.TotalCompressedSize is null ||
                result.DataPageOffset is null)
                throw reader.Format("Column metadata is missing a required field.");
            return result;
        }

        static StatisticsWire ParseStatistics(ref ThriftCompactReader reader)
        {
            var result = new StatisticsWire();
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
                        reader.RequireType(field, CompactType.Binary);
                        result.LegacyMaximum = reader.ReadBinary();
                        break;
                    case 2:
                        reader.RequireType(field, CompactType.Binary);
                        result.LegacyMinimum = reader.ReadBinary();
                        break;
                    case 3:
                        reader.RequireType(field, CompactType.Int64);
                        result.NullCount = reader.ReadInt64();
                        break;
                    case 4:
                        reader.RequireType(field, CompactType.Int64);
                        result.DistinctCount = reader.ReadInt64();
                        break;
                    case 5:
                        reader.RequireType(field, CompactType.Binary);
                        result.Maximum = reader.ReadBinary();
                        break;
                    case 6:
                        reader.RequireType(field, CompactType.Binary);
                        result.Minimum = reader.ReadBinary();
                        break;
                    case 7:
                        result.IsMaximumExact = reader.ReadBoolean(field.Type);
                        break;
                    case 8:
                        result.IsMinimumExact = reader.ReadBoolean(field.Type);
                        break;
                    case 9:
                        reader.RequireType(field, CompactType.Int64);
                        result.NanCount = reader.ReadInt64();
                        break;
                    default:
                        reader.SkipField(field, 5);
                        break;
                }
            }
            return result;
        }

        static ParquetKeyValueMetadata[] ParseKeyValueList(
        ref ThriftCompactReader reader,
        CompactField field,
        ParquetReaderOptions options)
        {
            reader.RequireType(field, CompactType.List);
            var list = reader.ReadCollection();
            if (list.ElementType != CompactType.Struct)
                throw reader.Format("A key/value metadata list has an unexpected element type.");
            if (list.Count > options.MaximumKeyValueMetadataEntries)
                throw new ParquetLimitExceededException("Key/value metadata exceeds the configured entry limit.", reader.AbsoluteOffset);
            var result = new ParquetKeyValueMetadata[list.Count];
            for (var i = 0; i < result.Length; i++)
                result[i] = ParseKeyValue(ref reader);
            return result;
        }

        static ParquetKeyValueMetadata ParseKeyValue(ref ThriftCompactReader reader)
        {
            string? key = null;
            string? value = null;
            short previous = 0;
            ulong seen = 0;
            while (true)
            {
                var field = reader.ReadField(ref previous);
                if (field.Type == CompactType.Stop)
                    break;
                switch (field.Id)
                {
                    case 1:
                        Mark(ref seen, field.Id, ref reader);
                        reader.RequireType(field, CompactType.Binary);
                        key = reader.ReadString();
                        break;
                    case 2:
                        Mark(ref seen, field.Id, ref reader);
                        reader.RequireType(field, CompactType.Binary);
                        value = reader.ReadString();
                        break;
                    default:
                        reader.SkipField(field, 5);
                        break;
                }
            }
            if (key is null)
                throw reader.Format("A key/value metadata entry is missing its key.");
            return new ParquetKeyValueMetadata(key, value);
        }

        static ParquetColumnOrderKind[] ParseColumnOrders(
        ref ThriftCompactReader reader,
        CompactField field,
        ParquetReaderOptions options)
        {
            reader.RequireType(field, CompactType.List);
            var list = reader.ReadCollection();
            if (list.ElementType != CompactType.Struct)
                throw reader.Format("The column-order list has an unexpected element type.");
            if (list.Count > options.MaximumLeafColumns)
                throw new ParquetLimitExceededException("Column orders exceed the configured leaf-column limit.", reader.AbsoluteOffset);
            var result = new ParquetColumnOrderKind[list.Count];
            for (var i = 0; i < result.Length; i++)
            {
                short previous = 0;
                var member = reader.ReadField(ref previous);
                if (member.Type == CompactType.Stop || member.Type != CompactType.Struct)
                    throw reader.Format("A column-order union is empty or malformed.");
                reader.SkipValue(CompactType.Struct, 3, CompactBooleanEncoding.CollectionValue);
                result[i] = member.Id switch
                {
                    1 => ParquetColumnOrderKind.TypeDefined,
                    2 => ParquetColumnOrderKind.Ieee754Total,
                    3 => ParquetColumnOrderKind.Int96Timestamp,
                    _ => ParquetColumnOrderKind.Unknown,
                };
                var extra = reader.ReadField(ref previous);
                if (extra.Type != CompactType.Stop)
                    throw reader.Format("A column-order union contains multiple members.");
            }
            return result;
        }

        static int[] ParseInt32List(ref ThriftCompactReader reader, CompactField field, string description)
        {
            reader.RequireType(field, CompactType.List);
            var list = reader.ReadCollection();
            if (list.ElementType != CompactType.Int32)
                throw reader.Format($"The {description} list has an unexpected element type.");
            var result = new int[list.Count];
            for (var i = 0; i < result.Length; i++)
                result[i] = reader.ReadInt32();
            return result;
        }

        static string[] ParseStringList(ref ThriftCompactReader reader, CompactField field)
        {
            reader.RequireType(field, CompactType.List);
            var list = reader.ReadCollection();
            if (list.ElementType != CompactType.Binary)
                throw reader.Format("A string list has an unexpected element type.");
            var result = new string[list.Count];
            for (var i = 0; i < result.Length; i++)
                result[i] = reader.ReadString();
            return result;
        }

        static void Mark(ref ulong seen, int fieldId, ref ThriftCompactReader reader)
            => reader.MarkKnownField(ref seen, fieldId, CompactStructContext.Metadata);

        static ParquetFileMetadata BuildMetadata(
        FileMetadataWire wire,
        long sourceLength,
        long footerOffset,
        ParquetReaderOptions options)
        {
            var version = wire.Version ?? throw new InvalidOperationException("Validated file metadata has no version.");
            if (version is not 1 and not 2)
                throw new ParquetUnsupportedFeatureException("The file metadata version is unsupported.", footerOffset);
            var rowCount = RequireNonNegative(
                wire.RowCount ?? throw new InvalidOperationException("Validated file metadata has no row count."),
                "The file row count is negative.",
                footerOffset);

            var schema = BuildSchema(
                wire.Schema ?? throw new InvalidOperationException("Validated file metadata has no schema."),
                options,
                footerOffset);
            var columnOrders = wire.ColumnOrders ?? [];
            if (columnOrders.Length != 0 && columnOrders.Length != schema.Columns.Count)
                throw new ParquetFormatException("The column-order count does not match the primitive-leaf count.", byteOffset: footerOffset);

            var wireRowGroups = wire.RowGroups ??
                throw new InvalidOperationException("Validated file metadata has no row-group list.");
            var rowGroups = new ParquetRowGroup[wireRowGroups.Length];
            long globalRowOffset = 0;
            for (var rowGroupOrdinal = 0; rowGroupOrdinal < rowGroups.Length; rowGroupOrdinal++)
            {
                var rowGroup = wireRowGroups[rowGroupOrdinal];
                var groupRows = RequireNonNegative(
                    rowGroup.RowCount ?? throw new InvalidOperationException("A validated row group has no row count."),
                    "A row-group row count is negative.",
                    footerOffset);
                var totalBytes = RequireNonNegative(
                    rowGroup.TotalByteSize ?? throw new InvalidOperationException("A validated row group has no byte total."),
                    "A row-group byte total is negative.",
                    footerOffset);
                if (rowGroup.TotalCompressedSize is < 0)
                    throw new ParquetFormatException("A row-group compressed byte total is negative.", byteOffset: footerOffset);
                if (rowGroup.FileOffset is < 0)
                    throw new ParquetFormatException("A row-group file offset is negative.", byteOffset: footerOffset);

                var chunks = rowGroup.Columns ??
                    throw new InvalidOperationException("A validated row group has no column list.");
                if (chunks.Length != schema.Columns.Count)
                    throw new ParquetFormatException("A row-group column count does not match the schema.", byteOffset: footerOffset, rowGroupOrdinal: rowGroupOrdinal);
                var publicChunks = new ParquetColumnChunk[chunks.Length];
                for (var columnOrdinal = 0; columnOrdinal < chunks.Length; columnOrdinal++)
                {
                    publicChunks[columnOrdinal] = BuildColumnChunk(
                        chunks[columnOrdinal],
                        schema.Columns[columnOrdinal],
                        groupRows,
                        sourceLength,
                        footerOffset,
                        rowGroupOrdinal,
                        columnOrdinal);
                }

                rowGroups[rowGroupOrdinal] = new ParquetRowGroup(
                    rowGroupOrdinal,
                    globalRowOffset,
                    groupRows,
                    totalBytes,
                    rowGroup.TotalCompressedSize,
                    rowGroup.FileOffset,
                    publicChunks);
                try
                {
                    globalRowOffset = checked(globalRowOffset + groupRows);
                }
                catch (OverflowException exception)
                {
                    throw new ParquetFormatException("The aggregate row count overflows 64-bit arithmetic.", exception, footerOffset);
                }
            }
            if (globalRowOffset != rowCount)
                throw new ParquetFormatException("The file row count does not equal the row-group row-count sum.", byteOffset: footerOffset);

            return new ParquetFileMetadata(
                version,
                rowCount,
                schema,
                rowGroups,
                wire.CustomMetadata,
                wire.CreatedBy,
                columnOrders,
                wire.HasEncryptionAlgorithm || wire.HasFooterSigningKeyMetadata);
        }

        static ParquetSchema BuildSchema(SchemaElementWire[] wire, ParquetReaderOptions options, long footerOffset)
        {
            if (wire.Length == 0)
                throw new ParquetFormatException("The Parquet schema is empty.", byteOffset: footerOffset);
            var root = wire[0];
            if (root.TypeCode.HasValue || root.RepetitionCode is not (null or 0) || root.ChildCount is null or < 0)
                throw new ParquetFormatException("The root schema element is malformed.", byteOffset: footerOffset);

            var elements = new ParquetSchemaElement[wire.Length];
            var columnStorage = new ParquetColumn[wire.Length - 1];
            var columnCount = 0;
            var stack = new SchemaFrame[wire.Length];
            var stackCount = 0;
            var rootStatus = GetAnnotationStatus(root);
            elements[0] = BuildElement(root, 0, null, [], rootStatus, 0, 0);
            if (root.ChildCount.Value > 0)
                stack[stackCount++] = new SchemaFrame(0, root.ChildCount.Value);

            for (var i = 1; i < wire.Length; i++)
            {
                while (stackCount > 0 && stack[stackCount - 1].RemainingChildren == 0)
                    stackCount--;
                if (stackCount == 0)
                    throw new ParquetFormatException("The flattened schema contains an orphan element.", byteOffset: footerOffset);

                var parent = stack[stackCount - 1];
                stack[stackCount - 1] = new SchemaFrame(parent.ElementOrdinal, parent.RemainingChildren - 1);
                var current = wire[i];
                if (current.RepetitionCode is null)
                    throw new ParquetFormatException("A non-root schema element lacks repetition metadata.", byteOffset: footerOffset);
                var isLeaf = current.TypeCode.HasValue;
                if (isLeaf == current.ChildCount.HasValue || current.ChildCount is < 0)
                    throw new ParquetFormatException("A schema element does not describe exactly one leaf or group.", byteOffset: footerOffset);

                var parentElement = elements[parent.ElementOrdinal];
                var pathArray = new string[parentElement.Path.Count + 1];
                for (var p = 0; p < parentElement.Path.Count; p++)
                    pathArray[p] = parentElement.Path[p];
                pathArray[^1] = current.Name ??
                    throw new InvalidOperationException("A validated schema element has no name.");
                var path = Array.AsReadOnly(pathArray);

                var repetition = current.RepetitionCode.Value;
                var definitionLevel = parentElement.MaximumDefinitionLevel + (repetition is 1 or 2 ? 1 : 0);
                var repetitionLevel = parentElement.MaximumRepetitionLevel + (repetition == 2 ? 1 : 0);
                var status = GetAnnotationStatus(current);
                var element = BuildElement(current, i, parent.ElementOrdinal, path, status, definitionLevel, repetitionLevel);
                elements[i] = element;

                if (isLeaf)
                {
                    if (columnCount >= options.MaximumLeafColumns)
                        throw new ParquetLimitExceededException("The schema exceeds the configured leaf-column limit.", footerOffset);
                    var reason = GetUnsupportedReason(element);
                    columnStorage[columnCount] = new ParquetColumn(columnCount, element, reason is null, reason);
                    columnCount++;
                }
                else if (current.ChildCount is int childCount && childCount > 0)
                {
                    if (stackCount + 1 > options.MaximumThriftDepth)
                        throw new ParquetLimitExceededException("The schema exceeds the configured nesting depth.", footerOffset);
                    stack[stackCount++] = new SchemaFrame(i, childCount);
                }
            }

            while (stackCount > 0 && stack[stackCount - 1].RemainingChildren == 0)
                stackCount--;
            if (stackCount != 0)
                throw new ParquetFormatException("The flattened schema ends before all declared children.", byteOffset: footerOffset);
            if (columnCount != columnStorage.Length)
                Array.Resize(ref columnStorage, columnCount);
            return new ParquetSchema(elements, columnStorage);
        }

        static ParquetSchemaElement BuildElement(
        SchemaElementWire wire,
        int ordinal,
        int? parentOrdinal,
        IReadOnlyList<string> path,
        ParquetAnnotationStatus annotationStatus,
        int definitionLevel,
        int repetitionLevel) => new(
            ordinal,
            parentOrdinal,
            wire.Name ?? throw new InvalidOperationException("A validated schema element has no name."),
            path,
            wire.TypeCode,
            wire.RepetitionCode,
            wire.TypeLength,
            wire.ChildCount,
            wire.ConvertedTypeCode,
            wire.Scale,
            wire.Precision,
            wire.FieldId,
            wire.LogicalAnnotation,
            annotationStatus,
            definitionLevel,
            repetitionLevel);

        static ParquetAnnotationStatus GetAnnotationStatus(SchemaElementWire element)
        {
            var modern = element.LogicalAnnotation;
            var legacy = element.ConvertedTypeCode;
            if (!IsAnnotationStructurallyValid(element))
                return ParquetAnnotationStatus.Invalid;
            if (modern is null && legacy is null)
                return ParquetAnnotationStatus.None;
            if (modern is null)
                return ParquetAnnotationStatus.LegacyOnly;
            if (legacy is null)
                return ParquetAnnotationStatus.ModernOnly;
            return AreAnnotationsConsistent(modern, legacy.Value, element.Scale, element.Precision)
                ? ParquetAnnotationStatus.Consistent : ParquetAnnotationStatus.Conflict;
        }

        static bool IsAnnotationStructurallyValid(SchemaElementWire element)
        {
            var modern = element.LogicalAnnotation;
            if (modern?.Kind == ParquetLogicalTypeKind.Decimal)
            {
                if (modern.Precision is null or <= 0 || modern.Scale is null or < 0 || modern.Scale > modern.Precision)
                    return false;
                if (element.Scale != modern.Scale || element.Precision != modern.Precision)
                    return false;
            }
            if (element.ConvertedTypeCode == (int)ParquetConvertedType.Decimal &&
                (element.Precision is null or <= 0 || element.Scale is null or < 0 || element.Scale > element.Precision))
                return false;
            if (modern?.Kind == ParquetLogicalTypeKind.Integer && modern.IntegerBitWidth is not (8 or 16 or 32 or 64))
                return false;
            return true;
        }

        static bool AreAnnotationsConsistent(
        ParquetLogicalAnnotation modern,
        int legacyCode,
        int? scale,
        int? precision)
        {
            if (!Enum.IsDefined(typeof(ParquetConvertedType), legacyCode) || modern.Kind is null)
                return false;
            var legacy = (ParquetConvertedType)legacyCode;
            return modern.Kind switch
            {
                ParquetLogicalTypeKind.String => legacy == ParquetConvertedType.Utf8,
                ParquetLogicalTypeKind.Map => legacy == ParquetConvertedType.Map,
                ParquetLogicalTypeKind.List => legacy == ParquetConvertedType.List,
                ParquetLogicalTypeKind.Enum => legacy == ParquetConvertedType.Enum,
                ParquetLogicalTypeKind.Decimal => legacy == ParquetConvertedType.Decimal &&
                    modern.Scale == scale && modern.Precision == precision,
                ParquetLogicalTypeKind.Date => legacy == ParquetConvertedType.Date,
                ParquetLogicalTypeKind.Time => modern.TimeUnit switch
                {
                    ParquetTimeUnit.Milliseconds => legacy == ParquetConvertedType.TimeMilliseconds,
                    ParquetTimeUnit.Microseconds => legacy == ParquetConvertedType.TimeMicroseconds,
                    _ => false,
                },
                ParquetLogicalTypeKind.Timestamp => modern.TimeUnit switch
                {
                    ParquetTimeUnit.Milliseconds => legacy == ParquetConvertedType.TimestampMilliseconds,
                    ParquetTimeUnit.Microseconds => legacy == ParquetConvertedType.TimestampMicroseconds,
                    _ => false,
                },
                ParquetLogicalTypeKind.Integer => IntegerAnnotationMatches(modern, legacy),
                ParquetLogicalTypeKind.Json => legacy == ParquetConvertedType.Json,
                ParquetLogicalTypeKind.Bson => legacy == ParquetConvertedType.Bson,
                _ => false,
            };
        }

        static bool IntegerAnnotationMatches(ParquetLogicalAnnotation modern, ParquetConvertedType legacy)
        {
            if (modern.IntegerBitWidth is not int bitWidth || modern.IsIntegerSigned is not bool signed)
                return false;
            var expected = bitWidth switch
            {
                8 => signed ? ParquetConvertedType.Int8 : ParquetConvertedType.UInt8,
                16 => signed ? ParquetConvertedType.Int16 : ParquetConvertedType.UInt16,
                32 => signed ? ParquetConvertedType.Int32 : ParquetConvertedType.UInt32,
                64 => signed ? ParquetConvertedType.Int64 : ParquetConvertedType.UInt64,
                _ => (ParquetConvertedType)(-1),
            };
            return legacy == expected;
        }

        static string? GetUnsupportedReason(ParquetSchemaElement element)
        {
            if (element.ParentOrdinal != 0)
                return "Nested leaves are outside the Core 0.1 profile.";
            if (element.Repetition is not (ParquetRepetition.Required or ParquetRepetition.Optional))
                return "The repetition mode is outside the Core 0.1 profile.";
            if (element.MaximumRepetitionLevel != 0)
                return "Repeated values are outside the Core 0.1 profile.";
            if (element.PhysicalType is null)
                return "The physical type is unknown.";
            if (element.PhysicalType == ParquetPhysicalType.Int96)
                return "INT96 is outside the Core 0.1 profile.";
            if (element.PhysicalType == ParquetPhysicalType.FixedLengthByteArray && element.TypeLength is null or <= 0)
                return "The fixed-length byte width is missing or invalid.";
            return null;
        }

        static ParquetColumnChunk BuildColumnChunk(
        ColumnChunkWire chunk,
        ParquetColumn column,
        long rowCount,
        long sourceLength,
        long footerOffset,
        int rowGroupOrdinal,
        int columnOrdinal)
        {
            var metadata = chunk.Metadata ??
                throw new InvalidOperationException("A validated column chunk has no metadata.");
            var fileOffset = RequireNonNegative(
                chunk.FileOffset ?? throw new InvalidOperationException("A validated column chunk has no file offset."),
                "A column-chunk file offset is negative.",
                footerOffset);
            var valueCount = RequireNonNegative(
                metadata.ValueCount ?? throw new InvalidOperationException("Validated column metadata has no value count."),
                "A column-chunk value count is negative.",
                footerOffset);
            var uncompressed = RequireNonNegative(
                metadata.TotalUncompressedSize ?? throw new InvalidOperationException("Validated column metadata has no uncompressed size."),
                "A column-chunk uncompressed size is negative.",
                footerOffset);
            var compressed = RequireNonNegative(
                metadata.TotalCompressedSize ?? throw new InvalidOperationException("Validated column metadata has no compressed size."),
                "A column-chunk compressed size is negative.",
                footerOffset);
            var dataOffset = RequireNonNegative(
                metadata.DataPageOffset ?? throw new InvalidOperationException("Validated column metadata has no data-page offset."),
                "A data-page offset is negative.",
                footerOffset);
            var path = metadata.Path ?? throw new InvalidOperationException("Validated column metadata has no path.");
            var typeCode = metadata.TypeCode ?? throw new InvalidOperationException("Validated column metadata has no type code.");
            var codecCode = metadata.CodecCode ?? throw new InvalidOperationException("Validated column metadata has no codec code.");
            var encodingCodes = metadata.EncodingCodes ??
                throw new InvalidOperationException("Validated column metadata has no encoding list.");
            if (metadata.DictionaryPageOffset is < 0 || metadata.IndexPageOffset is < 0 || metadata.BloomFilterOffset is < 0)
                throw new ParquetFormatException("A column-chunk auxiliary offset is negative.", byteOffset: footerOffset, rowGroupOrdinal: rowGroupOrdinal, columnOrdinal: columnOrdinal);
            ValidatePair(chunk.OffsetIndexOffset, chunk.OffsetIndexLength, "offset index", sourceLength, footerOffset, rowGroupOrdinal, columnOrdinal);
            ValidatePair(chunk.ColumnIndexOffset, chunk.ColumnIndexLength, "column index", sourceLength, footerOffset, rowGroupOrdinal, columnOrdinal);
            if (metadata.BloomFilterLength is < 0)
                throw new ParquetFormatException("A bloom-filter length is negative.", byteOffset: footerOffset, rowGroupOrdinal: rowGroupOrdinal, columnOrdinal: columnOrdinal);
            if (metadata.BloomFilterOffset.HasValue && metadata.BloomFilterLength.HasValue)
                ValidateRange(metadata.BloomFilterOffset.Value, metadata.BloomFilterLength.Value, sourceLength, footerOffset, "bloom filter", rowGroupOrdinal, columnOrdinal);

            if (!path.SequenceEqual(column.Path, StringComparer.Ordinal))
                throw new ParquetFormatException("A column-chunk path does not match the schema leaf order.", byteOffset: footerOffset, rowGroupOrdinal: rowGroupOrdinal, columnOrdinal: columnOrdinal);
            if (metadata.TypeCode != column.SchemaElement.PhysicalTypeCode)
                throw new ParquetFormatException("A column-chunk physical type does not match the schema.", byteOffset: footerOffset, rowGroupOrdinal: rowGroupOrdinal, columnOrdinal: columnOrdinal);
            if (column.IsReadable && valueCount != rowCount)
                throw new ParquetFormatException("A flat column-chunk value count does not match its row group.", byteOffset: footerOffset, rowGroupOrdinal: rowGroupOrdinal, columnOrdinal: columnOrdinal);

            if (chunk.FilePath is null)
            {
                var firstPage = metadata.DictionaryPageOffset.HasValue
                    ? Math.Min(metadata.DictionaryPageOffset.Value, dataOffset)
                    : dataOffset;
                ValidateRange(firstPage, compressed, sourceLength, footerOffset, "column chunk", rowGroupOrdinal, columnOrdinal);
            }

            ParquetStatistics? statistics = null;
            if (metadata.Statistics is { } stats)
            {
                if (stats.NullCount is < 0 || stats.DistinctCount is < 0 || stats.NanCount is < 0)
                    throw new ParquetFormatException("Column statistics contain a negative count.", byteOffset: footerOffset, rowGroupOrdinal: rowGroupOrdinal, columnOrdinal: columnOrdinal);
                statistics = new ParquetStatistics(
                    stats.LegacyMinimum,
                    stats.LegacyMaximum,
                    stats.Minimum,
                    stats.Maximum,
                    stats.NullCount,
                    stats.DistinctCount,
                    stats.IsMinimumExact,
                    stats.IsMaximumExact,
                    stats.NanCount);
            }

            return new ParquetColumnChunk(
                columnOrdinal,
                column.Path,
                chunk.FilePath,
                fileOffset,
                typeCode,
                codecCode,
                encodingCodes,
                valueCount,
                uncompressed,
                compressed,
                dataOffset,
                metadata.DictionaryPageOffset,
                metadata.IndexPageOffset,
                chunk.OffsetIndexOffset,
                chunk.OffsetIndexLength,
                chunk.ColumnIndexOffset,
                chunk.ColumnIndexLength,
                metadata.BloomFilterOffset,
                metadata.BloomFilterLength,
                statistics,
                Array.AsReadOnly(metadata.CustomMetadata),
                chunk.HasCryptoMetadata,
                chunk.HasEncryptedMetadata);
        }

        static void ValidatePair(
        long? offset,
        int? length,
        string description,
        long sourceLength,
        long footerOffset,
        int rowGroupOrdinal,
        int columnOrdinal)
        {
            if (offset.HasValue != length.HasValue)
                throw new ParquetFormatException($"The {description} offset and length are not both present.", byteOffset: footerOffset, rowGroupOrdinal: rowGroupOrdinal, columnOrdinal: columnOrdinal);
            if (offset is long actualOffset && length is int actualLength)
                ValidateRange(actualOffset, actualLength, sourceLength, footerOffset, description, rowGroupOrdinal, columnOrdinal);
        }

        static void ValidateRange(
        long offset,
        long length,
        long sourceLength,
        long footerOffset,
        string description,
        int rowGroupOrdinal,
        int columnOrdinal)
        {
            if (offset < 0 || length < 0 || offset > sourceLength || length > sourceLength - offset)
                throw new ParquetFormatException($"The {description} range lies outside the input.", byteOffset: footerOffset, rowGroupOrdinal: rowGroupOrdinal, columnOrdinal: columnOrdinal);
            if (offset + length > footerOffset)
                throw new ParquetFormatException($"The {description} range overlaps the footer.", byteOffset: footerOffset, rowGroupOrdinal: rowGroupOrdinal, columnOrdinal: columnOrdinal);
        }

        static long RequireNonNegative(long value, string message, long offset)
        {
            if (value < 0)
                throw new ParquetFormatException(message, byteOffset: offset);
            return value;
        }
    }

    private readonly record struct ParsedTimeUnit(int Discriminator, ParquetTimeUnit? Unit);
    private readonly record struct SchemaFrame(int ElementOrdinal, int RemainingChildren);
}
