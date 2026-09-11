namespace Lokad.Parquet.Internal;

internal static class ParquetFooterParser
{
    public static ParquetFileMetadata Parse(
        ReadOnlySpan<byte> footer,
        long footerOffset,
        long sourceLength,
        ParquetReaderOptions options,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var reader = new ThriftCompactReader(footer, footerOffset, options, cancellationToken);
        try
        {
            var wire = ParseFileMetadata(ref reader, options);
            cancellationToken.ThrowIfCancellationRequested();
            if (!reader.IsAtEnd)
                throw reader.Format("Trailing bytes follow the Parquet file metadata.");
            return BuildMetadata(wire, sourceLength, footerOffset, options, cancellationToken);
        }
        catch (ThriftTruncatedException exception)
        {
            throw new ParquetFormatException("The Parquet footer contains truncated Thrift metadata.", ParquetErrorLocation.AtOffset(exception.ByteOffset));
        }

        static FileMetadataWire ParseFileMetadata(ref ThriftCompactReader reader, ParquetReaderOptions options)
        {
            reader.RequireDepth(1);
            var result = new FileMetadataWire();
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
                        result.CustomMetadata = ParseKeyValueList(ref reader, field, options, 1);
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
            reader.RequireDepth(1);
            reader.RequireType(field, CompactType.List);
            var list = reader.ReadCollection();
            if (list.ElementType != CompactType.Struct)
                throw reader.Format("The schema list has an unexpected element type.");
            if (list.Count == 0)
                throw reader.Format("The Parquet schema is empty.");
            if (list.Count > options.MaximumSchemaElements)
                throw new ParquetLimitExceededException("The schema exceeds the configured element limit.", ParquetErrorLocation.AtOffset(reader.AbsoluteOffset));

            reader.RequireCountFitsRemaining(list.Count);
            var result = new SchemaElementWire[list.Count];
            for (var i = 0; i < result.Length; i++)
            {
                reader.ObserveCancellation(i);
                result[i] = ParseSchemaElement(ref reader);
            }
            return result;
        }

        static SchemaElementWire ParseSchemaElement(ref ThriftCompactReader reader)
        {
            reader.RequireDepth(2);
            var result = new SchemaElementWire();
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
            reader.RequireDepth(3);
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
                        Enum.IsDefined((ParquetLogicalTypeKind)discriminator)
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
            reader.RequireDepth(4);
            int? scale = null;
            int? precision = null;
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
            reader.RequireDepth(4);
            bool? adjusted = null;
            int? unitDiscriminator = null;
            ParquetTimeUnit? unit = null;
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
            reader.RequireDepth(5);
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
            reader.RequireDepth(4);
            int? bitWidth = null;
            bool? signed = null;
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
            reader.RequireDepth(1);
            reader.RequireType(field, CompactType.List);
            var list = reader.ReadCollection();
            if (list.ElementType != CompactType.Struct)
                throw reader.Format("The row-group list has an unexpected element type.");
            if (list.Count > options.MaximumRowGroups)
                throw new ParquetLimitExceededException("The file exceeds the configured row-group limit.", ParquetErrorLocation.AtOffset(reader.AbsoluteOffset));
            reader.RequireCountFitsRemaining(list.Count);
            var result = new RowGroupWire[list.Count];
            for (var i = 0; i < result.Length; i++)
            {
                reader.ObserveCancellation(i);
                result[i] = ParseRowGroup(ref reader, options);
            }
            return result;
        }

        static RowGroupWire ParseRowGroup(ref ThriftCompactReader reader, ParquetReaderOptions options)
        {
            reader.RequireDepth(2);
            var result = new RowGroupWire();
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
            reader.RequireDepth(2);
            reader.RequireType(field, CompactType.List);
            var list = reader.ReadCollection();
            if (list.ElementType != CompactType.Struct)
                throw reader.Format("The column-chunk list has an unexpected element type.");
            if (list.Count > options.MaximumLeafColumns)
                throw new ParquetLimitExceededException("A row group exceeds the configured leaf-column limit.", ParquetErrorLocation.AtOffset(reader.AbsoluteOffset));
            reader.RequireCountFitsRemaining(list.Count);
            var result = new ColumnChunkWire[list.Count];
            for (var i = 0; i < result.Length; i++)
            {
                reader.ObserveCancellation(i);
                result[i] = ParseColumnChunk(ref reader, options);
            }
            return result;
        }

        static ColumnChunkWire ParseColumnChunk(ref ThriftCompactReader reader, ParquetReaderOptions options)
        {
            reader.RequireDepth(3);
            var result = new ColumnChunkWire();
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
            reader.RequireDepth(4);
            var result = new ColumnMetadataWire();
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
                        result.CustomMetadata = ParseKeyValueList(ref reader, field, options, 4);
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
            reader.RequireDepth(5);
            var result = new StatisticsWire();
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
        ParquetReaderOptions options,
        int depth)
        {
            reader.RequireDepth(depth);
            reader.RequireType(field, CompactType.List);
            var list = reader.ReadCollection();
            if (list.ElementType != CompactType.Struct)
                throw reader.Format("A key/value metadata list has an unexpected element type.");
            if (list.Count > options.MaximumKeyValueMetadataEntries)
                throw new ParquetLimitExceededException("Key/value metadata exceeds the configured entry limit.", ParquetErrorLocation.AtOffset(reader.AbsoluteOffset));
            reader.RequireCountFitsRemaining(list.Count);
            var result = new ParquetKeyValueMetadata[list.Count];
            for (var i = 0; i < result.Length; i++)
            {
                reader.ObserveCancellation(i);
                result[i] = ParseKeyValue(ref reader, depth + 1);
            }
            return result;
        }

        static ParquetKeyValueMetadata ParseKeyValue(ref ThriftCompactReader reader, int depth)
        {
            reader.RequireDepth(depth);
            string? key = null;
            string? value = null;
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
                        reader.SkipField(field, depth);
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
            reader.RequireDepth(1);
            reader.RequireType(field, CompactType.List);
            var list = reader.ReadCollection();
            if (list.ElementType != CompactType.Struct)
                throw reader.Format("The column-order list has an unexpected element type.");
            if (list.Count > options.MaximumLeafColumns)
                throw new ParquetLimitExceededException("Column orders exceed the configured leaf-column limit.", ParquetErrorLocation.AtOffset(reader.AbsoluteOffset));
            reader.RequireCountFitsRemaining(list.Count);
            var result = new ParquetColumnOrderKind[list.Count];
            for (var i = 0; i < result.Length; i++)
            {
                reader.ObserveCancellation(i);
                short previous = 0;
                var member = reader.ReadField(ref previous);
                if (member.Type == CompactType.Stop || member.Type != CompactType.Struct)
                    throw reader.Format("A column-order union is empty or malformed.");
                reader.SkipValue(CompactType.Struct, 2, CompactBooleanEncoding.CollectionValue);
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
            reader.RequireDepth(4);
            reader.RequireType(field, CompactType.List);
            var list = reader.ReadCollection();
            if (list.ElementType != CompactType.Int32)
                throw reader.Format($"The {description} list has an unexpected element type.");
            reader.RequireCountFitsRemaining(list.Count);
            var result = new int[list.Count];
            for (var i = 0; i < result.Length; i++)
            {
                reader.ObserveCancellation(i);
                result[i] = reader.ReadInt32();
            }
            return result;
        }

        static string[] ParseStringList(ref ThriftCompactReader reader, CompactField field)
        {
            reader.RequireDepth(4);
            reader.RequireType(field, CompactType.List);
            var list = reader.ReadCollection();
            if (list.ElementType != CompactType.Binary)
                throw reader.Format("A string list has an unexpected element type.");
            reader.RequireCountFitsRemaining(list.Count);
            var result = new string[list.Count];
            for (var i = 0; i < result.Length; i++)
            {
                reader.ObserveCancellation(i);
                result[i] = reader.ReadString();
            }
            return result;
        }

        static void Mark(ref ulong seen, int fieldId, ref ThriftCompactReader reader)
            => reader.MarkKnownField(ref seen, fieldId, CompactStructContext.Metadata);

        static ParquetFileMetadata BuildMetadata(
        FileMetadataWire wire,
        long sourceLength,
        long footerOffset,
        ParquetReaderOptions options,
        CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var version = wire.Version ?? throw new InvalidOperationException("Validated file metadata has no version.");
            if (version is not 1 and not 2)
                throw new ParquetUnsupportedFeatureException("The file metadata version is unsupported.", ParquetErrorLocation.AtOffset(footerOffset));
            var rowCount = RequireNonNegative(
                wire.RowCount ?? throw new InvalidOperationException("Validated file metadata has no row count."),
                "The file row count is negative.",
                footerOffset);

            var schema = BuildSchema(
                wire.Schema ?? throw new InvalidOperationException("Validated file metadata has no schema."),
                options,
                footerOffset,
                cancellationToken);
            var columnOrders = wire.ColumnOrders ?? [];
            if (columnOrders.Length != 0 && columnOrders.Length != schema.Columns.Count)
                throw new ParquetFormatException("The column-order count does not match the primitive-leaf count.", ParquetErrorLocation.AtOffset(footerOffset));

            var wireRowGroups = wire.RowGroups ??
                throw new InvalidOperationException("Validated file metadata has no row-group list.");
            var rowGroups = new ParquetRowGroup[wireRowGroups.Length];
            long globalRowOffset = 0;
            for (var rowGroupOrdinal = 0; rowGroupOrdinal < rowGroups.Length; rowGroupOrdinal++)
            {
                if ((rowGroupOrdinal & 15) == 0)
                    cancellationToken.ThrowIfCancellationRequested();
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
                    throw new ParquetFormatException("A row-group compressed byte total is negative.", ParquetErrorLocation.AtOffset(footerOffset));
                if (rowGroup.FileOffset is < 0)
                    throw new ParquetFormatException("A row-group file offset is negative.", ParquetErrorLocation.AtOffset(footerOffset));

                var chunks = rowGroup.Columns ??
                    throw new InvalidOperationException("A validated row group has no column list.");
                if (chunks.Length != schema.Columns.Count)
                    throw new ParquetFormatException("A row-group column count does not match the schema.", ParquetErrorLocation.AtRowGroup(footerOffset, rowGroupOrdinal));
                var publicChunks = new ParquetColumnChunk[chunks.Length];
                for (var columnOrdinal = 0; columnOrdinal < chunks.Length; columnOrdinal++)
                {
                    if ((columnOrdinal & 1023) == 0)
                        cancellationToken.ThrowIfCancellationRequested();
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
                    throw new ParquetFormatException("The aggregate row count overflows 64-bit arithmetic.", exception, ParquetErrorLocation.AtOffset(footerOffset));
                }
            }
            if (globalRowOffset != rowCount)
                throw new ParquetFormatException("The file row count does not equal the row-group row-count sum.", ParquetErrorLocation.AtOffset(footerOffset));

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

        static ParquetSchema BuildSchema(SchemaElementWire[] wire, ParquetReaderOptions options, long footerOffset, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (wire.Length == 0)
                throw new ParquetFormatException("The Parquet schema is empty.", ParquetErrorLocation.AtOffset(footerOffset));
            var root = wire[0];
            if (root.TypeCode.HasValue || root.RepetitionCode is not (null or 0) || root.ChildCount is null or < 0)
                throw new ParquetFormatException("The root schema element is malformed.", ParquetErrorLocation.AtOffset(footerOffset));

            var elements = new ParquetSchemaElement[wire.Length];
            var columnStorage = new ParquetColumn[wire.Length - 1];
            var columnCount = 0;
            var stack = new SchemaFrame[Math.Min(wire.Length, options.MaximumThriftDepth + 1)];
            var stackCount = 0;
            var rootAnnotation = ResolveAnnotation(root);
            elements[0] = BuildElement(root, 0, null, [], rootAnnotation, 0, 0);
            if (root.ChildCount.Value > 0)
                stack[stackCount++] = new SchemaFrame(0, root.ChildCount.Value);

            for (var i = 1; i < wire.Length; i++)
            {
                if ((i & 63) == 0)
                    cancellationToken.ThrowIfCancellationRequested();
                while (stackCount > 0 && stack[stackCount - 1].RemainingChildren == 0)
                    stackCount--;
                if (stackCount == 0)
                    throw new ParquetFormatException("The flattened schema contains an orphan element.", ParquetErrorLocation.AtOffset(footerOffset));

                var parent = stack[stackCount - 1];
                stack[stackCount - 1] = new SchemaFrame(parent.ElementOrdinal, parent.RemainingChildren - 1);
                var current = wire[i];
                if (current.RepetitionCode is null)
                    throw new ParquetFormatException("A non-root schema element lacks repetition metadata.", ParquetErrorLocation.AtOffset(footerOffset));
                var isLeaf = current.TypeCode.HasValue;
                if (isLeaf == current.ChildCount.HasValue || current.ChildCount is < 0)
                    throw new ParquetFormatException("A schema element does not describe exactly one leaf or group.", ParquetErrorLocation.AtOffset(footerOffset));

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
                var annotation = ResolveAnnotation(current);
                var element = BuildElement(current, i, parent.ElementOrdinal, path, annotation, definitionLevel, repetitionLevel);
                elements[i] = element;

                if (isLeaf &&
                    element.PhysicalType == ParquetPhysicalType.FixedLengthByteArray &&
                    element.TypeLength is null or <= 0)
                    throw new ParquetFormatException("A FIXED_LEN_BYTE_ARRAY schema element has a missing or invalid type length.", ParquetErrorLocation.AtOffset(footerOffset));

                if (isLeaf)
                {
                    if (columnCount >= options.MaximumLeafColumns)
                        throw new ParquetLimitExceededException("The schema exceeds the configured leaf-column limit.", ParquetErrorLocation.AtOffset(footerOffset));
                    var reason = GetUnsupportedReason(element);
                    columnStorage[columnCount] = new ParquetColumn(columnCount, element, reason is null, reason);
                    columnCount++;
                }
                else if (current.ChildCount is int childCount && childCount > 0)
                {
                    // Schema-tree depth deliberately shares MaximumThriftDepth to bound nesting without a separate option.
                    if (stackCount + 1 > options.MaximumThriftDepth)
                        throw new ParquetLimitExceededException("The schema exceeds the configured nesting depth.", ParquetErrorLocation.AtOffset(footerOffset));
                    stack[stackCount++] = new SchemaFrame(i, childCount);
                }
            }

            while (stackCount > 0 && stack[stackCount - 1].RemainingChildren == 0)
                stackCount--;
            if (stackCount != 0)
                throw new ParquetFormatException("The flattened schema ends before all declared children.", ParquetErrorLocation.AtOffset(footerOffset));
            if (columnCount != columnStorage.Length)
                Array.Resize(ref columnStorage, columnCount);
            return new ParquetSchema(elements, columnStorage);
        }

        static ParquetSchemaElement BuildElement(
        SchemaElementWire wire,
        int ordinal,
        int? parentOrdinal,
        IReadOnlyList<string> path,
        ResolvedAnnotation annotation,
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
            annotation.Status,
            annotation.Semantic,
            definitionLevel,
            repetitionLevel);

        static ResolvedAnnotation ResolveAnnotation(SchemaElementWire element)
        {
            var modern = element.LogicalAnnotation;
            var legacy = element.ConvertedTypeCode;
            if (!IsAnnotationStructurallyValid(element))
                return new(ParquetAnnotationStatus.Invalid, null);
            if (modern is null && legacy is null)
                return new(ParquetAnnotationStatus.None, null);

            var modernSemantic = modern is null ? null : FromModern(modern);
            var legacySemantic = legacy is null ? null : FromLegacy(legacy.Value, element.Scale, element.Precision);
            ParquetAnnotationStatus status;
            ParquetSemanticAnnotation? semantic;
            if (modern is null)
            {
                status = ParquetAnnotationStatus.LegacyOnly;
                semantic = legacySemantic;
            }
            else if (legacy is null)
            {
                status = ParquetAnnotationStatus.ModernOnly;
                semantic = modernSemantic;
            }
            else if (modernSemantic is not null && legacySemantic is not null &&
                     AreSemanticallyEqual(modernSemantic, legacySemantic))
            {
                status = ParquetAnnotationStatus.Consistent;
                semantic = modernSemantic;
            }
            else
            {
                return new(ParquetAnnotationStatus.Conflict, null);
            }

            return semantic is not null && !IsPhysicallyCompatible(element, semantic)
                ? new(ParquetAnnotationStatus.Invalid, null)
                : new(status, semantic);
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

        static ParquetSemanticAnnotation? FromModern(ParquetLogicalAnnotation annotation)
        {
            if (annotation.Kind is not { } kind)
                return null;
            var semanticKind = kind switch
            {
                ParquetLogicalTypeKind.String => ParquetSemanticTypeKind.String,
                ParquetLogicalTypeKind.Map => ParquetSemanticTypeKind.Map,
                ParquetLogicalTypeKind.List => ParquetSemanticTypeKind.List,
                ParquetLogicalTypeKind.Enum => ParquetSemanticTypeKind.Enum,
                ParquetLogicalTypeKind.Decimal => ParquetSemanticTypeKind.Decimal,
                ParquetLogicalTypeKind.Date => ParquetSemanticTypeKind.Date,
                ParquetLogicalTypeKind.Time => ParquetSemanticTypeKind.Time,
                ParquetLogicalTypeKind.Timestamp => ParquetSemanticTypeKind.Timestamp,
                ParquetLogicalTypeKind.Integer => ParquetSemanticTypeKind.Integer,
                ParquetLogicalTypeKind.Unknown => ParquetSemanticTypeKind.Unknown,
                ParquetLogicalTypeKind.Json => ParquetSemanticTypeKind.Json,
                ParquetLogicalTypeKind.Bson => ParquetSemanticTypeKind.Bson,
                ParquetLogicalTypeKind.Uuid => ParquetSemanticTypeKind.Uuid,
                ParquetLogicalTypeKind.Float16 => ParquetSemanticTypeKind.Float16,
                _ => (ParquetSemanticTypeKind?)null,
            };
            if (semanticKind is null ||
                semanticKind is ParquetSemanticTypeKind.Time or ParquetSemanticTypeKind.Timestamp && annotation.TimeUnit is null)
                return null;
            return new ParquetSemanticAnnotation(
                semanticKind.Value,
                annotation.Scale,
                annotation.Precision,
                annotation.TimeUnit,
                annotation.IsAdjustedToUtc,
                annotation.IntegerBitWidth,
                annotation.IsIntegerSigned);
        }

        static ParquetSemanticAnnotation? FromLegacy(int legacyCode, int? scale, int? precision)
        {
            if ((uint)legacyCode > (uint)ParquetConvertedType.Interval)
                return null;
            var legacy = (ParquetConvertedType)legacyCode;
            var kind = legacy switch
            {
                ParquetConvertedType.Utf8 => ParquetSemanticTypeKind.String,
                ParquetConvertedType.Map => ParquetSemanticTypeKind.Map,
                ParquetConvertedType.MapKeyValue => ParquetSemanticTypeKind.MapKeyValue,
                ParquetConvertedType.List => ParquetSemanticTypeKind.List,
                ParquetConvertedType.Enum => ParquetSemanticTypeKind.Enum,
                ParquetConvertedType.Decimal => ParquetSemanticTypeKind.Decimal,
                ParquetConvertedType.Date => ParquetSemanticTypeKind.Date,
                ParquetConvertedType.TimeMilliseconds or ParquetConvertedType.TimeMicroseconds =>
                    ParquetSemanticTypeKind.Time,
                ParquetConvertedType.TimestampMilliseconds or ParquetConvertedType.TimestampMicroseconds =>
                    ParquetSemanticTypeKind.Timestamp,
                ParquetConvertedType.UInt8 or ParquetConvertedType.UInt16 or ParquetConvertedType.UInt32 or
                    ParquetConvertedType.UInt64 or ParquetConvertedType.Int8 or ParquetConvertedType.Int16 or
                    ParquetConvertedType.Int32 or ParquetConvertedType.Int64 => ParquetSemanticTypeKind.Integer,
                ParquetConvertedType.Json => ParquetSemanticTypeKind.Json,
                ParquetConvertedType.Bson => ParquetSemanticTypeKind.Bson,
                ParquetConvertedType.Interval => ParquetSemanticTypeKind.Interval,
                _ => throw new ArgumentOutOfRangeException(nameof(legacyCode)),
            };
            var timeUnit = legacy switch
            {
                ParquetConvertedType.TimeMilliseconds or ParquetConvertedType.TimestampMilliseconds =>
                    ParquetTimeUnit.Milliseconds,
                ParquetConvertedType.TimeMicroseconds or ParquetConvertedType.TimestampMicroseconds =>
                    ParquetTimeUnit.Microseconds,
                _ => (ParquetTimeUnit?)null,
            };
            var integerWidth = legacy switch
            {
                ParquetConvertedType.UInt8 or ParquetConvertedType.Int8 => 8,
                ParquetConvertedType.UInt16 or ParquetConvertedType.Int16 => 16,
                ParquetConvertedType.UInt32 or ParquetConvertedType.Int32 => 32,
                ParquetConvertedType.UInt64 or ParquetConvertedType.Int64 => 64,
                _ => (int?)null,
            };
            var integerSigned = legacy switch
            {
                ParquetConvertedType.UInt8 or ParquetConvertedType.UInt16 or
                    ParquetConvertedType.UInt32 or ParquetConvertedType.UInt64 => false,
                ParquetConvertedType.Int8 or ParquetConvertedType.Int16 or
                    ParquetConvertedType.Int32 or ParquetConvertedType.Int64 => true,
                _ => (bool?)null,
            };
            return new ParquetSemanticAnnotation(
                kind,
                kind == ParquetSemanticTypeKind.Decimal ? scale : null,
                kind == ParquetSemanticTypeKind.Decimal ? precision : null,
                timeUnit,
                kind is ParquetSemanticTypeKind.Time or ParquetSemanticTypeKind.Timestamp ? true : null,
                integerWidth,
                integerSigned);
        }

        static bool AreSemanticallyEqual(ParquetSemanticAnnotation left, ParquetSemanticAnnotation right) =>
            left.Kind == right.Kind && left.Scale == right.Scale && left.Precision == right.Precision &&
            left.TimeUnit == right.TimeUnit && left.IsAdjustedToUtc == right.IsAdjustedToUtc &&
            left.IntegerBitWidth == right.IntegerBitWidth &&
            left.IsIntegerSigned == right.IsIntegerSigned;

        static bool IsPhysicallyCompatible(SchemaElementWire element, ParquetSemanticAnnotation annotation)
        {
            var physical = element.TypeCode is int typeCode && (uint)typeCode <= (uint)ParquetPhysicalType.FixedLengthByteArray
                ? (ParquetPhysicalType)typeCode : (ParquetPhysicalType?)null;
            return annotation.Kind switch
            {
                ParquetSemanticTypeKind.String or ParquetSemanticTypeKind.Enum or
                    ParquetSemanticTypeKind.Json or ParquetSemanticTypeKind.Bson =>
                    physical == ParquetPhysicalType.ByteArray,
                ParquetSemanticTypeKind.Decimal => IsValidDecimalPhysicalType(
                    physical, element.TypeLength, annotation.Precision),
                ParquetSemanticTypeKind.Date => physical == ParquetPhysicalType.Int32,
                ParquetSemanticTypeKind.Time => annotation.TimeUnit switch
                {
                    ParquetTimeUnit.Milliseconds => physical == ParquetPhysicalType.Int32,
                    ParquetTimeUnit.Microseconds or ParquetTimeUnit.Nanoseconds =>
                        physical == ParquetPhysicalType.Int64,
                    _ => false,
                },
                ParquetSemanticTypeKind.Timestamp => physical == ParquetPhysicalType.Int64,
                ParquetSemanticTypeKind.Integer => physical == (annotation.IntegerBitWidth == 64
                    ? ParquetPhysicalType.Int64 : ParquetPhysicalType.Int32),
                ParquetSemanticTypeKind.Uuid => physical == ParquetPhysicalType.FixedLengthByteArray &&
                    element.TypeLength == 16,
                ParquetSemanticTypeKind.Float16 => physical == ParquetPhysicalType.FixedLengthByteArray &&
                    element.TypeLength == 2,
                ParquetSemanticTypeKind.Interval => physical == ParquetPhysicalType.FixedLengthByteArray &&
                    element.TypeLength == 12,
                ParquetSemanticTypeKind.Map or ParquetSemanticTypeKind.MapKeyValue or
                    ParquetSemanticTypeKind.List => physical is null,
                ParquetSemanticTypeKind.Unknown => physical is not null,
                _ => false,
            };

            static bool IsValidDecimalPhysicalType(
                ParquetPhysicalType? physical,
                int? typeLength,
                int? precision)
            {
                if (precision is not > 0)
                    return false;
                return physical switch
                {
                    ParquetPhysicalType.Int32 => precision <= 9,
                    ParquetPhysicalType.Int64 => precision <= 18,
                    ParquetPhysicalType.ByteArray => true,
                    ParquetPhysicalType.FixedLengthByteArray when typeLength is > 0 =>
                        precision <= Math.Floor((8D * typeLength.Value - 1D) * Math.Log10(2D)),
                    _ => false,
                };
            }
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
                throw new ParquetFormatException("A column-chunk auxiliary offset is negative.", ParquetErrorLocation.AtChunk(footerOffset, rowGroupOrdinal, columnOrdinal));
            ValidatePair(chunk.OffsetIndexOffset, chunk.OffsetIndexLength, "offset index", sourceLength, footerOffset, rowGroupOrdinal, columnOrdinal);
            ValidatePair(chunk.ColumnIndexOffset, chunk.ColumnIndexLength, "column index", sourceLength, footerOffset, rowGroupOrdinal, columnOrdinal);
            if (metadata.BloomFilterLength is < 0)
                throw new ParquetFormatException("A bloom-filter length is negative.", ParquetErrorLocation.AtChunk(footerOffset, rowGroupOrdinal, columnOrdinal));
            if (metadata.BloomFilterLength.HasValue && !metadata.BloomFilterOffset.HasValue)
                throw new ParquetFormatException("A bloom-filter length without an offset is invalid.", ParquetErrorLocation.AtChunk(footerOffset, rowGroupOrdinal, columnOrdinal));
            if (metadata.BloomFilterOffset.HasValue && metadata.BloomFilterLength.HasValue)
                ValidateRange(metadata.BloomFilterOffset.Value, metadata.BloomFilterLength.Value, sourceLength, footerOffset, "bloom filter", rowGroupOrdinal, columnOrdinal);
            if (chunk.FilePath is null)
            {
                if (metadata.IndexPageOffset.HasValue)
                    ValidateOffset(metadata.IndexPageOffset.Value, sourceLength, footerOffset, "index page", rowGroupOrdinal, columnOrdinal);
                if (metadata.BloomFilterOffset.HasValue && !metadata.BloomFilterLength.HasValue)
                    ValidateOffset(metadata.BloomFilterOffset.Value, sourceLength, footerOffset, "bloom filter", rowGroupOrdinal, columnOrdinal);
            }

            if (!path.SequenceEqual(column.Path, StringComparer.Ordinal))
                throw new ParquetFormatException("A column-chunk path does not match the schema leaf order.", ParquetErrorLocation.AtChunk(footerOffset, rowGroupOrdinal, columnOrdinal));
            if (metadata.TypeCode != column.SchemaElement.PhysicalTypeCode)
                throw new ParquetFormatException("A column-chunk physical type does not match the schema.", ParquetErrorLocation.AtChunk(footerOffset, rowGroupOrdinal, columnOrdinal));
            if (column.IsReadable && valueCount != rowCount)
                throw new ParquetFormatException("A flat column-chunk value count does not match its row group.", ParquetErrorLocation.AtChunk(footerOffset, rowGroupOrdinal, columnOrdinal));

            if (chunk.FilePath is null)
            {
                var firstPage = metadata.DictionaryPageOffset.HasValue
                    ? Math.Min(metadata.DictionaryPageOffset.Value, dataOffset)
                    : dataOffset;
                ValidateRange(firstPage, compressed, sourceLength, footerOffset, "column chunk", rowGroupOrdinal, columnOrdinal);
                if (metadata.DictionaryPageOffset.HasValue)
                {
                    var dictionaryOffset = metadata.DictionaryPageOffset.Value;
                    if (dictionaryOffset < firstPage || dictionaryOffset >= firstPage + compressed)
                        throw new ParquetFormatException("A dictionary-page offset lies outside its column chunk.", ParquetErrorLocation.AtChunk(footerOffset, rowGroupOrdinal, columnOrdinal));
                }

                if (dataOffset < firstPage || dataOffset >= firstPage + compressed)
                    throw new ParquetFormatException("A data-page offset lies outside its column chunk.", ParquetErrorLocation.AtChunk(footerOffset, rowGroupOrdinal, columnOrdinal));
            }

            ParquetStatistics? statistics = null;
            if (metadata.Statistics is { } stats)
            {
                if (stats.NullCount is < 0 || stats.DistinctCount is < 0 || stats.NanCount is < 0)
                    throw new ParquetFormatException("Column statistics contain a negative count.", ParquetErrorLocation.AtChunk(footerOffset, rowGroupOrdinal, columnOrdinal));
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
                throw new ParquetFormatException($"The {description} offset and length are not both present.", ParquetErrorLocation.AtChunk(footerOffset, rowGroupOrdinal, columnOrdinal));
            if (offset is long actualOffset && length is int actualLength)
                ValidateRange(actualOffset, actualLength, sourceLength, footerOffset, description, rowGroupOrdinal, columnOrdinal);
        }

        static void ValidateOffset(
        long offset,
        long sourceLength,
        long footerOffset,
        string description,
        int rowGroupOrdinal,
        int columnOrdinal)
        {
            if (offset < 0 || offset >= sourceLength || offset >= footerOffset)
                throw new ParquetFormatException("The " + description + " offset lies outside the input.", ParquetErrorLocation.AtChunk(footerOffset, rowGroupOrdinal, columnOrdinal));
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
                throw new ParquetFormatException($"The {description} range lies outside the input.", ParquetErrorLocation.AtChunk(footerOffset, rowGroupOrdinal, columnOrdinal));
            if (offset + length > footerOffset)
                throw new ParquetFormatException($"The {description} range overlaps the footer.", ParquetErrorLocation.AtChunk(footerOffset, rowGroupOrdinal, columnOrdinal));
        }

        static long RequireNonNegative(long value, string message, long offset)
        {
            if (value < 0)
                throw new ParquetFormatException(message, ParquetErrorLocation.AtOffset(offset));
            return value;
        }
    }

    private readonly record struct ParsedTimeUnit(int Discriminator, ParquetTimeUnit? Unit);
    private readonly record struct ResolvedAnnotation(
        ParquetAnnotationStatus Status,
        ParquetSemanticAnnotation? Semantic);
    private readonly record struct SchemaFrame(int ElementOrdinal, int RemainingChildren);
}
