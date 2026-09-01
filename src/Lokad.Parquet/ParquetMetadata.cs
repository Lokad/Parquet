using System.Collections.ObjectModel;

namespace Lokad.Parquet;

/// <summary>Modern logical annotation retained from one schema element.</summary>
public sealed class ParquetLogicalAnnotation
{
    internal ParquetLogicalAnnotation(
        int discriminator,
        ParquetLogicalTypeKind? kind,
        int? scale,
        int? precision,
        int? timeUnitDiscriminator,
        ParquetTimeUnit? timeUnit,
        bool? isAdjustedToUtc,
        int? integerBitWidth,
        bool? isIntegerSigned)
    {
        Discriminator = discriminator;
        Kind = kind;
        Scale = scale;
        Precision = precision;
        TimeUnitDiscriminator = timeUnitDiscriminator;
        TimeUnit = timeUnit;
        IsAdjustedToUtc = isAdjustedToUtc;
        IntegerBitWidth = integerBitWidth;
        IsIntegerSigned = isIntegerSigned;
    }

    /// <summary>Gets the raw Thrift union field identifier.</summary>
    public int Discriminator { get; }

    /// <summary>Gets the known logical kind, or null for an unknown discriminator.</summary>
    public ParquetLogicalTypeKind? Kind { get; }

    /// <summary>Gets the decimal scale carried by the modern annotation.</summary>
    public int? Scale { get; }

    /// <summary>Gets the decimal precision carried by the modern annotation.</summary>
    public int? Precision { get; }

    /// <summary>Gets the raw time-unit union field identifier.</summary>
    public int? TimeUnitDiscriminator { get; }

    /// <summary>Gets the time or timestamp unit.</summary>
    public ParquetTimeUnit? TimeUnit { get; }

    /// <summary>Gets whether a time or timestamp is adjusted to UTC.</summary>
    public bool? IsAdjustedToUtc { get; }

    /// <summary>Gets the declared integer bit width.</summary>
    public int? IntegerBitWidth { get; }

    /// <summary>Gets whether the declared integer is signed.</summary>
    public bool? IsIntegerSigned { get; }
}

/// <summary>One element in the flattened, depth-first Parquet schema.</summary>
public sealed class ParquetSchemaElement
{
    internal ParquetSchemaElement(
        int ordinal,
        int? parentOrdinal,
        string name,
        IReadOnlyList<string> path,
        int? physicalTypeCode,
        int? repetitionCode,
        int? typeLength,
        int? childCount,
        int? convertedTypeCode,
        int? scale,
        int? precision,
        int? fieldId,
        ParquetLogicalAnnotation? logicalAnnotation,
        ParquetAnnotationStatus annotationStatus,
        int maximumDefinitionLevel,
        int maximumRepetitionLevel)
    {
        Ordinal = ordinal;
        ParentOrdinal = parentOrdinal;
        Name = name;
        Path = path;
        PhysicalTypeCode = physicalTypeCode;
        RepetitionCode = repetitionCode;
        TypeLength = typeLength;
        ChildCount = childCount;
        ConvertedTypeCode = convertedTypeCode;
        Scale = scale;
        Precision = precision;
        FieldId = fieldId;
        LogicalAnnotation = logicalAnnotation;
        AnnotationStatus = annotationStatus;
        MaximumDefinitionLevel = maximumDefinitionLevel;
        MaximumRepetitionLevel = maximumRepetitionLevel;
    }

    /// <summary>Gets this element's ordinal in the flattened schema.</summary>
    public int Ordinal { get; }

    /// <summary>Gets the parent element ordinal, or null for the root.</summary>
    public int? ParentOrdinal { get; }

    /// <summary>Gets the element name.</summary>
    public string Name { get; }

    /// <summary>Gets the path from the root, excluding the root name.</summary>
    public IReadOnlyList<string> Path { get; }

    /// <summary>Gets the raw physical-type enum value, or null for a group.</summary>
    public int? PhysicalTypeCode { get; }

    /// <summary>Gets the known physical type, or null for a group or unknown value.</summary>
    public ParquetPhysicalType? PhysicalType => PhysicalTypeCode is int code && Enum.IsDefined(typeof(ParquetPhysicalType), code)
        ? (ParquetPhysicalType)code : null;

    /// <summary>Gets the raw repetition enum value, or null for the root.</summary>
    public int? RepetitionCode { get; }

    /// <summary>Gets the known repetition mode, or null when absent or unknown.</summary>
    public ParquetRepetition? Repetition => RepetitionCode is int code && Enum.IsDefined(typeof(ParquetRepetition), code)
        ? (ParquetRepetition)code : null;

    /// <summary>Gets the declared type length.</summary>
    public int? TypeLength { get; }

    /// <summary>Gets the declared child count, or null for a leaf.</summary>
    public int? ChildCount { get; }

    /// <summary>Gets the raw legacy converted-type enum value.</summary>
    public int? ConvertedTypeCode { get; }

    /// <summary>Gets the known legacy converted type.</summary>
    public ParquetConvertedType? ConvertedType => ConvertedTypeCode is int code && Enum.IsDefined(typeof(ParquetConvertedType), code)
        ? (ParquetConvertedType)code : null;

    /// <summary>Gets the schema-element decimal scale.</summary>
    public int? Scale { get; }

    /// <summary>Gets the schema-element decimal precision.</summary>
    public int? Precision { get; }

    /// <summary>Gets the optional field identifier.</summary>
    public int? FieldId { get; }

    /// <summary>Gets the modern logical annotation.</summary>
    public ParquetLogicalAnnotation? LogicalAnnotation { get; }

    /// <summary>Gets the relationship between the modern and legacy annotations.</summary>
    public ParquetAnnotationStatus AnnotationStatus { get; }

    /// <summary>Gets the maximum definition level at this element.</summary>
    public int MaximumDefinitionLevel { get; }

    /// <summary>Gets the maximum repetition level at this element.</summary>
    public int MaximumRepetitionLevel { get; }

    /// <summary>Gets whether this element is a primitive leaf.</summary>
    public bool IsLeaf => PhysicalTypeCode.HasValue;
}

/// <summary>One primitive leaf column in file order.</summary>
public sealed class ParquetColumn
{
    internal ParquetColumn(int ordinal, ParquetSchemaElement schemaElement, bool isReadable, string? unsupportedReason)
    {
        Ordinal = ordinal;
        SchemaElement = schemaElement;
        IsReadable = isReadable;
        UnsupportedReason = unsupportedReason;
    }

    /// <summary>Gets the zero-based primitive-leaf ordinal.</summary>
    public int Ordinal { get; }

    /// <summary>Gets the corresponding schema element.</summary>
    public ParquetSchemaElement SchemaElement { get; }

    /// <summary>Gets the leaf name.</summary>
    public string Name => SchemaElement.Name;

    /// <summary>Gets the complete schema path.</summary>
    public IReadOnlyList<string> Path => SchemaElement.Path;

    /// <summary>Gets whether Core 0.1 can scan this leaf.</summary>
    public bool IsReadable { get; }

    /// <summary>Gets why this leaf is not readable, or null when readable.</summary>
    public string? UnsupportedReason { get; }
}

/// <summary>Immutable schema and primitive-leaf descriptors.</summary>
public sealed class ParquetSchema
{
    internal ParquetSchema(ParquetSchemaElement[] elements, ParquetColumn[] columns)
    {
        Elements = Array.AsReadOnly(elements);
        Columns = Array.AsReadOnly(columns);
    }

    /// <summary>Gets all schema elements in flattened depth-first order.</summary>
    public ReadOnlyCollection<ParquetSchemaElement> Elements { get; }

    /// <summary>Gets all primitive leaves in file order.</summary>
    public ReadOnlyCollection<ParquetColumn> Columns { get; }

    /// <summary>Gets an unambiguous top-level leaf by its exact case-sensitive name.</summary>
    /// <param name="name">The top-level leaf name.</param>
    /// <returns>The matching column descriptor.</returns>
    public ParquetColumn GetColumn(string name)
    {
        ArgumentNullException.ThrowIfNull(name);
        ParquetColumn? match = null;
        foreach (var column in Columns)
        {
            if (column.Path.Count != 1 || !string.Equals(column.Name, name, StringComparison.Ordinal))
                continue;
            if (match is not null)
                throw new ArgumentException($"The top-level column name '{name}' is ambiguous.", nameof(name));
            match = column;
        }
        return match ?? throw new ArgumentException($"No top-level column is named '{name}'.", nameof(name));
    }
}

/// <summary>Raw, untrusted statistics retained from a column chunk.</summary>
public sealed class ParquetStatistics
{
    internal ParquetStatistics(
        byte[]? legacyMinimum,
        byte[]? legacyMaximum,
        byte[]? minimum,
        byte[]? maximum,
        long? nullCount,
        long? distinctCount,
        bool? isMinimumExact,
        bool? isMaximumExact,
        long? nanCount)
    {
        LegacyMinimum = legacyMinimum;
        LegacyMaximum = legacyMaximum;
        Minimum = minimum;
        Maximum = maximum;
        NullCount = nullCount;
        DistinctCount = distinctCount;
        IsMinimumExact = isMinimumExact;
        IsMaximumExact = isMaximumExact;
        NanCount = nanCount;
    }

    /// <summary>Gets the deprecated signed-order minimum bytes.</summary>
    public ReadOnlyMemory<byte>? LegacyMinimum { get; }

    /// <summary>Gets the deprecated signed-order maximum bytes.</summary>
    public ReadOnlyMemory<byte>? LegacyMaximum { get; }

    /// <summary>Gets the column-order minimum bytes.</summary>
    public ReadOnlyMemory<byte>? Minimum { get; }

    /// <summary>Gets the column-order maximum bytes.</summary>
    public ReadOnlyMemory<byte>? Maximum { get; }

    /// <summary>Gets the declared null count, preserving absence.</summary>
    public long? NullCount { get; }

    /// <summary>Gets the declared distinct count, preserving absence.</summary>
    public long? DistinctCount { get; }

    /// <summary>Gets whether the declared minimum is exact.</summary>
    public bool? IsMinimumExact { get; }

    /// <summary>Gets whether the declared maximum is exact.</summary>
    public bool? IsMaximumExact { get; }

    /// <summary>Gets the declared NaN count, preserving absence.</summary>
    public long? NanCount { get; }
}

/// <summary>One custom key/value metadata entry.</summary>
public sealed class ParquetKeyValueMetadata
{
    internal ParquetKeyValueMetadata(string key, string? value)
    {
        Key = key;
        Value = value;
    }

    /// <summary>Gets the metadata key.</summary>
    public string Key { get; }

    /// <summary>Gets the optional metadata value.</summary>
    public string? Value { get; }
}

/// <summary>Metadata for one leaf column chunk in one row group.</summary>
public sealed class ParquetColumnChunk
{
    internal ParquetColumnChunk(
        int columnOrdinal,
        IReadOnlyList<string> path,
        string? externalFilePath,
        long fileOffset,
        int physicalTypeCode,
        int codecCode,
        int[] encodingCodes,
        long valueCount,
        long totalUncompressedSize,
        long totalCompressedSize,
        long dataPageOffset,
        long? dictionaryPageOffset,
        long? indexPageOffset,
        long? offsetIndexOffset,
        int? offsetIndexLength,
        long? columnIndexOffset,
        int? columnIndexLength,
        long? bloomFilterOffset,
        int? bloomFilterLength,
        ParquetStatistics? statistics,
        IReadOnlyList<ParquetKeyValueMetadata> customMetadata,
        bool hasCryptoMetadata,
        bool hasEncryptedMetadata)
    {
        ColumnOrdinal = columnOrdinal;
        Path = path;
        ExternalFilePath = externalFilePath;
        FileOffset = fileOffset;
        PhysicalTypeCode = physicalTypeCode;
        CompressionCodecCode = codecCode;
        EncodingCodes = Array.AsReadOnly(encodingCodes);
        ValueCount = valueCount;
        TotalUncompressedSize = totalUncompressedSize;
        TotalCompressedSize = totalCompressedSize;
        DataPageOffset = dataPageOffset;
        DictionaryPageOffset = dictionaryPageOffset;
        IndexPageOffset = indexPageOffset;
        OffsetIndexOffset = offsetIndexOffset;
        OffsetIndexLength = offsetIndexLength;
        ColumnIndexOffset = columnIndexOffset;
        ColumnIndexLength = columnIndexLength;
        BloomFilterOffset = bloomFilterOffset;
        BloomFilterLength = bloomFilterLength;
        Statistics = statistics;
        CustomMetadata = customMetadata;
        HasCryptoMetadata = hasCryptoMetadata;
        HasEncryptedMetadata = hasEncryptedMetadata;
    }

    /// <summary>Gets the primitive-leaf ordinal.</summary>
    public int ColumnOrdinal { get; }
    /// <summary>Gets the schema path recorded by the chunk.</summary>
    public IReadOnlyList<string> Path { get; }
    /// <summary>Gets the external file path, when declared.</summary>
    public string? ExternalFilePath { get; }
    /// <summary>Gets the deprecated column-chunk file offset.</summary>
    public long FileOffset { get; }
    /// <summary>Gets the raw physical-type code.</summary>
    public int PhysicalTypeCode { get; }
    /// <summary>Gets the known physical type.</summary>
    public ParquetPhysicalType? PhysicalType => Enum.IsDefined(typeof(ParquetPhysicalType), PhysicalTypeCode)
        ? (ParquetPhysicalType)PhysicalTypeCode : null;
    /// <summary>Gets the raw compression-codec code.</summary>
    public int CompressionCodecCode { get; }
    /// <summary>Gets the known compression codec.</summary>
    public ParquetCompressionCodec? CompressionCodec => Enum.IsDefined(typeof(ParquetCompressionCodec), CompressionCodecCode)
        ? (ParquetCompressionCodec)CompressionCodecCode : null;
    /// <summary>Gets the advertised raw encoding codes.</summary>
    public ReadOnlyCollection<int> EncodingCodes { get; }
    /// <summary>Gets the declared logical value count.</summary>
    public long ValueCount { get; }
    /// <summary>Gets the declared total uncompressed page bytes.</summary>
    public long TotalUncompressedSize { get; }
    /// <summary>Gets the declared total compressed page bytes.</summary>
    public long TotalCompressedSize { get; }
    /// <summary>Gets the first data-page offset.</summary>
    public long DataPageOffset { get; }
    /// <summary>Gets the optional dictionary-page offset.</summary>
    public long? DictionaryPageOffset { get; }
    /// <summary>Gets the optional legacy index-page offset.</summary>
    public long? IndexPageOffset { get; }
    /// <summary>Gets the optional offset-index offset.</summary>
    public long? OffsetIndexOffset { get; }
    /// <summary>Gets the optional offset-index length.</summary>
    public int? OffsetIndexLength { get; }
    /// <summary>Gets the optional column-index offset.</summary>
    public long? ColumnIndexOffset { get; }
    /// <summary>Gets the optional column-index length.</summary>
    public int? ColumnIndexLength { get; }
    /// <summary>Gets the optional bloom-filter offset.</summary>
    public long? BloomFilterOffset { get; }
    /// <summary>Gets the optional bloom-filter length.</summary>
    public int? BloomFilterLength { get; }
    /// <summary>Gets raw chunk statistics.</summary>
    public ParquetStatistics? Statistics { get; }
    /// <summary>Gets chunk-level custom metadata.</summary>
    public IReadOnlyList<ParquetKeyValueMetadata> CustomMetadata { get; }
    /// <summary>Gets whether column crypto metadata is present.</summary>
    public bool HasCryptoMetadata { get; }
    /// <summary>Gets whether encrypted column metadata is present.</summary>
    public bool HasEncryptedMetadata { get; }
}

/// <summary>Metadata for one row group.</summary>
public sealed class ParquetRowGroup
{
    internal ParquetRowGroup(
        int ordinal,
        long rowOffset,
        long rowCount,
        long totalByteSize,
        long? totalCompressedSize,
        long? fileOffset,
        ParquetColumnChunk[] columns)
    {
        Ordinal = ordinal;
        RowOffset = rowOffset;
        RowCount = rowCount;
        TotalByteSize = totalByteSize;
        TotalCompressedSize = totalCompressedSize;
        FileOffset = fileOffset;
        Columns = Array.AsReadOnly(columns);
    }

    /// <summary>Gets the zero-based row-group ordinal.</summary>
    public int Ordinal { get; }
    /// <summary>Gets the global row offset.</summary>
    public long RowOffset { get; }
    /// <summary>Gets the row count.</summary>
    public long RowCount { get; }
    /// <summary>Gets the advisory declared uncompressed byte total.</summary>
    public long TotalByteSize { get; }
    /// <summary>Gets the advisory declared compressed byte total.</summary>
    public long? TotalCompressedSize { get; }
    /// <summary>Gets the optional advisory row-group file offset.</summary>
    public long? FileOffset { get; }
    /// <summary>Gets the leaf column chunks in schema order.</summary>
    public ReadOnlyCollection<ParquetColumnChunk> Columns { get; }
}

/// <summary>Validated immutable footer metadata.</summary>
public sealed class ParquetFileMetadata
{
    internal ParquetFileMetadata(
        int version,
        long rowCount,
        ParquetSchema schema,
        ParquetRowGroup[] rowGroups,
        ParquetKeyValueMetadata[] customMetadata,
        string? createdBy,
        ParquetColumnOrderKind[] columnOrders,
        bool isEncrypted)
    {
        Version = version;
        RowCount = rowCount;
        Schema = schema;
        RowGroups = Array.AsReadOnly(rowGroups);
        CustomMetadata = Array.AsReadOnly(customMetadata);
        CreatedBy = createdBy;
        ColumnOrders = Array.AsReadOnly(columnOrders);
        IsEncrypted = isEncrypted;
    }

    /// <summary>Gets the Parquet file metadata version.</summary>
    public int Version { get; }
    /// <summary>Gets the declared file row count.</summary>
    public long RowCount { get; }
    /// <summary>Gets the complete schema.</summary>
    public ParquetSchema Schema { get; }
    /// <summary>Gets row groups in file order.</summary>
    public ReadOnlyCollection<ParquetRowGroup> RowGroups { get; }
    /// <summary>Gets file-level custom metadata.</summary>
    public ReadOnlyCollection<ParquetKeyValueMetadata> CustomMetadata { get; }
    /// <summary>Gets the optional producer description.</summary>
    public string? CreatedBy { get; }
    /// <summary>Gets column orders in primitive-leaf order, or an empty list when absent.</summary>
    public ReadOnlyCollection<ParquetColumnOrderKind> ColumnOrders { get; }
    /// <summary>Gets whether plaintext-footer encryption metadata is present.</summary>
    public bool IsEncrypted { get; }
}
