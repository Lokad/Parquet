namespace Lokad.Parquet.Internal;

internal sealed class SchemaElementWire
{
    public int? TypeCode { get; set; }
    public int? TypeLength { get; set; }
    public int? RepetitionCode { get; set; }
    public string? Name { get; set; }
    public int? ChildCount { get; set; }
    public int? ConvertedTypeCode { get; set; }
    public int? Scale { get; set; }
    public int? Precision { get; set; }
    public int? FieldId { get; set; }
    public ParquetLogicalAnnotation? LogicalAnnotation { get; set; }
}

internal sealed class StatisticsWire
{
    public byte[]? LegacyMaximum { get; set; }
    public byte[]? LegacyMinimum { get; set; }
    public long? NullCount { get; set; }
    public long? DistinctCount { get; set; }
    public byte[]? Maximum { get; set; }
    public byte[]? Minimum { get; set; }
    public bool? IsMaximumExact { get; set; }
    public bool? IsMinimumExact { get; set; }
    public long? NanCount { get; set; }
}

internal sealed class ColumnMetadataWire
{
    public int? TypeCode { get; set; }
    public int[]? EncodingCodes { get; set; }
    public string[]? Path { get; set; }
    public int? CodecCode { get; set; }
    public long? ValueCount { get; set; }
    public long? TotalUncompressedSize { get; set; }
    public long? TotalCompressedSize { get; set; }
    public ParquetKeyValueMetadata[] CustomMetadata { get; set; } = [];
    public long? DataPageOffset { get; set; }
    public long? IndexPageOffset { get; set; }
    public long? DictionaryPageOffset { get; set; }
    public StatisticsWire? Statistics { get; set; }
    public long? BloomFilterOffset { get; set; }
    public int? BloomFilterLength { get; set; }
}

internal sealed class ColumnChunkWire
{
    public string? FilePath { get; set; }
    public long? FileOffset { get; set; }
    public ColumnMetadataWire? Metadata { get; set; }
    public long? OffsetIndexOffset { get; set; }
    public int? OffsetIndexLength { get; set; }
    public long? ColumnIndexOffset { get; set; }
    public int? ColumnIndexLength { get; set; }
    public bool HasCryptoMetadata { get; set; }
    public bool HasEncryptedMetadata { get; set; }
}

internal sealed class RowGroupWire
{
    public ColumnChunkWire[]? Columns { get; set; }
    public long? TotalByteSize { get; set; }
    public long? RowCount { get; set; }
    public long? FileOffset { get; set; }
    public long? TotalCompressedSize { get; set; }
}

internal sealed class FileMetadataWire
{
    public int? Version { get; set; }
    public SchemaElementWire[]? Schema { get; set; }
    public long? RowCount { get; set; }
    public RowGroupWire[]? RowGroups { get; set; }
    public ParquetKeyValueMetadata[] CustomMetadata { get; set; } = [];
    public string? CreatedBy { get; set; }
    public ParquetColumnOrderKind[]? ColumnOrders { get; set; }
    public bool HasEncryptionAlgorithm { get; set; }
    public bool HasFooterSigningKeyMetadata { get; set; }
}
