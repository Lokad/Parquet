namespace Lokad.Parquet;

/// <summary>Immutable safety and resource limits used while opening and scanning a file.</summary>
public sealed class ParquetReaderOptions
{
    /// <summary>Gets the default reader options.</summary>
    public static ParquetReaderOptions Default { get; } = new();

    /// <summary>Gets the maximum footer size in bytes.</summary>
    public int MaximumFooterBytes { get; init; } = 64 * 1024 * 1024;

    /// <summary>Gets the maximum Thrift nesting depth.</summary>
    public int MaximumThriftDepth { get; init; } = 64;

    /// <summary>Gets the maximum number of elements in one Thrift container.</summary>
    public int MaximumThriftContainerElements { get; init; } = 1_048_576;

    /// <summary>Gets the maximum number of schema elements.</summary>
    public int MaximumSchemaElements { get; init; } = 16_384;

    /// <summary>Gets the maximum number of primitive leaf columns.</summary>
    public int MaximumLeafColumns { get; init; } = 4_096;

    /// <summary>Gets the maximum number of row groups.</summary>
    public int MaximumRowGroups { get; init; } = 65_536;

    /// <summary>Gets the maximum number of key/value metadata entries.</summary>
    public int MaximumKeyValueMetadataEntries { get; init; } = 16_384;

    /// <summary>Gets the maximum aggregate encoded bytes of decoded metadata strings.</summary>
    public int MaximumMetadataStringBytes { get; init; } = 16 * 1024 * 1024;

    /// <summary>Gets the maximum serialized page-header size in bytes.</summary>
    public int MaximumPageHeaderBytes { get; init; } = 1024 * 1024;

    /// <summary>Gets the maximum compressed page payload in bytes.</summary>
    public int MaximumCompressedPageBytes { get; init; } = 64 * 1024 * 1024;

    /// <summary>Gets the maximum uncompressed page payload in bytes.</summary>
    public int MaximumUncompressedPageBytes { get; init; } = 256 * 1024 * 1024;

    /// <summary>Gets the maximum number of pages in one column chunk.</summary>
    public int MaximumPagesPerColumnChunk { get; init; } = 1_048_576;

    /// <summary>Gets the maximum number of dictionary entries.</summary>
    public int MaximumDictionaryEntries { get; init; } = 16_777_216;

    /// <summary>Gets the maximum dictionary page and decoded dictionary bytes.</summary>
    /// <remarks>The serialized (uncompressed) dictionary page size is capped at this limit
    /// before decoding, and the decoded layout is capped after entry lengths are known.</remarks>
    public int MaximumDictionaryBytes { get; init; } = 256 * 1024 * 1024;

    /// <summary>Gets the maximum logical values in one page.</summary>
    public int MaximumValuesPerPage { get; init; } = 16_777_216;

    /// <summary>Gets the maximum size of one binary value.</summary>
    public int MaximumBinaryValueBytes { get; init; } = 64 * 1024 * 1024;

    /// <summary>Gets the maximum rows in one output batch.</summary>
    public int MaximumRowsPerBatch { get; init; } = 1_048_576;

    /// <summary>Gets the maximum binary payload bytes in one output batch.</summary>
    public int MaximumBinaryBatchBytes { get; init; } = 256 * 1024 * 1024;

    /// <summary>Gets the maximum pooled bytes owned by one open file and its batches.</summary>
    public long MaximumScanPooledBytes { get; init; } = 512L * 1024 * 1024;

}
