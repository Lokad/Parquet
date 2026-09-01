namespace Lokad.Parquet;

/// <summary>Identifies who disposes an input supplied by the caller.</summary>
public enum ParquetSourceOwnership
{
    /// <summary>The caller retains responsibility for disposal.</summary>
    Caller = 0,
    /// <summary>The opened Parquet file disposes the input.</summary>
    ParquetFile = 1,
}

/// <summary>Physical storage types defined by the Parquet format.</summary>
public enum ParquetPhysicalType
{
    /// <summary>One-bit boolean values.</summary>
    Boolean = 0,
    /// <summary>Little-endian signed 32-bit integers.</summary>
    Int32 = 1,
    /// <summary>Little-endian signed 64-bit integers.</summary>
    Int64 = 2,
    /// <summary>Legacy 12-byte integer values.</summary>
    Int96 = 3,
    /// <summary>IEEE 754 binary32 values.</summary>
    Float = 4,
    /// <summary>IEEE 754 binary64 values.</summary>
    Double = 5,
    /// <summary>Length-prefixed byte arrays.</summary>
    ByteArray = 6,
    /// <summary>Fixed-width byte arrays.</summary>
    FixedLengthByteArray = 7,
}

/// <summary>Schema repetition modes defined by the Parquet format.</summary>
public enum ParquetRepetition
{
    /// <summary>The value is present exactly once.</summary>
    Required = 0,
    /// <summary>The value is either null or present once.</summary>
    Optional = 1,
    /// <summary>The value may be repeated.</summary>
    Repeated = 2,
}

/// <summary>Value encodings defined by the Parquet format.</summary>
public enum ParquetEncoding
{
    /// <summary>Plain physical encoding.</summary>
    Plain = 0,
    /// <summary>Legacy dictionary marker.</summary>
    PlainDictionary = 2,
    /// <summary>Run-length/bit-packed hybrid encoding.</summary>
    RunLength = 3,
    /// <summary>Deprecated bit-packed encoding.</summary>
    BitPacked = 4,
    /// <summary>Delta binary-packed integers.</summary>
    DeltaBinaryPacked = 5,
    /// <summary>Delta length byte arrays.</summary>
    DeltaLengthByteArray = 6,
    /// <summary>Delta byte arrays.</summary>
    DeltaByteArray = 7,
    /// <summary>Modern dictionary marker.</summary>
    RunLengthDictionary = 8,
    /// <summary>Byte-stream split encoding.</summary>
    ByteStreamSplit = 9,
}

/// <summary>Compression codecs defined by the Parquet format.</summary>
public enum ParquetCompressionCodec
{
    /// <summary>No compression.</summary>
    Uncompressed = 0,
    /// <summary>Snappy block compression.</summary>
    Snappy = 1,
    /// <summary>Gzip compression.</summary>
    Gzip = 2,
    /// <summary>LZO compression.</summary>
    Lzo = 3,
    /// <summary>Brotli compression.</summary>
    Brotli = 4,
    /// <summary>Deprecated Hadoop LZ4 compression.</summary>
    Lz4 = 5,
    /// <summary>Zstandard compression.</summary>
    Zstandard = 6,
    /// <summary>Raw LZ4 block compression.</summary>
    Lz4Raw = 7,
}

/// <summary>Legacy converted-type annotations defined by Parquet.</summary>
public enum ParquetConvertedType
{
    /// <summary>UTF-8 text.</summary>
    Utf8 = 0,
    /// <summary>Map group.</summary>
    Map = 1,
    /// <summary>Map key/value group.</summary>
    MapKeyValue = 2,
    /// <summary>List group.</summary>
    List = 3,
    /// <summary>Enumerated UTF-8 text.</summary>
    Enum = 4,
    /// <summary>Decimal value.</summary>
    Decimal = 5,
    /// <summary>Days since the Unix epoch.</summary>
    Date = 6,
    /// <summary>Milliseconds since midnight.</summary>
    TimeMilliseconds = 7,
    /// <summary>Microseconds since midnight.</summary>
    TimeMicroseconds = 8,
    /// <summary>Milliseconds since the Unix epoch.</summary>
    TimestampMilliseconds = 9,
    /// <summary>Microseconds since the Unix epoch.</summary>
    TimestampMicroseconds = 10,
    /// <summary>Unsigned 8-bit integer.</summary>
    UInt8 = 11,
    /// <summary>Unsigned 16-bit integer.</summary>
    UInt16 = 12,
    /// <summary>Unsigned 32-bit integer.</summary>
    UInt32 = 13,
    /// <summary>Unsigned 64-bit integer.</summary>
    UInt64 = 14,
    /// <summary>Signed 8-bit integer.</summary>
    Int8 = 15,
    /// <summary>Signed 16-bit integer.</summary>
    Int16 = 16,
    /// <summary>Signed 32-bit integer.</summary>
    Int32 = 17,
    /// <summary>Signed 64-bit integer.</summary>
    Int64 = 18,
    /// <summary>JSON document.</summary>
    Json = 19,
    /// <summary>BSON document.</summary>
    Bson = 20,
    /// <summary>Legacy interval.</summary>
    Interval = 21,
}

/// <summary>Modern logical-type union members defined by the pinned format.</summary>
public enum ParquetLogicalTypeKind
{
    /// <summary>No modern annotation is present.</summary>
    None = 0,
    /// <summary>UTF-8 text.</summary>
    String = 1,
    /// <summary>Map group.</summary>
    Map = 2,
    /// <summary>List group.</summary>
    List = 3,
    /// <summary>Enumerated text.</summary>
    Enum = 4,
    /// <summary>Decimal value.</summary>
    Decimal = 5,
    /// <summary>Date value.</summary>
    Date = 6,
    /// <summary>Time-of-day value.</summary>
    Time = 7,
    /// <summary>Timestamp value.</summary>
    Timestamp = 8,
    /// <summary>Signed or unsigned integer.</summary>
    Integer = 10,
    /// <summary>Always-null value.</summary>
    Unknown = 11,
    /// <summary>JSON document.</summary>
    Json = 12,
    /// <summary>BSON document.</summary>
    Bson = 13,
    /// <summary>UUID value.</summary>
    Uuid = 14,
    /// <summary>IEEE 754 binary16 value.</summary>
    Float16 = 15,
    /// <summary>Variant value.</summary>
    Variant = 16,
    /// <summary>Geometry value.</summary>
    Geometry = 17,
    /// <summary>Geography value.</summary>
    Geography = 18,
    /// <summary>File reference.</summary>
    File = 19,
}

/// <summary>Units used by Parquet time and timestamp annotations.</summary>
public enum ParquetTimeUnit
{
    /// <summary>Milliseconds.</summary>
    Milliseconds = 1,
    /// <summary>Microseconds.</summary>
    Microseconds = 2,
    /// <summary>Nanoseconds.</summary>
    Nanoseconds = 3,
}

/// <summary>Relationship between modern and legacy annotations.</summary>
public enum ParquetAnnotationStatus
{
    /// <summary>No annotation is present.</summary>
    None,
    /// <summary>Only a modern annotation is present.</summary>
    ModernOnly,
    /// <summary>Only a legacy annotation is present.</summary>
    LegacyOnly,
    /// <summary>Both annotations are present and consistent.</summary>
    Consistent,
    /// <summary>Both annotations are present but conflict.</summary>
    Conflict,
    /// <summary>An annotation violates its structural constraints.</summary>
    Invalid,
}

/// <summary>Column ordering used to interpret raw minimum and maximum statistics.</summary>
public enum ParquetColumnOrderKind
{
    /// <summary>No column order was declared.</summary>
    Absent = 0,
    /// <summary>The physical or logical type defines the order.</summary>
    TypeDefined = 1,
    /// <summary>IEEE 754 total ordering.</summary>
    Ieee754Total = 2,
    /// <summary>Legacy chronological INT96 ordering.</summary>
    Int96Timestamp = 3,
    /// <summary>An unknown future union member.</summary>
    Unknown = 255,
}
