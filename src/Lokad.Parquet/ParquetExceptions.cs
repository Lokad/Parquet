namespace Lokad.Parquet;

/// <summary>Closed base class for classified errors caused by Parquet input.</summary>
public abstract class ParquetException : Exception
{
    /// <summary>Initializes a Parquet error with structured context.</summary>
    private protected ParquetException(
        string message,
        Exception? innerException,
        long? byteOffset,
        int? rowGroupOrdinal,
        int? columnOrdinal,
        int? pageOrdinal)
        : base(message, innerException)
    {
        ByteOffset = byteOffset;
        RowGroupOrdinal = rowGroupOrdinal;
        ColumnOrdinal = columnOrdinal;
        PageOrdinal = pageOrdinal;
    }

    /// <summary>Gets the relevant input offset, when known.</summary>
    public long? ByteOffset { get; }

    /// <summary>Gets the relevant row-group ordinal, when known.</summary>
    public int? RowGroupOrdinal { get; }

    /// <summary>Gets the relevant leaf-column ordinal, when known.</summary>
    public int? ColumnOrdinal { get; }

    /// <summary>Gets the relevant page ordinal, when known.</summary>
    public int? PageOrdinal { get; }
}

/// <summary>Identifies where rejected Parquet input was found. Absence is real: only the recorded scopes are known.</summary>
internal readonly record struct ParquetErrorLocation(
    long? ByteOffset,
    int? RowGroupOrdinal,
    int? ColumnOrdinal,
    int? PageOrdinal)
{
    /// <summary>Locates file-level input without a row group.</summary>
    internal static ParquetErrorLocation AtOffset(long byteOffset) => new(byteOffset, null, null, null);

    /// <summary>Locates input within one row group.</summary>
    internal static ParquetErrorLocation AtRowGroup(long byteOffset, int rowGroupOrdinal) => new(byteOffset, rowGroupOrdinal, null, null);

    /// <summary>Locates input within one column chunk.</summary>
    internal static ParquetErrorLocation AtChunk(long byteOffset, int rowGroupOrdinal, int columnOrdinal) => new(byteOffset, rowGroupOrdinal, columnOrdinal, null);

    /// <summary>Locates input within one page.</summary>
    internal static ParquetErrorLocation AtPage(long byteOffset, int rowGroupOrdinal, int columnOrdinal, int pageOrdinal) => new(byteOffset, rowGroupOrdinal, columnOrdinal, pageOrdinal);

    /// <summary>Locates a schema leaf without an input offset.</summary>
    internal static ParquetErrorLocation AtColumn(int columnOrdinal) => new(null, null, columnOrdinal, null);

    /// <summary>Locates a chunk-level concern without an input offset.</summary>
    internal static ParquetErrorLocation AtRowGroupColumn(int rowGroupOrdinal, int columnOrdinal) => new(null, rowGroupOrdinal, columnOrdinal, null);

    /// <summary>Locates a page-level concern without an input offset.</summary>
    internal static ParquetErrorLocation AtRowGroupColumnPage(int rowGroupOrdinal, int columnOrdinal, int pageOrdinal) => new(null, rowGroupOrdinal, columnOrdinal, pageOrdinal);
}

/// <summary>Indicates malformed, inconsistent, truncated, or corrupt Parquet input.</summary>
public sealed class ParquetFormatException : ParquetException
{
    internal ParquetFormatException(string message, ParquetErrorLocation location)
        : base(message, null, location.ByteOffset, location.RowGroupOrdinal, location.ColumnOrdinal, location.PageOrdinal) { }

    internal ParquetFormatException(string message, Exception? innerException, ParquetErrorLocation location)
        : base(message, innerException, location.ByteOffset, location.RowGroupOrdinal, location.ColumnOrdinal, location.PageOrdinal) { }

    internal ParquetFormatException(string message)
        : base(message, null, null, null, null, null) { }


}

/// <summary>Indicates a well-formed Parquet feature outside the supported profile.</summary>
public sealed class ParquetUnsupportedFeatureException : ParquetException
{
    internal ParquetUnsupportedFeatureException(string message, ParquetErrorLocation location)
        : base(message, null, location.ByteOffset, location.RowGroupOrdinal, location.ColumnOrdinal, location.PageOrdinal) { }

    internal ParquetUnsupportedFeatureException(string message)
        : base(message, null, null, null, null, null) { }


}

/// <summary>Indicates that input exceeds a configured reader safety limit.</summary>
public sealed class ParquetLimitExceededException : ParquetException
{
    internal ParquetLimitExceededException(string message, ParquetErrorLocation location)
        : base(message, null, location.ByteOffset, location.RowGroupOrdinal, location.ColumnOrdinal, location.PageOrdinal) { }

    internal ParquetLimitExceededException(string message)
        : base(message, null, null, null, null, null) { }


}
