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

/// <summary>Indicates malformed, inconsistent, truncated, or corrupt Parquet input.</summary>
public sealed class ParquetFormatException : ParquetException
{
    internal ParquetFormatException(string message)
        : base(message, null, null, null, null, null) { }

    internal ParquetFormatException(string message, long byteOffset)
        : base(message, null, byteOffset, null, null, null) { }

    internal ParquetFormatException(string message, Exception innerException, long byteOffset)
        : base(message, innerException, byteOffset, null, null, null) { }

    internal ParquetFormatException(
        string message,
        long byteOffset,
        int rowGroupOrdinal)
        : base(message, null, byteOffset, rowGroupOrdinal, null, null) { }

    internal ParquetFormatException(
        string message,
        long byteOffset,
        int rowGroupOrdinal,
        int columnOrdinal)
        : base(message, null, byteOffset, rowGroupOrdinal, columnOrdinal, null) { }

    internal ParquetFormatException(
        string message,
        long byteOffset,
        int rowGroupOrdinal,
        int columnOrdinal,
        int pageOrdinal)
        : base(message, null, byteOffset, rowGroupOrdinal, columnOrdinal, pageOrdinal) { }

    internal ParquetFormatException(
        string message,
        int columnOrdinal)
        : base(message, null, null, null, columnOrdinal, null) { }

    internal ParquetFormatException(
        string message,
        Exception? innerException,
        long? byteOffset,
        int? rowGroupOrdinal,
        int? columnOrdinal,
        int? pageOrdinal)
        : base(message, innerException, byteOffset, rowGroupOrdinal, columnOrdinal, pageOrdinal) { }
}

/// <summary>Indicates a well-formed Parquet feature outside the supported profile.</summary>
public sealed class ParquetUnsupportedFeatureException : ParquetException
{
    internal ParquetUnsupportedFeatureException(string message)
        : base(message, null, null, null, null, null) { }

    internal ParquetUnsupportedFeatureException(string message, long byteOffset)
        : base(message, null, byteOffset, null, null, null) { }

    internal ParquetUnsupportedFeatureException(string message, int columnOrdinal)
        : base(message, null, null, null, columnOrdinal, null) { }

    internal ParquetUnsupportedFeatureException(
        string message,
        int rowGroupOrdinal,
        int columnOrdinal)
        : base(message, null, null, rowGroupOrdinal, columnOrdinal, null) { }

    internal ParquetUnsupportedFeatureException(
        string message,
        long byteOffset,
        int rowGroupOrdinal,
        int columnOrdinal,
        int pageOrdinal)
        : base(message, null, byteOffset, rowGroupOrdinal, columnOrdinal, pageOrdinal) { }
}

/// <summary>Indicates that input exceeds a configured reader safety limit.</summary>
public sealed class ParquetLimitExceededException : ParquetException
{
    internal ParquetLimitExceededException(string message)
        : base(message, null, null, null, null, null) { }

    internal ParquetLimitExceededException(string message, long byteOffset)
        : base(message, null, byteOffset, null, null, null) { }

    internal ParquetLimitExceededException(
        string message,
        int rowGroupOrdinal,
        int columnOrdinal,
        int pageOrdinal)
        : base(message, null, null, rowGroupOrdinal, columnOrdinal, pageOrdinal) { }

    internal ParquetLimitExceededException(
        string message,
        long byteOffset,
        int rowGroupOrdinal,
        int columnOrdinal,
        int pageOrdinal)
        : base(message, null, byteOffset, rowGroupOrdinal, columnOrdinal, pageOrdinal) { }
}
