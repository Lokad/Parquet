namespace Lokad.Parquet;

/// <summary>Immutable page-sizing options for a flat Parquet writer.</summary>
public sealed class ParquetWriterOptions
{
    /// <summary>Gets the default writer options.</summary>
    public static ParquetWriterOptions Default { get; } = new();

    /// <summary>Gets the target number of PLAIN bytes per page, between 8 and 16 MiB.</summary>
    /// <remarks>A larger UTF-8 value that fits the hard page limit is written on its own page.</remarks>
    public int TargetPageBytes { get; init; } = 256 * 1024;

    /// <summary>Gets the maximum number of values per page, between 1 and 1,048,576.</summary>
    public int TargetPageValues { get; init; } = 65_536;
}
