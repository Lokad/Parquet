using System.Collections.ObjectModel;

namespace Lokad.Parquet;

/// <summary>A zero-based, half-open global row interval.</summary>
public readonly record struct ParquetRowRange
{
    /// <summary>Initializes a row interval.</summary>
    /// <param name="start">The zero-based first row.</param>
    /// <param name="count">The non-negative number of rows.</param>
    public ParquetRowRange(long start, long count)
    {
        if (start < 0)
            throw new ArgumentOutOfRangeException(nameof(start));
        if (count < 0)
            throw new ArgumentOutOfRangeException(nameof(count));
        try
        {
            _ = checked(start + count);
        }
        catch (OverflowException exception)
        {
            throw new ArgumentOutOfRangeException(nameof(count), exception.Message);
        }
        Start = start;
        Count = count;
    }

    /// <summary>Gets the zero-based first row.</summary>
    public long Start { get; }

    /// <summary>Gets the number of rows.</summary>
    public long Count { get; }

    /// <summary>Gets the exclusive interval end.</summary>
    public long End => Start + Count;
}

/// <summary>Immutable projection, row selection, and batching options for one scan.</summary>
public sealed class ParquetScanOptions
{
    /// <summary>Initializes a descriptor projection with default row selection and batching.</summary>
    /// <param name="columns">Column descriptors in output order.</param>
    public ParquetScanOptions(IReadOnlyList<ParquetColumn> columns)
        : this(columns, null, null, 65_536) { }

    /// <summary>Initializes a descriptor projection with explicit row selection and batching.</summary>
    /// <param name="columns">Column descriptors in output order.</param>
    /// <param name="rowGroups">Optional row-group set; null selects every row group.</param>
    /// <param name="rowRange">Optional global row interval.</param>
    /// <param name="targetBatchRowCount">Preferred output rows per batch.</param>
    public ParquetScanOptions(
        IReadOnlyList<ParquetColumn> columns,
        IReadOnlyList<ParquetRowGroup>? rowGroups,
        ParquetRowRange? rowRange,
        int targetBatchRowCount)
    {
        ArgumentNullException.ThrowIfNull(columns);
        var columnSnapshot = columns.ToArray();
        if (columnSnapshot.Any(static column => column is null))
            throw new ArgumentException("A projected column descriptor cannot be null.", nameof(columns));
        Columns = Array.AsReadOnly(columnSnapshot);

        if (rowGroups is not null)
        {
            var rowGroupSnapshot = rowGroups.ToArray();
            if (rowGroupSnapshot.Any(static rowGroup => rowGroup is null))
                throw new ArgumentException("A selected row-group descriptor cannot be null.", nameof(rowGroups));
            RowGroups = Array.AsReadOnly(rowGroupSnapshot);
        }
        RowRange = rowRange;
        TargetBatchRowCount = targetBatchRowCount;
    }

    /// <summary>Gets projected columns in output order.</summary>
    public ReadOnlyCollection<ParquetColumn> Columns { get; }

    /// <summary>Gets the explicit row-group set, or null for all row groups.</summary>
    public ReadOnlyCollection<ParquetRowGroup>? RowGroups { get; }

    /// <summary>Gets the optional global row interval.</summary>
    public ParquetRowRange? RowRange { get; }

    /// <summary>Gets the preferred output rows per batch.</summary>
    public int TargetBatchRowCount { get; }

}
