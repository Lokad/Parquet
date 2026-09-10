using System.Collections.ObjectModel;

namespace Lokad.Parquet;

internal sealed class BatchLifetime
{
    public bool IsDisposed { get; private set; }
    public void Dispose() => IsDisposed = true;
    public void ThrowIfDisposed() => ObjectDisposedException.ThrowIf(IsDisposed, typeof(ParquetBatch));
}

// Shared owner-release policy for batch disposal: every owner is released and the
// first failure is preserved for the caller to rethrow.
internal static class BatchOwnerDisposal
{
    internal static void DisposeAll(IDisposable[] owners)
    {
        Exception? failure = null;
        foreach (var owner in owners)
        {
            try
            {
                owner.Dispose();
            }
            catch (Exception exception) when (failure is null)
            {
                failure = exception;
            }
            catch (Exception)
            {
            }
        }

        if (failure is not null)
            System.Runtime.ExceptionServices.ExceptionDispatchInfo.Capture(failure).Throw();
    }
}

internal sealed class DecodedColumnBatch : IDisposable
{
    // The single-column scan publishes these pieces directly into its public batch,
    // so both batches share one lifetime and one owner array instead of wrapping
    // the decoded batch in a second set.
    internal readonly BatchLifetime Lifetime;
    internal readonly IDisposable[] Owners;

    internal DecodedColumnBatch(
        long rowOffset,
        int rowGroupOrdinal,
        long rowOffsetInGroup,
        int rowCount,
        ParquetColumnBatch column,
        BatchLifetime lifetime,
        IDisposable[] owners)
    {
        RowOffset = rowOffset;
        RowGroupOrdinal = rowGroupOrdinal;
        RowOffsetInGroup = rowOffsetInGroup;
        RowCount = rowCount;
        Column = column;
        Lifetime = lifetime;
        Owners = owners;
    }

    internal long RowOffset { get; }
    internal int RowGroupOrdinal { get; }
    internal long RowOffsetInGroup { get; }
    internal int RowCount { get; }
    internal ParquetColumnBatch Column { get; }
    internal bool IsDisposed => Lifetime.IsDisposed;

    public void Dispose()
    {
        if (Lifetime.IsDisposed)
            return;
        Lifetime.Dispose();
        BatchOwnerDisposal.DisposeAll(Owners);
    }
}

/// <summary>Validity bits for one batch column.</summary>
public sealed class ParquetValidity
{
    private readonly BatchLifetime _lifetime;
    private readonly ReadOnlyMemory<byte> _bits;

    internal ParquetValidity(BatchLifetime lifetime, int rowCount, ReadOnlyMemory<byte> bits, bool isAllValid)
    {
        _lifetime = lifetime;
        RowCount = rowCount;
        _bits = bits;
        IsAllValid = isAllValid;
    }

    /// <summary>Gets the number of represented rows.</summary>
    public int RowCount { get; }

    /// <summary>Gets whether all rows are implicitly valid.</summary>
    public bool IsAllValid { get; }

    /// <summary>Gets the least-significant-bit-first validity bitmap, or empty memory when all rows are valid.</summary>
    public ReadOnlyMemory<byte> Bits
    {
        get
        {
            _lifetime.ThrowIfDisposed();
            return _bits;
        }
    }

    /// <summary>Reports whether one row is non-null.</summary>
    /// <param name="rowIndex">The zero-based row within the batch.</param>
    /// <returns>True for a non-null row.</returns>
    public bool IsValid(int rowIndex)
    {
        _lifetime.ThrowIfDisposed();
        ArgumentOutOfRangeException.ThrowIfNegative(rowIndex);
        ArgumentOutOfRangeException.ThrowIfGreaterThanOrEqual(rowIndex, RowCount);
        return IsAllValid || (_bits.Span[rowIndex >> 3] & (1 << (rowIndex & 7))) != 0;
    }
}

/// <summary>Closed base class for one projected physical column in an output batch.</summary>
public abstract class ParquetColumnBatch
{
    private protected ParquetColumnBatch(
        BatchLifetime lifetime,
        ParquetColumn column,
        int rowCount,
        ParquetValidity validity)
    {
        Lifetime = lifetime;
        Column = column;
        RowCount = rowCount;
        Validity = validity;
    }

    private protected BatchLifetime Lifetime { get; }

    /// <summary>Gets the projected column descriptor.</summary>
    public ParquetColumn Column { get; }

    /// <summary>Gets the number of row-aligned value slots.</summary>
    public int RowCount { get; }

    /// <summary>Gets the column validity.</summary>
    public ParquetValidity Validity { get; }
}

/// <summary>A contiguous fixed-width primitive column.</summary>
/// <typeparam name="T">The physical .NET value type.</typeparam>
public sealed class ParquetPrimitiveColumnBatch<T> : ParquetColumnBatch where T : unmanaged
{
    private readonly ReadOnlyMemory<T> _values;

    internal ParquetPrimitiveColumnBatch(
        BatchLifetime lifetime,
        ParquetColumn column,
        ReadOnlyMemory<T> values,
        ParquetValidity validity)
        : base(lifetime, column, values.Length, validity) => _values = values;

    /// <summary>Gets one value slot per row; null-row slots are unspecified and must not be read without checking validity. The view is invalid after its batch is disposed.</summary>
    public ReadOnlyMemory<T> Values
    {
        get
        {
            Lifetime.ThrowIfDisposed();
            return _values;
        }
    }
}

/// <summary>A variable-length byte-array column with one offsets entry per row boundary.</summary>
public sealed class ParquetBinaryColumnBatch : ParquetColumnBatch
{
    private readonly ReadOnlyMemory<byte> _payload;
    private readonly ReadOnlyMemory<int> _offsets;

    internal ParquetBinaryColumnBatch(
        BatchLifetime lifetime,
        ParquetColumn column,
        int rowCount,
        ReadOnlyMemory<byte> payload,
        ReadOnlyMemory<int> offsets,
        ParquetValidity validity)
        : base(lifetime, column, rowCount, validity)
    {
        _payload = payload;
        _offsets = offsets;
    }

    /// <summary>Gets contiguous bytes for all non-null and empty values; the view is invalid after batch disposal.</summary>
    public ReadOnlyMemory<byte> Payload
    {
        get
        {
            Lifetime.ThrowIfDisposed();
            return _payload;
        }
    }

    /// <summary>Gets monotonically non-decreasing offsets with length <c>RowCount + 1</c>; a null row has equal adjacent offsets. The view is invalid after batch disposal.</summary>
    public ReadOnlyMemory<int> Offsets
    {
        get
        {
            Lifetime.ThrowIfDisposed();
            return _offsets;
        }
    }
}

/// <summary>A fixed-length byte-array column.</summary>
public sealed class ParquetFixedLengthByteArrayColumnBatch : ParquetColumnBatch
{
    private readonly ReadOnlyMemory<byte> _payload;

    internal ParquetFixedLengthByteArrayColumnBatch(
        BatchLifetime lifetime,
        ParquetColumn column,
        int rowCount,
        int typeWidth,
        ReadOnlyMemory<byte> payload,
        ParquetValidity validity)
        : base(lifetime, column, rowCount, validity)
    {
        TypeWidth = typeWidth;
        _payload = payload;
    }

    /// <summary>Gets the positive byte width of each row slot.</summary>
    public int TypeWidth { get; }

    /// <summary>Gets exactly <c>RowCount * TypeWidth</c> bytes; bytes in null-row slots are unspecified. The view is invalid after batch disposal.</summary>
    public ReadOnlyMemory<byte> Payload
    {
        get
        {
            Lifetime.ThrowIfDisposed();
            return _payload;
        }
    }
}

/// <summary>A row-aligned, disposable projected batch.</summary>
public sealed class ParquetBatch : IDisposable
{
    private readonly BatchLifetime _lifetime;
    private readonly IDisposable[] _owners;

    internal ParquetBatch(
        long rowOffset,
        int rowGroupOrdinal,
        long rowOffsetInGroup,
        int rowCount,
        ParquetColumnBatch[] columns,
        BatchLifetime lifetime,
        IDisposable[] owners)
    {
        RowOffset = rowOffset;
        RowGroupOrdinal = rowGroupOrdinal;
        RowOffsetInGroup = rowOffsetInGroup;
        RowCount = rowCount;
        Columns = Array.AsReadOnly(columns);
        _lifetime = lifetime;
        _owners = owners;
    }

    /// <summary>Gets the zero-based global first row.</summary>
    public long RowOffset { get; }

    /// <summary>Gets the source row-group ordinal.</summary>
    public int RowGroupOrdinal { get; }

    /// <summary>Gets the first row offset within the source row group.</summary>
    public long RowOffsetInGroup { get; }

    /// <summary>Gets the positive batch row count.</summary>
    public int RowCount { get; }

    /// <summary>Gets projected columns in caller order.</summary>
    public ReadOnlyCollection<ParquetColumnBatch> Columns { get; }

    /// <summary>Gets whether the batch has been disposed.</summary>
    public bool IsDisposed => _lifetime.IsDisposed;

    /// <summary>Invalidates views and returns every owned buffer exactly once.</summary>
    public void Dispose()
    {
        if (_lifetime.IsDisposed)
            return;
        _lifetime.Dispose();
        BatchOwnerDisposal.DisposeAll(_owners);
    }
}
