namespace Lokad.Parquet.Benchmarks;

/// <summary>Physical value layout of one work-census case.</summary>
public enum CensusPhysicalType
{
    /// <summary>One-byte boolean slots.</summary>
    Boolean = 0,
    /// <summary>Little-endian signed 32-bit integers.</summary>
    Int32 = 1,
    /// <summary>Little-endian signed 64-bit integers.</summary>
    Int64 = 2,
    /// <summary>IEEE 754 binary32 values.</summary>
    Float = 3,
    /// <summary>IEEE 754 binary64 values.</summary>
    Double = 4,
    /// <summary>Fixed-width byte slices of the declared width.</summary>
    FixedLengthByteArray = 5,
    /// <summary>Length-prefixed UTF-8 bytes with an offsets entry per row boundary.</summary>
    Utf8 = 6,
    /// <summary>Length-prefixed raw bytes with an offsets entry per row boundary.</summary>
    ByteArray = 7,
}

/// <summary>Explicit decoded-layout description of one work-census case.</summary>
/// <param name="PhysicalType">The projected physical type shared by the case columns.</param>
/// <param name="TypeWidthBytes">The byte width of one fixed-width value slot; unused for variable-width layouts.</param>
/// <param name="Nullable">Whether lanes can produce nulls and therefore a validity bitmap.</param>
/// <param name="Consumer">The census consumer that drains the case batches.</param>
internal sealed record CensusCaseLayout(
    CensusPhysicalType PhysicalType,
    int TypeWidthBytes,
    bool Nullable,
    string Consumer)
{
    /// <summary>Describes one frozen catalog lane from its workload token.</summary>
    /// <param name="workload">The catalog workload token.</param>
    /// <returns>The explicit layout of the lane.</returns>
    internal static CensusCaseLayout ForCatalogLane(ScanWorkload workload)
    {
        if (ScanWorkloadCatalog.IsString(workload))
            return new CensusCaseLayout(CensusPhysicalType.Utf8, 0, false, "utf8");
        if (workload == ScanWorkload.NullableInt32Plain)
            return new CensusCaseLayout(CensusPhysicalType.Int32, sizeof(int), true, "nullable-int32");
        return ScanWorkloadCatalog.GetColumnCount(workload) > 1
            ? new CensusCaseLayout(CensusPhysicalType.Int32, sizeof(int), false, "multi-int32")
            : new CensusCaseLayout(CensusPhysicalType.Int32, sizeof(int), false, "int32");
    }
}

/// <summary>Denominators derived from an explicit case layout.</summary>
public static class CensusLayout
{
    /// <summary>Reports the snapshot name of one physical layout.</summary>
    /// <param name="physicalType">The projected physical type.</param>
    /// <returns>The lowercase snapshot name.</returns>
    public static string NameOf(CensusPhysicalType physicalType) => physicalType switch
    {
        CensusPhysicalType.Boolean => "boolean",
        CensusPhysicalType.Int32 => "int32",
        CensusPhysicalType.Int64 => "int64",
        CensusPhysicalType.Float => "float",
        CensusPhysicalType.Double => "double",
        CensusPhysicalType.FixedLengthByteArray => "fixed",
        CensusPhysicalType.Utf8 => "utf8",
        CensusPhysicalType.ByteArray => "binary",
        _ => throw new ArgumentOutOfRangeException(nameof(physicalType)),
    };

    /// <summary>Computes the decoded-layout byte size of one census pass.</summary>
    /// <param name="physicalType">The projected physical type.</param>
    /// <param name="typeWidthBytes">The byte width of one fixed-width value slot; validated only for fixed-width layouts.</param>
    /// <param name="nullable">Whether lanes can produce nulls and therefore a validity bitmap.</param>
    /// <param name="rowCount">The number of emitted rows.</param>
    /// <param name="columnCount">The number of projected columns.</param>
    /// <param name="utf8PayloadBytes">The UTF-8 payload bytes; used only for UTF-8 layouts and must be zero otherwise.</param>
    /// <param name="binaryPayloadBytes">The raw binary payload bytes; used only for variable-width binary layouts and must be zero otherwise.</param>
    /// <returns>Value bytes plus validity bitmap bytes where lanes can produce nulls, UTF-8 payload plus offsets, or binary payload plus offsets.</returns>
    public static long LogicalOutputBytes(
        CensusPhysicalType physicalType,
        int typeWidthBytes,
        bool nullable,
        int rowCount,
        int columnCount,
        int utf8PayloadBytes,
        int binaryPayloadBytes)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(rowCount);
        ArgumentOutOfRangeException.ThrowIfNegative(columnCount);
        ArgumentOutOfRangeException.ThrowIfNegative(utf8PayloadBytes);
        ArgumentOutOfRangeException.ThrowIfNegative(binaryPayloadBytes);
        if (physicalType == CensusPhysicalType.Utf8)
        {
            if (binaryPayloadBytes != 0)
                throw new ArgumentException("A UTF-8 layout carries no binary payload.", nameof(binaryPayloadBytes));
            return checked((long)utf8PayloadBytes + (long)columnCount * (rowCount + 1L) * sizeof(int));
        }

        if (physicalType == CensusPhysicalType.ByteArray)
        {
            if (utf8PayloadBytes != 0)
                throw new ArgumentException("A binary layout carries no UTF-8 payload.", nameof(utf8PayloadBytes));
            return checked((long)binaryPayloadBytes + (long)columnCount * (rowCount + 1L) * sizeof(int)) +
                (nullable ? checked((long)columnCount * ((rowCount + 7) / 8)) : 0);
        }

        if (binaryPayloadBytes != 0)
            throw new ArgumentException("A fixed-width layout carries no binary payload.", nameof(binaryPayloadBytes));
        var slotWidth = physicalType switch
        {
            CensusPhysicalType.Boolean => 1,
            CensusPhysicalType.Int32 or CensusPhysicalType.Float => sizeof(int),
            CensusPhysicalType.Int64 or CensusPhysicalType.Double => sizeof(long),
            CensusPhysicalType.FixedLengthByteArray when typeWidthBytes > 0 => typeWidthBytes,
            CensusPhysicalType.FixedLengthByteArray => throw new ArgumentOutOfRangeException(nameof(typeWidthBytes)),
            _ => throw new ArgumentOutOfRangeException(nameof(physicalType)),
        };
        return checked((long)rowCount * columnCount * slotWidth) +
            (nullable ? checked((long)columnCount * ((rowCount + 7) / 8)) : 0);
    }
}

