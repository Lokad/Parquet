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
    /// <summary>Contiguous UTF-8 payload bytes with an offsets entry per row boundary.</summary>
    Utf8 = 6,
    /// <summary>Contiguous raw payload bytes with an offsets entry per row boundary.</summary>
    ByteArray = 7,
}

/// <summary>Census consumer draining one work-census case.</summary>
internal enum CensusConsumer
{
    /// <summary>Required fixed-width INT32 batches.</summary>
    Int32 = 0,
    /// <summary>Nullable fixed-width INT32 batches.</summary>
    NullableInt32 = 1,
    /// <summary>Multi-column required INT32 batches.</summary>
    MultiInt32 = 2,
    /// <summary>Nullable boolean batches.</summary>
    Boolean = 3,
    /// <summary>Required UTF-8 batches.</summary>
    Utf8 = 4,
    /// <summary>Required fixed-width INT64 batches.</summary>
    Int64 = 5,
    /// <summary>Required fixed-width FLOAT batches.</summary>
    Float = 6,
    /// <summary>Required fixed-width DOUBLE batches.</summary>
    Double = 7,
    /// <summary>Nullable fixed-width byte-slice batches.</summary>
    Fixed = 8,
    /// <summary>Nullable fixed-width INT64 batches.</summary>
    NullableInt64 = 9,
    /// <summary>Required variable-width binary batches.</summary>
    Binary = 10,
    /// <summary>Nullable variable-width binary batches.</summary>
    NullableBinary = 11,
}

/// <summary>Snapshot names for census consumers.</summary>
internal static class CensusConsumerNames
{
    /// <summary>Reports the snapshot name of one census consumer.</summary>
    internal static string SnapshotName(CensusConsumer consumer) => consumer switch
    {
        CensusConsumer.Int32 => "int32",
        CensusConsumer.NullableInt32 => "nullable-int32",
        CensusConsumer.MultiInt32 => "multi-int32",
        CensusConsumer.Boolean => "boolean",
        CensusConsumer.Utf8 => "utf8",
        CensusConsumer.Int64 => "int64",
        CensusConsumer.Float => "float",
        CensusConsumer.Double => "double",
        CensusConsumer.Fixed => "fixed",
        CensusConsumer.NullableInt64 => "nullable-int64",
        CensusConsumer.Binary => "binary",
        CensusConsumer.NullableBinary => "nullable-binary",
        _ => throw new ArgumentOutOfRangeException(nameof(consumer)),
    };
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
    CensusConsumer Consumer)
{
    /// <summary>Describes one frozen catalog lane from its workload token.</summary>
    /// <param name="workload">The catalog workload token.</param>
    /// <returns>The explicit layout of the lane.</returns>
    internal static CensusCaseLayout ForCatalogLane(ScanWorkload workload)
    {
        if (ScanWorkloadCatalog.IsString(workload))
            return new CensusCaseLayout(CensusPhysicalType.Utf8, 0, false, CensusConsumer.Utf8);
        return workload switch
        {
            ScanWorkload.NullableInt32Plain => new CensusCaseLayout(CensusPhysicalType.Int32, sizeof(int), true, CensusConsumer.NullableInt32),
            ScanWorkload.TwoRequiredInt32Plain or ScanWorkload.EightRequiredInt32Plain => new CensusCaseLayout(CensusPhysicalType.Int32, sizeof(int), false, CensusConsumer.MultiInt32),
            ScanWorkload.RequiredInt32Plain or ScanWorkload.RequiredInt32Snappy => new CensusCaseLayout(CensusPhysicalType.Int32, sizeof(int), false, CensusConsumer.Int32),
            _ => throw new InvalidOperationException($"Unknown census catalog workload: {workload}."),
        };
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


/// <summary>One work-census case with its frozen shape.</summary>
/// <param name="Name">The census case name recorded in snapshots.</param>
/// <param name="Workload">The workload token selecting the fixture and consumer.</param>
/// <param name="Consumer">The census consumer that drains the case batches.</param>
/// <param name="PhysicalType">The projected physical type shared by the case columns.</param>
/// <param name="TypeWidthBytes">The byte width of one fixed-width value slot; unused for variable-width layouts.</param>
/// <param name="Nullable">Whether lanes can produce nulls and therefore a validity bitmap.</param>
/// <param name="PassProjections">The projected column ordinals of every expected pass, in pass order.</param>
internal sealed record CensusCatalogCase(
    string Name,
    ScanWorkload Workload,
    CensusConsumer Consumer,
    CensusPhysicalType PhysicalType,
    int TypeWidthBytes,
    bool Nullable,
    IReadOnlyList<IReadOnlyList<int>> PassProjections);

/// <summary>The single frozen work-census catalog.</summary>
/// <remarks>Paired lanes mirror their catalog-lane derivation; diagnostic cases
/// carry explicit literals. The census run reconciles every measured case against
/// this list, and the parity catalog exports it for report reconciliation.</remarks>
internal static class CensusCatalog
{
    internal static IReadOnlyList<CensusCatalogCase> Cases { get; } =
    [
        new CensusCatalogCase("RequiredInt32Plain", ScanWorkload.RequiredInt32Plain, CensusConsumer.Int32, CensusPhysicalType.Int32, sizeof(int), false, [[0], [0]]),
        new CensusCatalogCase("NullableInt32Plain", ScanWorkload.NullableInt32Plain, CensusConsumer.NullableInt32, CensusPhysicalType.Int32, sizeof(int), true, [[0], [0]]),
        new CensusCatalogCase("RequiredInt32Snappy", ScanWorkload.RequiredInt32Snappy, CensusConsumer.Int32, CensusPhysicalType.Int32, sizeof(int), false, [[0], [0]]),
        new CensusCatalogCase("RequiredStringPlain", ScanWorkload.RequiredStringPlain, CensusConsumer.Utf8, CensusPhysicalType.Utf8, 0, false, [[0], [0]]),
        new CensusCatalogCase("RequiredStringSnappy", ScanWorkload.RequiredStringSnappy, CensusConsumer.Utf8, CensusPhysicalType.Utf8, 0, false, [[0], [0]]),
        new CensusCatalogCase("RequiredStringDictionary", ScanWorkload.RequiredStringDictionary, CensusConsumer.Utf8, CensusPhysicalType.Utf8, 0, false, [[0], [0]]),
        new CensusCatalogCase("RequiredStringDictionarySnappy", ScanWorkload.RequiredStringDictionarySnappy, CensusConsumer.Utf8, CensusPhysicalType.Utf8, 0, false, [[0], [0]]),
        new CensusCatalogCase("TwoRequiredInt32Plain", ScanWorkload.TwoRequiredInt32Plain, CensusConsumer.MultiInt32, CensusPhysicalType.Int32, sizeof(int), false, [[0, 1], [1]]),
        new CensusCatalogCase("EightRequiredInt32Plain", ScanWorkload.EightRequiredInt32Plain, CensusConsumer.MultiInt32, CensusPhysicalType.Int32, sizeof(int), false, [[0, 1, 2, 3, 4, 5, 6, 7], [4, 5, 6, 7]]),
        new CensusCatalogCase("UnevenInt32Plain", ScanWorkload.TwoRequiredInt32Plain, CensusConsumer.MultiInt32, CensusPhysicalType.Int32, sizeof(int), false, [[0, 1], [1]]),
        new CensusCatalogCase("NarrowInt32Plain", ScanWorkload.EightRequiredInt32Plain, CensusConsumer.Int32, CensusPhysicalType.Int32, sizeof(int), false, [[0], [1]]),
        new CensusCatalogCase("RequiredInt32RowRange", ScanWorkload.RequiredInt32Plain, CensusConsumer.Int32, CensusPhysicalType.Int32, sizeof(int), false, [[0]]),
        new CensusCatalogCase("SmallRowGroupsInt32Plain", ScanWorkload.RequiredInt32Plain, CensusConsumer.Int32, CensusPhysicalType.Int32, sizeof(int), false, [[0], [0]]),
        new CensusCatalogCase("CompressibleInt32Snappy", ScanWorkload.RequiredInt32Snappy, CensusConsumer.Int32, CensusPhysicalType.Int32, sizeof(int), false, [[0], [0]]),
        new CensusCatalogCase("NullableBooleanPlain", ScanWorkload.NullableBooleanPlain, CensusConsumer.Boolean, CensusPhysicalType.Boolean, 1, true, [[0], [0]]),
        new CensusCatalogCase("LowCardinalityStringDictionary", ScanWorkload.RequiredStringDictionary, CensusConsumer.Utf8, CensusPhysicalType.Utf8, 0, false, [[0], [0]]),
        new CensusCatalogCase("RequiredInt64Plain", ScanWorkload.RequiredInt64Plain, CensusConsumer.Int64, CensusPhysicalType.Int64, sizeof(long), false, [[0], [0]]),
        new CensusCatalogCase("RequiredFloatPlain", ScanWorkload.RequiredFloatPlain, CensusConsumer.Float, CensusPhysicalType.Float, sizeof(float), false, [[0], [0]]),
        new CensusCatalogCase("RequiredDoublePlain", ScanWorkload.RequiredDoublePlain, CensusConsumer.Double, CensusPhysicalType.Double, sizeof(double), false, [[0], [0]]),
        new CensusCatalogCase("NullableInt64Plain", ScanWorkload.NullableInt64Plain, CensusConsumer.NullableInt64, CensusPhysicalType.Int64, sizeof(long), true, [[0], [0]]),
        new CensusCatalogCase("NullableInt32DenseNulls", ScanWorkload.NullableInt32Plain, CensusConsumer.NullableInt32, CensusPhysicalType.Int32, sizeof(int), true, [[0], [0]]),
        new CensusCatalogCase("HighCardinalityStringDictionary", ScanWorkload.RequiredStringDictionary, CensusConsumer.Utf8, CensusPhysicalType.Utf8, 0, false, [[0], [0]]),
        new CensusCatalogCase("RequiredInt32V2", ScanWorkload.RequiredInt32V2, CensusConsumer.Int32, CensusPhysicalType.Int32, sizeof(int), false, [[0], [0]]),
        new CensusCatalogCase("NullableInt32V2", ScanWorkload.NullableInt32V2, CensusConsumer.NullableInt32, CensusPhysicalType.Int32, sizeof(int), true, [[0], [0]]),
        new CensusCatalogCase("NullableBinaryPlain", ScanWorkload.NullableBinaryPlain, CensusConsumer.NullableBinary, CensusPhysicalType.ByteArray, 0, true, [[0], [0]]),
        new CensusCatalogCase("MisalignedMultiPage", ScanWorkload.TwoRequiredInt32Plain, CensusConsumer.MultiInt32, CensusPhysicalType.Int32, sizeof(int), false, [[0, 1], [0, 1]]),
        new CensusCatalogCase("CrcInt64Dictionary", ScanWorkload.CrcInt64Dictionary, CensusConsumer.Int64, CensusPhysicalType.Int64, sizeof(long), false, [[0], [0]]),
        new CensusCatalogCase("CrcBinaryDictionarySnappy", ScanWorkload.CrcBinaryDictionarySnappy, CensusConsumer.Binary, CensusPhysicalType.ByteArray, 0, false, [[1], [1]]),
        new CensusCatalogCase("NullableFixedByteArrayPlain", ScanWorkload.NullableFixedByteArrayPlain, CensusConsumer.Fixed, CensusPhysicalType.FixedLengthByteArray, 4, true, [[0], [0]]),
    ];
}
