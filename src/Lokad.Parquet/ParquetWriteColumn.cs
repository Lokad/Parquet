namespace Lokad.Parquet;

/// <summary>Describes one named, required column in a flat writer schema.</summary>
public sealed class ParquetWriteColumn
{
    /// <summary>Creates a column with the given case-sensitive name and physical type.</summary>
    /// <remarks>The writer annotates Int32 columns as DATE and ByteArray columns as STRING.</remarks>
    public ParquetWriteColumn(string name, ParquetPhysicalType physicalType)
    {
        Name = name;
        PhysicalType = physicalType;
    }

    /// <summary>Gets the column name.</summary>
    public string Name { get; }

    /// <summary>Gets the physical storage type.</summary>
    public ParquetPhysicalType PhysicalType { get; }
}
