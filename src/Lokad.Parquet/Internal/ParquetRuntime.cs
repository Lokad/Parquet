namespace Lokad.Parquet.Internal;

internal static class ParquetRuntime
{
    public const string ForceScalarSwitch = "Lokad.Parquet.ForceScalar";

    public static bool ForceScalar =>
        AppContext.TryGetSwitch(ForceScalarSwitch, out var enabled) && enabled;
}
