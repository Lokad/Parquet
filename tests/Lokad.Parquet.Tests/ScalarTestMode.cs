namespace Lokad.Parquet.Tests;

// CI scalar leg: when LOKAD_PARQUET_FORCE_SCALAR is 1, every decoder in this
// process takes its forced-scalar lane, so the same suite executes both
// supported modes. test.ps1 -ForceScalar sets it; it is never set by default.
internal static class ScalarTestMode
{
    [System.Runtime.CompilerServices.ModuleInitializer]
    internal static void Initialize()
    {
        if (string.Equals(
            System.Environment.GetEnvironmentVariable("LOKAD_PARQUET_FORCE_SCALAR"),
            "1",
            System.StringComparison.Ordinal))
        {
            System.AppContext.SetSwitch("Lokad.Parquet.ForceScalar", true);
        }
    }

    // Lane-forcing scope for scalar/vector equivalence facts: forces the
    // scalar lane and returns the ambient process mode, so the finally block
    // restores vector mode by default and preserves the CI scalar leg.
    internal static bool BeginForcedScalar()
    {
        var forced = System.AppContext.TryGetSwitch("Lokad.Parquet.ForceScalar", out var enabled) && enabled;
        System.AppContext.SetSwitch("Lokad.Parquet.ForceScalar", true);
        return forced;
    }

    internal static void EndForcedScalar(bool forced) =>
        System.AppContext.SetSwitch("Lokad.Parquet.ForceScalar", forced);
}
