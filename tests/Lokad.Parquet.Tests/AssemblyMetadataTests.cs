using System.Reflection;

namespace Lokad.Parquet.Tests;

public sealed class AssemblyMetadataTests
{
    [Fact]
    public void CoreAssemblyHasExpectedName()
    {
        var assembly = Assembly.Load("Lokad.Parquet");

        Assert.Equal("Lokad.Parquet", assembly.GetName().Name);
    }
}
