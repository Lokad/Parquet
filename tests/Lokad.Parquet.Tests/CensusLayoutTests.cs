using System.Reflection;

namespace Lokad.Parquet.Tests;

public sealed class CensusLayoutTests
{
    [Fact]
    public void BoolDenominatorIsSlotsPlusValidity()
    {
        Assert.Equal(9216L, LogicalOutputBytes("Boolean", 1, true, 8192, 1, 0, 0));
    }

    [Fact]
    public void Int32DenominatorsUseFourByteSlots()
    {
        Assert.Equal(262144L, LogicalOutputBytes("Int32", 4, false, 65536, 1, 0, 0));
        Assert.Equal(270336L, LogicalOutputBytes("Int32", 4, true, 65536, 1, 0, 0));
    }

    [Fact]
    public void Int64FloatDoubleDenominatorsUseNativeWidths()
    {
        Assert.Equal(1600L, LogicalOutputBytes("Int64", 8, false, 100, 2, 0, 0));
        Assert.Equal(40L, LogicalOutputBytes("Float", 0, false, 10, 1, 0, 0));
        Assert.Equal(80L, LogicalOutputBytes("Double", 0, false, 10, 1, 0, 0));
    }

    [Fact]
    public void FixedDenominatorsUseDeclaredWidth()
    {
        Assert.Equal(80L, LogicalOutputBytes("FixedLengthByteArray", 16, false, 5, 1, 0, 0));
        Assert.Equal(122L, LogicalOutputBytes("FixedLengthByteArray", 12, true, 10, 1, 0, 0));
    }

    [Fact]
    public void Utf8DenominatorIsPayloadPlusOffsets()
    {
        Assert.Equal(144L, LogicalOutputBytes("Utf8", 0, false, 10, 1, 100, 0));
        Assert.Equal(188L, LogicalOutputBytes("Utf8", 0, false, 10, 2, 100, 0));
    }

    [Fact]
    public void BinaryDenominatorIsPayloadPlusOffsets()
    {
        Assert.Equal(44718L, LogicalOutputBytes("ByteArray", 0, true, 8192, 1, 0, 10922));
        Assert.Equal(12004L, LogicalOutputBytes("ByteArray", 0, false, 1000, 1, 0, 8000));
    }

    [Fact]
    public void CorrectedBooleanGateTripsInsteadOfHiding()
    {
        const long recordedPeak = 58368L;
        const long correctedDenominator = 9216L;
        const long hiddenDenominator = 33792L;
        Assert.True(recordedPeak > 6 * correctedDenominator);
        Assert.True(recordedPeak <= 6 * hiddenDenominator);
        Assert.Equal(9216L, LogicalOutputBytes("Boolean", 1, true, 8192, 1, 0, 0));
    }

    [Fact]
    public void BooleanLaneHasTruthfulIdentity()
    {
        var assembly = BenchmarkReflection.BenchmarkAssembly();
        var workloadType = BenchmarkReflection.RequireType(assembly, "Lokad.Parquet.Benchmarks.ScanWorkload");
        var booleanLane = Enum.Parse(workloadType, "NullableBooleanPlain");
        var catalogType = BenchmarkReflection.RequireType(assembly, "Lokad.Parquet.Benchmarks.ScanWorkloadCatalog");
        var labels = catalogType.GetProperty("Labels", BindingFlags.NonPublic | BindingFlags.Static)?.GetValue(null) as
            System.Collections.IEnumerable ??
            throw new InvalidOperationException("The benchmark workload labels are unavailable.");
        var label = string.Empty;
        foreach (var entry in labels)
        {
            var entryType = entry.GetType();
            var key = entryType.GetProperty("Key")?.GetValue(entry);
            if (Equals(key, booleanLane))
                label = entryType.GetProperty("Value")?.GetValue(entry) as string ?? string.Empty;
        }

        Assert.Contains("BOOLEAN", label, StringComparison.OrdinalIgnoreCase);
        var isString = BenchmarkReflection.RequireStaticMethod(catalogType, "IsString", null);
        Assert.False(Assert.IsType<bool>(isString.Invoke(null, [booleanLane])));
        var columnCount = BenchmarkReflection.RequireStaticMethod(catalogType, "GetColumnCount", null);
        Assert.Equal(1, Assert.IsType<int>(columnCount.Invoke(null, [booleanLane])));
        var parity = catalogType.GetProperty("ParityWorkloads", BindingFlags.NonPublic | BindingFlags.Static)?.GetValue(null) as
            System.Collections.IEnumerable ??
            throw new InvalidOperationException("The benchmark parity catalog is unavailable.");
        foreach (var entry in parity)
            Assert.NotEqual(booleanLane, entry);
    }


    [Fact]
    public void UnknownWorkloadHasNoCatalogLane()
    {
        // B04: unfamiliar workloads fail instead of silently defaulting to
        // required INT32, where a wrong denominator could hide.
        var assembly = BenchmarkReflection.BenchmarkAssembly();
        var layoutType = BenchmarkReflection.RequireType(assembly, "Lokad.Parquet.Benchmarks.CensusCaseLayout");
        var workloadType = BenchmarkReflection.RequireType(assembly, "Lokad.Parquet.Benchmarks.ScanWorkload");
        var derive = BenchmarkReflection.RequireStaticMethod(layoutType, "ForCatalogLane", null);
        var bogus = Enum.ToObject(workloadType, 999);
        var thrown = Assert.Throws<TargetInvocationException>(() => derive.Invoke(null, [bogus]));
        Assert.IsType<InvalidOperationException>(thrown.InnerException);
    }
    [Fact]
    public void StaticCatalogMatchesVerifierCensusShapes()
    {
        // B04: the single C# catalog agrees with the independently authored
        // verifier shapes on names, workloads, consumers, layouts and passes.
        var assembly = BenchmarkReflection.BenchmarkAssembly();
        var catalogType = BenchmarkReflection.RequireType(assembly, "Lokad.Parquet.Benchmarks.CensusCatalog");
        var cases = catalogType.GetProperty("Cases", BindingFlags.NonPublic | BindingFlags.Static)?.GetValue(null) as System.Collections.IEnumerable ??
            throw new InvalidOperationException("The benchmark census catalog is empty.");
        var namesType = BenchmarkReflection.RequireType(assembly, "Lokad.Parquet.Benchmarks.CensusConsumerNames");
        var snapshotName = BenchmarkReflection.RequireStaticMethod(namesType, "SnapshotName", null);
        var layoutType = BenchmarkReflection.RequireType(assembly, "Lokad.Parquet.Benchmarks.CensusLayout");
        var nameOf = BenchmarkReflection.RequireStaticMethod(layoutType, "NameOf", null);
        var matched = 0;
        foreach (var entry in cases)
        {
            var entryType = entry.GetType();
            var name = Assert.IsType<string>(entryType.GetProperty("Name")?.GetValue(entry));
            var expected = FindDescriptor(name);
            Assert.Equal(expected.Workload, entryType.GetProperty("Workload")?.GetValue(entry)?.ToString());
            var consumer = entryType.GetProperty("Consumer")?.GetValue(entry) ??
                throw new InvalidOperationException($"The static catalog case '{name}' has no consumer.");
            Assert.Equal(expected.Consumer, Assert.IsType<string>(snapshotName.Invoke(null, [consumer])));
            var physical = entryType.GetProperty("PhysicalType")?.GetValue(entry) ??
                throw new InvalidOperationException($"The static catalog case '{name}' has no physical type.");
            Assert.Equal(expected.PhysicalType, Assert.IsType<string>(nameOf.Invoke(null, [physical])));
            Assert.Equal(expected.ValueWidthBytes, Assert.IsType<int>(entryType.GetProperty("TypeWidthBytes")?.GetValue(entry)));
            Assert.Equal(expected.Nullable, Assert.IsType<bool>(entryType.GetProperty("Nullable")?.GetValue(entry)));
            var projections = Assert.IsAssignableFrom<System.Collections.Generic.IEnumerable<object>>(entryType.GetProperty("PassProjections")?.GetValue(entry));
            var actual = projections.Select(static projection => Assert.IsAssignableFrom<System.Collections.Generic.IEnumerable<int>>(projection).ToArray()).ToArray();
            Assert.Equal(expected.Passes.Length, actual.Length);
            for (var pass = 0; pass < actual.Length; pass++)
                Assert.Equal(expected.Passes[pass].Projection, actual[pass]);
            matched++;
        }

        Assert.Equal(BenchmarkReportQuartet.CensusDiagnosticDescriptors.Length + 1, matched);

        static (string Workload, string Consumer, string PhysicalType, int ValueWidthBytes, bool Nullable, (int[] Projection, int Target)[] Passes) FindDescriptor(string name)
        {
            foreach (var descriptor in BenchmarkReportQuartet.CensusDiagnosticDescriptors)
            {
                if (descriptor.Name == name)
                    return (descriptor.Workload, descriptor.Consumer, descriptor.PhysicalType, descriptor.ValueWidthBytes, descriptor.Nullable, descriptor.Passes);
            }

            if (name == "RequiredInt32Plain")
                return ("RequiredInt32Plain", "int32", "int32", 4, false, [(new[] { 0 }, 65536), (new[] { 0 }, 4096)]);
            throw new InvalidOperationException($"The static catalog carries an unexpected case '{name}'.");
        }
    }
    [Fact]
    public void CatalogLaneLayoutsDeriveFromWorkloadTokens()
    {
        var assembly = BenchmarkReflection.BenchmarkAssembly();
        var layoutType = BenchmarkReflection.RequireType(assembly, "Lokad.Parquet.Benchmarks.CensusCaseLayout");
        var workloadType = BenchmarkReflection.RequireType(assembly, "Lokad.Parquet.Benchmarks.ScanWorkload");
        var derive = BenchmarkReflection.RequireStaticMethod(layoutType, "ForCatalogLane", null);
        var nullableInt32 = derive.Invoke(null, [Enum.Parse(workloadType, "NullableInt32Plain")]) ??
            throw new InvalidOperationException("The benchmark layout derivation returned nothing.");
        Assert.Equal("Int32", layoutType.GetProperty("PhysicalType")?.GetValue(nullableInt32)?.ToString());
        Assert.True(Assert.IsType<bool>(layoutType.GetProperty("Nullable")?.GetValue(nullableInt32)));
        var requiredInt32 = derive.Invoke(null, [Enum.Parse(workloadType, "RequiredInt32Plain")]) ??
            throw new InvalidOperationException("The benchmark layout derivation returned nothing.");
        Assert.False(Assert.IsType<bool>(layoutType.GetProperty("Nullable")?.GetValue(requiredInt32)));
        var consumer = layoutType.GetProperty("Consumer")?.GetValue(requiredInt32) ?? throw new InvalidOperationException("The benchmark layout derivation returned nothing.");
        Assert.Equal("Int32", consumer.ToString());
        var names = BenchmarkReflection.RequireType(assembly, "Lokad.Parquet.Benchmarks.CensusConsumerNames");
        var snapshot = BenchmarkReflection.RequireStaticMethod(names, "SnapshotName", null);
        Assert.Equal("int32", Assert.IsType<string>(snapshot.Invoke(null, [consumer])));
    }

    [Fact]
    public void SnapshotRecordsLayoutDimensions()
    {
        var assembly = BenchmarkReflection.BenchmarkAssembly();
        var snapshotCase = BenchmarkReflection.RequireType(assembly, "Lokad.Parquet.Benchmarks.WorkCensusCase");
        foreach (var property in new[] { "PhysicalType", "ValueWidthBytes", "Nullable", "Consumer", "RowRangeStart", "RowRangeCount" })
            Assert.NotNull(snapshotCase.GetProperty(property));
        Assert.Equal(typeof(bool), snapshotCase.GetProperty("Nullable")?.PropertyType);
        var pass = BenchmarkReflection.RequireType(assembly, "Lokad.Parquet.Benchmarks.CensusPassMeasurement");
        Assert.NotNull(pass.GetProperty("Projection"));
        Assert.NotNull(pass.GetProperty("Target"));
    }

    private static long LogicalOutputBytes(string physicalType, int typeWidthBytes, bool nullable, int rowCount, int columnCount, int utf8PayloadBytes, int binaryPayloadBytes)
    {
        var assembly = BenchmarkReflection.BenchmarkAssembly();
        var layout = BenchmarkReflection.RequireType(assembly, "Lokad.Parquet.Benchmarks.CensusLayout");
        var physical = BenchmarkReflection.RequireType(assembly, "Lokad.Parquet.Benchmarks.CensusPhysicalType");
        var method = BenchmarkReflection.RequireStaticMethod(layout, "LogicalOutputBytes", null);
        return Assert.IsType<long>(method.Invoke(null, [Enum.Parse(physical, physicalType), typeWidthBytes, nullable, rowCount, columnCount, utf8PayloadBytes, binaryPayloadBytes]));
    }
}


