namespace Lokad.Parquet.Tests;

using System.Reflection;

public sealed class DefinitionLevelCodecTests
{
    [Fact]
    public void V2LevelByteCountSumsSections()
    {
        var codec = ResolveCodec();
        var method = ResolveMethod(codec, "GetV2LevelByteCount");
        var v2 = CreateV2(definitionLevelsByteLength: 3, repetitionLevelsByteLength: 4);
        var location = CreateLocation(codec.Assembly);
        var result = method.Invoke(null, [v2, location]);
        Assert.Equal(7, Assert.IsType<int>(result));
    }

    [Fact]
    public void V2LevelByteCountOverflowIsMalformedWithCallerLocation()
    {
        var codec = ResolveCodec();
        var method = ResolveMethod(codec, "GetV2LevelByteCount");
        var v2 = CreateV2(definitionLevelsByteLength: 1, repetitionLevelsByteLength: int.MaxValue);
        var location = CreateLocation(codec.Assembly);
        var exception = Assert.Throws<TargetInvocationException>(() => method.Invoke(null, [v2, location]));
        var format = Assert.IsType<ParquetFormatException>(exception.InnerException);
        Assert.Equal(11L, format.ByteOffset);
        Assert.Equal(2, format.RowGroupOrdinal);
        Assert.Equal(3, format.ColumnOrdinal);
        Assert.Equal(5, format.PageOrdinal);
    }

    private static Type ResolveCodec()
    {
        var assembly = typeof(ParquetFile).Assembly;
        return assembly.GetType("Lokad.Parquet.Internal.DefinitionLevelCodec") ??
            throw new InvalidOperationException("DefinitionLevelCodec was not found.");
    }

    private static MethodInfo ResolveMethod(Type codec, string name)
    {
        return codec.GetMethod(name, BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Static) ??
            throw new InvalidOperationException($"{name} was not found.");
    }

    private static object CreateV2(int definitionLevelsByteLength, int repetitionLevelsByteLength)
    {
        var assembly = typeof(ParquetFile).Assembly;
        var type = assembly.GetType("Lokad.Parquet.Internal.ValidatedDataPageV2") ??
            throw new InvalidOperationException("ValidatedDataPageV2 was not found.");
        return Activator.CreateInstance(
            type,
            10,
            0,
            10,
            0,
            definitionLevelsByteLength,
            repetitionLevelsByteLength,
            true) ?? throw new InvalidOperationException("ValidatedDataPageV2 could not be created.");
    }

    private static object CreateLocation(Assembly assembly)
    {
        var type = assembly.GetType("Lokad.Parquet.ParquetErrorLocation") ??
            throw new InvalidOperationException("ParquetErrorLocation was not found.");
        var factory = type.GetMethod("AtPage", BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Static) ??
            throw new InvalidOperationException("AtPage was not found.");
        return factory.Invoke(null, [11L, 2, 3, 5]) ??
            throw new InvalidOperationException("A page location could not be created.");
    }
}
