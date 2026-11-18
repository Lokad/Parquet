namespace Lokad.Parquet.Tests;

internal static class CensusCatalogFixture
{
    internal sealed record Case(string Name, string Workload, string PhysicalType,
        int ValueWidthBytes, bool Nullable, string Consumer, int[][] PassProjections);

    internal static readonly Case[] Cases =
    [
        new("RequiredInt32Plain", "RequiredInt32Plain", "int32", 4, false, "int32", [[0], [0]]),
        new("UnevenInt32Plain", "TwoRequiredInt32Plain", "int32", 4, false, "multi-int32", [[0, 1], [1]]),
        new("NarrowInt32Plain", "EightRequiredInt32Plain", "int32", 4, false, "int32", [[0], [1]]),
        new("RequiredInt32RowRange", "RequiredInt32Plain", "int32", 4, false, "int32", [[0]]),
        new("SmallRowGroupsInt32Plain", "RequiredInt32Plain", "int32", 4, false, "int32", [[0], [0]]),
        new("CompressibleInt32Snappy", "RequiredInt32Snappy", "int32", 4, false, "int32", [[0], [0]]),
        new("NullableBooleanPlain", "NullableBooleanPlain", "boolean", 1, true, "boolean", [[0], [0]]),
        new("LowCardinalityStringDictionary", "RequiredStringDictionary", "utf8", 0, false, "utf8", [[0], [0]]),
        new("RequiredInt64Plain", "RequiredInt64Plain", "int64", 8, false, "int64", [[0], [0]]),
        new("RequiredFloatPlain", "RequiredFloatPlain", "float", 4, false, "float", [[0], [0]]),
        new("RequiredDoublePlain", "RequiredDoublePlain", "double", 8, false, "double", [[0], [0]]),
        new("NullableInt64Plain", "NullableInt64Plain", "int64", 8, true, "nullable-int64", [[0], [0]]),
        new("NullableFixedByteArrayPlain", "NullableFixedByteArrayPlain", "fixed", 4, true, "fixed", [[0], [0]]),
        new("NullableBinaryPlain", "NullableBinaryPlain", "binary", 0, true, "nullable-binary", [[0], [0]]),
        new("RequiredInt32V2", "RequiredInt32V2", "int32", 4, false, "int32", [[0], [0]]),
        new("NullableInt32V2", "NullableInt32V2", "int32", 4, true, "nullable-int32", [[0], [0]]),
        new("HighCardinalityStringDictionary", "RequiredStringDictionary", "utf8", 0, false, "utf8", [[0], [0]]),
        new("NullableInt32DenseNulls", "NullableInt32Plain", "int32", 4, true, "nullable-int32", [[0], [0]]),
        new("CrcInt64Dictionary", "CrcInt64Dictionary", "int64", 8, false, "int64", [[0], [0]]),
        new("CrcBinaryDictionarySnappy", "CrcBinaryDictionarySnappy", "binary", 0, false, "binary", [[1], [1]]),
        new("MisalignedMultiPage", "TwoRequiredInt32Plain", "int32", 4, false, "multi-int32", [[0, 1], [0, 1]]),
        new("NullableInt32Plain", "NullableInt32Plain", "int32", 4, true, "nullable-int32", [[0], [0]]),
        new("RequiredInt32Snappy", "RequiredInt32Snappy", "int32", 4, false, "int32", [[0], [0]]),
        new("RequiredStringPlain", "RequiredStringPlain", "utf8", 0, false, "utf8", [[0], [0]]),
        new("RequiredStringSnappy", "RequiredStringSnappy", "utf8", 0, false, "utf8", [[0], [0]]),
        new("RequiredStringDictionary", "RequiredStringDictionary", "utf8", 0, false, "utf8", [[0], [0]]),
        new("RequiredStringDictionarySnappy", "RequiredStringDictionarySnappy", "utf8", 0, false, "utf8", [[0], [0]]),
        new("TwoRequiredInt32Plain", "TwoRequiredInt32Plain", "int32", 4, false, "multi-int32", [[0, 1], [1]]),
        new("EightRequiredInt32Plain", "EightRequiredInt32Plain", "int32", 4, false, "multi-int32", [[0, 1, 2, 3, 4, 5, 6, 7], [4, 5, 6, 7]]),
    ];
}
