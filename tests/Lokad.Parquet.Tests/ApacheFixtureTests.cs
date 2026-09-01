using System.Buffers.Binary;
using System.Security.Cryptography;

namespace Lokad.Parquet.Tests;

public sealed class ApacheFixtureTests
{
    public static TheoryData<string, int, string[]> AllTypeFixtures => new()
    {
        {
            "alltypes_plain.parquet",
            8,
            [
                "ced85c280a221fe8ce042728e1c485c22be70cac20a1d6243b697b3235d99406",
                "5abc4aa3f4ec6acef614162cd49332c32477ed78e30ab3b894ab9a1cdce55fbd",
                "a479ed2b4bb75fa2ed2f4da4f22791d1c77fc99e14aaecad307cfda116133e47",
                "a479ed2b4bb75fa2ed2f4da4f22791d1c77fc99e14aaecad307cfda116133e47",
                "a479ed2b4bb75fa2ed2f4da4f22791d1c77fc99e14aaecad307cfda116133e47",
                "c898d79ab43eed8399420bda8db5f88857bedd001a015620a35864933d1af7e6",
                "0ec8707d48fa3d3f68e3dda6861ecd33622dec1876beab7ea26525a3e8e41483",
                "cefe8519f761e60ca7635077d5b78b4e351243a26932a084d1106358d0e267cc",
                "99b521b3637d4cdc4352df9cb089fe7997e6b4b760f58f49b78c6c7977686ca5",
                "c9ecd3f6719c419bd1cba74a142f0f422b388a44da622a6723541dea795c0605",
            ]
        },
        {
            "alltypes_dictionary.parquet",
            2,
            [
                "f6e682693946efd95436dc72ca0613a30801f9844a80aedebca97d34d2e6cb6d",
                "b4276eb61c86fba4d7a62dd0bbde0a08f3c445407397ce79f7a8d10046c41fd9",
                "f6e682693946efd95436dc72ca0613a30801f9844a80aedebca97d34d2e6cb6d",
                "f6e682693946efd95436dc72ca0613a30801f9844a80aedebca97d34d2e6cb6d",
                "f6e682693946efd95436dc72ca0613a30801f9844a80aedebca97d34d2e6cb6d",
                "79ee6f1672e863375c2f7c40df4d569371a8007c91df2757b64e8d4fbaaf9695",
                "079a7a3e5eb581c76a768ee115c9a47a5ac22207073e689c86fe1cbb8d69a01b",
                "e42eeb6d2bdf78e1cae43272e2b950b8d44f688a64c4ad160d9ea008994a10e0",
                "926655e1c55ab752b3eccd1d82cf251bd13648ca496391e4aef2db02b7edff9f",
                "2d7f2d0973086d630bffbb8069112a4e828eccf43850efe06d9925cf480de5f9",
            ]
        },
    };

    [Theory]
    [InlineData(
        "rle-dict-snappy-checksum.parquet",
        1000,
        "b4e1c8ce8ea209fb64ee37db3c5b356b0952a756d919b5b31f69e1dc49087cf2",
        "7466464aa99cfabca1449d81db1f859a10208da4e72ba5b5d12df8ecf4f42a59")]
    [InlineData(
        "plain-dict-uncompressed-checksum.parquet",
        1000,
        "b4e1c8ce8ea209fb64ee37db3c5b356b0952a756d919b5b31f69e1dc49087cf2",
        "d9a68cf545cad4092b4330216b8a7a3dc7d9c093ad3eb08783ce8c1cae17f3b9")]
    public async Task DictionaryAndChecksumFixturesMatchIndependentValueHashes(
        string name,
        int expectedRows,
        string expectedInt64Hash,
        string expectedBinaryHash)
    {
        await using var file = await ParquetFile.OpenAsync(GetFixturePath(name));

        Assert.Equal(expectedRows, file.Metadata.RowCount);
        Assert.Equal(2, file.Metadata.Schema.Columns.Count);
        Assert.Equal(expectedInt64Hash, await HashColumnAsync(file, 0));
        Assert.Equal(expectedBinaryHash, await HashColumnAsync(file, 1));
    }

    [Fact]
    public async Task OptionalBinaryFixtureMatchesIndependentValueHash()
    {
        await using var file = await ParquetFile.OpenAsync(GetFixturePath("binary.parquet"));

        Assert.Equal(12, file.Metadata.RowCount);
        Assert.Single(file.Metadata.Schema.Columns);
        Assert.Equal(
            "400b01af12b728f2251c72e17688d9f3471d330f4b3dc9d3efd215f46ab76f1d",
            await HashColumnAsync(file, 0));
    }

    [Theory]
    [MemberData(nameof(AllTypeFixtures))]
    public async Task AllCorePrimitiveTypesMatchIndependentValueHashesBesideUnsupportedInt96(
        string name,
        int expectedRows,
        string[] expectedHashes)
    {
        await using var file = await ParquetFile.OpenAsync(GetFixturePath(name));
        var columns = file.Metadata.Schema.Columns;

        Assert.Equal(expectedRows, file.Metadata.RowCount);
        Assert.Equal(11, columns.Count);
        Assert.Equal(
            [
                ParquetPhysicalType.Int32,
                ParquetPhysicalType.Boolean,
                ParquetPhysicalType.Int32,
                ParquetPhysicalType.Int32,
                ParquetPhysicalType.Int32,
                ParquetPhysicalType.Int64,
                ParquetPhysicalType.Float,
                ParquetPhysicalType.Double,
                ParquetPhysicalType.ByteArray,
                ParquetPhysicalType.ByteArray,
            ],
            columns.Take(10).Select(static column => column.SchemaElement.PhysicalType));
        Assert.All(columns.Take(10), static column => Assert.True(column.IsReadable));
        Assert.Equal(ParquetPhysicalType.Int96, columns[10].SchemaElement.PhysicalType);
        Assert.False(columns[10].IsReadable);

        for (var ordinal = 0; ordinal < expectedHashes.Length; ordinal++)
            Assert.Equal(expectedHashes[ordinal], await HashColumnAsync(file, ordinal));
    }

    [Fact]
    public async Task FixedLengthByteArrayMatchesIndependentValueHash()
    {
        await using var file = await ParquetFile.OpenAsync(GetFixturePath("fixed_length_byte_array.parquet"));

        Assert.Equal(1000, file.Metadata.RowCount);
        Assert.Equal(
            "ccef1cbacb37a62e63bd7be4128dc95a1808f97b2f1d7eed79755d916bcc9abd",
            await HashColumnAsync(file, 0));
    }

    private static async Task<string> HashColumnAsync(ParquetFile file, int ordinal)
    {
        using var hash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        var lengthBytes = new byte[4];
        var int64Bytes = new byte[sizeof(long)];
        await foreach (var batch in file.ScanAsync(
            new ParquetScanOptions([file.Metadata.Schema.Columns[ordinal]])))
        {
            using (batch)
            {
                var column = batch.Columns[0];
                for (var row = 0; row < batch.RowCount; row++)
                {
                    if (!column.Validity.IsValid(row))
                    {
                        hash.AppendData([0]);
                        continue;
                    }

                    hash.AppendData([1]);
                    if (column is ParquetPrimitiveColumnBatch<bool> booleans)
                    {
                        BinaryPrimitives.WriteInt32LittleEndian(lengthBytes, 1);
                        hash.AppendData(lengthBytes);
                        hash.AppendData([booleans.Values.Span[row] ? (byte)1 : (byte)0]);
                    }
                    else if (column is ParquetPrimitiveColumnBatch<int> int32Values)
                    {
                        BinaryPrimitives.WriteInt32LittleEndian(lengthBytes, sizeof(int));
                        hash.AppendData(lengthBytes);
                        BinaryPrimitives.WriteInt32LittleEndian(lengthBytes, int32Values.Values.Span[row]);
                        hash.AppendData(lengthBytes);
                    }
                    else if (column is ParquetPrimitiveColumnBatch<long> int64Values)
                    {
                        BinaryPrimitives.WriteInt32LittleEndian(lengthBytes, sizeof(long));
                        hash.AppendData(lengthBytes);
                        BinaryPrimitives.WriteInt64LittleEndian(int64Bytes, int64Values.Values.Span[row]);
                        hash.AppendData(int64Bytes);
                    }
                    else if (column is ParquetPrimitiveColumnBatch<float> singleValues)
                    {
                        BinaryPrimitives.WriteInt32LittleEndian(lengthBytes, sizeof(float));
                        hash.AppendData(lengthBytes);
                        BinaryPrimitives.WriteInt32LittleEndian(
                            lengthBytes,
                            BitConverter.SingleToInt32Bits(singleValues.Values.Span[row]));
                        hash.AppendData(lengthBytes);
                    }
                    else if (column is ParquetPrimitiveColumnBatch<double> doubleValues)
                    {
                        BinaryPrimitives.WriteInt32LittleEndian(lengthBytes, sizeof(double));
                        hash.AppendData(lengthBytes);
                        BinaryPrimitives.WriteInt64LittleEndian(
                            int64Bytes,
                            BitConverter.DoubleToInt64Bits(doubleValues.Values.Span[row]));
                        hash.AppendData(int64Bytes);
                    }
                    else if (column is ParquetBinaryColumnBatch binary)
                    {
                        var offsets = binary.Offsets.Span;
                        var bytes = binary.Payload.Span[offsets[row]..offsets[row + 1]];
                        BinaryPrimitives.WriteInt32LittleEndian(lengthBytes, bytes.Length);
                        hash.AppendData(lengthBytes);
                        hash.AppendData(bytes);
                    }
                    else if (column is ParquetFixedLengthByteArrayColumnBatch fixedBytes)
                    {
                        var bytes = fixedBytes.Payload.Span.Slice(
                            checked(row * fixedBytes.TypeWidth),
                            fixedBytes.TypeWidth);
                        BinaryPrimitives.WriteInt32LittleEndian(lengthBytes, bytes.Length);
                        hash.AppendData(lengthBytes);
                        hash.AppendData(bytes);
                    }
                    else
                    {
                        throw new InvalidOperationException("The Apache fixture has an unexpected physical view.");
                    }
                }
            }
        }
        return Convert.ToHexStringLower(hash.GetHashAndReset());
    }

    private static string GetFixturePath(string name) => Path.Combine(
        RepositoryTestPaths.Root,
        "tests",
        "fixtures",
        "apache-parquet-testing",
        name);
}
