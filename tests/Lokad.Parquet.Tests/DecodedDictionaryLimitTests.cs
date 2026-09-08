namespace Lokad.Parquet.Tests;

public sealed class DecodedDictionaryLimitTests
{
    [Fact]
    public async Task BooleanDecodedBytesRespectDictionaryLimit()
    {
        var bytes = ParquetFixtureBuilder.CreateInt32(new() { PhysicalTypeCode = (int)ParquetPhysicalType.Boolean, PhysicalValues = new bool[] { false }, DictionaryValues = new bool[16], DictionaryIndices = [0] });
        await Assert.ThrowsAsync<ParquetLimitExceededException>(async () =>
        {
            await using var file = await ParquetFile.OpenAsync(bytes, new() { MaximumDictionaryBytes = 2 }, CancellationToken.None);
            await foreach (var batch in file.ScanAsync(new([file.Metadata.Schema.Columns[0]])))
            {
                batch.Dispose();
            }
        });
    }

    [Fact]
    public async Task BooleanDecodedBytesAtExactLimitOpens()
    {
        var bytes = ParquetFixtureBuilder.CreateInt32(new() { PhysicalTypeCode = (int)ParquetPhysicalType.Boolean, PhysicalValues = new bool[] { false }, DictionaryValues = new bool[16], DictionaryIndices = [0] });
        await using var file = await ParquetFile.OpenAsync(bytes, new() { MaximumDictionaryBytes = 16 }, CancellationToken.None);
        await foreach (var batch in file.ScanAsync(new([file.Metadata.Schema.Columns[0]])))
        {
            batch.Dispose();
        }
    }

    [Fact]
    public async Task BinaryDecodedBytesRespectDictionaryLimit()
    {
        // Four 3-byte entries: 28 serialized bytes but 32 decoded bytes
        // ((4 + 1) offsets plus payload). A limit of 31 passes the page-header
        // check yet must fail the decoded-layout check.
        var bytes = ParquetFixtureBuilder.CreateInt32(new()
        {
            PhysicalTypeCode = (int)ParquetPhysicalType.ByteArray,
            PhysicalValues = new byte[][] { [10], [20], [30], [40] },
            DictionaryValues = new byte[][] { [1, 2, 3], [4, 5, 6], [7, 8, 9], [10, 11, 12] },
            DictionaryIndices = [0, 1, 2, 3],
        });
        await Assert.ThrowsAsync<ParquetLimitExceededException>(async () =>
        {
            await using var file = await ParquetFile.OpenAsync(bytes, new() { MaximumDictionaryBytes = 31 }, CancellationToken.None);
            await foreach (var batch in file.ScanAsync(new([file.Metadata.Schema.Columns[0]])))
            {
                batch.Dispose();
            }
        });
    }

    [Fact]
    public async Task BinaryDecodedBytesAtExactLimitOpens()
    {
        var bytes = ParquetFixtureBuilder.CreateInt32(new()
        {
            PhysicalTypeCode = (int)ParquetPhysicalType.ByteArray,
            PhysicalValues = new byte[][] { [10], [20], [30], [40] },
            DictionaryValues = new byte[][] { [1, 2, 3], [4, 5, 6], [7, 8, 9], [10, 11, 12] },
            DictionaryIndices = [0, 1, 2, 3],
        });
        await using var file = await ParquetFile.OpenAsync(bytes, new() { MaximumDictionaryBytes = 32 }, CancellationToken.None);
        await foreach (var batch in file.ScanAsync(new([file.Metadata.Schema.Columns[0]])))
        {
            batch.Dispose();
        }
    }

    [Fact]
    public async Task FixedDictionaryAtExactLimitOpens()
    {
        // Fixed dictionaries decode to their serialized size (2 entries of width 2
        // occupy 4 bytes), so the page-header check covers the decoded layout.
        var bytes = ParquetFixtureBuilder.CreateInt32(new()
        {
            PhysicalTypeCode = (int)ParquetPhysicalType.FixedLengthByteArray,
            TypeLength = 2,
            PhysicalValues = new byte[][] { [3, 4], [1, 2] },
            DictionaryValues = new byte[][] { [1, 2], [3, 4] },
            DictionaryIndices = [1, 0],
        });
        await using var file = await ParquetFile.OpenAsync(bytes, new() { MaximumDictionaryBytes = 4 }, CancellationToken.None);
        await foreach (var batch in file.ScanAsync(new([file.Metadata.Schema.Columns[0]])))
        {
            batch.Dispose();
        }
    }

    [Fact]
    public async Task FixedDictionaryBelowLimitIsRejected()
    {
        var bytes = ParquetFixtureBuilder.CreateInt32(new()
        {
            PhysicalTypeCode = (int)ParquetPhysicalType.FixedLengthByteArray,
            TypeLength = 2,
            PhysicalValues = new byte[][] { [3, 4], [1, 2] },
            DictionaryValues = new byte[][] { [1, 2], [3, 4] },
            DictionaryIndices = [1, 0],
        });
        await Assert.ThrowsAsync<ParquetLimitExceededException>(async () =>
        {
            await using var file = await ParquetFile.OpenAsync(bytes, new() { MaximumDictionaryBytes = 3 }, CancellationToken.None);
            await foreach (var batch in file.ScanAsync(new([file.Metadata.Schema.Columns[0]])))
            {
                batch.Dispose();
            }
        });
    }
}
