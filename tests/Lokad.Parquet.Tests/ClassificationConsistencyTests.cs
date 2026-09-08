namespace Lokad.Parquet.Tests;

public sealed class ClassificationConsistencyTests
{
    [Theory]
    [InlineData((int)ParquetPhysicalType.Int32)]
    [InlineData((int)ParquetPhysicalType.ByteArray)]
    public async Task ShortV1LevelsAreMalformedNotUnsupported(int physicalTypeCode)
    {
        byte[] bytes = physicalTypeCode == (int)ParquetPhysicalType.ByteArray
            ? ParquetFixtureBuilder.CreateInt32(new() { PhysicalTypeCode = (int)ParquetPhysicalType.ByteArray, PhysicalValues = new byte[][] { [1], [2], [3] }, Repetition = ParquetRepetition.Optional, Validity = [true, false, true], PageHeaderOverrides = new() { V1DefinitionLevelByteLength = -1 } })
            : ParquetFixtureBuilder.CreateInt32(new() { Values = [1, 0, 2], Repetition = ParquetRepetition.Optional, Validity = [true, false, true], PageHeaderOverrides = new() { V1DefinitionLevelByteLength = -1 } });
        await using var file = await ParquetFile.OpenAsync(new MemoryStream(bytes, writable: false));
        var exception = await Assert.ThrowsAsync<ParquetFormatException>(async () =>
        {
            await foreach (var batch in file.ScanAsync(new([file.Metadata.Schema.Columns[0]])))
            {
                batch.Dispose();
            }
        });
        Assert.NotNull(exception.ByteOffset);
        Assert.Equal(0, exception.RowGroupOrdinal);
        Assert.Equal(0, exception.ColumnOrdinal);
    }

    [Fact]
    public async Task WrongDefinitionEncodingIsUnsupported()
    {
        var bytes = ParquetFixtureBuilder.CreateInt32(new() { Values = [1, 0, 2], Repetition = ParquetRepetition.Optional, Validity = [true, false, true], PageHeaderOverrides = new() { DefinitionEncodingCode = 99 } });
        await using var file = await ParquetFile.OpenAsync(new MemoryStream(bytes, writable: false));
        await Assert.ThrowsAsync<ParquetUnsupportedFeatureException>(async () =>
        {
            await foreach (var batch in file.ScanAsync(new([file.Metadata.Schema.Columns[0]])))
            {
                batch.Dispose();
            }
        });
    }

    [Fact]
    public async Task OversizedHeaderBeyondLimitIsLimitExceeded()
    {
        var bytes = ParquetFixtureBuilder.CreateInt32(new() { Values = [1], PageHeaderOverrides = new() { HeaderPaddingBytes = 512 } });
        await using var file = await ParquetFile.OpenAsync(new MemoryStream(bytes, writable: false), ParquetSourceOwnership.Caller, new ParquetReaderOptions { MaximumPageHeaderBytes = 128 }, CancellationToken.None);
        var exception = await Assert.ThrowsAsync<ParquetLimitExceededException>(async () =>
        {
            await foreach (var batch in file.ScanAsync(new([file.Metadata.Schema.Columns[0]])))
            {
                batch.Dispose();
            }
        });
        Assert.NotNull(exception.ByteOffset);
    }

    [Fact]
    public async Task TruncatedHeaderAtChunkEndIsMalformedNotLimitExceeded()
    {
        // A one-byte advertised chunk ends mid-header while the configured header
        // limit stays generous: the reader must report the enclosing chunk as
        // exhausted (malformed), never a limit breach. The sealed taxonomy keeps
        // LimitExceeded distinct, so this pin fails if the two are conflated again.
        var bytes = ParquetFixtureBuilder.CreateInt32(new() { Values = [1], ChunkTotalCompressedSize = 1 });
        var exception = await Assert.ThrowsAsync<ParquetFormatException>(async () =>
        {
            await using var file = await ParquetFile.OpenAsync(new MemoryStream(bytes, writable: false));
            await foreach (var batch in file.ScanAsync(new([file.Metadata.Schema.Columns[0]])))
            {
                batch.Dispose();
            }
        });
        Assert.NotNull(exception.ByteOffset);
        Assert.Contains("enclosing column chunk", exception.Message, StringComparison.Ordinal);
    }
}
