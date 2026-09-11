namespace Lokad.Parquet.Tests;

public sealed class ScanPreflightTests
{
    [Fact]
    public async Task PageValueCountPreflightAvoidsPayloadRead()
    {
        using var tracker = new PoolTracker();
        var bytes = ParquetFixtureBuilder.CreateInt32(new() { Values = Enumerable.Range(0, 4096).ToArray() });
        var source = new SizedCountingSource(bytes);
        await using var file = await ParquetFile.OpenAsync(source, ParquetSourceOwnership.Caller, new ParquetReaderOptions { MaximumValuesPerPage = 1 }, CancellationToken.None);
        source.Sizes.Clear();
        var exception = await Assert.ThrowsAsync<ParquetLimitExceededException>(async () =>
        {
            await foreach (var batch in file.ScanAsync(new([file.Metadata.Schema.Columns[0]])))
                batch.Dispose();
        });
        Assert.Equal(0, exception.RowGroupOrdinal);
        Assert.Equal(0, exception.ColumnOrdinal);
        Assert.Equal(0, exception.PageOrdinal);
        Assert.NotEmpty(source.Sizes);
        Assert.All(source.Sizes, static size => Assert.True(size <= 1024, $"A rejected page performed a {size}-byte read."));
        Assert.DoesNotContain(16384, source.Sizes);
    }

    [Fact]
    public async Task DictionaryByteLimitPreflightAvoidsPayloadRead()
    {
        using var tracker = new PoolTracker();
        var bytes = ParquetFixtureBuilder.CreateInt32(new()
        {
            Values = [0],
            DictionaryValues = Enumerable.Range(0, 4096).ToArray(),
            DictionaryIndices = [0],
        });
        var source = new SizedCountingSource(bytes);
        await using var file = await ParquetFile.OpenAsync(source, ParquetSourceOwnership.Caller, new ParquetReaderOptions { MaximumDictionaryBytes = 8 }, CancellationToken.None);
        source.Sizes.Clear();
        await Assert.ThrowsAsync<ParquetLimitExceededException>(async () =>
        {
            await foreach (var batch in file.ScanAsync(new([file.Metadata.Schema.Columns[0]])))
                batch.Dispose();
        });
        Assert.NotEmpty(source.Sizes);
        Assert.All(source.Sizes, static size => Assert.True(size <= 1024, $"A rejected dictionary performed a {size}-byte read."));
        Assert.DoesNotContain(16384, source.Sizes);
    }

    [Fact]
    public async Task CompressedDictionaryByteLimitPreflightAvoidsPayloadReadAndDecompression()
    {
        using var tracker = new PoolTracker();
        var bytes = ParquetFixtureBuilder.CreateInt32(new()
        {
            Values = [0],
            DictionaryValues = Enumerable.Range(0, 12).ToArray(),
            DictionaryIndices = [0],
            CompressionCodec = ParquetCompressionCodec.Snappy,
        });
        var source = new SizedCountingSource(bytes);
        await using var file = await ParquetFile.OpenAsync(source, ParquetSourceOwnership.Caller, new ParquetReaderOptions { MaximumDictionaryBytes = 8 }, CancellationToken.None);
        source.Sizes.Clear();
        await Assert.ThrowsAsync<ParquetLimitExceededException>(async () =>
        {
            await foreach (var batch in file.ScanAsync(new([file.Metadata.Schema.Columns[0]])))
                batch.Dispose();
        });
        Assert.NotEmpty(source.Sizes);
        Assert.All(source.Sizes, static size => Assert.True(size <= 256, $"A rejected compressed dictionary performed a {size}-byte read."));
        Assert.DoesNotContain(50, source.Sizes);
    }

    [Fact]
    public async Task UnsupportedValueEncodingPreflightAvoidsPayloadRead()
    {
        using var tracker = new PoolTracker();
        var bytes = ParquetFixtureBuilder.CreateInt32(new()
        {
            Values = Enumerable.Range(0, 256).ToArray(),
            PageHeaderOverrides = new() { ValueEncodingCode = 99 },
        });
        var source = new SizedCountingSource(bytes);
        await using var file = await ParquetFile.OpenAsync(source);
        source.Sizes.Clear();
        var exception = await Assert.ThrowsAsync<ParquetUnsupportedFeatureException>(async () =>
        {
            await foreach (var batch in file.ScanAsync(new([file.Metadata.Schema.Columns[0]])))
                batch.Dispose();
        });
        Assert.Equal(0, exception.RowGroupOrdinal);
        Assert.Equal(0, exception.ColumnOrdinal);
        Assert.Equal(0, exception.PageOrdinal);
        Assert.NotEmpty(source.Sizes);
        Assert.All(source.Sizes, static size => Assert.True(size <= 1024, $"A rejected encoding performed a {size}-byte read."));
    }

    [Fact]
    public async Task RequiredV2RowMismatchPreflightAvoidsPayloadRead()
    {
        using var tracker = new PoolTracker();
        var bytes = ParquetFixtureBuilder.CreateInt32(new()
        {
            PhysicalTypeCode = (int)ParquetPhysicalType.Int64,
            PhysicalValues = new long[] { 1, 2, 3 },
            PageVersion = FixturePageVersion.DataPageV2,
            PageHeaderOverrides = new() { ValueCount = 2 },
        });
        var source = new SizedCountingSource(bytes);
        await using var file = await ParquetFile.OpenAsync(source);
        source.Sizes.Clear();
        var exception = await Assert.ThrowsAsync<ParquetFormatException>(async () =>
        {
            await foreach (var batch in file.ScanAsync(new([file.Metadata.Schema.Columns[0]])))
                batch.Dispose();
        });
        Assert.Equal("A required flat V2 page has inconsistent row, null, or level fields.", exception.Message);
        Assert.Equal(0, exception.RowGroupOrdinal);
        Assert.Equal(0, exception.ColumnOrdinal);
        Assert.Equal(0, exception.PageOrdinal);
        Assert.NotEmpty(source.Sizes);
        Assert.All(source.Sizes, static size => Assert.True(size <= 1024, $"A rejected V2 page performed a {size}-byte read."));
    }

    [Fact]
    public async Task OptionalV1DefinitionEncodingPreflightAvoidsPayloadRead()
    {
        using var tracker = new PoolTracker();
        var bytes = ParquetFixtureBuilder.CreateInt32(new()
        {
            Values = [1, 2, 3],
            Repetition = ParquetRepetition.Optional,
            Validity = [true, true, true],
            PageHeaderOverrides = new() { DefinitionEncodingCode = 99 },
        });
        var source = new SizedCountingSource(bytes);
        await using var file = await ParquetFile.OpenAsync(source);
        source.Sizes.Clear();
        await Assert.ThrowsAsync<ParquetUnsupportedFeatureException>(async () =>
        {
            await foreach (var batch in file.ScanAsync(new([file.Metadata.Schema.Columns[0]])))
                batch.Dispose();
        });
        Assert.NotEmpty(source.Sizes);
        Assert.All(source.Sizes, static size => Assert.True(size <= 1024, $"A rejected definition encoding performed a {size}-byte read."));
    }

    [Fact]
    public async Task UnsupportedEncodingTakesPrecedenceOverValueLimitWithoutPayloadRead()
    {
        using var tracker = new PoolTracker();
        var bytes = ParquetFixtureBuilder.CreateInt32(new()
        {
            Values = Enumerable.Range(0, 64).ToArray(),
            PageHeaderOverrides = new() { ValueEncodingCode = 99 },
        });
        var source = new SizedCountingSource(bytes);
        await using var file = await ParquetFile.OpenAsync(source, ParquetSourceOwnership.Caller, new ParquetReaderOptions { MaximumValuesPerPage = 1 }, CancellationToken.None);
        source.Sizes.Clear();
        await Assert.ThrowsAsync<ParquetUnsupportedFeatureException>(async () =>
        {
            await foreach (var batch in file.ScanAsync(new([file.Metadata.Schema.Columns[0]])))
                batch.Dispose();
        });
        Assert.NotEmpty(source.Sizes);
        Assert.All(source.Sizes, static size => Assert.True(size <= 1024, $"A rejected page performed a {size}-byte read."));
    }

    [Fact]
    public async Task ProjectedUnsupportedChunkRejectedBeforeAnyPayloadRead()
    {
        using var tracker = new PoolTracker();
        var bytes = ParquetFixtureBuilder.CreateBinaryColumns(
        [
            new FixtureBinaryColumn
            {
                Name = "left",
                PhysicalTypeCode = (int)ParquetPhysicalType.Int32,
                PhysicalValues = new int[] { 1, 2, 3 },
            },
            new FixtureBinaryColumn
            {
                Name = "right",
                PhysicalTypeCode = (int)ParquetPhysicalType.Int32,
                PhysicalValues = new int[] { 4, 5, 6 },
                FooterCodec = ParquetCompressionCodec.Gzip,
            },
        ]);
        var source = new SizedCountingSource(bytes);
        await using var file = await ParquetFile.OpenAsync(source);
        var readsAfterOpen = source.Sizes.Count;
        var exception = Assert.Throws<ParquetUnsupportedFeatureException>(() =>
            file.ScanAsync(new ParquetScanOptions([file.Metadata.Schema.Columns[0], file.Metadata.Schema.Columns[1]])).GetAsyncEnumerator());
        Assert.Equal(0, exception.RowGroupOrdinal);
        Assert.Equal(1, exception.ColumnOrdinal);
        Assert.Equal(readsAfterOpen, source.Sizes.Count);
    }

    [Fact]
    public async Task UnselectedUnsupportedChunkRemainsInspectable()
    {
        using var tracker = new PoolTracker();
        var bytes = ParquetFixtureBuilder.CreateBinaryColumns(
        [
            new FixtureBinaryColumn
            {
                Name = "left",
                PhysicalTypeCode = (int)ParquetPhysicalType.Int32,
                PhysicalValues = new int[] { 1, 2, 3 },
            },
            new FixtureBinaryColumn
            {
                Name = "right",
                PhysicalTypeCode = (int)ParquetPhysicalType.Int32,
                PhysicalValues = new int[] { 4, 5, 6 },
                FooterCodec = ParquetCompressionCodec.Gzip,
            },
        ]);
        await using var file = await ParquetFile.OpenAsync(new MemoryStream(bytes, writable: false));
        Assert.False(file.Metadata.Schema.Columns[1].IsReadable == false && file.Metadata.Schema.Columns[1].UnsupportedReason is null);
        await using var enumerator = file.ScanAsync(new([file.Metadata.Schema.Columns[0]])).GetAsyncEnumerator();
        Assert.True(await enumerator.MoveNextAsync());
        using (var batch = enumerator.Current)
            Assert.Equal([1, 2, 3], Assert.IsType<ParquetPrimitiveColumnBatch<int>>(batch.Columns[0]).Values.ToArray());
        Assert.False(await enumerator.MoveNextAsync());
    }

    [Fact]
    public async Task SingleColumnLaterGroupUnsupportedCodecRejectedBeforeAnyPayloadRead()
    {
        var rejected = await RejectSingleColumnLaterGroupAsync(new FixtureRowGroupFooter { Codec = ParquetCompressionCodec.Gzip });
        Assert.Equal(1, rejected.Exception.RowGroupOrdinal);
        Assert.Equal(0, rejected.Exception.ColumnOrdinal);
        Assert.Equal(rejected.ReadsAfterOpen, rejected.ReadsAfterScan);
    }

    [Fact]
    public async Task SingleColumnLaterGroupExternalChunkRejectedBeforeAnyPayloadRead()
    {
        var rejected = await RejectSingleColumnLaterGroupAsync(new FixtureRowGroupFooter { ExternalFilePath = "external.parquet" });
        Assert.Equal(1, rejected.Exception.RowGroupOrdinal);
        Assert.Equal(0, rejected.Exception.ColumnOrdinal);
        Assert.Equal(rejected.ReadsAfterOpen, rejected.ReadsAfterScan);
    }

    [Fact]
    public async Task SingleColumnLaterGroupEncryptedChunkRejectedBeforeAnyPayloadRead()
    {
        var rejected = await RejectSingleColumnLaterGroupAsync(new FixtureRowGroupFooter { HasCryptoMetadata = true });
        Assert.Equal(1, rejected.Exception.RowGroupOrdinal);
        Assert.Equal(0, rejected.Exception.ColumnOrdinal);
        Assert.Equal(rejected.ReadsAfterOpen, rejected.ReadsAfterScan);
    }
    private static async Task<(ParquetUnsupportedFeatureException Exception, int ReadsAfterOpen, int ReadsAfterScan)> RejectSingleColumnLaterGroupAsync(
        FixtureRowGroupFooter second)
    {
        using var tracker = new PoolTracker();
        var bytes = ParquetFixtureBuilder.CreateRequiredInt32RowGroups(
            [[1, 2], [3, 4]],
            [new FixtureRowGroupFooter(), second]);
        var source = new SizedCountingSource(bytes);
        await using var file = await ParquetFile.OpenAsync(source);
        var readsAfterOpen = source.Sizes.Count;
        var exception = Assert.Throws<ParquetUnsupportedFeatureException>(() =>
            file.ScanAsync(new([file.Metadata.Schema.Columns[0]])).GetAsyncEnumerator());
        return (exception, readsAfterOpen, source.Sizes.Count);
    }
    [Fact]
    public async Task SingleColumnExcludedGroupScansExactly()
    {
        using var tracker = new PoolTracker();
        var bytes = ParquetFixtureBuilder.CreateRequiredInt32RowGroups(
            [[1, 2], [3, 4]],
            [new FixtureRowGroupFooter(), new FixtureRowGroupFooter { Codec = ParquetCompressionCodec.Gzip }]);
        await using var file = await ParquetFile.OpenAsync(new MemoryStream(bytes, writable: false));
        await using var enumerator = file.ScanAsync(new(
            [file.Metadata.Schema.Columns[0]],
            [file.Metadata.RowGroups[0]],
            null,
            64)).GetAsyncEnumerator();
        var collected = new List<int>();
        while (await enumerator.MoveNextAsync())
        {
            using var batch = enumerator.Current;
            collected.AddRange(Assert.IsType<ParquetPrimitiveColumnBatch<int>>(batch.Columns[0]).Values.ToArray());
        }

        Assert.Equal([1, 2], collected);
    }

    [Fact]
    public async Task SingleColumnEmptySelectionIgnoresUnsupportedGroup()
    {
        using var tracker = new PoolTracker();
        var bytes = ParquetFixtureBuilder.CreateRequiredInt32RowGroups(
            [[1, 2], [3, 4]],
            [new FixtureRowGroupFooter(), new FixtureRowGroupFooter { Codec = ParquetCompressionCodec.Gzip }]);
        await using var file = await ParquetFile.OpenAsync(new MemoryStream(bytes, writable: false));
        await using var emptyGroups = file.ScanAsync(new(
            [file.Metadata.Schema.Columns[0]],
            [],
            null,
            64)).GetAsyncEnumerator();
        Assert.False(await emptyGroups.MoveNextAsync());
        await using var emptyRange = file.ScanAsync(new(
            [file.Metadata.Schema.Columns[0]],
            null,
            new ParquetRowRange(0, 0),
            64)).GetAsyncEnumerator();
        Assert.False(await emptyRange.MoveNextAsync());
    }
    [Fact]
    public async Task CancelledPreflightBalancesPools()
    {
        using var tracker = new PoolTracker();
        var bytes = ParquetFixtureBuilder.CreateInt32(new() { Values = Enumerable.Range(0, 64).ToArray() });
        await using var file = await ParquetFile.OpenAsync(new MemoryStream(bytes, writable: false));
        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
        {
            await foreach (var batch in file.ScanAsync(new([file.Metadata.Schema.Columns[0]]), cancellation.Token))
                batch.Dispose();
        });
    }

    [Fact]
    public async Task UnadvertisedDictionaryAtDataOffsetScansExactly()
    {
        // Parquet.NET omits dictionary_page_offset while placing the dictionary
        // page exactly at data_page_offset: the reader admits that exact shape
        // and decodes the following pages normally.
        using var tracker = new PoolTracker();
        var bytes = ParquetFixtureBuilder.CreateInt32(new()
        {
            Values = [20, 10, 20, 30],
            DictionaryValues = new int[] { 10, 20, 30 },
            DictionaryIndices = [1, 0, 1, 2],
            DictionaryMode = FixtureDictionaryMode.UnadvertisedBeforeDataPage,
        });
        await using var file = await ParquetFile.OpenAsync(new MemoryStream(bytes, writable: false));
        Assert.Null(file.Metadata.RowGroups[0].Columns[0].DictionaryPageOffset);
        await using var enumerator = file.ScanAsync(new([file.Metadata.Schema.Columns[0]])).GetAsyncEnumerator();
        Assert.True(await enumerator.MoveNextAsync());
        using (var batch = enumerator.Current)
            Assert.Equal([20, 10, 20, 30], Assert.IsType<ParquetPrimitiveColumnBatch<int>>(batch.Columns[0]).Values.ToArray());
        Assert.False(await enumerator.MoveNextAsync());
    }

    [Fact]
    public async Task UnadvertisedBinaryDictionarySnappyScansExactly()
    {
        using var tracker = new PoolTracker();
        var bytes = ParquetFixtureBuilder.CreateInt32(new()
        {
            PhysicalTypeCode = (int)ParquetPhysicalType.ByteArray,
            PhysicalValues = new byte[][] { [4, 5], [1], [4, 5], [6] },
            DictionaryValues = new byte[][] { [1], [4, 5], [6] },
            DictionaryIndices = [1, 0, 1, 2],
            DictionaryMode = FixtureDictionaryMode.UnadvertisedBeforeDataPage,
            CompressionCodec = ParquetCompressionCodec.Snappy,
            SnappyCopyEncoding = true,
        });
        await using var file = await ParquetFile.OpenAsync(new MemoryStream(bytes, writable: false));
        await using var enumerator = file.ScanAsync(new([file.Metadata.Schema.Columns[0]])).GetAsyncEnumerator();
        Assert.True(await enumerator.MoveNextAsync());
        using (var batch = enumerator.Current)
        {
            var column = Assert.IsType<ParquetBinaryColumnBatch>(batch.Columns[0]);
            Assert.Equal([0, 2, 3, 5, 6], column.Offsets.ToArray());
            Assert.Equal([4, 5, 1, 4, 5, 6], column.Payload.ToArray());
        }
        Assert.False(await enumerator.MoveNextAsync());
    }

    [Fact]
    public async Task UnadvertisedOptionalDictionaryScansExactly()
    {
        using var tracker = new PoolTracker();
        var bytes = ParquetFixtureBuilder.CreateInt32(new()
        {
            Values = [20, 0, 10],
            Repetition = ParquetRepetition.Optional,
            Validity = [true, false, true],
            DictionaryValues = new int[] { 10, 20 },
            DictionaryIndices = [1, 0],
            DictionaryMode = FixtureDictionaryMode.UnadvertisedBeforeDataPage,
        });
        await using var file = await ParquetFile.OpenAsync(new MemoryStream(bytes, writable: false));
        await using var enumerator = file.ScanAsync(new([file.Metadata.Schema.Columns[0]])).GetAsyncEnumerator();
        Assert.True(await enumerator.MoveNextAsync());
        using (var batch = enumerator.Current)
        {
            var column = Assert.IsType<ParquetPrimitiveColumnBatch<int>>(batch.Columns[0]);
            Assert.Equal([true, false, true], Enumerable.Range(0, 3).Select(column.Validity.IsValid));
            Assert.Equal(20, column.Values.Span[0]);
            Assert.Equal(10, column.Values.Span[2]);
        }
        Assert.False(await enumerator.MoveNextAsync());
    }

    [Fact]
    public async Task UnadvertisedDictionaryHonorsEntryLimit()
    {
        // Tolerance admits the offset shape only; header-known limits still fire.
        using var tracker = new PoolTracker();
        var bytes = ParquetFixtureBuilder.CreateInt32(new()
        {
            Values = [20, 10],
            DictionaryValues = new int[] { 10, 20 },
            DictionaryIndices = [1, 0],
            DictionaryMode = FixtureDictionaryMode.UnadvertisedBeforeDataPage,
        });
        await using var file = await ParquetFile.OpenAsync(
            new MemoryStream(bytes, writable: false),
            ParquetSourceOwnership.Caller,
            new ParquetReaderOptions { MaximumDictionaryEntries = 1 },
            CancellationToken.None);
        var exception = await Assert.ThrowsAsync<ParquetLimitExceededException>(async () =>
        {
            await foreach (var batch in file.ScanAsync(new([file.Metadata.Schema.Columns[0]])))
                batch.Dispose();
        });
        Assert.Equal("A dictionary exceeds the configured entry or byte limit.", exception.Message);
    }

    [Fact]
    public async Task UnadvertisedPlainFallbackAfterDictionaryScans()
    {
        using var tracker = new PoolTracker();
        var bytes = ParquetFixtureBuilder.CreateInt32(new()
        {
            Values = [20, 10],
            DictionaryValues = new int[] { 10, 20 },
            DictionaryIndices = [1, 0],
            DictionaryMode = FixtureDictionaryMode.UnadvertisedBeforeDataPage,
            TrailingPlainValues = new int[] { 30, 40 },
        });
        var observed = new List<int>();
        await using var file = await ParquetFile.OpenAsync(new MemoryStream(bytes, writable: false));
        await foreach (var batch in file.ScanAsync(new([file.Metadata.Schema.Columns[0]])))
        {
            using (batch)
                observed.AddRange(Assert.IsType<ParquetPrimitiveColumnBatch<int>>(batch.Columns[0]).Values.ToArray());
        }

        Assert.Equal([20, 10, 30, 40], observed);
    }
    private sealed class SizedCountingSource(byte[] bytes) : IParquetRandomAccessSource
    {
        public readonly List<int> Sizes = [];
        public long Length => bytes.Length;
        public ValueTask ReadExactlyAsync(long offset, ArraySegment<byte> destination, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            Sizes.Add(destination.Count);
            bytes.AsMemory(checked((int)offset), destination.Count).CopyTo(destination.AsMemory());
            return ValueTask.CompletedTask;
        }

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }
}
