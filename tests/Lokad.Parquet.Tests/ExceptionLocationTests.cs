namespace Lokad.Parquet.Tests;

using System.Reflection;

public sealed class ExceptionLocationTests
{
    [Fact]
    public async Task TrailingMagicReportsFileOffset()
    {
        var bytes = ParquetFixtureBuilder.CreateInt32(new() { Values = [1] });
        bytes[^1] = 0;
        var exception = await Assert.ThrowsAsync<ParquetFormatException>(async () =>
            await ParquetFile.OpenAsync(bytes, ParquetReaderOptions.Default, CancellationToken.None));
        Assert.Equal(bytes.Length - 4, exception.ByteOffset);
        Assert.Null(exception.RowGroupOrdinal);
        Assert.Null(exception.ColumnOrdinal);
        Assert.Null(exception.PageOrdinal);
    }

    [Fact]
    public async Task NegativePageSizeReportsPageLocation()
    {
        // Page-header parse errors keep the offending offset and gain the page
        // identity from the boundary being parsed.
        var bytes = ParquetFixtureBuilder.CreateInt32(new() { Values = [1, 2, 3], PageHeaderOverrides = new() { CompressedSize = -1 } });
        await using var file = await ParquetFile.OpenAsync(new MemoryStream(bytes, writable: false));
        var exception = await Assert.ThrowsAsync<ParquetFormatException>(async () =>
        {
            await foreach (var batch in file.ScanAsync(new([file.Metadata.Schema.Columns[0]])))
                batch.Dispose();
        });
        Assert.NotNull(exception.ByteOffset);
        Assert.Equal(0, exception.RowGroupOrdinal);
        Assert.Equal(0, exception.ColumnOrdinal);
        Assert.Equal(0, exception.PageOrdinal);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(2)]
    public async Task MalformedSnappyBlockReportsPageLocation(int sourceKind)
    {
        // A Snappy block declaring more output than its destination fails inside the
        // decoder without location; the page boundary annotates it on the immediate,
        // delayed, and direct-memory source paths alike. Source kinds: direct, stream,
        // and asynchronous custom source.
        using var tracker = new PoolTracker();
        byte[] bytes = ParquetFixtureBuilder.CreateInt32(new()
        {
            Values = [1, 2, 3],
            CompressionCodec = ParquetCompressionCodec.Snappy,
            PageHeaderOverrides = new() { UncompressedSize = 100 },
        });
        await using var file = await OpenByKindAsync(bytes, sourceKind);
        var exception = await Assert.ThrowsAsync<ParquetFormatException>(async () =>
        {
            await foreach (var batch in file.ScanAsync(new([file.Metadata.Schema.Columns[0]])))
                batch.Dispose();
        });
        Assert.Equal(4L, exception.ByteOffset);
        Assert.Equal(0, exception.RowGroupOrdinal);
        Assert.Equal(0, exception.ColumnOrdinal);
        Assert.Equal(0, exception.PageOrdinal);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(2)]
    public async Task MalformedPageHeaderReportsPageLocation(int sourceKind)
    {
        // A negative declared page size fails inside header parsing with only an
        // offset; the page boundary keeps that offset and adds the page identity.
        using var tracker = new PoolTracker();
        byte[] bytes = ParquetFixtureBuilder.CreateInt32(new()
        {
            Values = [1, 2, 3],
            PageHeaderOverrides = new() { UncompressedSize = -1 },
        });
        await using var file = await OpenByKindAsync(bytes, sourceKind);
        var exception = await Assert.ThrowsAsync<ParquetFormatException>(async () =>
        {
            await foreach (var batch in file.ScanAsync(new([file.Metadata.Schema.Columns[0]])))
                batch.Dispose();
        });
        Assert.NotNull(exception.ByteOffset);
        Assert.Equal(0, exception.RowGroupOrdinal);
        Assert.Equal(0, exception.ColumnOrdinal);
        Assert.Equal(0, exception.PageOrdinal);
    }

    [Fact]
    public async Task DictionarySerializedBytesRespectDictionaryLimit()
    {
        // Four 3-byte entries: 28 serialized bytes but 32 decoded bytes. A limit of 20
        // fails the serialized page-bytes check at the header, before decoding starts.
        using var tracker = new PoolTracker();
        byte[] bytes = ParquetFixtureBuilder.CreateInt32(new()
        {
            PhysicalTypeCode = (int)ParquetPhysicalType.ByteArray,
            PhysicalValues = new byte[][] { [10], [20], [30], [40] },
            DictionaryValues = new byte[][] { [1, 2, 3], [4, 5, 6], [7, 8, 9], [10, 11, 12] },
            DictionaryIndices = [0, 1, 2, 3],
        });
        await using var file = await ParquetFile.OpenAsync(bytes, new() { MaximumDictionaryBytes = 20 }, CancellationToken.None);
        var exception = await Assert.ThrowsAsync<ParquetLimitExceededException>(async () =>
        {
            await foreach (var batch in file.ScanAsync(new([file.Metadata.Schema.Columns[0]])))
                batch.Dispose();
        });
        Assert.Equal("A dictionary exceeds the configured entry or byte limit.", exception.Message);
        Assert.Equal(4L, exception.ByteOffset);
        Assert.Equal(0, exception.RowGroupOrdinal);
        Assert.Equal(0, exception.ColumnOrdinal);
        Assert.Equal(0, exception.PageOrdinal);
    }

    private static ValueTask<ParquetFile> OpenByKindAsync(byte[] bytes, int sourceKind) => sourceKind switch
    {
        0 => ParquetFile.OpenAsync((ReadOnlyMemory<byte>)bytes),
        1 => ParquetFile.OpenAsync(new MemoryStream(bytes, writable: false)),
        _ => ParquetFile.OpenAsync(new DelayedSource(bytes)),
    };

    private sealed class DelayedSource(byte[] bytes) : IParquetRandomAccessSource
    {
        public long Length => bytes.LongLength;
        public async ValueTask ReadExactlyAsync(long offset, ArraySegment<byte> destination, CancellationToken cancellationToken)
        {
            await Task.Yield();
            bytes.AsMemory(checked((int)offset), destination.Count).CopyTo(destination.AsMemory());
        }

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    [Fact]
    public async Task AuxiliaryOffsetOutsideInputReportsChunkLocation()
    {
        var bytes = ParquetFixtureBuilder.CreateInt32(new() { Values = [1], AuxiliaryOffset = 1_000_000, AuxiliaryLength = 1 });
        var exception = await Assert.ThrowsAsync<ParquetFormatException>(async () =>
            await ParquetFile.OpenAsync(bytes, ParquetReaderOptions.Default, CancellationToken.None));
        Assert.NotNull(exception.ByteOffset);
        Assert.Equal(0, exception.RowGroupOrdinal);
        Assert.Equal(0, exception.ColumnOrdinal);
        Assert.Null(exception.PageOrdinal);
    }

    [Fact]
    public async Task UnreadableColumnReportsColumnLocation()
    {
        var path = Path.Combine(RepositoryTestPaths.Root, "tests", "fixtures", "apache-parquet-testing", "alltypes_plain.parquet");
        await using var file = await ParquetFile.OpenAsync(path);
        Assert.False(file.Metadata.Schema.Columns[10].IsReadable);
        var exception = Assert.Throws<ParquetUnsupportedFeatureException>(
            () => file.ScanAsync(new([file.Metadata.Schema.Columns[10]])).GetAsyncEnumerator());
        Assert.Null(exception.ByteOffset);
        Assert.Null(exception.RowGroupOrdinal);
        Assert.Equal(10, exception.ColumnOrdinal);
        Assert.Null(exception.PageOrdinal);
    }

    [Fact]
    public async Task BinaryBatchLimitReportsPageLocationWithoutOffset()
    {
        using var tracker = new PoolTracker();
        var bytes = ParquetFixtureBuilder.CreateInt32(new() { PhysicalTypeCode = (int)ParquetPhysicalType.ByteArray, PhysicalValues = new byte[][] { new byte[17] } });
        await using var file = await ParquetFile.OpenAsync(
            new MemoryStream(bytes, writable: false),
            ParquetSourceOwnership.Caller,
            new ParquetReaderOptions { MaximumBinaryBatchBytes = 16 },
            CancellationToken.None);
        var exception = await Assert.ThrowsAsync<ParquetLimitExceededException>(async () =>
        {
            await foreach (var batch in file.ScanAsync(new([file.Metadata.Schema.Columns[0]], null, null, 1)))
                batch.Dispose();
        });
        Assert.Null(exception.ByteOffset);
        Assert.Equal(0, exception.RowGroupOrdinal);
        Assert.Equal(0, exception.ColumnOrdinal);
        Assert.Equal(0, exception.PageOrdinal);
    }

    [Fact]
    public void LocationFactoriesFlowToEveryExceptionType()
    {
        var assembly = typeof(ParquetFile).Assembly;
        var locationType = assembly.GetType("Lokad.Parquet.ParquetErrorLocation") ??
            throw new InvalidOperationException("The error location was not found.");
        object? Location(string name, params object[] arguments)
        {
            var method = locationType.GetMethod(name, BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Static) ??
                throw new InvalidOperationException("A location factory was not found.");
            return method.Invoke(null, arguments);
        }
        void AssertLocation(object? location, long? offset, int? rowGroup, int? column, int? page)
        {
            Assert.Equal((object?)offset, locationType.GetProperty("ByteOffset")?.GetValue(location));
            Assert.Equal((object?)rowGroup, locationType.GetProperty("RowGroupOrdinal")?.GetValue(location));
            Assert.Equal((object?)column, locationType.GetProperty("ColumnOrdinal")?.GetValue(location));
            Assert.Equal((object?)page, locationType.GetProperty("PageOrdinal")?.GetValue(location));
        }
        AssertLocation(Location("AtOffset", 7L), 7L, null, null, null);
        AssertLocation(Location("AtRowGroup", 7L, 1), 7L, 1, null, null);
        AssertLocation(Location("AtChunk", 7L, 1, 2), 7L, 1, 2, null);
        AssertLocation(Location("AtPage", 7L, 1, 2, 3), 7L, 1, 2, 3);
        AssertLocation(Location("AtColumn", 4), null, null, 4, null);
        AssertLocation(Location("AtRowGroupColumn", 1, 2), null, 1, 2, null);
        AssertLocation(Location("AtRowGroupColumnPage", 1, 2, 3), null, 1, 2, 3);
        foreach (var name in new[] { "ParquetFormatException", "ParquetUnsupportedFeatureException", "ParquetLimitExceededException" })
        {
            var exceptionType = assembly.GetType("Lokad.Parquet." + name) ??
                throw new InvalidOperationException("An exception type was not found.");
            var constructor = exceptionType.GetConstructor(BindingFlags.NonPublic | BindingFlags.Instance, null, [typeof(string), locationType], null) ??
                throw new InvalidOperationException("A location constructor was not found.");
            var exception = (ParquetException)constructor.Invoke(["boom", Location("AtPage", 7L, 1, 2, 3)]);
            Assert.Equal("boom", exception.Message);
            Assert.Equal(7L, exception.ByteOffset);
            Assert.Equal(1, exception.RowGroupOrdinal);
            Assert.Equal(2, exception.ColumnOrdinal);
            Assert.Equal(3, exception.PageOrdinal);
        }
        var formatType = assembly.GetType("Lokad.Parquet.ParquetFormatException") ??
            throw new InvalidOperationException("The format exception was not found.");
        var innerConstructor = formatType.GetConstructor(BindingFlags.NonPublic | BindingFlags.Instance, null, [typeof(string), typeof(Exception), locationType], null) ??
            throw new InvalidOperationException("A location inner constructor was not found.");
        var inner = new InvalidOperationException("inner");
        var wrapped = (ParquetException)innerConstructor.Invoke(["wrapped", inner, Location("AtOffset", 9L)]);
        Assert.Same(inner, wrapped.InnerException);
        Assert.Equal(9L, wrapped.ByteOffset);
    }
}
