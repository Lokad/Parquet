namespace Lokad.Parquet.Tests;

public sealed class ScanTests
{
    [Fact]
    public async Task ScansRequiredPlainInt32Page()
    {
        var bytes = ParquetFixtureBuilder.CreateInt32(new()
        {
            Values = [10, -2, 42],
            CrcMode = FixtureCrcMode.Valid,
        });
        await using var file = await ParquetFile.OpenAsync(new MemoryStream(bytes, writable: false));

        var batches = new List<int[]>();
        await foreach (var batch in file.ScanAsync(new([file.Metadata.Schema.Columns[0]])))
        {
            using (batch)
            {
                Assert.Equal(0, batch.RowOffset);
                Assert.Equal(0, batch.RowGroupOrdinal);
                Assert.Equal(3, batch.RowCount);
                var column = Assert.IsType<ParquetPrimitiveColumnBatch<int>>(Assert.Single(batch.Columns));
                Assert.True(column.Validity.IsAllValid);
                batches.Add(column.Values.ToArray());
            }
        }

        Assert.Single(batches);
        Assert.Equal([10, -2, 42], batches[0]);
    }

    [Fact]
    public async Task ScansPubliclyVisibleMemoryStreamWithNonZeroOrigin()
    {
        var bytes = ParquetFixtureBuilder.CreateInt32(new() { Values = [10, -2, 42] });
        var container = new byte[checked(bytes.Length + 23)];
        bytes.AsSpan().CopyTo(container.AsSpan(17));
        using var stream = new MemoryStream(container, 17, bytes.Length, false, true);
        await using var file = await ParquetFile.OpenAsync(stream);

        await using var enumerator = file.ScanAsync(new([file.Metadata.Schema.Columns[0]])).GetAsyncEnumerator();
        Assert.True(await enumerator.MoveNextAsync());
        using var batch = enumerator.Current;
        var values = Assert.IsType<ParquetPrimitiveColumnBatch<int>>(batch.Columns[0]).Values;
        Assert.Equal([10, -2, 42], values.ToArray());
    }

    [Theory]
    [InlineData(ParquetCompressionCodec.Uncompressed)]
    [InlineData(ParquetCompressionCodec.Snappy)]
    public async Task ScansMemoryWithNonZeroOrigin(ParquetCompressionCodec compressionCodec)
    {
        var bytes = ParquetFixtureBuilder.CreateInt32(new()
        {
            Values = [10, -2, 42],
            CompressionCodec = compressionCodec,
            CrcMode = FixtureCrcMode.Valid,
        });
        var container = new byte[checked(bytes.Length + 23)];
        bytes.AsSpan().CopyTo(container.AsSpan(17));
        Memory<byte> content = container.AsMemory(17, bytes.Length);
        await using var file = await ParquetFile.OpenAsync(content);

        await using var enumerator = file.ScanAsync(new([file.Metadata.Schema.Columns[0]])).GetAsyncEnumerator();
        Assert.True(await enumerator.MoveNextAsync());
        using var batch = enumerator.Current;
        var values = Assert.IsType<ParquetPrimitiveColumnBatch<int>>(batch.Columns[0]).Values;
        Assert.Equal([10, -2, 42], values.ToArray());
    }

    [Fact]
    public async Task MemoryOpenAcceptsOptionsAndCancellation()
    {
        ReadOnlyMemory<byte> content = ParquetFixtureBuilder.CreateInt32(new() { Values = [1] });
        var options = new ParquetReaderOptions { MaximumRowsPerBatch = 17 };
        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
            await ParquetFile.OpenAsync(content, options, cancellation.Token));

        await using var file = await ParquetFile.OpenAsync(content, options, CancellationToken.None);
        Assert.Same(options, file.Options);
    }

    [Fact]
    public async Task ScansSnappyCompressedRequiredInt32Page()
    {
        var bytes = ParquetFixtureBuilder.CreateInt32(new()
        {
            Values = [10, -2, 42],
            CompressionCodec = ParquetCompressionCodec.Snappy,
            CrcMode = FixtureCrcMode.Valid,
        });
        await using var file = await ParquetFile.OpenAsync(new MemoryStream(bytes, writable: false));

        await using var enumerator = file.ScanAsync(new([file.Metadata.Schema.Columns[0]])).GetAsyncEnumerator();
        Assert.True(await enumerator.MoveNextAsync());
        using var batch = enumerator.Current;
        Assert.Equal([10, -2, 42], Assert.IsType<ParquetPrimitiveColumnBatch<int>>(batch.Columns[0]).Values.ToArray());
    }

    [Fact]
    public async Task ScansOptionalInt32WithExactValidity()
    {
        var bytes = ParquetFixtureBuilder.CreateInt32(new()
        {
            Values = [10, 20, 30, 40],
            Repetition = ParquetRepetition.Optional,
            Validity = [true, false, true, false],
        });
        await using var file = await ParquetFile.OpenAsync(new MemoryStream(bytes, writable: false));

        await using var enumerator = file.ScanAsync(new([file.Metadata.Schema.Columns[0]])).GetAsyncEnumerator();
        Assert.True(await enumerator.MoveNextAsync());
        using var batch = enumerator.Current;
        var column = Assert.IsType<ParquetPrimitiveColumnBatch<int>>(batch.Columns[0]);
        Assert.False(column.Validity.IsAllValid);
        Assert.Equal([true, false, true, false], Enumerable.Range(0, 4).Select(column.Validity.IsValid));
        Assert.Equal(10, column.Values.Span[0]);
        Assert.Equal(30, column.Values.Span[2]);
        Assert.Equal([0b0000_0101], column.Validity.Bits.ToArray());
    }

    [Fact]
    public async Task ScansRequiredBooleanInt64FloatAndDouble()
    {
        static async Task AssertPrimitiveScan<T>(T[] expected, ParquetPhysicalType physicalType)
            where T : unmanaged
        {
            var bytes = ParquetFixtureBuilder.CreateInt32(new()
            {
                PhysicalTypeCode = (int)physicalType,
                PhysicalValues = expected,
            });
            await using var file = await ParquetFile.OpenAsync(new MemoryStream(bytes, writable: false));
            await using var enumerator = file.ScanAsync(new([file.Metadata.Schema.Columns[0]])).GetAsyncEnumerator();

            Assert.True(await enumerator.MoveNextAsync());
            using var batch = enumerator.Current;
            var actual = Assert.IsType<ParquetPrimitiveColumnBatch<T>>(batch.Columns[0]).Values.ToArray();
            if (typeof(T) == typeof(float))
            {
                Assert.Equal(
                    expected.Cast<float>().Select(BitConverter.SingleToInt32Bits),
                    actual.Cast<float>().Select(BitConverter.SingleToInt32Bits));
            }
            else if (typeof(T) == typeof(double))
            {
                Assert.Equal(
                    expected.Cast<double>().Select(BitConverter.DoubleToInt64Bits),
                    actual.Cast<double>().Select(BitConverter.DoubleToInt64Bits));
            }
            else
            {
                Assert.Equal(expected, actual);
            }
        }

        await AssertPrimitiveScan([true, false, true], ParquetPhysicalType.Boolean);
        await AssertPrimitiveScan([long.MinValue, 0L, long.MaxValue], ParquetPhysicalType.Int64);
        await AssertPrimitiveScan([1.5f, BitConverter.Int32BitsToSingle(unchecked((int)0x7FC01234))], ParquetPhysicalType.Float);
        await AssertPrimitiveScan([-1.25d, BitConverter.Int64BitsToDouble(unchecked((long)0x7FF8000012345678))], ParquetPhysicalType.Double);
    }

    [Fact]
    public async Task ScansOptionalInt64WithoutConflatingNullAndDefault()
    {
        var bytes = ParquetFixtureBuilder.CreateInt32(new()
        {
            PhysicalTypeCode = (int)ParquetPhysicalType.Int64,
            PhysicalValues = new long[] { 0, 11, 0 },
            Repetition = ParquetRepetition.Optional,
            Validity = [true, false, true],
        });
        await using var file = await ParquetFile.OpenAsync(new MemoryStream(bytes, writable: false));
        await using var enumerator = file.ScanAsync(new([file.Metadata.Schema.Columns[0]])).GetAsyncEnumerator();

        Assert.True(await enumerator.MoveNextAsync());
        using var batch = enumerator.Current;
        var column = Assert.IsType<ParquetPrimitiveColumnBatch<long>>(batch.Columns[0]);
        Assert.Equal([true, false, true], Enumerable.Range(0, 3).Select(column.Validity.IsValid));
        Assert.Equal(0, column.Values.Span[0]);
        Assert.Equal(0, column.Values.Span[2]);
    }

    [Fact]
    public async Task ScansRequiredByteArraysWithExactOffsets()
    {
        var bytes = ParquetFixtureBuilder.CreateInt32(new()
        {
            PhysicalTypeCode = (int)ParquetPhysicalType.ByteArray,
            PhysicalValues = new byte[][] { [], [1, 2], [3] },
        });
        await using var file = await ParquetFile.OpenAsync(new MemoryStream(bytes, writable: false));
        await using var enumerator = file.ScanAsync(new([file.Metadata.Schema.Columns[0]])).GetAsyncEnumerator();

        Assert.True(await enumerator.MoveNextAsync());
        using var batch = enumerator.Current;
        var column = Assert.IsType<ParquetBinaryColumnBatch>(batch.Columns[0]);
        Assert.True(column.Validity.IsAllValid);
        Assert.Equal([0, 0, 2, 3], column.Offsets.ToArray());
        Assert.Equal([1, 2, 3], column.Payload.ToArray());
    }

    [Fact]
    public async Task ScansOptionalSnappyByteArraysWithoutConflatingNullAndEmpty()
    {
        var bytes = ParquetFixtureBuilder.CreateInt32(new()
        {
            PhysicalTypeCode = (int)ParquetPhysicalType.ByteArray,
            PhysicalValues = new byte[][] { [], [9], [2, 3] },
            Repetition = ParquetRepetition.Optional,
            Validity = [true, false, true],
            PageVersion = FixturePageVersion.DataPageV2,
            CompressionCodec = ParquetCompressionCodec.Snappy,
        });
        await using var file = await ParquetFile.OpenAsync(new MemoryStream(bytes, writable: false));
        await using var enumerator = file.ScanAsync(new([file.Metadata.Schema.Columns[0]])).GetAsyncEnumerator();

        Assert.True(await enumerator.MoveNextAsync());
        using var batch = enumerator.Current;
        var column = Assert.IsType<ParquetBinaryColumnBatch>(batch.Columns[0]);
        Assert.Equal([true, false, true], Enumerable.Range(0, 3).Select(column.Validity.IsValid));
        Assert.Equal([0, 0, 0, 2], column.Offsets.ToArray());
        Assert.Equal([2, 3], column.Payload.ToArray());
    }

    [Fact]
    public async Task ScansAllNullByteArrayPage()
    {
        var bytes = ParquetFixtureBuilder.CreateInt32(new()
        {
            PhysicalTypeCode = (int)ParquetPhysicalType.ByteArray,
            PhysicalValues = new byte[][] { [1], [2], [3] },
            Repetition = ParquetRepetition.Optional,
            Validity = [false, false, false],
        });
        await using var file = await ParquetFile.OpenAsync(new MemoryStream(bytes, writable: false));
        await using var enumerator = file.ScanAsync(new([file.Metadata.Schema.Columns[0]])).GetAsyncEnumerator();

        Assert.True(await enumerator.MoveNextAsync());
        using var batch = enumerator.Current;
        var column = Assert.IsType<ParquetBinaryColumnBatch>(batch.Columns[0]);
        Assert.Equal([false, false, false], Enumerable.Range(0, 3).Select(column.Validity.IsValid));
        Assert.Equal([0, 0, 0, 0], column.Offsets.ToArray());
        Assert.Empty(column.Payload.ToArray());
    }

    [Fact]
    public async Task RejectsByteArrayValueOverConfiguredLimit()
    {
        var bytes = ParquetFixtureBuilder.CreateInt32(new()
        {
            PhysicalTypeCode = (int)ParquetPhysicalType.ByteArray,
            PhysicalValues = new byte[][] { [1, 2] },
        });
        await using var file = await ParquetFile.OpenAsync(
            new MemoryStream(bytes, writable: false),
            ParquetSourceOwnership.Caller,
            new ParquetReaderOptions { MaximumBinaryValueBytes = 1 },
            CancellationToken.None);

        await Assert.ThrowsAsync<ParquetLimitExceededException>(async () =>
        {
            await foreach (var batch in file.ScanAsync(new([file.Metadata.Schema.Columns[0]])))
                batch.Dispose();
        });
    }

    [Fact]
    public async Task OptionalValidityClearsUnusedTailBits()
    {
        var validity = new[] { true, false, true, false, true, false, true, false, true };
        var bytes = ParquetFixtureBuilder.CreateInt32(new()
        {
            Values = Enumerable.Range(0, validity.Length).ToArray(),
            Repetition = ParquetRepetition.Optional,
            Validity = validity,
        });
        await using var file = await ParquetFile.OpenAsync(new MemoryStream(bytes, writable: false));
        await using var enumerator = file.ScanAsync(new([file.Metadata.Schema.Columns[0]])).GetAsyncEnumerator();

        Assert.True(await enumerator.MoveNextAsync());
        using var batch = enumerator.Current;
        var column = Assert.IsType<ParquetPrimitiveColumnBatch<int>>(batch.Columns[0]);
        Assert.Equal([0b0101_0101, 0b0000_0001], column.Validity.Bits.ToArray());
    }

    [Fact]
    public async Task ShortensBinaryBatchesAtValueBoundaries()
    {
        var bytes = ParquetFixtureBuilder.CreateInt32(new()
        {
            PhysicalTypeCode = (int)ParquetPhysicalType.ByteArray,
            PhysicalValues = new byte[][] { [1, 2], [3, 4], [5, 6] },
        });
        await using var file = await ParquetFile.OpenAsync(
            new MemoryStream(bytes, writable: false),
            ParquetSourceOwnership.ParquetFile,
            new ParquetReaderOptions { MaximumBinaryBatchBytes = 3 },
            CancellationToken.None);
        var payloads = new List<byte[]>();

        await foreach (var batch in file.ScanAsync(new([file.Metadata.Schema.Columns[0]])))
        {
            using (batch)
                payloads.Add(Assert.IsType<ParquetBinaryColumnBatch>(batch.Columns[0]).Payload.ToArray());
        }

        Assert.Equal(3, payloads.Count);
        Assert.Equal([1, 2], payloads[0]);
        Assert.Equal([3, 4], payloads[1]);
        Assert.Equal([5, 6], payloads[2]);
    }

    [Fact]
    public async Task ScansOptionalFixedLengthByteArraysWithRowAlignedPayload()
    {
        var bytes = ParquetFixtureBuilder.CreateInt32(new()
        {
            PhysicalTypeCode = (int)ParquetPhysicalType.FixedLengthByteArray,
            TypeLength = 2,
            PhysicalValues = new byte[][] { [1, 2], [9, 9], [3, 4] },
            Repetition = ParquetRepetition.Optional,
            Validity = [true, false, true],
        });
        await using var file = await ParquetFile.OpenAsync(new MemoryStream(bytes, writable: false));
        await using var enumerator = file.ScanAsync(new([file.Metadata.Schema.Columns[0]])).GetAsyncEnumerator();

        Assert.True(await enumerator.MoveNextAsync());
        using var batch = enumerator.Current;
        var column = Assert.IsType<ParquetFixedLengthByteArrayColumnBatch>(batch.Columns[0]);
        Assert.Equal(2, column.TypeWidth);
        Assert.Equal([true, false, true], Enumerable.Range(0, 3).Select(column.Validity.IsValid));
        Assert.Equal([1, 2], column.Payload.Slice(0, 2).ToArray());
        Assert.Equal([3, 4], column.Payload.Slice(4, 2).ToArray());
    }

    [Fact]
    public async Task ScansRequiredDataPageV2()
    {
        var bytes = ParquetFixtureBuilder.CreateInt32(new()
        {
            PhysicalTypeCode = (int)ParquetPhysicalType.Int64,
            PhysicalValues = new long[] { 10, -20, 30 },
            PageVersion = FixturePageVersion.DataPageV2,
        });
        await using var file = await ParquetFile.OpenAsync(new MemoryStream(bytes, writable: false));
        await using var enumerator = file.ScanAsync(new([file.Metadata.Schema.Columns[0]])).GetAsyncEnumerator();

        Assert.True(await enumerator.MoveNextAsync());
        using var batch = enumerator.Current;
        Assert.Equal([10, -20, 30], Assert.IsType<ParquetPrimitiveColumnBatch<long>>(batch.Columns[0]).Values.ToArray());
    }

    [Fact]
    public async Task ScansOptionalSnappyDataPageV2WithUncompressedLevels()
    {
        var bytes = ParquetFixtureBuilder.CreateInt32(new()
        {
            Values = [10, 20, 30, 40],
            Repetition = ParquetRepetition.Optional,
            Validity = [false, true, false, true],
            PageVersion = FixturePageVersion.DataPageV2,
            CompressionCodec = ParquetCompressionCodec.Snappy,
            CrcMode = FixtureCrcMode.Valid,
        });
        await using var file = await ParquetFile.OpenAsync(new MemoryStream(bytes, writable: false));
        await using var enumerator = file.ScanAsync(new([file.Metadata.Schema.Columns[0]])).GetAsyncEnumerator();

        Assert.True(await enumerator.MoveNextAsync());
        using var batch = enumerator.Current;
        var column = Assert.IsType<ParquetPrimitiveColumnBatch<int>>(batch.Columns[0]);
        Assert.Equal([false, true, false, true], Enumerable.Range(0, 4).Select(column.Validity.IsValid));
        Assert.Equal(20, column.Values.Span[1]);
        Assert.Equal(40, column.Values.Span[3]);
    }

    [Fact]
    public async Task ExpandsDictionaryEncodedInt32Page()
    {
        var bytes = ParquetFixtureBuilder.CreateInt32(new()
        {
            Values = [20, 10, 20, 20],
            DictionaryValues = new int[] { 10, 20 },
            DictionaryIndices = [1, 0, 1, 1],
            AdvertisedEncodings = [(int)ParquetEncoding.Plain],
            CrcMode = FixtureCrcMode.Valid,
        });
        await using var file = await ParquetFile.OpenAsync(new MemoryStream(bytes, writable: false));
        await using var enumerator = file.ScanAsync(new([file.Metadata.Schema.Columns[0]])).GetAsyncEnumerator();

        Assert.True(await enumerator.MoveNextAsync());
        using var batch = enumerator.Current;
        Assert.Equal([20, 10, 20, 20], Assert.IsType<ParquetPrimitiveColumnBatch<int>>(batch.Columns[0]).Values.ToArray());
    }

    [Fact]
    public async Task AcceptsLegacyPlainDictionaryPageMarker()
    {
        var bytes = ParquetFixtureBuilder.CreateInt32(new()
        {
            Values = [20, 10],
            DictionaryValues = new int[] { 10, 20 },
            DictionaryIndices = [1, 0],
            DictionaryMode = FixtureDictionaryMode.LegacyEncodingBeforeDataPage,
        });
        await using var file = await ParquetFile.OpenAsync(new MemoryStream(bytes, writable: false));
        await using var enumerator = file.ScanAsync(new([file.Metadata.Schema.Columns[0]])).GetAsyncEnumerator();

        Assert.True(await enumerator.MoveNextAsync());
        using var batch = enumerator.Current;
        Assert.Equal([20, 10], Assert.IsType<ParquetPrimitiveColumnBatch<int>>(batch.Columns[0]).Values.ToArray());
    }

    [Fact]
    public async Task ExpandsDictionariesForEveryFixedWidthPrimitiveType()
    {
        static async Task AssertDictionaryScan<T>(
            T[] expected,
            T[] dictionary,
            int[] indices,
            ParquetPhysicalType physicalType)
            where T : unmanaged
        {
            var bytes = ParquetFixtureBuilder.CreateInt32(new()
            {
                PhysicalTypeCode = (int)physicalType,
                PhysicalValues = expected,
                DictionaryValues = dictionary,
                DictionaryIndices = indices,
            });
            await using var file = await ParquetFile.OpenAsync(new MemoryStream(bytes, writable: false));
            await using var enumerator = file.ScanAsync(new([file.Metadata.Schema.Columns[0]])).GetAsyncEnumerator();

            Assert.True(await enumerator.MoveNextAsync());
            using var batch = enumerator.Current;
            var actual = Assert.IsType<ParquetPrimitiveColumnBatch<T>>(batch.Columns[0]).Values.ToArray();
            if (typeof(T) == typeof(float))
            {
                Assert.Equal(
                    expected.Cast<float>().Select(BitConverter.SingleToInt32Bits),
                    actual.Cast<float>().Select(BitConverter.SingleToInt32Bits));
            }
            else if (typeof(T) == typeof(double))
            {
                Assert.Equal(
                    expected.Cast<double>().Select(BitConverter.DoubleToInt64Bits),
                    actual.Cast<double>().Select(BitConverter.DoubleToInt64Bits));
            }
            else
            {
                Assert.Equal(expected, actual);
            }
        }

        await AssertDictionaryScan(
            new bool[] { true, false, true },
            new bool[] { false, true },
            [1, 0, 1],
            ParquetPhysicalType.Boolean);
        await AssertDictionaryScan(
            new long[] { long.MaxValue, long.MinValue, long.MaxValue },
            new long[] { long.MinValue, long.MaxValue },
            [1, 0, 1],
            ParquetPhysicalType.Int64);
        await AssertDictionaryScan(
            new float[] { -2.5f, 1.25f, -2.5f },
            new float[] { 1.25f, -2.5f },
            [1, 0, 1],
            ParquetPhysicalType.Float);
        await AssertDictionaryScan(
            new double[] { -2.5d, 1.25d, -2.5d },
            new double[] { 1.25d, -2.5d },
            [1, 0, 1],
            ParquetPhysicalType.Double);
    }

    [Fact]
    public async Task ExpandsOptionalSnappyDictionaryDataPageV2()
    {
        var bytes = ParquetFixtureBuilder.CreateInt32(new()
        {
            Values = [20, 999, 10],
            Repetition = ParquetRepetition.Optional,
            Validity = [true, false, true],
            DictionaryValues = new int[] { 10, 20 },
            DictionaryIndices = [1, 0],
            PageVersion = FixturePageVersion.DataPageV2,
            CompressionCodec = ParquetCompressionCodec.Snappy,
        });
        await using var file = await ParquetFile.OpenAsync(new MemoryStream(bytes, writable: false));
        await using var enumerator = file.ScanAsync(new([file.Metadata.Schema.Columns[0]])).GetAsyncEnumerator();

        Assert.True(await enumerator.MoveNextAsync());
        using var batch = enumerator.Current;
        var column = Assert.IsType<ParquetPrimitiveColumnBatch<int>>(batch.Columns[0]);
        Assert.Equal([true, false, true], Enumerable.Range(0, 3).Select(column.Validity.IsValid));
        Assert.Equal(20, column.Values.Span[0]);
        Assert.Equal(10, column.Values.Span[2]);
    }

    [Fact]
    public async Task ExpandsOptionalSnappyByteArrayDictionaryDataPageV2()
    {
        var bytes = ParquetFixtureBuilder.CreateInt32(new()
        {
            PhysicalTypeCode = (int)ParquetPhysicalType.ByteArray,
            PhysicalValues = new byte[][] { [4, 5], [], [1] },
            Repetition = ParquetRepetition.Optional,
            Validity = [true, false, true],
            DictionaryValues = new byte[][] { [1], [4, 5] },
            DictionaryIndices = [1, 0],
            PageVersion = FixturePageVersion.DataPageV2,
            CompressionCodec = ParquetCompressionCodec.Snappy,
        });
        await using var file = await ParquetFile.OpenAsync(new MemoryStream(bytes, writable: false));
        await using var enumerator = file.ScanAsync(new([file.Metadata.Schema.Columns[0]])).GetAsyncEnumerator();

        Assert.True(await enumerator.MoveNextAsync());
        using var batch = enumerator.Current;
        var column = Assert.IsType<ParquetBinaryColumnBatch>(batch.Columns[0]);
        Assert.Equal([true, false, true], Enumerable.Range(0, 3).Select(column.Validity.IsValid));
        Assert.Equal([0, 2, 2, 3], column.Offsets.ToArray());
        Assert.Equal([4, 5, 1], column.Payload.ToArray());
    }

    [Fact]
    public async Task ExpandsFixedLengthByteArrayDictionary()
    {
        var bytes = ParquetFixtureBuilder.CreateInt32(new()
        {
            PhysicalTypeCode = (int)ParquetPhysicalType.FixedLengthByteArray,
            TypeLength = 2,
            PhysicalValues = new byte[][] { [3, 4], [1, 2], [3, 4] },
            DictionaryValues = new byte[][] { [1, 2], [3, 4] },
            DictionaryIndices = [1, 0, 1],
        });
        await using var file = await ParquetFile.OpenAsync(new MemoryStream(bytes, writable: false));
        await using var enumerator = file.ScanAsync(new([file.Metadata.Schema.Columns[0]])).GetAsyncEnumerator();

        Assert.True(await enumerator.MoveNextAsync());
        using var batch = enumerator.Current;
        var column = Assert.IsType<ParquetFixedLengthByteArrayColumnBatch>(batch.Columns[0]);
        Assert.Equal(2, column.TypeWidth);
        Assert.Equal([3, 4, 1, 2, 3, 4], column.Payload.ToArray());
    }

    [Fact]
    public async Task RejectsDictionaryIndexOutsideDictionary()
    {
        var bytes = ParquetFixtureBuilder.CreateInt32(new()
        {
            Values = [0],
            DictionaryValues = new int[] { 10, 20, 30 },
            DictionaryIndices = [3],
        });
        await using var file = await ParquetFile.OpenAsync(new MemoryStream(bytes, writable: false));

        await Assert.ThrowsAsync<ParquetFormatException>(async () =>
        {
            await foreach (var batch in file.ScanAsync(new([file.Metadata.Schema.Columns[0]])))
                batch.Dispose();
        });
    }

    [Fact]
    public async Task RejectsDuplicateDictionaryPageBeforeData()
    {
        var bytes = ParquetFixtureBuilder.CreateInt32(new()
        {
            Values = [10],
            DictionaryValues = new int[] { 10 },
            DictionaryIndices = [0],
            DictionaryMode = FixtureDictionaryMode.DuplicateBeforeDataPage,
        });
        await using var file = await ParquetFile.OpenAsync(new MemoryStream(bytes, writable: false));

        await Assert.ThrowsAsync<ParquetFormatException>(async () =>
        {
            await foreach (var batch in file.ScanAsync(new([file.Metadata.Schema.Columns[0]])))
                batch.Dispose();
        });
    }

    [Fact]
    public async Task RejectsDictionaryEncodedDataBeforeDictionaryPage()
    {
        var bytes = ParquetFixtureBuilder.CreateInt32(new()
        {
            Values = [10],
            DictionaryValues = new int[] { 10 },
            DictionaryIndices = [0],
            DictionaryMode = FixtureDictionaryMode.AfterDataPage,
        });
        await using var file = await ParquetFile.OpenAsync(new MemoryStream(bytes, writable: false));

        await Assert.ThrowsAsync<ParquetFormatException>(async () =>
        {
            await foreach (var batch in file.ScanAsync(new([file.Metadata.Schema.Columns[0]])))
                batch.Dispose();
        });
    }

    [Fact]
    public async Task SupportsPlainFallbackAfterDictionaryEncodedPage()
    {
        var bytes = ParquetFixtureBuilder.CreateInt32(new()
        {
            Values = [20, 10],
            DictionaryValues = new int[] { 10, 20 },
            DictionaryIndices = [1, 0],
            TrailingPlainValues = new int[] { 30, 40 },
            CrcMode = FixtureCrcMode.Valid,
        });
        await using var file = await ParquetFile.OpenAsync(new MemoryStream(bytes, writable: false));
        var observed = new List<int>();

        await foreach (var batch in file.ScanAsync(new([file.Metadata.Schema.Columns[0]])))
        {
            using (batch)
                observed.AddRange(Assert.IsType<ParquetPrimitiveColumnBatch<int>>(batch.Columns[0]).Values.ToArray());
        }

        Assert.Equal([20, 10, 30, 40], observed);
    }

    [Fact]
    public async Task ProjectsByExactTopLevelName()
    {
        var bytes = ParquetFixtureBuilder.CreateInt32(new() { Values = [7] });
        await using var file = await ParquetFile.OpenAsync(new MemoryStream(bytes, writable: false));

        await using var enumerator = file.ScanAsync(
            new ParquetScanOptions([file.Metadata.Schema.GetColumn("value")])).GetAsyncEnumerator();
        Assert.True(await enumerator.MoveNextAsync());
        using var batch = enumerator.Current;
        Assert.Equal([7], Assert.IsType<ParquetPrimitiveColumnBatch<int>>(batch.Columns[0]).Values.ToArray());
    }

    [Fact]
    public async Task RejectsAmbiguousTopLevelName()
    {
        var bytes = ParquetFixtureBuilder.CreateInt32(new()
        {
            RowGroupMode = FixtureRowGroupMode.Omitted,
            ColumnNames = ["same", "same"],
        });
        await using var file = await ParquetFile.OpenAsync(new MemoryStream(bytes, writable: false));

        Assert.Throws<ArgumentException>(() => file.Metadata.Schema.GetColumn("same"));
    }

    [Fact]
    public async Task RejectsDuplicateProjectionBeforeDecoderSelection()
    {
        var bytes = ParquetFixtureBuilder.CreateInt32(new() { Values = [7] });
        await using var file = await ParquetFile.OpenAsync(new MemoryStream(bytes, writable: false));

        Assert.Throws<ArgumentException>(() =>
            file.ScanAsync(new ParquetScanOptions([file.Metadata.Schema.Columns[0], file.Metadata.Schema.Columns[0]])).GetAsyncEnumerator());
    }

    [Fact]
    public async Task RejectsDuplicateRowGroupSelection()
    {
        var bytes = ParquetFixtureBuilder.CreateInt32(new() { Values = [7] });
        await using var file = await ParquetFile.OpenAsync(new MemoryStream(bytes, writable: false));

        Assert.Throws<ArgumentException>(() =>
            file.ScanAsync(new ParquetScanOptions(
                [file.Metadata.Schema.Columns[0]],
                [file.Metadata.RowGroups[0], file.Metadata.RowGroups[0]],
                null,
                65_536)).GetAsyncEnumerator());
    }

    [Fact]
    public async Task RejectsRangeOutsideTheFile()
    {
        var bytes = ParquetFixtureBuilder.CreateInt32(new() { Values = [7] });
        await using var file = await ParquetFile.OpenAsync(new MemoryStream(bytes, writable: false));

        Assert.Throws<ArgumentOutOfRangeException>(() =>
            file.ScanAsync(new ParquetScanOptions(
                [file.Metadata.Schema.Columns[0]],
                null,
                new(1, 1),
                65_536)).GetAsyncEnumerator());
    }

    [Fact]
    public async Task SplitsPageAtTargetBatchSize()
    {
        var bytes = ParquetFixtureBuilder.CreateInt32(new() { Values = [0, 1, 2, 3, 4] });
        await using var file = await ParquetFile.OpenAsync(new MemoryStream(bytes, writable: false));
        var observedOffsets = new List<long>();
        var observedValues = new List<int[]>();

        await foreach (var batch in file.ScanAsync(new(
            [file.Metadata.Schema.Columns[0]],
            null,
            null,
            2)))
        {
            using (batch)
            {
                var column = Assert.IsType<ParquetPrimitiveColumnBatch<int>>(batch.Columns[0]);
                observedOffsets.Add(batch.RowOffset);
                observedValues.Add(column.Values.ToArray());
            }
        }

        Assert.Equal([0L, 2L, 4L], observedOffsets);
        Assert.Equal([0, 1], observedValues[0]);
        Assert.Equal([2, 3], observedValues[1]);
        Assert.Equal([4], observedValues[2]);
    }

    [Fact]
    public async Task AppliesGlobalHalfOpenRowRange()
    {
        var bytes = ParquetFixtureBuilder.CreateInt32(new() { Values = [0, 1, 2, 3, 4] });
        await using var file = await ParquetFile.OpenAsync(new MemoryStream(bytes, writable: false));
        var observed = new List<int>();

        await foreach (var batch in file.ScanAsync(new(
            [file.Metadata.Schema.Columns[0]],
            null,
            new(1, 3),
            65_536)))
        {
            using (batch)
            {
                Assert.Equal(1, batch.RowOffset);
                observed.AddRange(Assert.IsType<ParquetPrimitiveColumnBatch<int>>(batch.Columns[0]).Values.ToArray());
            }
        }

        Assert.Equal([1, 2, 3], observed);
    }

    [Fact]
    public async Task AlignsMultipleColumnsAcrossDifferentPageBoundariesInProjectionOrder()
    {
        var bytes = ParquetFixtureBuilder.CreateRequiredInt32Columns(
        [
            new RequiredInt32FixtureColumn
            {
                Name = "left",
                Pages = [[10, 11], [12, 13, 14]],
            },
            new RequiredInt32FixtureColumn
            {
                Name = "right",
                Pages = [[20], [21, 22], [23, 24]],
            },
        ]);
        await using var file = await ParquetFile.OpenAsync(new MemoryStream(bytes, writable: false));
        var observedOffsets = new List<long>();
        var observedLeft = new List<int>();
        var observedRight = new List<int>();

        await foreach (var batch in file.ScanAsync(new ParquetScanOptions([file.Metadata.Schema.Columns[1], file.Metadata.Schema.Columns[0]])))
        {
            using (batch)
            {
                observedOffsets.Add(batch.RowOffset);
                Assert.Equal(batch.RowCount, batch.Columns[0].RowCount);
                Assert.Equal(batch.RowCount, batch.Columns[1].RowCount);
                Assert.Equal("right", batch.Columns[0].Column.Name);
                Assert.Equal("left", batch.Columns[1].Column.Name);
                observedRight.AddRange(Assert.IsType<ParquetPrimitiveColumnBatch<int>>(batch.Columns[0]).Values.ToArray());
                observedLeft.AddRange(Assert.IsType<ParquetPrimitiveColumnBatch<int>>(batch.Columns[1]).Values.ToArray());
            }
        }

        Assert.Equal([0, 1, 2, 3], observedOffsets);
        Assert.Equal([10, 11, 12, 13, 14], observedLeft);
        Assert.Equal([20, 21, 22, 23, 24], observedRight);
    }

    [Fact]
    public async Task RetainsAnnotationsOnGeneratedMultiColumnFixtures()
    {
        var bytes = ParquetFixtureBuilder.CreateRequiredInt32Columns(
        [
            new RequiredInt32FixtureColumn
            {
                Name = "modern",
                Pages = [[10]],
                LogicalTypeDiscriminator = (int)ParquetLogicalTypeKind.Date,
            },
            new RequiredInt32FixtureColumn
            {
                Name = "legacy",
                Pages = [[20]],
                ConvertedType = (int)ParquetConvertedType.Date,
            },
        ]);
        await using var file = await ParquetFile.OpenAsync(new MemoryStream(bytes, writable: false));

        Assert.Equal(ParquetAnnotationStatus.ModernOnly,
            file.Metadata.Schema.Columns[0].SchemaElement.AnnotationStatus);
        Assert.Equal(ParquetAnnotationStatus.LegacyOnly,
            file.Metadata.Schema.Columns[1].SchemaElement.AnnotationStatus);
        Assert.All(file.Metadata.Schema.Columns, column =>
            Assert.Equal(ParquetSemanticTypeKind.Date, column.SchemaElement.SemanticAnnotation?.Kind));
    }

    [Fact]
    public async Task DisposingAlignedMultiColumnBatchInvalidatesEveryColumnView()
    {
        using var tracker = new PoolTracker();
        var bytes = ParquetFixtureBuilder.CreateRequiredInt32Columns(
        [
            new RequiredInt32FixtureColumn { Name = "left", Pages = [[10, 11]] },
            new RequiredInt32FixtureColumn { Name = "right", Pages = [[20, 21]] },
        ]);
        await using var file = await ParquetFile.OpenAsync(new MemoryStream(bytes, writable: false));
        await using var enumerator = file.ScanAsync(new ParquetScanOptions([file.Metadata.Schema.Columns[0], file.Metadata.Schema.Columns[1]])).GetAsyncEnumerator();

        Assert.True(await enumerator.MoveNextAsync());
        var batch = enumerator.Current;
        var left = Assert.IsType<ParquetPrimitiveColumnBatch<int>>(batch.Columns[0]);
        var right = Assert.IsType<ParquetPrimitiveColumnBatch<int>>(batch.Columns[1]);
        Assert.Equal([10, 11], left.Values.ToArray());
        Assert.Equal([20, 21], right.Values.ToArray());

        batch.Dispose();
        batch.Dispose();

        Assert.Throws<ObjectDisposedException>(() => left.Values);
        Assert.Throws<ObjectDisposedException>(() => right.Values);
        Assert.Throws<ObjectDisposedException>(() => left.Validity.IsValid(0));
        Assert.Throws<ObjectDisposedException>(() => right.Validity.IsValid(0));
        Assert.False(await enumerator.MoveNextAsync());
    }

    [Fact]
    public async Task PreservesRowGroupBoundariesAndOffsets()
    {
        var bytes = ParquetFixtureBuilder.CreateRequiredInt32RowGroups(
        [
            [10, 11],
            [20, 21, 22],
        ]);
        await using var file = await ParquetFile.OpenAsync(new MemoryStream(bytes, writable: false));
        var observedGlobalOffsets = new List<long>();
        var observedGroups = new List<int>();
        var observedGroupOffsets = new List<long>();
        var observedValues = new List<int[]>();

        await foreach (var batch in file.ScanAsync(new(
            [file.Metadata.Schema.Columns[0]],
            null,
            null,
            16)))
        {
            using (batch)
            {
                observedGlobalOffsets.Add(batch.RowOffset);
                observedGroups.Add(batch.RowGroupOrdinal);
                observedGroupOffsets.Add(batch.RowOffsetInGroup);
                observedValues.Add(Assert.IsType<ParquetPrimitiveColumnBatch<int>>(batch.Columns[0]).Values.ToArray());
            }
        }

        Assert.Equal([0L, 2L], observedGlobalOffsets);
        Assert.Equal([0, 1], observedGroups);
        Assert.Equal([0L, 0L], observedGroupOffsets);
        Assert.Equal([10, 11], observedValues[0]);
        Assert.Equal([20, 21, 22], observedValues[1]);
    }

    [Fact]
    public async Task ExplicitEmptyRowGroupSelectionYieldsNothing()
    {
        var bytes = ParquetFixtureBuilder.CreateInt32(new() { Values = [1] });
        await using var file = await ParquetFile.OpenAsync(new MemoryStream(bytes, writable: false));
        var count = 0;

        await foreach (var batch in file.ScanAsync(new(
            [file.Metadata.Schema.Columns[0]],
            Array.Empty<ParquetRowGroup>(),
            null,
            65_536)))
        {
            batch.Dispose();
            count++;
        }

        Assert.Equal(0, count);
    }

    [Fact]
    public async Task RequiresCurrentBatchDisposalBeforeAdvancing()
    {
        using var tracker = new PoolTracker();
        var bytes = ParquetFixtureBuilder.CreateInt32(new() { Values = [1, 2] });
        await using var file = await ParquetFile.OpenAsync(new MemoryStream(bytes, writable: false));
        await using var enumerator = file.ScanAsync(new(
            [file.Metadata.Schema.Columns[0]],
            null,
            null,
            1)).GetAsyncEnumerator();

        Assert.True(await enumerator.MoveNextAsync());
        var rentsWithHeldBatch = tracker.RentCount;
        await Assert.ThrowsAsync<InvalidOperationException>(async () => await enumerator.MoveNextAsync());
        Assert.Equal(rentsWithHeldBatch, tracker.RentCount);
        enumerator.Current.Dispose();
        Assert.True(await enumerator.MoveNextAsync());
        enumerator.Current.Dispose();
        Assert.False(await enumerator.MoveNextAsync());
    }

    [Fact]
    public async Task DisposedBatchInvalidatesViewsAndDisposeIsIdempotent()
    {
        var bytes = ParquetFixtureBuilder.CreateInt32(new() { Values = [1] });
        await using var file = await ParquetFile.OpenAsync(new MemoryStream(bytes, writable: false));
        await using var enumerator = file.ScanAsync(new([file.Metadata.Schema.Columns[0]])).GetAsyncEnumerator();
        Assert.True(await enumerator.MoveNextAsync());
        var batch = enumerator.Current;
        var column = Assert.IsType<ParquetPrimitiveColumnBatch<int>>(batch.Columns[0]);

        batch.Dispose();
        batch.Dispose();

        Assert.Throws<ObjectDisposedException>(() => column.Values);
        Assert.Throws<ObjectDisposedException>(() => column.Validity.IsValid(0));
    }

    [Fact]
    public async Task RejectsInvalidPageCrcBeforeYieldingBatch()
    {
        var bytes = ParquetFixtureBuilder.CreateInt32(new()
        {
            Values = [1],
            CrcMode = FixtureCrcMode.Corrupt,
        });
        await using var file = await ParquetFile.OpenAsync(new MemoryStream(bytes, writable: false));

        await Assert.ThrowsAsync<ParquetFormatException>(async () =>
        {
            await foreach (var batch in file.ScanAsync(new([file.Metadata.Schema.Columns[0]])))
                batch.Dispose();
        });
    }

    [Fact]
    public async Task EnforcesPageHeaderPayloadValueAndPageCountLimits()
    {
        var oversizedHeader = ParquetFixtureBuilder.CreateInt32(new()
        {
            Values = [1],
            PageHeaderOverrides = new() { HeaderPaddingBytes = 512 },
        });
        await AssertScanFailureAsync<ParquetLimitExceededException>(
            oversizedHeader,
            new ParquetReaderOptions { MaximumPageHeaderBytes = 128 });

        var oversizedPayload = ParquetFixtureBuilder.CreateInt32(new()
        {
            Values = [1],
            PageHeaderOverrides = new() { UncompressedSize = 17 },
        });
        await AssertScanFailureAsync<ParquetLimitExceededException>(
            oversizedPayload,
            new ParquetReaderOptions { MaximumUncompressedPageBytes = 16 });

        var tooManyValues = ParquetFixtureBuilder.CreateInt32(new()
        {
            Values = [1],
            PageHeaderOverrides = new() { ValueCount = 2 },
        });
        await AssertScanFailureAsync<ParquetLimitExceededException>(
            tooManyValues,
            new ParquetReaderOptions { MaximumValuesPerPage = 1 });

        var tooManyPages = ParquetFixtureBuilder.CreateInt32(new()
        {
            Values = [1],
            TrailingPlainValues = new int[] { 2 },
        });
        await AssertScanFailureAsync<ParquetLimitExceededException>(
            tooManyPages,
            new ParquetReaderOptions { MaximumPagesPerColumnChunk = 1 });
    }

    [Fact]
    public async Task RejectsRleEncodedBooleanValues()
    {
        // SPEC 6.2 narrows booleans to PLAIN-only in Core 0.1: an RLE-encoded
        // boolean page must be rejected, never misdecoded as bit-packed values.
        var bytes = ParquetFixtureBuilder.CreateInt32(new()
        {
            PhysicalTypeCode = (int)ParquetPhysicalType.Boolean,
            PhysicalValues = new bool[] { true, false, true },
            PageHeaderOverrides = new() { ValueEncodingCode = (int)ParquetEncoding.RunLength },
        });
        await AssertScanFailureAsync<ParquetUnsupportedFeatureException>(
            bytes,
            ParquetReaderOptions.Default);
    }

    [Fact]
    public async Task ClassifiesNegativeSizesAndUnsupportedPageFeatures()
    {
        var negativeSize = ParquetFixtureBuilder.CreateInt32(new()
        {
            Values = [1],
            PageHeaderOverrides = new() { CompressedSize = -1 },
        });
        await AssertScanFailureAsync<ParquetFormatException>(negativeSize, ParquetReaderOptions.Default);

        var unsupportedPage = ParquetFixtureBuilder.CreateInt32(new()
        {
            Values = [1],
            PageHeaderOverrides = new() { TypeCode = 1 },
        });
        await AssertScanFailureAsync<ParquetUnsupportedFeatureException>(
            unsupportedPage,
            ParquetReaderOptions.Default);

        var unsupportedEncoding = ParquetFixtureBuilder.CreateInt32(new()
        {
            Values = [1],
            PageHeaderOverrides = new() { ValueEncodingCode = 99 },
        });
        await AssertScanFailureAsync<ParquetUnsupportedFeatureException>(
            unsupportedEncoding,
            ParquetReaderOptions.Default);
    }

    [Theory]
    [InlineData(-1)]
    [InlineData(1_000)]
    public async Task RejectsInvalidV1DefinitionLevelBoundaries(int declaredLength)
    {
        var bytes = ParquetFixtureBuilder.CreateInt32(new()
        {
            Values = [1, 0, 2],
            Repetition = ParquetRepetition.Optional,
            Validity = [true, false, true],
            PageHeaderOverrides = new() { V1DefinitionLevelByteLength = declaredLength },
        });

        await AssertScanFailureAsync<ParquetFormatException>(bytes, ParquetReaderOptions.Default);
    }

    [Fact]
    public async Task SkipsAnEmptyV1PageBeforeADataPage()
    {
        var bytes = ParquetFixtureBuilder.CreateInt32(new()
        {
            Values = [],
            TrailingPlainValues = new int[] { 7 },
        });
        await using var file = await ParquetFile.OpenAsync(new MemoryStream(bytes, writable: false));

        await using var enumerator = file.ScanAsync(new([file.Metadata.Schema.Columns[0]])).GetAsyncEnumerator();
        Assert.True(await enumerator.MoveNextAsync());
        var batch = enumerator.Current;
        Assert.Equal([7], Assert.IsType<ParquetPrimitiveColumnBatch<int>>(batch.Columns[0]).Values.ToArray());
        batch.Dispose();
        Assert.False(await enumerator.MoveNextAsync());
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ScansAllValidOptionalInt32WithSlicedBatches(bool useV2)
    {
        using var tracker = new PoolTracker();
        var bytes = ParquetFixtureBuilder.CreateInt32(new()
        {
            Values = [1, 2, 3],
            Repetition = ParquetRepetition.Optional,
            PageVersion = useV2 ? FixturePageVersion.DataPageV2 : FixturePageVersion.DataPageV1,
        });
        await using (var file = await ParquetFile.OpenAsync(new MemoryStream(bytes, writable: false)))
        {
            await using var enumerator = file.ScanAsync(new([file.Metadata.Schema.Columns[0]], null, null, 2)).GetAsyncEnumerator();
            Assert.True(await enumerator.MoveNextAsync());
            using (var first = enumerator.Current)
            {
                Assert.Equal(2, first.RowCount);
                var column = Assert.IsType<ParquetPrimitiveColumnBatch<int>>(first.Columns[0]);
                Assert.True(column.Validity.IsAllValid);
                Assert.Equal([1, 2], column.Values.ToArray());
            }
            Assert.True(await enumerator.MoveNextAsync());
            using (var second = enumerator.Current)
            {
                Assert.Equal(1, second.RowCount);
                var column = Assert.IsType<ParquetPrimitiveColumnBatch<int>>(second.Columns[0]);
                Assert.True(column.Validity.IsAllValid);
                Assert.Equal([3], column.Values.ToArray());
            }
            Assert.False(await enumerator.MoveNextAsync());
        }
    }

    [Fact]
    public async Task ScansAllValidOptionalInt32WithFullPageTransferAndRowRange()
    {
        using var tracker = new PoolTracker();
        var bytes = ParquetFixtureBuilder.CreateInt32(new()
        {
            Values = [1, 2, 3],
            Repetition = ParquetRepetition.Optional,
        });
        await using (var file = await ParquetFile.OpenAsync(new MemoryStream(bytes, writable: false)))
        {
            await using var full = file.ScanAsync(new([file.Metadata.Schema.Columns[0]], null, null, 3)).GetAsyncEnumerator();
            Assert.True(await full.MoveNextAsync());
            using (var batch = full.Current)
            {
                Assert.Equal(3, batch.RowCount);
                Assert.True(Assert.IsType<ParquetPrimitiveColumnBatch<int>>(batch.Columns[0]).Validity.IsAllValid);
            }
            Assert.False(await full.MoveNextAsync());
        }
        await using (var rangedFile = await ParquetFile.OpenAsync(new MemoryStream(bytes, writable: false)))
        {
            var range = new ParquetRowRange(1, 2);
            await using var ranged = rangedFile.ScanAsync(new([rangedFile.Metadata.Schema.Columns[0]], null, range, 2)).GetAsyncEnumerator();
            Assert.True(await ranged.MoveNextAsync());
            using (var batch = ranged.Current)
            {
                Assert.Equal([2, 3], Assert.IsType<ParquetPrimitiveColumnBatch<int>>(batch.Columns[0]).Values.ToArray());
                Assert.True(Assert.IsType<ParquetPrimitiveColumnBatch<int>>(batch.Columns[0]).Validity.IsAllValid);
            }
            Assert.False(await ranged.MoveNextAsync());
        }
    }
    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public async Task RejectsInvalidTargetBatchSizeForSingleColumnScans(int target)
    {
        var bytes = ParquetFixtureBuilder.CreateInt32(new() { Values = [1, 2, 3] });
        await using var file = await ParquetFile.OpenAsync(new MemoryStream(bytes, writable: false));
        Assert.Throws<ArgumentOutOfRangeException>(() => file.ScanAsync(new([file.Metadata.Schema.Columns[0]], null, null, target)).GetAsyncEnumerator());
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public async Task RejectsInvalidTargetBatchSizeForProjectedScans(int target)
    {
        var bytes = ParquetFixtureBuilder.CreateRequiredInt32Columns(
        [
            new RequiredInt32FixtureColumn { Name = "a", Pages = [[1, 2, 3]] },
            new RequiredInt32FixtureColumn { Name = "b", Pages = [[4, 5, 6]] },
        ]);
        await using var file = await ParquetFile.OpenAsync(new MemoryStream(bytes, writable: false));
        var columns = file.Metadata.Schema.Columns;
        Assert.Throws<ArgumentOutOfRangeException>(() => file.ScanAsync(new([columns[0], columns[1]], null, null, target)).GetAsyncEnumerator());
    }

    [Fact]
    public async Task RejectsTargetAboveReaderMaximumForBothScanWidths()
    {
        var singleBytes = ParquetFixtureBuilder.CreateInt32(new() { Values = [1, 2, 3] });
        await using var singleFile = await ParquetFile.OpenAsync(
            new MemoryStream(singleBytes, writable: false),
            ParquetSourceOwnership.Caller,
            new ParquetReaderOptions { MaximumRowsPerBatch = 1 },
            CancellationToken.None);
        Assert.Throws<ArgumentOutOfRangeException>(() => singleFile.ScanAsync(new([singleFile.Metadata.Schema.Columns[0]], null, null, 3)).GetAsyncEnumerator());

        var multiBytes = ParquetFixtureBuilder.CreateRequiredInt32Columns(
        [
            new RequiredInt32FixtureColumn { Name = "a", Pages = [[1, 2, 3]] },
            new RequiredInt32FixtureColumn { Name = "b", Pages = [[4, 5, 6]] },
        ]);
        await using var multiFile = await ParquetFile.OpenAsync(
            new MemoryStream(multiBytes, writable: false),
            ParquetSourceOwnership.Caller,
            new ParquetReaderOptions { MaximumRowsPerBatch = 1 },
            CancellationToken.None);
        Assert.Throws<ArgumentOutOfRangeException>(() => multiFile.ScanAsync(new([multiFile.Metadata.Schema.Columns[0], multiFile.Metadata.Schema.Columns[1]], null, null, 3)).GetAsyncEnumerator());
    }

    [Fact]
    public async Task AcceptsTargetAtExactReaderMaximumForBothScanWidths()
    {
        // The over-limit check is strictly greater-than: a target equal to the
        // configured maximum opens on both scan widths.
        var singleBytes = ParquetFixtureBuilder.CreateInt32(new() { Values = [1, 2, 3] });
        await using var singleFile = await ParquetFile.OpenAsync(
            new MemoryStream(singleBytes, writable: false),
            ParquetSourceOwnership.Caller,
            new ParquetReaderOptions { MaximumRowsPerBatch = 3 },
            CancellationToken.None);
        await using var singleEnumerator = singleFile.ScanAsync(new([singleFile.Metadata.Schema.Columns[0]], null, null, 3)).GetAsyncEnumerator();
        Assert.True(await singleEnumerator.MoveNextAsync());
        Assert.Equal([1, 2, 3], Assert.IsType<ParquetPrimitiveColumnBatch<int>>(singleEnumerator.Current.Columns[0]).Values.ToArray());
        singleEnumerator.Current.Dispose();
        Assert.False(await singleEnumerator.MoveNextAsync());

        var multiBytes = ParquetFixtureBuilder.CreateRequiredInt32Columns(
        [
            new RequiredInt32FixtureColumn { Name = "a", Pages = [[1, 2, 3]] },
            new RequiredInt32FixtureColumn { Name = "b", Pages = [[4, 5, 6]] },
        ]);
        await using var multiFile = await ParquetFile.OpenAsync(
            new MemoryStream(multiBytes, writable: false),
            ParquetSourceOwnership.Caller,
            new ParquetReaderOptions { MaximumRowsPerBatch = 3 },
            CancellationToken.None);
        await using var multiEnumerator = multiFile.ScanAsync(new([multiFile.Metadata.Schema.Columns[0], multiFile.Metadata.Schema.Columns[1]], null, null, 3)).GetAsyncEnumerator();
        Assert.True(await multiEnumerator.MoveNextAsync());
        Assert.Equal([1, 2, 3], Assert.IsType<ParquetPrimitiveColumnBatch<int>>(multiEnumerator.Current.Columns[0]).Values.ToArray());
        Assert.Equal([4, 5, 6], Assert.IsType<ParquetPrimitiveColumnBatch<int>>(multiEnumerator.Current.Columns[1]).Values.ToArray());
        multiEnumerator.Current.Dispose();
        Assert.False(await multiEnumerator.MoveNextAsync());
    }

    [Fact]
    public async Task RejectsInvalidTargetBeforePayloadReadsForEmptySelection()
    {
        var bytes = ParquetFixtureBuilder.CreateRequiredInt32Columns(
        [
            new RequiredInt32FixtureColumn { Name = "a", Pages = [[1, 2, 3]] },
            new RequiredInt32FixtureColumn { Name = "b", Pages = [[4, 5, 6]] },
        ]);
        await using var file = await ParquetFile.OpenAsync(new MemoryStream(bytes, writable: false));
        var emptyRange = new ParquetRowRange(0, 0);
        Assert.Throws<ArgumentOutOfRangeException>(() => file.ScanAsync(new([file.Metadata.Schema.Columns[0], file.Metadata.Schema.Columns[1]], null, emptyRange, 0)).GetAsyncEnumerator());
    }
    private static async Task AssertScanFailureAsync<TException>(
            byte[] bytes,
            ParquetReaderOptions options)
            where TException : Exception
    {
        await using var file = await ParquetFile.OpenAsync(
            new MemoryStream(bytes, writable: false),
            ParquetSourceOwnership.Caller,
            options,
            CancellationToken.None);
        await Assert.ThrowsAsync<TException>(async () =>
        {
            await foreach (var batch in file.ScanAsync(new([file.Metadata.Schema.Columns[0]])))
                batch.Dispose();
        });
    }

}
