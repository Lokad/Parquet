namespace Lokad.Parquet.Tests;

// R05: dictionary decoding failures carry page identity: every length, index
// and trailing-byte error records its row group, column and page ordinals,
// hybrid errors keep their inner error, and delayed reads behave identically.
public sealed class DictionaryLocationTests
{
    [Fact]
    public async Task RequiredInt32InvalidIndexCarriesPageIdentity()
    {
        // PLAN repro: a three-entry dictionary with index 3.
        var bytes = ParquetFixtureBuilder.CreateInt32(new()
        {
            Values = [10],
            DictionaryValues = new int[] { 10, 20, 30 },
            DictionaryIndices = [3],
        });
        var exception = await ScanSingleColumnForFailureAsync(bytes, false);
        Assert.Contains("outside the dictionary", exception.Message, StringComparison.Ordinal);
        AssertPageIdentity(exception, 0, 0, 1);
    }

    [Fact]
    public async Task OptionalInt32V1InvalidIndexCarriesPageIdentity()
    {
        const int rows = 64;
        var dictionary = new int[] { 10, 20, 30 };
        var validity = new bool[rows];
        var physical = new List<int>();
        for (var row = 0; row < rows; row++)
        {
            validity[row] = (row & 3) != 0;
            if (validity[row])
                physical.Add(row % dictionary.Length);
        }
        physical[^1] = 3;
        var values = new int[rows];
        var bytes = ParquetFixtureBuilder.CreateInt32(new()
        {
            PhysicalValues = values,
            Repetition = ParquetRepetition.Optional,
            Validity = validity,
            DictionaryValues = dictionary,
            DictionaryIndices = physical.ToArray(),
        });
        var exception = await ScanSingleColumnForFailureAsync(bytes, false);
        Assert.Contains("outside the dictionary", exception.Message, StringComparison.Ordinal);
        AssertPageIdentity(exception, 0, 0, 1);
    }

    [Fact]
    public async Task OptionalInt32V2InvalidIndexCarriesPageIdentity()
    {
        const int rows = 64;
        var dictionary = new int[] { 10, 20, 30 };
        var indices = new int[rows];
        for (var row = 0; row < rows; row++)
            indices[row] = row % dictionary.Length;
        indices[^1] = 3;
        var values = indices.Select(index => index < dictionary.Length ? dictionary[index] : 0).ToArray();
        var bytes = ParquetFixtureBuilder.CreateInt32(new()
        {
            PhysicalValues = values,
            Repetition = ParquetRepetition.Optional,
            Validity = Enumerable.Repeat(true, rows).ToArray(),
            DictionaryValues = dictionary,
            DictionaryIndices = indices,
            PageVersion = FixturePageVersion.DataPageV2,
        });
        var exception = await ScanSingleColumnForFailureAsync(bytes, false);
        Assert.Contains("outside the dictionary", exception.Message, StringComparison.Ordinal);
        AssertPageIdentity(exception, 0, 0, 1);
    }

    [Fact]
    public async Task BinaryDictionaryInvalidIndexCarriesPageIdentity()
    {
        const int rows = 64;
        var dictionary = new byte[][] { [1], [2, 3], [4, 5, 6] };
        var validity = new bool[rows];
        var physical = new List<int>();
        for (var row = 0; row < rows; row++)
        {
            validity[row] = (row & 3) != 0;
            if (validity[row])
                physical.Add(row % dictionary.Length);
        }
        physical[^1] = 3;
        var values = new byte[rows][];
        for (var row = 0; row < rows; row++)
            values[row] = validity[row] ? dictionary[0] : [];
        var bytes = ParquetFixtureBuilder.CreateInt32(new()
        {
            PhysicalTypeCode = (int)ParquetPhysicalType.ByteArray,
            PhysicalValues = values,
            Repetition = ParquetRepetition.Optional,
            Validity = validity,
            DictionaryValues = dictionary,
            DictionaryIndices = physical.ToArray(),
        });
        var exception = await ScanSingleColumnForFailureAsync(bytes, false);
        Assert.Contains("outside the dictionary", exception.Message, StringComparison.Ordinal);
        AssertPageIdentity(exception, 0, 0, 1);
    }

    [Fact]
    public async Task FixedDictionaryInvalidIndexCarriesPageIdentity()
    {
        const int rows = 64;
        var dictionary = new byte[][] { [1, 2], [3, 4], [5, 6] };
        var validity = new bool[rows];
        var physical = new List<int>();
        for (var row = 0; row < rows; row++)
        {
            validity[row] = (row & 3) != 0;
            if (validity[row])
                physical.Add(row % dictionary.Length);
        }
        physical[^1] = 3;
        var values = new byte[rows][];
        for (var row = 0; row < rows; row++)
            values[row] = validity[row] ? dictionary[0] : new byte[2];
        var bytes = ParquetFixtureBuilder.CreateInt32(new()
        {
            PhysicalTypeCode = (int)ParquetPhysicalType.FixedLengthByteArray,
            TypeLength = 2,
            PhysicalValues = values,
            Repetition = ParquetRepetition.Optional,
            Validity = validity,
            DictionaryValues = dictionary,
            DictionaryIndices = physical.ToArray(),
        });
        var exception = await ScanSingleColumnForFailureAsync(bytes, false);
        Assert.Contains("outside the dictionary", exception.Message, StringComparison.Ordinal);
        AssertPageIdentity(exception, 0, 0, 1);
    }

    [Fact]
    public async Task TrailingIndexRunCarriesPageIdentity()
    {
        var dictionary = new int[] { 10, 20 };
        var indices = new int[] { 1, 0, 1 };
        var values = indices.Select(index => dictionary[index]).ToArray();
        var bytes = ParquetFixtureBuilder.CreateInt32(new()
        {
            PhysicalValues = values,
            DictionaryValues = dictionary,
            DictionaryIndices = indices,
            AppendTrailingIndexRun = true,
        });
        var exception = await ScanSingleColumnForFailureAsync(bytes, false);
        Assert.Contains("trailing index bytes", exception.Message, StringComparison.Ordinal);
        AssertPageIdentity(exception, 0, 0, 1);
    }

    [Fact]
    public async Task BitPackedTrailingRunCarriesPageIdentity()
    {
        var dictionary = new int[] { 10, 20, 30 };
        var indices = new int[40];
        for (var row = 0; row < indices.Length; row++)
            indices[row] = row % dictionary.Length;
        var values = indices.Select(index => dictionary[index]).ToArray();
        var bytes = ParquetFixtureBuilder.CreateInt32(new()
        {
            PhysicalValues = values,
            DictionaryValues = dictionary,
            DictionaryIndices = indices,
            BitPackedIndices = true,
            AppendTrailingIndexRun = true,
        });
        var exception = await ScanSingleColumnForFailureAsync(bytes, false);
        Assert.Contains("trailing index bytes", exception.Message, StringComparison.Ordinal);
        AssertPageIdentity(exception, 0, 0, 1);
    }

    [Fact]
    public async Task InvalidIndexBitWidthCarriesPageIdentity()
    {
        // An index bit width the hybrid decoder rejects exercises the merge path:
        // the page boundary supplies the identity while the inner error is kept.
        var dictionary = new int[] { 10, 20, 30 };
        var indices = new int[] { 0, 1, 2 };
        var values = indices.Select(index => dictionary[index]).ToArray();
        var bytes = ParquetFixtureBuilder.CreateInt32(new()
        {
            PhysicalValues = values,
            DictionaryValues = dictionary,
            DictionaryIndices = indices,
            DictionaryIndexBitWidth = 64,
        });
        var exception = await ScanSingleColumnForFailureAsync(bytes, false);
        Assert.Contains("invalid bit width", exception.Message, StringComparison.Ordinal);
        AssertPageIdentity(exception, 0, 0, 1);
        Assert.NotNull(exception.InnerException);
    }

    [Fact]
    public async Task OpaqueSourceInvalidIndexCarriesPageIdentity()
    {
        var bytes = ParquetFixtureBuilder.CreateInt32(new()
        {
            Values = [10],
            DictionaryValues = new int[] { 10, 20, 30 },
            DictionaryIndices = [3],
        });
        var exception = await ScanSingleColumnForFailureAsync(bytes, true);
        Assert.Contains("outside the dictionary", exception.Message, StringComparison.Ordinal);
        AssertPageIdentity(exception, 0, 0, 1);
    }

    [Fact]
    public async Task Int32DictionaryLengthMismatchCarriesDictionaryPageIdentity()
    {
        var bytes = ParquetFixtureBuilder.CreateInt32(new()
        {
            Values = [10, 20, 10],
            DictionaryValues = new int[] { 10, 20, 30 },
            DictionaryIndices = [0, 1, 0],
            PageHeaderOverrides = new ParquetPageHeaderOverrides { DictionaryValueCount = 5 },
        });
        var exception = await ScanSingleColumnForFailureAsync(bytes, false);
        Assert.Contains("does not match its entry count", exception.Message, StringComparison.Ordinal);
        AssertPageIdentity(exception, 0, 0, 0);
    }

    [Fact]
    public async Task BinaryDictionaryLengthMismatchCarriesDictionaryPageIdentity()
    {
        var dictionary = new byte[][] { [1], [2, 3], [4, 5, 6] };
        var values = new byte[][] { [1], [2, 3], [1] };
        var bytes = ParquetFixtureBuilder.CreateInt32(new()
        {
            PhysicalTypeCode = (int)ParquetPhysicalType.ByteArray,
            PhysicalValues = values,
            DictionaryValues = dictionary,
            DictionaryIndices = [0, 1, 0],
            PageHeaderOverrides = new ParquetPageHeaderOverrides { DictionaryValueCount = 5 },
        });
        var exception = await ScanSingleColumnForFailureAsync(bytes, false);
        Assert.Contains("dictionary length is truncated", exception.Message, StringComparison.Ordinal);
        AssertPageIdentity(exception, 0, 0, 0);
    }

    [Fact]
    public async Task FixedDictionaryLengthMismatchCarriesDictionaryPageIdentity()
    {
        var dictionary = new byte[][] { [1, 2], [3, 4], [5, 6] };
        var values = new byte[][] { [1, 2], [3, 4], [1, 2] };
        var bytes = ParquetFixtureBuilder.CreateInt32(new()
        {
            PhysicalTypeCode = (int)ParquetPhysicalType.FixedLengthByteArray,
            TypeLength = 2,
            PhysicalValues = values,
            DictionaryValues = dictionary,
            DictionaryIndices = [0, 1, 0],
            PageHeaderOverrides = new ParquetPageHeaderOverrides { DictionaryValueCount = 5 },
        });
        var exception = await ScanSingleColumnForFailureAsync(bytes, false);
        Assert.Contains("payload length is inconsistent", exception.Message, StringComparison.Ordinal);
        AssertPageIdentity(exception, 0, 0, 0);
    }

    private static async Task<ParquetFormatException> ScanSingleColumnForFailureAsync(byte[] fixture, bool opaque)
    {
        using var tracker = new PoolTracker();
        if (opaque)
        {
            var source = new DictionaryLocationSource(fixture);
            await using var file = await ParquetFile.OpenAsync(source, ParquetSourceOwnership.Caller, new ParquetReaderOptions(), CancellationToken.None);
            return await CaptureAsync(file);
        }

        await using var memoryFile = await ParquetFile.OpenAsync(fixture, new ParquetReaderOptions(), CancellationToken.None);
        return await CaptureAsync(memoryFile);

        static async Task<ParquetFormatException> CaptureAsync(ParquetFile openFile)
        {
            return await Assert.ThrowsAsync<ParquetFormatException>(async () =>
            {
                await foreach (var batch in openFile.ScanAsync(new([openFile.Metadata.Schema.Columns[0]])))
                    batch.Dispose();
            });
        }
    }

    private static void AssertPageIdentity(ParquetFormatException exception, int rowGroup, int column, int page)
    {
        Assert.Equal(rowGroup, exception.RowGroupOrdinal);
        Assert.Equal(column, exception.ColumnOrdinal);
        Assert.Equal(page, exception.PageOrdinal);
        Assert.NotNull(exception.ByteOffset);
    }

    private sealed class DictionaryLocationSource(byte[] bytes) : IParquetRandomAccessSource
    {
        public long Length => bytes.Length;

        public ValueTask ReadExactlyAsync(long offset, ArraySegment<byte> destination, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            bytes.AsMemory(checked((int)offset), destination.Count).CopyTo(destination.AsMemory());
            return ValueTask.CompletedTask;
        }

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }
}
