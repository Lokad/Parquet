using Xunit.Abstractions;

namespace Lokad.Parquet.Tests;

// Item-19 dictionary expansion coverage: bitmap definition levels for optional
// pages with fused index bounds checks. Expected values come from locally built
// oracles; peak facts mirror the census lanes and need the exclusive
// collection below for PoolTracker isolation.
[Collection("Scan batch allocation")]
public sealed class DictionaryExpansionTests
{
    private readonly ITestOutputHelper _output;

    public DictionaryExpansionTests(ITestOutputHelper output) => _output = output;

    [Fact]
    public async Task RequiredDictionaryCoalescedRunsRoundTrip()
    {
        var dictionary = new int[] { 10, 20, 30 };
        var indices = new int[200];
        for (var row = 0; row < indices.Length; row++)
            indices[row] = (row / 50) % dictionary.Length;
        var expected = indices.Select(index => dictionary[index]).ToArray();
        var bytes = ParquetFixtureBuilder.CreateInt32(new()
        {
            PhysicalValues = expected,
            DictionaryValues = dictionary,
            DictionaryIndices = indices,
            CoalesceIndexRuns = true,
        });
        await ScanRequiredInt32Async(bytes, expected);
    }

    [Fact]
    public async Task RequiredDictionaryBitPackedRoundTrip()
    {
        var dictionary = new int[] { 3, 6, 9, 12, 15 };
        var indices = new int[100];
        for (var row = 0; row < indices.Length; row++)
            indices[row] = (row * 7 + 1) % dictionary.Length;
        var expected = indices.Select(index => dictionary[index]).ToArray();
        var bytes = ParquetFixtureBuilder.CreateInt32(new()
        {
            PhysicalValues = expected,
            DictionaryValues = dictionary,
            DictionaryIndices = indices,
            BitPackedIndices = true,
        });
        await ScanRequiredInt32Async(bytes, expected);
    }

    [Fact]
    public async Task RequiredDictionaryBitPackedPaddedTailRoundTrip()
    {
        var dictionary = new int[] { 3, 6, 9, 12, 15 };
        var indices = new int[70];
        for (var row = 0; row < indices.Length; row++)
            indices[row] = (row * 3 + 2) % dictionary.Length;
        var expected = indices.Select(index => dictionary[index]).ToArray();
        var bytes = ParquetFixtureBuilder.CreateInt32(new()
        {
            PhysicalValues = expected,
            DictionaryValues = dictionary,
            DictionaryIndices = indices,
            BitPackedIndices = true,
        });
        await ScanRequiredInt32Async(bytes, expected);
    }

    [Fact]
    public async Task RequiredDictionaryWideIndicesRoundTrip()
    {
        var dictionary = Enumerable.Range(0, 300).Select(static value => value * 11 - 500).ToArray();
        var indices = new int[500];
        for (var row = 0; row < indices.Length; row++)
            indices[row] = (row * 37) % dictionary.Length;
        var expected = indices.Select(index => dictionary[index]).ToArray();
        var bytes = ParquetFixtureBuilder.CreateInt32(new()
        {
            PhysicalValues = expected,
            DictionaryValues = dictionary,
            DictionaryIndices = indices,
            CoalesceIndexRuns = true,
        });
        await ScanRequiredInt32Async(bytes, expected);
    }

    [Fact]
    public async Task OptionalDictionaryAllValidReportsAllValid()
    {
        const int rows = 64;
        var dictionary = new int[] { 10, 20 };
        var indices = new int[rows];
        for (var row = 0; row < rows; row++)
            indices[row] = row & 1;
        var values = indices.Select(index => dictionary[index]).ToArray();
        var bytes = ParquetFixtureBuilder.CreateInt32(new()
        {
            PhysicalValues = values,
            Repetition = ParquetRepetition.Optional,
            Validity = Enumerable.Repeat(true, rows).ToArray(),
            DictionaryValues = dictionary,
            DictionaryIndices = indices,
        });
        using var tracker = new PoolTracker();
        await using var file = await ParquetFile.OpenAsync(bytes, new ParquetReaderOptions(), CancellationToken.None);
        await using var enumerator = file.ScanAsync(new([file.Metadata.Schema.Columns[0]])).GetAsyncEnumerator();
        Assert.True(await enumerator.MoveNextAsync());
        using (var batch = enumerator.Current)
        {
            var column = Assert.IsType<ParquetPrimitiveColumnBatch<int>>(batch.Columns[0]);
            Assert.True(column.Validity.IsAllValid);
            Assert.Equal(values, column.Values.ToArray());
        }
        Assert.False(await enumerator.MoveNextAsync());
    }

    [Fact]
    public async Task OptionalDictionaryAllValidV2ReportsAllValid()
    {
        const int rows = 64;
        var dictionary = new int[] { 10, 20 };
        var indices = new int[rows];
        for (var row = 0; row < rows; row++)
            indices[row] = (row + 1) & 1;
        var values = indices.Select(index => dictionary[index]).ToArray();
        var bytes = ParquetFixtureBuilder.CreateInt32(new()
        {
            PhysicalValues = values,
            Repetition = ParquetRepetition.Optional,
            Validity = Enumerable.Repeat(true, rows).ToArray(),
            DictionaryValues = dictionary,
            DictionaryIndices = indices,
            PageVersion = FixturePageVersion.DataPageV2,
        });
        using var tracker = new PoolTracker();
        await using var file = await ParquetFile.OpenAsync(bytes, new ParquetReaderOptions(), CancellationToken.None);
        await using var enumerator = file.ScanAsync(new([file.Metadata.Schema.Columns[0]])).GetAsyncEnumerator();
        Assert.True(await enumerator.MoveNextAsync());
        using (var batch = enumerator.Current)
        {
            var column = Assert.IsType<ParquetPrimitiveColumnBatch<int>>(batch.Columns[0]);
            Assert.True(column.Validity.IsAllValid);
            Assert.Equal(values, column.Values.ToArray());
        }
        Assert.False(await enumerator.MoveNextAsync());
    }

    [Fact]
    public async Task OptionalDictionaryAllNullRoundTrips()
    {
        const int rows = 64;
        var dictionary = new int[] { 10, 20 };
        var values = new int[rows];
        var bytes = ParquetFixtureBuilder.CreateInt32(new()
        {
            PhysicalValues = values,
            Repetition = ParquetRepetition.Optional,
            Validity = new bool[rows],
            DictionaryValues = dictionary,
            DictionaryIndices = [],
        });
        await ScanOptionalPrimitiveAsync(bytes, new int?[rows], null, null, 65_536);
    }

    [Fact]
    public async Task OptionalDictionaryAllNullV2RoundTrips()
    {
        const int rows = 64;
        var dictionary = new int[] { 10, 20 };
        var values = new int[rows];
        var bytes = ParquetFixtureBuilder.CreateInt32(new()
        {
            PhysicalValues = values,
            Repetition = ParquetRepetition.Optional,
            Validity = new bool[rows],
            DictionaryValues = dictionary,
            DictionaryIndices = [],
            PageVersion = FixturePageVersion.DataPageV2,
        });
        await ScanOptionalPrimitiveAsync(bytes, new int?[rows], null, null, 65_536);
    }

    [Fact]
    public async Task OptionalDictionaryMixedV1RoundTrips()
    {
        const int rows = 256;
        var dictionary = new int[] { 10, 20, 30 };
        var validity = new bool[rows];
        var physical = new List<int>();
        var expected = new int?[rows];
        for (var row = 0; row < rows; row++)
        {
            validity[row] = (row & 7) != 0;
            if (validity[row])
            {
                var index = (row * 5 + 1) % dictionary.Length;
                physical.Add(index);
                expected[row] = dictionary[index];
            }
        }
        var values = new int[rows];
        for (var row = 0; row < rows; row++)
            values[row] = expected[row] ?? 0;
        var bytes = ParquetFixtureBuilder.CreateInt32(new()
        {
            PhysicalValues = values,
            Repetition = ParquetRepetition.Optional,
            Validity = validity,
            DictionaryValues = dictionary,
            DictionaryIndices = physical.ToArray(),
        });
        await ScanOptionalPrimitiveAsync(bytes, expected, null, null, 65_536);
    }

    [Fact]
    public async Task OptionalDictionaryMixedV2RoundTrips()
    {
        const int rows = 256;
        var dictionary = new int[] { 10, 20, 30 };
        var validity = new bool[rows];
        var physical = new List<int>();
        var expected = new int?[rows];
        for (var row = 0; row < rows; row++)
        {
            validity[row] = (row & 3) != 0;
            if (validity[row])
            {
                var index = (row * 5 + 1) % dictionary.Length;
                physical.Add(index);
                expected[row] = dictionary[index];
            }
        }
        var values = new int[rows];
        for (var row = 0; row < rows; row++)
            values[row] = expected[row] ?? 0;
        var bytes = ParquetFixtureBuilder.CreateInt32(new()
        {
            PhysicalValues = values,
            Repetition = ParquetRepetition.Optional,
            Validity = validity,
            DictionaryValues = dictionary,
            DictionaryIndices = physical.ToArray(),
            PageVersion = FixturePageVersion.DataPageV2,
        });
        await ScanOptionalPrimitiveAsync(bytes, expected, null, null, 65_536);
    }

    [Fact]
    public async Task OptionalDictionarySparseAndDenseRoundTrips()
    {
        await ScanDensityAsync(1024, 16, 1);
        await ScanDensityAsync(1024, 16, 15);

        async Task ScanDensityAsync(int rows, int period, int validPerPeriod)
        {
            var dictionary = new int[] { 7, 42 };
            var validity = new bool[rows];
            var physical = new List<int>();
            var expected = new int?[rows];
            for (var row = 0; row < rows; row++)
            {
                validity[row] = (row % period) < validPerPeriod;
                if (validity[row])
                {
                    var index = row & 1;
                    physical.Add(index);
                    expected[row] = dictionary[index];
                }
            }
            var values = new int[rows];
            for (var row = 0; row < rows; row++)
                values[row] = expected[row] ?? 0;
            var bytes = ParquetFixtureBuilder.CreateInt32(new()
            {
                PhysicalValues = values,
                Repetition = ParquetRepetition.Optional,
                Validity = validity,
                DictionaryValues = dictionary,
                DictionaryIndices = physical.ToArray(),
            });
            await ScanOptionalPrimitiveAsync(bytes, expected, null, null, 65_536);
        }
    }

    [Fact]
    public async Task OptionalBooleanDictionaryMixedRoundTrips()
    {
        const int rows = 128;
        var dictionary = new bool[] { false, true };
        var validity = new bool[rows];
        var physical = new List<int>();
        var expected = new bool?[rows];
        for (var row = 0; row < rows; row++)
        {
            validity[row] = (row & 3) != 0;
            if (validity[row])
            {
                var index = (row + 1) & 1;
                physical.Add(index);
                expected[row] = dictionary[index];
            }
        }
        var values = new bool[rows];
        for (var row = 0; row < rows; row++)
            values[row] = expected[row] ?? false;
        var bytes = ParquetFixtureBuilder.CreateInt32(new()
        {
            PhysicalTypeCode = (int)ParquetPhysicalType.Boolean,
            PhysicalValues = values,
            Repetition = ParquetRepetition.Optional,
            Validity = validity,
            DictionaryValues = dictionary,
            DictionaryIndices = physical.ToArray(),
        });
        await ScanOptionalPrimitiveAsync(bytes, expected, null, null, 65_536);
    }

    [Fact]
    public async Task OptionalInt64DictionaryMixedRoundTrips()
    {
        const int rows = 256;
        var dictionary = new long[] { long.MinValue, long.MaxValue };
        var validity = new bool[rows];
        var physical = new List<int>();
        var expected = new long?[rows];
        for (var row = 0; row < rows; row++)
        {
            validity[row] = (row & 7) != 0;
            if (validity[row])
            {
                var index = row & 1;
                physical.Add(index);
                expected[row] = dictionary[index];
            }
        }
        var values = new long[rows];
        for (var row = 0; row < rows; row++)
            values[row] = expected[row] ?? 0L;
        var bytes = ParquetFixtureBuilder.CreateInt32(new()
        {
            PhysicalTypeCode = (int)ParquetPhysicalType.Int64,
            PhysicalValues = values,
            Repetition = ParquetRepetition.Optional,
            Validity = validity,
            DictionaryValues = dictionary,
            DictionaryIndices = physical.ToArray(),
        });
        await ScanOptionalPrimitiveAsync(bytes, expected, null, null, 65_536);
    }

    [Fact]
    public async Task OptionalBinaryDictionaryMixedRoundTrips()
    {
        const int rows = 128;
        var dictionary = new byte[][] { [1], [4, 5], [6, 7, 8] };
        var validity = new bool[rows];
        var physical = new List<int>();
        var expected = new byte[]?[rows];
        for (var row = 0; row < rows; row++)
        {
            validity[row] = (row & 3) != 0;
            if (validity[row])
            {
                var index = (row * 2 + 1) % dictionary.Length;
                physical.Add(index);
                expected[row] = dictionary[index];
            }
        }
        var values = new byte[rows][];
        for (var row = 0; row < rows; row++)
            values[row] = expected[row] ?? [];
        var bytes = ParquetFixtureBuilder.CreateInt32(new()
        {
            PhysicalTypeCode = (int)ParquetPhysicalType.ByteArray,
            PhysicalValues = values,
            Repetition = ParquetRepetition.Optional,
            Validity = validity,
            DictionaryValues = dictionary,
            DictionaryIndices = physical.ToArray(),
        });
        await ScanOptionalBinaryAsync(bytes, expected, 65_536);
    }

    [Fact]
    public async Task OptionalFixedDictionaryMixedV1RoundTrips()
    {
        await ScanFixedMixedAsync(FixturePageVersion.DataPageV1);
    }

    [Fact]
    public async Task OptionalFixedDictionaryMixedV2RoundTrips()
    {
        await ScanFixedMixedAsync(FixturePageVersion.DataPageV2);
    }

    [Fact]
    public async Task OptionalDictionaryInvalidIndexFailsBalanced()
    {
        using var tracker = new PoolTracker();
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
        await using var file = await ParquetFile.OpenAsync(bytes, new ParquetReaderOptions(), CancellationToken.None);
        var exception = await Assert.ThrowsAsync<ParquetFormatException>(async () =>
        {
            await foreach (var batch in file.ScanAsync(new([file.Metadata.Schema.Columns[0]])))
                batch.Dispose();
        });
        Assert.Contains("outside the dictionary", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task AllValidOptionalDictionaryInvalidIndexFails()
    {
        using var tracker = new PoolTracker();
        const int rows = 64;
        var dictionary = new int[] { 10, 20, 30 };
        var indices = new int[rows];
        for (var row = 0; row < rows; row++)
            indices[row] = row % dictionary.Length;
        indices[^1] = 3;
        var values = new int[rows];
        var bytes = ParquetFixtureBuilder.CreateInt32(new()
        {
            PhysicalValues = values,
            Repetition = ParquetRepetition.Optional,
            Validity = Enumerable.Repeat(true, rows).ToArray(),
            DictionaryValues = dictionary,
            DictionaryIndices = indices,
        });
        await using var file = await ParquetFile.OpenAsync(bytes, new ParquetReaderOptions(), CancellationToken.None);
        var exception = await Assert.ThrowsAsync<ParquetFormatException>(async () =>
        {
            await foreach (var batch in file.ScanAsync(new([file.Metadata.Schema.Columns[0]])))
                batch.Dispose();
        });
        Assert.Contains("outside the dictionary", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task OptionalBinaryDictionaryInvalidIndexFailsBalanced()
    {
        using var tracker = new PoolTracker();
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
        await using var file = await ParquetFile.OpenAsync(bytes, new ParquetReaderOptions(), CancellationToken.None);
        var exception = await Assert.ThrowsAsync<ParquetFormatException>(async () =>
        {
            await foreach (var batch in file.ScanAsync(new([file.Metadata.Schema.Columns[0]])))
                batch.Dispose();
        });
        Assert.Contains("outside the dictionary", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task OptionalFixedDictionaryInvalidIndexFailsBalanced()
    {
        using var tracker = new PoolTracker();
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
        await using var file = await ParquetFile.OpenAsync(bytes, new ParquetReaderOptions(), CancellationToken.None);
        var exception = await Assert.ThrowsAsync<ParquetFormatException>(async () =>
        {
            await foreach (var batch in file.ScanAsync(new([file.Metadata.Schema.Columns[0]])))
                batch.Dispose();
        });
        Assert.Contains("outside the dictionary", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task RequiredDictionaryTrailingIndexRunFailsBalanced()
    {
        using var tracker = new PoolTracker();
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
        await using var file = await ParquetFile.OpenAsync(bytes, new ParquetReaderOptions(), CancellationToken.None);
        var exception = await Assert.ThrowsAsync<ParquetFormatException>(async () =>
        {
            await foreach (var batch in file.ScanAsync(new([file.Metadata.Schema.Columns[0]])))
                batch.Dispose();
        });
        Assert.Contains("trailing index bytes", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task BitPackedDictionaryTrailingIndexRunFailsBalanced()
    {
        using var tracker = new PoolTracker();
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
        await using var file = await ParquetFile.OpenAsync(bytes, new ParquetReaderOptions(), CancellationToken.None);
        var exception = await Assert.ThrowsAsync<ParquetFormatException>(async () =>
        {
            await foreach (var batch in file.ScanAsync(new([file.Metadata.Schema.Columns[0]])))
                batch.Dispose();
        });
        Assert.Contains("trailing index bytes", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task OptionalDictionaryPartialBatchesRoundTrip()
    {
        const int rows = 100;
        var dictionary = new int[] { 5, 15, 25 };
        var validity = new bool[rows];
        var physical = new List<int>();
        var expected = new int?[rows];
        for (var row = 0; row < rows; row++)
        {
            validity[row] = (row % 5) != 0;
            if (validity[row])
            {
                var index = (row * 3 + 1) % dictionary.Length;
                physical.Add(index);
                expected[row] = dictionary[index];
            }
        }
        var values = new int[rows];
        for (var row = 0; row < rows; row++)
            values[row] = expected[row] ?? 0;
        var bytes = ParquetFixtureBuilder.CreateInt32(new()
        {
            PhysicalValues = values,
            Repetition = ParquetRepetition.Optional,
            Validity = validity,
            DictionaryValues = dictionary,
            DictionaryIndices = physical.ToArray(),
        });
        await ScanOptionalPrimitiveAsync(bytes, expected, null, null, 7);
    }

    [Fact]
    public async Task OptionalDictionaryRowRangeRoundTrips()
    {
        const int rows = 64;
        var dictionary = new int[] { 5, 15, 25 };
        var validity = new bool[rows];
        var physical = new List<int>();
        var expected = new int?[rows];
        for (var row = 0; row < rows; row++)
        {
            validity[row] = (row % 4) != 0;
            if (validity[row])
            {
                var index = (row * 3 + 1) % dictionary.Length;
                physical.Add(index);
                expected[row] = dictionary[index];
            }
        }
        var values = new int[rows];
        for (var row = 0; row < rows; row++)
            values[row] = expected[row] ?? 0;
        var bytes = ParquetFixtureBuilder.CreateInt32(new()
        {
            PhysicalValues = values,
            Repetition = ParquetRepetition.Optional,
            Validity = validity,
            DictionaryValues = dictionary,
            DictionaryIndices = physical.ToArray(),
        });
        await ScanOptionalPrimitiveAsync(bytes, expected[10..35], null, new ParquetRowRange(10, 25), 65_536);
    }

    [Fact]
    public async Task LargeDictionaryMixedRoundTripsWithPeak()
    {
        const int rows = 8192;
        var dictionary = Enumerable.Range(0, 1024).Select(static value => value * 13 - 4000).ToArray();
        var validity = new bool[rows];
        var physical = new int[rows];
        var expected = new int?[rows];
        var cursor = 0;
        for (var row = 0; row < rows; row++)
        {
            validity[row] = (row & 7) != 0;
            if (validity[row])
            {
                var index = (row * 31 + 7) % dictionary.Length;
                physical[cursor++] = index;
                expected[row] = dictionary[index];
            }
        }
        var values = new int[rows];
        for (var row = 0; row < rows; row++)
            values[row] = expected[row] ?? 0;
        var bytes = ParquetFixtureBuilder.CreateInt32(new()
        {
            PhysicalValues = values,
            Repetition = ParquetRepetition.Optional,
            Validity = validity,
            DictionaryValues = dictionary,
            DictionaryIndices = physical[..cursor],
        });
        var peak = await ScanOptionalPrimitiveAsync(bytes, expected, null, null, 65_536);
        _output.WriteLine($"large optional-dict-int32 peak pooled {peak} bytes.");
        Assert.True(peak <= 85_000L);
    }

    [Fact]
    public async Task OptionalDictionaryMixedPeakStaysBounded()
    {
        const int rows = 65536;
        var dictionary = Enumerable.Range(0, 256).Select(static value => value * 7 + 1).ToArray();
        var validity = new bool[rows];
        var physical = new int[rows];
        var expected = new int?[rows];
        var cursor = 0;
        for (var row = 0; row < rows; row++)
        {
            validity[row] = (row & 7) != 0;
            if (validity[row])
            {
                var index = (row * 13 + 5) % dictionary.Length;
                physical[cursor++] = index;
                expected[row] = dictionary[index];
            }
        }
        var values = new int[rows];
        for (var row = 0; row < rows; row++)
            values[row] = expected[row] ?? 0;
        var bytes = ParquetFixtureBuilder.CreateInt32(new()
        {
            PhysicalValues = values,
            Repetition = ParquetRepetition.Optional,
            Validity = validity,
            DictionaryValues = dictionary,
            DictionaryIndices = physical[..cursor],
        });
        var peak = await ScanOptionalPrimitiveAsync(bytes, expected, null, null, 65_536);
        _output.WriteLine($"optional-dict-int32 peak pooled {peak} bytes.");
        Assert.True(peak <= 600_000L);

        async Task ScanOnceAsync()
        {
            await using var scanFile = await ParquetFile.OpenAsync(bytes, new ParquetReaderOptions(), CancellationToken.None);
            var projection = new ParquetColumn[] { scanFile.Metadata.Schema.Columns[0] };
            await foreach (var batch in scanFile.ScanAsync(new(projection, null, null, 65_536)))
                batch.Dispose();
        }

        for (var warmup = 0; warmup < 2; warmup++)
            await ScanOnceAsync();
        var allocatedBefore = GC.GetTotalAllocatedBytes(true);
        await ScanOnceAsync();
        _output.WriteLine($"optional-dict-int32 allocated {GC.GetTotalAllocatedBytes(true) - allocatedBefore} bytes per full scan.");
    }

    [Fact]
    public async Task OptionalBinaryDictionaryMixedPeakStaysBounded()
    {
        const int rows = 32768;
        var dictionary = Enumerable.Range(0, 64).Select(static value => new byte[] { (byte)value, (byte)(value + 1) }).ToArray();
        var validity = new bool[rows];
        var physical = new int[rows];
        var expected = new byte[]?[rows];
        var cursor = 0;
        for (var row = 0; row < rows; row++)
        {
            validity[row] = (row & 7) != 0;
            if (validity[row])
            {
                var index = (row * 11 + 3) % dictionary.Length;
                physical[cursor++] = index;
                expected[row] = dictionary[index];
            }
        }
        var values = new byte[rows][];
        for (var row = 0; row < rows; row++)
            values[row] = expected[row] ?? [];
        var bytes = ParquetFixtureBuilder.CreateInt32(new()
        {
            PhysicalTypeCode = (int)ParquetPhysicalType.ByteArray,
            PhysicalValues = values,
            Repetition = ParquetRepetition.Optional,
            Validity = validity,
            DictionaryValues = dictionary,
            DictionaryIndices = physical[..cursor],
        });
        var peak = await ScanOptionalBinaryAsync(bytes, expected, 65_536);
        _output.WriteLine($"optional-dict-binary peak pooled {peak} bytes.");
        Assert.True(peak <= 520_000L);
    }

    private static async Task ScanRequiredInt32Async(byte[] fixture, int[] expected)
    {
        using var tracker = new PoolTracker();
        var collected = new List<int>();
        await using var file = await ParquetFile.OpenAsync(fixture, new ParquetReaderOptions(), CancellationToken.None);
        await foreach (var batch in file.ScanAsync(new([file.Metadata.Schema.Columns[0]])))
        {
            var column = Assert.IsType<ParquetPrimitiveColumnBatch<int>>(batch.Columns[0]);
            Assert.True(column.Validity.IsAllValid);
            collected.AddRange(column.Values.ToArray());
            batch.Dispose();
        }

        Assert.Equal(expected, collected.ToArray());
    }

    private async Task<long> ScanOptionalPrimitiveAsync<T>(byte[] fixture, T?[] expected, IReadOnlyList<ParquetRowGroup>? rowGroups, ParquetRowRange? rowRange, int targetBatchRowCount)
        where T : unmanaged
    {
        using var tracker = new PoolTracker();
        var collected = new List<T?>();
        await using var file = await ParquetFile.OpenAsync(fixture, new ParquetReaderOptions(), CancellationToken.None);
        var projection = new ParquetColumn[] { file.Metadata.Schema.Columns[0] };
        await foreach (var batch in file.ScanAsync(new(projection, rowGroups, rowRange, targetBatchRowCount)))
        {
            var column = Assert.IsType<ParquetPrimitiveColumnBatch<T>>(batch.Columns[0]);
            var slots = column.Values.Span;
            if (column.Validity.IsAllValid)
            {
                foreach (var value in slots)
                    collected.Add(value);
            }
            else
            {
                var bits = column.Validity.Bits.Span;
                for (var row = 0; row < slots.Length; row++)
                    collected.Add(((bits[row >> 3] & (1 << (row & 7))) != 0) ? (T?)slots[row] : null);
            }

            batch.Dispose();
        }

        Assert.Equal((System.Collections.Generic.IEnumerable<T?>)expected, (System.Collections.Generic.IEnumerable<T?>)collected.ToArray());
        return tracker.PeakOutstandingBytes;
    }

    private async Task<long> ScanOptionalBinaryAsync(byte[] fixture, byte[]?[] expected, int targetBatchRowCount)
    {
        using var tracker = new PoolTracker();
        var collected = new List<byte[]?>();
        await using var file = await ParquetFile.OpenAsync(fixture, new ParquetReaderOptions(), CancellationToken.None);
        var projection = new ParquetColumn[] { file.Metadata.Schema.Columns[0] };
        await foreach (var batch in file.ScanAsync(new(projection, null, null, targetBatchRowCount)))
        {
            var column = Assert.IsType<ParquetBinaryColumnBatch>(batch.Columns[0]);
            var offsets = column.Offsets.Span;
            var payload = column.Payload.Span;
            for (var row = 0; row < offsets.Length - 1; row++)
                collected.Add(column.Validity.IsValid(row) ? payload[offsets[row]..offsets[row + 1]].ToArray() : null);
            batch.Dispose();
        }

        Assert.Equal(expected.Length, collected.Count);
        for (var row = 0; row < expected.Length; row++)
            Assert.Equal(expected[row], collected[row]);
        return tracker.PeakOutstandingBytes;
    }

    private async Task ScanFixedMixedAsync(FixturePageVersion pageVersion)
    {
        const int rows = 128;
        const int width = 3;
        var dictionary = new byte[][] { [1, 2, 3], [4, 5, 6] };
        var validity = new bool[rows];
        var physical = new List<int>();
        var expected = new byte[]?[rows];
        for (var row = 0; row < rows; row++)
        {
            validity[row] = (row & 3) != 0;
            if (validity[row])
            {
                var index = row & 1;
                physical.Add(index);
                expected[row] = dictionary[index];
            }
        }
        var values = new byte[rows][];
        for (var row = 0; row < rows; row++)
            values[row] = expected[row] ?? new byte[width];
        var bytes = ParquetFixtureBuilder.CreateInt32(new()
        {
            PhysicalTypeCode = (int)ParquetPhysicalType.FixedLengthByteArray,
            TypeLength = width,
            PhysicalValues = values,
            Repetition = ParquetRepetition.Optional,
            Validity = validity,
            DictionaryValues = dictionary,
            DictionaryIndices = physical.ToArray(),
            PageVersion = pageVersion,
        });
        using var tracker = new PoolTracker();
        var collected = new List<byte[]>();
        var observedValidity = new List<bool>();
        await using var file = await ParquetFile.OpenAsync(bytes, new ParquetReaderOptions(), CancellationToken.None);
        var projection = new ParquetColumn[] { file.Metadata.Schema.Columns[0] };
        await foreach (var batch in file.ScanAsync(new(projection, null, null, 65_536)))
        {
            var column = Assert.IsType<ParquetFixedLengthByteArrayColumnBatch>(batch.Columns[0]);
            Assert.Equal(width, column.TypeWidth);
            var payload = column.Payload.Span;
            for (var row = 0; row < payload.Length / width; row++)
            {
                observedValidity.Add(column.Validity.IsValid(row));
                collected.Add(payload.Slice(row * width, width).ToArray());
            }
            batch.Dispose();
        }

        Assert.Equal(expected.Length, collected.Count);
        for (var row = 0; row < expected.Length; row++)
        {
            Assert.Equal(validity[row], observedValidity[row]);
            Assert.Equal(expected[row] ?? new byte[width], collected[row]);
        }
    }
}
