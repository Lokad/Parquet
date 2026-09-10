using Xunit.Abstractions;

namespace Lokad.Parquet.Tests;

// Bitmap-based optional decoding for non-INT32 primitives: every lane covers
// all-valid, all-null and mixed null density with exact values and null
// positions, plus V1/V2 variants and malformed-level wiring. Expected values
// come from locally built oracles, so these facts pin scalar equivalence with
// the retired int-level expansion. Peak facts mirror the census lanes.
[Collection("Scan batch allocation")]
public sealed class OptionalBitmapDecodingTests
{
    private readonly ITestOutputHelper _output;

    public OptionalBitmapDecodingTests(ITestOutputHelper output) => _output = output;

    [Fact]
    public async Task NullableBooleanAllValidRoundTrips()
    {
        var values = new bool[64];
        for (var row = 0; row < values.Length; row++)
            values[row] = (row & 3) != 0;
        var bytes = ParquetFixtureBuilder.CreateInt32(new()
        {
            PhysicalTypeCode = (int)ParquetPhysicalType.Boolean,
            PhysicalValues = values,
            Repetition = ParquetRepetition.Optional,
            Validity = Enumerable.Repeat(true, values.Length).ToArray(),
        });
        await ScanNullableAsync(bytes, values.Select(static value => (bool?)value).ToArray());
    }

    [Fact]
    public async Task NullableBooleanAllNullRoundTrips()
    {
        var values = new bool[64];
        var bytes = ParquetFixtureBuilder.CreateInt32(new()
        {
            PhysicalTypeCode = (int)ParquetPhysicalType.Boolean,
            PhysicalValues = values,
            Repetition = ParquetRepetition.Optional,
            Validity = new bool[values.Length],
        });
        await ScanNullableAsync(bytes, new bool?[values.Length]);
    }

    [Fact]
    public async Task NullableBooleanMixedRoundTrips()
    {
        const int rows = 8192;
        var values = new bool[rows];
        var validity = new bool[rows];
        var expected = new bool?[rows];
        for (var row = 0; row < rows; row++)
        {
            values[row] = (row & 1) == 0;
            validity[row] = (row & 7) != 0;
            expected[row] = validity[row] ? values[row] : null;
        }

        var bytes = ParquetFixtureBuilder.CreateInt32(new()
        {
            PhysicalTypeCode = (int)ParquetPhysicalType.Boolean,
            PhysicalValues = values,
            Repetition = ParquetRepetition.Optional,
            Validity = validity,
        });
        await ScanNullableAsync(bytes, expected);
    }

    [Fact]
    public async Task NullableBooleanMixedV2RoundTrips()
    {
        const int rows = 256;
        var values = new bool[rows];
        var validity = new bool[rows];
        var expected = new bool?[rows];
        for (var row = 0; row < rows; row++)
        {
            values[row] = (row & 3) == 0;
            validity[row] = (row & 3) != 0;
            expected[row] = validity[row] ? values[row] : null;
        }

        var bytes = ParquetFixtureBuilder.CreateInt32(new()
        {
            PhysicalTypeCode = (int)ParquetPhysicalType.Boolean,
            PhysicalValues = values,
            Repetition = ParquetRepetition.Optional,
            Validity = validity,
            PageVersion = FixturePageVersion.DataPageV2,
        });
        await ScanNullableAsync(bytes, expected);
    }

    [Fact]
    public async Task NullableBooleanPeakBelowSixTimesOutput()
    {
        const int rows = 8192;
        var values = new bool[rows];
        var validity = new bool[rows];
        var expected = new bool?[rows];
        for (var row = 0; row < rows; row++)
        {
            values[row] = (row & 1) == 0;
            validity[row] = (row & 7) != 0;
            expected[row] = validity[row] ? values[row] : null;
        }

        var bytes = ParquetFixtureBuilder.CreateInt32(new()
        {
            PhysicalTypeCode = (int)ParquetPhysicalType.Boolean,
            PhysicalValues = values,
            Repetition = ParquetRepetition.Optional,
            Validity = validity,
        });
        var outcome = await ScanNullableAsync(bytes, expected);
        _output.WriteLine($"boolean peak pooled {outcome.PeakOutstandingBytes} bytes.");
        Assert.True(outcome.PeakOutstandingBytes <= 45_000L);
    }

    [Fact]
    public async Task NullableInt64MixedRoundTrips()
    {
        const int rows = 1024;
        var values = new long[rows];
        var validity = new bool[rows];
        var expected = new long?[rows];
        for (var row = 0; row < rows; row++)
        {
            values[row] = row * 1_000_003L + 7;
            validity[row] = (row & 7) != 0;
            expected[row] = validity[row] ? values[row] : null;
        }

        var bytes = ParquetFixtureBuilder.CreateInt32(new()
        {
            PhysicalTypeCode = (int)ParquetPhysicalType.Int64,
            PhysicalValues = values,
            Repetition = ParquetRepetition.Optional,
            Validity = validity,
        });
        await ScanNullableAsync(bytes, expected);
    }

    [Fact]
    public async Task NullableInt64AllValidRoundTrips()
    {
        const int rows = 256;
        var values = new long[rows];
        for (var row = 0; row < rows; row++)
            values[row] = row * 1_000_003L + 7;
        var bytes = ParquetFixtureBuilder.CreateInt32(new()
        {
            PhysicalTypeCode = (int)ParquetPhysicalType.Int64,
            PhysicalValues = values,
            Repetition = ParquetRepetition.Optional,
            Validity = Enumerable.Repeat(true, rows).ToArray(),
        });
        await ScanNullableAsync(bytes, values.Select(static value => (long?)value).ToArray());
    }

    [Fact]
    public async Task NullableInt64MixedV2RoundTrips()
    {
        const int rows = 256;
        var values = new long[rows];
        var validity = new bool[rows];
        var expected = new long?[rows];
        for (var row = 0; row < rows; row++)
        {
            values[row] = row * 1_000_003L + 7;
            validity[row] = (row & 3) != 0;
            expected[row] = validity[row] ? values[row] : null;
        }

        var bytes = ParquetFixtureBuilder.CreateInt32(new()
        {
            PhysicalTypeCode = (int)ParquetPhysicalType.Int64,
            PhysicalValues = values,
            Repetition = ParquetRepetition.Optional,
            Validity = validity,
            PageVersion = FixturePageVersion.DataPageV2,
        });
        await ScanNullableAsync(bytes, expected);
    }

    [Fact]
    public async Task NullableInt64PeakBelowSixTimesOutput()
    {
        const int rows = 8192;
        var values = new long[rows];
        var validity = new bool[rows];
        var expected = new long?[rows];
        for (var row = 0; row < rows; row++)
        {
            values[row] = row * 1_000_003L + 7;
            validity[row] = (row & 7) != 0;
            expected[row] = validity[row] ? values[row] : null;
        }

        var bytes = ParquetFixtureBuilder.CreateInt32(new()
        {
            PhysicalTypeCode = (int)ParquetPhysicalType.Int64,
            PhysicalValues = values,
            Repetition = ParquetRepetition.Optional,
            Validity = validity,
        });
        var outcome = await ScanNullableAsync(bytes, expected);
        _output.WriteLine($"int64 peak pooled {outcome.PeakOutstandingBytes} bytes.");
        Assert.True(outcome.PeakOutstandingBytes <= 180_000L);
    }

    [Fact]
    public async Task NullableFloatMixedRoundTrips()
    {
        const int rows = 256;
        var values = new float[rows];
        var validity = new bool[rows];
        var expected = new float?[rows];
        for (var row = 0; row < rows; row++)
        {
            values[row] = row * 0.5f + 1f;
            validity[row] = (row & 3) != 0;
            expected[row] = validity[row] ? values[row] : null;
        }

        var bytes = ParquetFixtureBuilder.CreateInt32(new()
        {
            PhysicalTypeCode = (int)ParquetPhysicalType.Float,
            PhysicalValues = values,
            Repetition = ParquetRepetition.Optional,
            Validity = validity,
        });
        await ScanNullableAsync(bytes, expected);
    }

    [Fact]
    public async Task NullableFloatAllValidRoundTrips()
    {
        const int rows = 64;
        var values = new float[rows];
        for (var row = 0; row < rows; row++)
            values[row] = row * 0.5f + 1f;
        var bytes = ParquetFixtureBuilder.CreateInt32(new()
        {
            PhysicalTypeCode = (int)ParquetPhysicalType.Float,
            PhysicalValues = values,
            Repetition = ParquetRepetition.Optional,
            Validity = Enumerable.Repeat(true, rows).ToArray(),
        });
        await ScanNullableAsync(bytes, values.Select(static value => (float?)value).ToArray());
    }

    [Fact]
    public async Task NullableDoubleMixedRoundTrips()
    {
        const int rows = 256;
        var values = new double[rows];
        var validity = new bool[rows];
        var expected = new double?[rows];
        for (var row = 0; row < rows; row++)
        {
            values[row] = row * 0.5 + 1.0;
            validity[row] = (row & 3) != 0;
            expected[row] = validity[row] ? values[row] : null;
        }

        var bytes = ParquetFixtureBuilder.CreateInt32(new()
        {
            PhysicalTypeCode = (int)ParquetPhysicalType.Double,
            PhysicalValues = values,
            Repetition = ParquetRepetition.Optional,
            Validity = validity,
        });
        await ScanNullableAsync(bytes, expected);
    }

    [Fact]
    public async Task NullableDoubleAllValidRoundTrips()
    {
        const int rows = 64;
        var values = new double[rows];
        for (var row = 0; row < rows; row++)
            values[row] = row * 0.5 + 1.0;
        var bytes = ParquetFixtureBuilder.CreateInt32(new()
        {
            PhysicalTypeCode = (int)ParquetPhysicalType.Double,
            PhysicalValues = values,
            Repetition = ParquetRepetition.Optional,
            Validity = Enumerable.Repeat(true, rows).ToArray(),
        });
        await ScanNullableAsync(bytes, values.Select(static value => (double?)value).ToArray());
    }

    [Fact]
    public async Task CorruptPagePrefixFailsBalanced()
    {
        var values = new bool[64];
        for (var row = 0; row < values.Length; row++)
            values[row] = (row & 1) == 0;
        var bytes = ParquetFixtureBuilder.CreateInt32(new()
        {
            PhysicalTypeCode = (int)ParquetPhysicalType.Boolean,
            PhysicalValues = values,
            Repetition = ParquetRepetition.Optional,
            Validity = Enumerable.Repeat(true, values.Length).ToArray(),
        });
        using var tracker = new PoolTracker();
        var corrupted = (byte[])bytes.Clone();
        await using (var probe = await ParquetFile.OpenAsync(bytes, new ParquetReaderOptions(), CancellationToken.None))
            corrupted[(int)probe.Metadata.RowGroups[0].Columns[0].DataPageOffset] = 0;
        await using var file = await ParquetFile.OpenAsync(corrupted, new ParquetReaderOptions(), CancellationToken.None);
        await Assert.ThrowsAnyAsync<ParquetException>(async () =>
        {
            await foreach (var batch in file.ScanAsync(new([file.Metadata.Schema.Columns[0]])))
                batch.Dispose();
        });
    }

    private static async Task<NullableScanOutcome<T>> ScanNullableAsync<T>(byte[] fixture, T?[] expected)
        where T : unmanaged
    {
        using var tracker = new PoolTracker();
        var collected = new List<T?>();
        await using var file = await ParquetFile.OpenAsync(fixture, new ParquetReaderOptions(), CancellationToken.None);
        await foreach (var batch in file.ScanAsync(new([file.Metadata.Schema.Columns[0]])))
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

        var outcome = collected.ToArray();
        Assert.Equal((System.Collections.Generic.IEnumerable<T?>)expected, (System.Collections.Generic.IEnumerable<T?>)outcome);
        return new NullableScanOutcome<T>(outcome, tracker.PeakOutstandingBytes);
    }

    private sealed record NullableScanOutcome<T>(T?[] Values, long PeakOutstandingBytes) where T : unmanaged;
}












