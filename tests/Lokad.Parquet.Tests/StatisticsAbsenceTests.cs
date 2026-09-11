namespace Lokad.Parquet.Tests;

// R02: statistics absence stays absent through footer parsing: a missing struct,
// a counts-only struct, explicitly empty extrema, and nonempty legacy/modern
// extrema each round-trip with absence preserved.
public sealed class StatisticsAbsenceTests
{
    [Fact]
    public async Task AbsentStatisticsStayAbsent()
    {
        var bytes = ParquetFixtureBuilder.CreateInt32(new() { Values = [10, -2, 42] });
        Assert.Null(await ReadStatisticsAsync(bytes));
    }

    [Fact]
    public async Task CountsOnlyStatisticsPreserveAbsence()
    {
        var bytes = ParquetFixtureBuilder.CreateInt32(new()
        {
            Values = [10, -2, 42],
            StatisticsNullCount = 0,
            StatisticsDistinctCount = 3,
        });
        var statistics = Assert.IsType<ParquetStatistics>(await ReadStatisticsAsync(bytes));
        Assert.Null(statistics.LegacyMinimum);
        Assert.Null(statistics.LegacyMaximum);
        Assert.Null(statistics.Minimum);
        Assert.Null(statistics.Maximum);
        Assert.Equal(0, statistics.NullCount);
        Assert.Equal(3, statistics.DistinctCount);
    }

    [Fact]
    public async Task EmptyExtremaStayPresent()
    {
        var bytes = ParquetFixtureBuilder.CreateInt32(new()
        {
            Values = [10, -2, 42],
            StatisticsMinimum = [],
            StatisticsMaximum = [],
            StatisticsLegacyMinimum = [],
            StatisticsLegacyMaximum = [],
            StatisticsNullCount = 0,
        });
        var statistics = Assert.IsType<ParquetStatistics>(await ReadStatisticsAsync(bytes));
        Assert.Empty(Assert.IsType<ReadOnlyMemory<byte>>(statistics.Minimum).ToArray());
        Assert.Empty(Assert.IsType<ReadOnlyMemory<byte>>(statistics.Maximum).ToArray());
        Assert.Empty(Assert.IsType<ReadOnlyMemory<byte>>(statistics.LegacyMinimum).ToArray());
        Assert.Empty(Assert.IsType<ReadOnlyMemory<byte>>(statistics.LegacyMaximum).ToArray());
        Assert.Equal(0, statistics.NullCount);
    }

    [Fact]
    public async Task LegacyAndModernExtremaRoundTrip()
    {
        var bytes = ParquetFixtureBuilder.CreateInt32(new()
        {
            Values = [10, -2, 42],
            StatisticsMinimum = [0x02],
            StatisticsMaximum = [0x08],
            StatisticsLegacyMinimum = [0x01],
            StatisticsLegacyMaximum = [0x09],
            StatisticsNullCount = 1,
            StatisticsDistinctCount = 2,
        });
        var statistics = Assert.IsType<ParquetStatistics>(await ReadStatisticsAsync(bytes));
        Assert.Equal([0x02], Assert.IsType<ReadOnlyMemory<byte>>(statistics.Minimum).ToArray());
        Assert.Equal([0x08], Assert.IsType<ReadOnlyMemory<byte>>(statistics.Maximum).ToArray());
        Assert.Equal([0x01], Assert.IsType<ReadOnlyMemory<byte>>(statistics.LegacyMinimum).ToArray());
        Assert.Equal([0x09], Assert.IsType<ReadOnlyMemory<byte>>(statistics.LegacyMaximum).ToArray());
        Assert.Equal(1, statistics.NullCount);
        Assert.Equal(2, statistics.DistinctCount);
        Assert.Null(statistics.IsMinimumExact);
        Assert.Null(statistics.IsMaximumExact);
        Assert.Null(statistics.NanCount);
    }

    private static async Task<ParquetStatistics?> ReadStatisticsAsync(byte[] fixture)
    {
        await using var file = await ParquetFile.OpenAsync(new MemoryStream(fixture, writable: false));
        Assert.Single(file.Metadata.RowGroups);
        Assert.Single(file.Metadata.RowGroups[0].Columns);
        return file.Metadata.RowGroups[0].Columns[0].Statistics;
    }
}
