namespace Lokad.Parquet.Tests;

public sealed class ThriftDepthLimitTests
{
    [Fact]
    public async Task KnownNestedMetadataRespectsDepthLimit()
    {
        var bytes = ParquetFixtureBuilder.CreateInt32(new() { Values = [1] });
        await Assert.ThrowsAsync<ParquetLimitExceededException>(async () =>
        {
            await ParquetFile.OpenAsync(bytes, new() { MaximumThriftDepth = 1 }, CancellationToken.None);
        });
    }

    [Fact]
    public async Task DepthLimitAtSufficientDepthOpens()
    {
        var bytes = ParquetFixtureBuilder.CreateInt32(new() { Values = [1] });
        await using var file = await ParquetFile.OpenAsync(bytes, new() { MaximumThriftDepth = 64 }, CancellationToken.None);
        Assert.NotNull(file.Metadata);
    }

    [Fact]
    public async Task FlatMetadataOpensAtDepthFive()
    {
        // Control: flat known metadata opens at depth 5, so a limit-5 rejection
        // of the nested fixture below must come from the skipped branch.
        var bytes = ParquetFixtureBuilder.CreateInt32(new() { Values = [1] });
        await using var file = await ParquetFile.OpenAsync(bytes, new() { MaximumThriftDepth = 5 }, CancellationToken.None);
        Assert.NotNull(file.Metadata);
    }

    [Fact]
    public async Task SkippedNestingBeyondLimitIsRejected()
    {
        // Five nested unknown structs need skip depth 6 while flat known
        // metadata opens at depth 5, so limit 5 isolates skipped-nesting
        // enforcement from known-structure enforcement.
        var bytes = ParquetFixtureBuilder.CreateInt32(new()
        {
            Values = [1],
            ExtraFileMetadataFields = footer => WriteNestedUnknownStructs(footer, 5),
        });
        await Assert.ThrowsAsync<ParquetLimitExceededException>(async () =>
        {
            await ParquetFile.OpenAsync(bytes, new() { MaximumThriftDepth = 5 }, CancellationToken.None);
        });
    }

    [Fact]
    public async Task SkippedNestingAtExactLimitOpens()
    {
        var bytes = ParquetFixtureBuilder.CreateInt32(new()
        {
            Values = [1],
            ExtraFileMetadataFields = footer => WriteNestedUnknownStructs(footer, 5),
        });
        await using var file = await ParquetFile.OpenAsync(bytes, new() { MaximumThriftDepth = 6 }, CancellationToken.None);
        Assert.NotNull(file.Metadata);
    }

    [Fact]
    public async Task SkippedNestingWithEmptyTerminalStructRespectsLimit()
    {
        // Same five-level nesting but the innermost struct is empty: its
        // immediate STOP still enforces the depth entry check at depth 6.
        var bytes = ParquetFixtureBuilder.CreateInt32(new()
        {
            Values = [1],
            ExtraFileMetadataFields = footer =>
            {
                short previous = 7;
                footer.StructField(ref previous, 100, () =>
                {
                    WriteEmptyTerminalLevel(footer, 5);
                    footer.Stop();
                });
            },
        });
        await Assert.ThrowsAsync<ParquetLimitExceededException>(async () =>
        {
            await ParquetFile.OpenAsync(bytes, new() { MaximumThriftDepth = 5 }, CancellationToken.None);
        });
        await using var opened = await ParquetFile.OpenAsync(bytes, new() { MaximumThriftDepth = 6 }, CancellationToken.None);
        Assert.NotNull(opened.Metadata);
    }

    private static void WriteEmptyTerminalLevel(CompactTestWriter writer, int remaining)
    {
        // An empty terminal struct writes no fields of its own: the trailing
        // Stop below closes the level that opened it, so the innermost level
        // closes empty. Each call opens exactly one struct, so the call count
        // equals the struct count below the unknown-field struct.
        short previous = 0;
        if (remaining != 0)
        {
            writer.StructField(ref previous, 1, () => WriteEmptyTerminalLevel(writer, remaining - 1));
            writer.Stop();
        }
    }

    private static void WriteNestedUnknownStructs(CompactTestWriter writer, int levels)
    {
        short previous = 7;
        writer.StructField(ref previous, 100, () =>
        {
            WriteNestedLevel(writer, levels - 1);
            writer.Stop();
        });
    }

    private static void WriteNestedLevel(CompactTestWriter writer, int remaining)
    {
        short previous = 0;
        if (remaining == 0)
        {
            writer.Int32Field(ref previous, 1, 42);
        }
        else
        {
            writer.StructField(ref previous, 1, () => WriteNestedLevel(writer, remaining - 1));
            writer.Stop();
        }
    }
}
