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

    [Fact]
    public async Task FileKeyValueUnknownNestingRespectsEnclosingDepth()
    {
        // A key/value entry nests five unknown structs plus an int leaf inside a
        // file-level entry (struct depth 2), so the deepest check is 7: limit 6
        // rejects, limit 7 accepts, and limit 8 accepts while the old fixed depth 5
        // skip rejected it.
        static byte[] BytesWithNestedEntry()
        {
            return ParquetFixtureBuilder.CreateInt32(new()
            {
                Values = [1],
                ExtraFileMetadataFields = footer =>
                {
                    short previous = 4;
                    footer.ListField(ref previous, 5, CompactTestType.Struct, 1, () =>
                    {
                        short entry = 0;
                        footer.StringField(ref entry, 1, "k");
                        footer.StringField(ref entry, 2, "v");
                        WriteNestedUnknownStructs(footer, 5);
                        footer.Stop();
                    });
                },
            });
        }

        byte[] bytes = BytesWithNestedEntry();
        await Assert.ThrowsAsync<ParquetLimitExceededException>(async () =>
        {
            await ParquetFile.OpenAsync(bytes, new() { MaximumThriftDepth = 6 }, CancellationToken.None);
        });
        await using (var atExactLimit = await ParquetFile.OpenAsync(bytes, new() { MaximumThriftDepth = 7 }, CancellationToken.None))
        {
            Assert.NotNull(atExactLimit.Metadata);
        }

        await using (var aboveLimit = await ParquetFile.OpenAsync(bytes, new() { MaximumThriftDepth = 8 }, CancellationToken.None))
        {
            Assert.NotNull(aboveLimit.Metadata);
        }
    }

    [Fact]
    public async Task ColumnOrderMemberSkipRespectsEnclosingDepth()
    {
        // A schema-only file carrying one type-defined column order exercises the
        // union-member body skip at the enclosing element depth 2: limit 1 rejects
        // at the schema elements, limit 2 opens, and the default opens.
        byte[] bytes = ParquetFixtureBuilder.CreateInt32(new()
        {
            RowGroupMode = FixtureRowGroupMode.Omitted,
            ColumnOrder = FixtureColumnOrder.TypeDefined,
        });
        await Assert.ThrowsAsync<ParquetLimitExceededException>(async () =>
        {
            await ParquetFile.OpenAsync(bytes, new() { MaximumThriftDepth = 1 }, CancellationToken.None);
        });
        await using (var atExactLimit = await ParquetFile.OpenAsync(bytes, new() { MaximumThriftDepth = 2 }, CancellationToken.None))
        {
            Assert.NotNull(atExactLimit.Metadata);
        }

        await using (var file = await ParquetFile.OpenAsync(new MemoryStream(bytes, writable: false)))
        {
            Assert.NotNull(file.Metadata);
        }
    }

    [Fact]
    public async Task TimestampAnnotationRespectsExactDepthBoundary()
    {
        // A time-millis annotation nests five structs deep, so limit 4 rejects at
        // the time-unit entry while limit 5 opens. This pins the known logical and
        // time-union chain, which shares its numbering with the skip paths.
        byte[] bytes = ParquetFixtureBuilder.CreateInt32(new()
        {
            Values = [1],
            LogicalTypeDiscriminator = (int)ParquetLogicalTypeKind.Time,
            TimeUnitDiscriminator = 1,
            TimeAdjustedToUtc = true,
        });
        await Assert.ThrowsAsync<ParquetLimitExceededException>(async () =>
        {
            await ParquetFile.OpenAsync(bytes, new() { MaximumThriftDepth = 4 }, CancellationToken.None);
        });
        await using (var file = await ParquetFile.OpenAsync(bytes, new() { MaximumThriftDepth = 5 }, CancellationToken.None))
        {
            var column = file.Metadata.Schema.Columns[0];
            Assert.Equal(ParquetLogicalTypeKind.Time, Assert.IsType<ParquetLogicalAnnotation>(column.SchemaElement.LogicalAnnotation).Kind);
        }
    }

    [Fact]
    public async Task SkippedListNestingRespectsExactDepthBoundary()
    {
        // An unknown list/struct chain three levels deep plus an int leaf reaches
        // depth 7 through alternating containers: limit 6 rejects, limit 7 opens.
        static byte[] BytesWithNestedLists()
        {
            return ParquetFixtureBuilder.CreateInt32(new()
            {
                Values = [1],
                ExtraFileMetadataFields = footer =>
                {
                    short previous = 4;
                    footer.ListField(ref previous, 100, CompactTestType.Struct, 1, () => WriteNestedListLevel(footer, 2));
                },
            });
        }

        static void WriteNestedListLevel(CompactTestWriter writer, int remaining)
        {
            // One list element: bare struct fields plus a terminator, with no field
            // header. The element holds either the int leaf or the next list.
            short previous = 0;
            if (remaining == 0)
            {
                writer.Int32Field(ref previous, 1, 42);
            }
            else
            {
                writer.ListField(ref previous, 1, CompactTestType.Struct, 1, () => WriteNestedListLevel(writer, remaining - 1));
            }

            writer.Stop();
        }

        byte[] bytes = BytesWithNestedLists();
        await Assert.ThrowsAsync<ParquetLimitExceededException>(async () =>
        {
            await ParquetFile.OpenAsync(bytes, new() { MaximumThriftDepth = 6 }, CancellationToken.None);
        });
        await using (var file = await ParquetFile.OpenAsync(bytes, new() { MaximumThriftDepth = 7 }, CancellationToken.None))
        {
            Assert.NotNull(file.Metadata);
        }
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
