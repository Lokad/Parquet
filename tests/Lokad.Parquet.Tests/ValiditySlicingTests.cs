namespace Lokad.Parquet.Tests;

using System.Reflection;

public sealed class ValiditySlicingTests
{
    [Fact]
    public async Task SingleColumnSlicingHandlesUnalignedStartsAndPartialTails()
    {
        using var tracker = new PoolTracker();
        var validity = new[] { true, false, true, true, false, true, false, true, true, false };
        var bytes = ParquetFixtureBuilder.CreateInt32(new()
        {
            Values = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
            Repetition = ParquetRepetition.Optional,
            Validity = validity,
        });
        var observedValidity = new List<bool>();
        var globalIndex = 0;
        await using (var file = await ParquetFile.OpenAsync(new MemoryStream(bytes, writable: false)))
        {
            await foreach (var batch in file.ScanAsync(new([file.Metadata.Schema.Columns[0]], null, null, 4)))
            {
                using (batch)
                {
                    var column = Assert.IsType<ParquetPrimitiveColumnBatch<int>>(batch.Columns[0]);
                    for (var i = 0; i < batch.RowCount; i++)
                    {
                        observedValidity.Add(column.Validity.IsValid(i));
                        if (column.Validity.IsValid(i))
                        {
                            Assert.Equal(globalIndex + i, column.Values.Span[i]);
                        }
                    }

                    globalIndex += batch.RowCount;
                }
            }
        }

        Assert.Equal(validity, observedValidity);
        Assert.Equal(10, globalIndex);
    }

    [Fact]
    public async Task SingleColumnSlicingWithRowRangeOffsetStartsMidByte()
    {
        using var tracker = new PoolTracker();
        var validity = new[] { true, false, true, true, false, true, false, true, true, false };
        var bytes = ParquetFixtureBuilder.CreateInt32(new()
        {
            Values = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
            Repetition = ParquetRepetition.Optional,
            Validity = validity,
        });
        await using (var file = await ParquetFile.OpenAsync(new MemoryStream(bytes, writable: false)))
        {
            var range = new ParquetRowRange(3, 6);
            var observed = new List<bool>();
            await foreach (var batch in file.ScanAsync(new([file.Metadata.Schema.Columns[0]], null, range, 4)))
            {
                using (batch)
                {
                    var column = Assert.IsType<ParquetPrimitiveColumnBatch<int>>(batch.Columns[0]);
                    for (var i = 0; i < batch.RowCount; i++)
                    {
                        observed.Add(column.Validity.IsValid(i));
                    }
                }
            }

            Assert.Equal(validity[3..9], observed);
        }
    }

    [Theory]
    [InlineData(0, 10)]
    [InlineData(3, 6)]
    [InlineData(7, 3)]
    [InlineData(4, 1)]
    public void SharedSlicerHandlesUnalignedStartsAndPartialTails(int sourceOffset, int count)
    {
        var pattern = new[] { true, false, true, true, false, true, false, true, true, false };
        var sourceBits = BuildBits(pattern);
        var owner = InvokeCopySlice(sourceBits, sourceOffset, count, out var bits, out var allValid);
        try
        {
            var expectedAllValid = pattern.Skip(sourceOffset).Take(count).All(static v => v);
            Assert.Equal(expectedAllValid, allValid);
            if (expectedAllValid)
            {
                Assert.Null(owner);
                Assert.True(bits.IsEmpty);
                return;
            }

            Assert.NotNull(owner);
            for (var i = 0; i < count; i++)
            {
                var expected = pattern[sourceOffset + i];
                var actual = (bits.Span[i >> 3] & (1 << (i & 7))) != 0;
                Assert.Equal(expected, actual);
            }

            var trailingBits = count & 7;
            if (trailingBits != 0)
            {
                var last = bits.Span[(count - 1) >> 3] >> trailingBits;
                Assert.Equal(0, last);
            }
        }
        finally
        {
            owner?.Dispose();
        }
    }

    [Fact]
    public void SharedSlicerCollapsesAllValidToNull()
    {
        var sourceBits = BuildBits([true, true, true, true, true, true, true, true, true]);
        var owner = InvokeCopySlice(sourceBits, 1, 8, out var bits, out var allValid);
        Assert.True(allValid);
        Assert.Null(owner);
        Assert.True(bits.IsEmpty);
    }

    private static byte[] BuildBits(bool[] pattern)
    {
        var bytes = new byte[(pattern.Length + 7) / 8];
        for (var i = 0; i < pattern.Length; i++)
        {
            if (pattern[i])
            {
                bytes[i >> 3] |= (byte)(1 << (i & 7));
            }
        }

        return bytes;
    }

    private static IDisposable? InvokeCopySlice(
        byte[] sourceBits,
        int sourceOffset,
        int count,
        out ReadOnlyMemory<byte> bits,
        out bool allValid)
    {
        var assembly = typeof(ParquetFile).Assembly;
        var budgetType = assembly.GetType("Lokad.Parquet.Internal.ParquetScanMemoryBudget") ??
            throw new InvalidOperationException("Budget was not found.");
        var budget = Activator.CreateInstance(budgetType, 1024L * 1024L) ??
            throw new InvalidOperationException("Budget could not be created.");
        var type = assembly.GetType("Lokad.Parquet.Internal.ValidityBitmap") ??
            throw new InvalidOperationException("ValidityBitmap was not found.");
        var method = type.GetMethod("CopySlice", BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Static) ??
            throw new InvalidOperationException("CopySlice was not found.");
        var args = new object?[] { new ReadOnlyMemory<byte>(sourceBits), sourceOffset, count, budget, CancellationToken.None, null, null };
        var owner = (IDisposable?)method.Invoke(null, args);
        if (args[5] is not ReadOnlyMemory<byte> bitsValue)
            throw new InvalidOperationException("Sliced bits were not returned.");
        if (args[6] is not bool allValidValue)
            throw new InvalidOperationException("Sliced validity was not returned.");
        bits = bitsValue;
        allValid = allValidValue;
        return owner;
    }
}
