namespace Lokad.Parquet.Tests;

using System.Buffers.Binary;
using System.Reflection;

public sealed class ScanMemoryFeasibilityTests
{
    [Fact]
    public async Task MalformedPageValueCountFailsBeforeDisproportionateRent()
    {
        // One row claims 1,000,000 page values under a 128-byte budget. The page
        // must fail against its enclosing row group before any megabyte-scale rent.
        var outstanding = new PoolOutstandingArrays();
        var largestRent = 0;
        PoolTracker.SetObservers(
            (array, _) =>
            {
                outstanding.NoteRent(array);
                largestRent = Math.Max(largestRent, Buffer.ByteLength(array));
            },
            (array, _) => outstanding.NoteReturn(array));
        try
        {
            byte[] bytes = ParquetFixtureBuilder.CreateInt32(new()
            {
                Values = [1],
                Repetition = ParquetRepetition.Optional,
                PageHeaderOverrides = new() { ValueCount = 1_000_000 },
            });
            await using var file = await ParquetFile.OpenAsync((ReadOnlyMemory<byte>)bytes, new() { MaximumScanPooledBytes = 128 }, CancellationToken.None);
            await using var scan = file.ScanAsync(new(file.Metadata.Schema.Columns)).GetAsyncEnumerator();
            await Assert.ThrowsAsync<ParquetFormatException>(async () => await scan.MoveNextAsync());
            Assert.True(largestRent < 4096, $"A malformed page count rented {largestRent} bytes.");
        }
        finally
        {
            PoolTracker.ClearObservers();
        }

        Assert.True(outstanding.IsEmpty);
    }

    [Fact]
    public async Task OversizedBinaryDictionaryFailsBeforeDisproportionateRent()
    {
        // 100,000 dictionary entries under a 1KB budget, past header scale but far
        // below the 400KB offset rent. That rent must fail its minimum-budget check
        // before touching the array.
        var outstanding = new PoolOutstandingArrays();
        var largestRent = 0;
        PoolTracker.SetObservers(
            (array, _) =>
            {
                outstanding.NoteRent(array);
                largestRent = Math.Max(largestRent, Buffer.ByteLength(array));
            },
            (array, _) => outstanding.NoteReturn(array));
        try
        {
            var entries = new byte[100000][];
            for (var i = 0; i < entries.Length; i++)
                entries[i] = BitConverter.GetBytes(i);
            var physical = new byte[100000][];
            for (var i = 0; i < physical.Length; i++)
                physical[i] = entries[i];
            var indices = new int[100000];
            for (var i = 0; i < indices.Length; i++)
                indices[i] = i;
            byte[] bytes = ParquetFixtureBuilder.CreateInt32(new()
            {
                PhysicalTypeCode = (int)ParquetPhysicalType.ByteArray,
                PhysicalValues = physical,
                DictionaryValues = entries,
                DictionaryIndices = indices,
            });
            await using var file = await ParquetFile.OpenAsync((ReadOnlyMemory<byte>)bytes, new() { MaximumScanPooledBytes = 1024 }, CancellationToken.None);
            await using var scan = file.ScanAsync(new(file.Metadata.Schema.Columns)).GetAsyncEnumerator();
            await Assert.ThrowsAsync<ParquetLimitExceededException>(async () => await scan.MoveNextAsync());
            Assert.True(largestRent < 4096, $"An oversized dictionary rented {largestRent} bytes.");
        }
        finally
        {
            PoolTracker.ClearObservers();
        }

        Assert.True(outstanding.IsEmpty);
    }

    [Fact]
    public async Task LyingFooterContainerCountFailsWithoutCountSizedAllocation()
    {
        // A 16,000-element schema list in a footer of a few dozen bytes must fail on
        // the remaining-input check instead of allocating a 128KB array first.
        static byte[] BuildLyingSchemaFooter(int schemaElementCount)
        {
            var footer = new CompactTestWriter();
            short previous = 0;
            footer.Int32Field(ref previous, 1, 1);
            footer.ListField(ref previous, 2, CompactTestType.Struct, schemaElementCount, () => { });
            footer.Int64Field(ref previous, 3, 0);
            footer.ListField(ref previous, 4, CompactTestType.Struct, 0, () => { });
            footer.Stop();
            var footerBytes = footer.ToArray();
            var file = new MemoryStream();
            file.Write("PAR1"u8);
            file.Write(footerBytes);
            Span<byte> tail = stackalloc byte[8];
            BinaryPrimitives.WriteInt32LittleEndian(tail, footerBytes.Length);
            "PAR1"u8.CopyTo(tail[4..]);
            file.Write(tail);
            return file.ToArray();
        }

        byte[] bytes = BuildLyingSchemaFooter(16000);
        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();
        var before = GC.GetAllocatedBytesForCurrentThread();
        await Assert.ThrowsAsync<ParquetFormatException>(async () =>
        {
            await ParquetFile.OpenAsync(new MemoryStream(bytes, writable: false));
        });
        var allocated = GC.GetAllocatedBytesForCurrentThread() - before;
        Assert.True(allocated < 65536, $"A lying footer container allocated {allocated} bytes.");
    }

    [Fact]
    public void ScanMemoryBudgetAccountsReservationsAndTransientPeaks()
    {
        var assembly = typeof(ParquetFile).Assembly;
        var budgetType = assembly.GetType("Lokad.Parquet.Internal.ParquetScanMemoryBudget") ??
            throw new InvalidOperationException("The scan memory budget was not found.");
        MethodInfo reserve = budgetType.GetMethod("Reserve", BindingFlags.Public | BindingFlags.Instance) ??
            throw new InvalidOperationException("The budget reserve method was not found.");
        MethodInfo release = budgetType.GetMethod("Release", BindingFlags.Public | BindingFlags.Instance) ??
            throw new InvalidOperationException("The budget release method was not found.");
        MethodInfo noteTransient = budgetType.GetMethod("NoteTransientAttempt", BindingFlags.Public | BindingFlags.Instance) ??
            throw new InvalidOperationException("The transient attempt method was not found.");
        MethodInfo precheck = budgetType.GetMethod("ThrowIfMinimumExceedsRemaining", BindingFlags.Public | BindingFlags.Instance) ??
            throw new InvalidOperationException("The pre-rent budget check was not found.");
        object budget = Activator.CreateInstance(budgetType, 512L) ??
            throw new InvalidOperationException("The scan memory budget could not be created.");

        // A minimum that fits the empty budget passes; anything larger fails early.
        precheck.Invoke(budget, [512L]);
        TargetInvocationException precheckFailure = Assert.Throws<TargetInvocationException>(() => precheck.Invoke(budget, [513L]));
        Assert.IsType<ParquetLimitExceededException>(precheckFailure.InnerException);

        noteTransient.Invoke(budget, [100L]);
        Assert.Equal(100L, Assert.IsType<long>(budgetType.GetProperty("PeakTransientBytes")?.GetValue(budget)));

        reserve.Invoke(budget, [512L]);
        Assert.Equal(512L, Assert.IsType<long>(budgetType.GetProperty("PeakRetainedBytes")?.GetValue(budget)));

        // The transient peak reports simultaneous live bytes: retained plus attempt.
        noteTransient.Invoke(budget, [100L]);
        Assert.Equal(612L, Assert.IsType<long>(budgetType.GetProperty("PeakTransientBytes")?.GetValue(budget)));

        // A reservation beyond the remainder fails without changing the balance.
        TargetInvocationException reserveFailure = Assert.Throws<TargetInvocationException>(() => reserve.Invoke(budget, [1L]));
        Assert.IsType<ParquetLimitExceededException>(reserveFailure.InnerException);
        TargetInvocationException minimumFailure = Assert.Throws<TargetInvocationException>(() => precheck.Invoke(budget, [1L]));
        Assert.IsType<ParquetLimitExceededException>(minimumFailure.InnerException);

        release.Invoke(budget, [512L]);
        Assert.Equal(0L, Assert.IsType<long>(budgetType.GetField("_retainedBytes", BindingFlags.NonPublic | BindingFlags.Instance)?.GetValue(budget)));
    }

}
