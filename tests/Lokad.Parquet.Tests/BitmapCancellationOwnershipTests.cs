namespace Lokad.Parquet.Tests;

using System.Reflection;

public sealed class BitmapCancellationOwnershipTests
{
    private static readonly PropertyInfo ScanBudgetProperty =
        typeof(ParquetFile).GetProperty("ScanMemoryBudget", BindingFlags.NonPublic | BindingFlags.Instance) ??
        throw new InvalidOperationException("The scan memory budget was not found.");

    [Fact]
    public async Task OptionalBinaryDecodeCancellationReleasesEveryOwner()
    {
        // Optional BYTE_ARRAY decode builds validity through the shared bitmap helper,
        // which previously leaked its rent when cancellation landed inside the fill loop.
        byte[] bytes = ParquetFixtureBuilder.CreateInt32(new()
        {
            PhysicalTypeCode = (int)ParquetPhysicalType.ByteArray,
            PhysicalValues = new byte[][] { [1, 2], [3, 4], [5, 6] },
            Repetition = ParquetRepetition.Optional,
            Validity = [true, false, true],
        });
        await AssertDecodeCancellationBalancedAsync(bytes, 1, 3);
    }

    [Fact]
    public async Task OptionalDictionaryDecodeCancellationReleasesEveryOwner()
    {
        // Optional BYTE_ARRAY dictionary expansion builds validity through the shared
        // bitmap helper, which previously leaked its rent when cancellation landed inside
        // the fill loop.
        byte[] bytes = ParquetFixtureBuilder.CreateInt32(new()
        {
            PhysicalTypeCode = (int)ParquetPhysicalType.ByteArray,
            PhysicalValues = new byte[][] { [4, 5], [], [1] },
            Repetition = ParquetRepetition.Optional,
            Validity = [true, false, true],
            DictionaryValues = new byte[][] { [1], [4, 5] },
            DictionaryIndices = [1, 0],
        });
        await AssertDecodeCancellationBalancedAsync(bytes, 1, 3);
    }

    [Fact]
    public async Task SlicedSingleColumnCancellationReleasesEveryOwner()
    {
        // A one row target forces the single column emitter to slice page validity through
        // the shared slicer, which previously leaked its rent when cancellation landed inside
        // the copy loop.
        byte[] bytes = ParquetFixtureBuilder.CreateInt32(new()
        {
            Values = [1, 2, 3],
            Repetition = ParquetRepetition.Optional,
            Validity = [false, true, true],
        });
        await AssertDecodeCancellationBalancedAsync(bytes, 2, 1);
    }

    [Fact]
    public void CopySlicePreCancelledTokenThrowsBeforeRent()
    {
        // A pre-cancelled token must surface before any rent, so the pool stays balanced.
        object budget = CreateBudget(1024L * 1024L);
        var outstanding = new PoolOutstandingArrays();
        int rents = 0;
        PoolTracker.SetObservers(
            (array, _) =>
            {
                rents++;
                outstanding.NoteRent(array);
            },
            (array, _) =>
            {
                outstanding.NoteReturn(array);
            });
        try
        {
            using CancellationTokenSource cancellation = new();
            cancellation.Cancel();
            TargetInvocationException exception = Assert.Throws<TargetInvocationException>(() =>
            {
                InvokeCopySlice(new byte[] { 255, 0 }, 0, 9, budget, cancellation.Token);
            });
            Assert.IsType<OperationCanceledException>(exception.InnerException);
            Assert.Equal(0, rents);
            Assert.True(outstanding.IsEmpty);
            AssertBudgetBalanced(budget);
        }
        finally
        {
            PoolTracker.ClearObservers();
        }
    }

    [Fact]
    public void CopySliceCancellationAfterRentReleasesOwner()
    {
        // Cancelling from the rent observer lands between the rent and the first periodic check.
        object budget = CreateBudget(1024L * 1024L);
        var outstanding = new PoolOutstandingArrays();
        using CancellationTokenSource cancellation = new();
        bool triggered = false;
        PoolTracker.SetObservers(
            (array, requested) =>
            {
                outstanding.NoteRent(array);

                if (array is byte[] && requested == 375)
                {
                    triggered = true;
                    cancellation.Cancel();
                }
            },
            (array, _) =>
            {
                outstanding.NoteReturn(array);
            });
        try
        {
            TargetInvocationException exception = Assert.Throws<TargetInvocationException>(() =>
            {
                InvokeCopySlice(new byte[375], 0, 3000, budget, cancellation.Token);
            });
            Assert.IsType<OperationCanceledException>(exception.InnerException);
            Assert.True(triggered);
            Assert.True(outstanding.IsEmpty);
            AssertBudgetBalanced(budget);
        }
        finally
        {
            PoolTracker.ClearObservers();
        }
    }

    [Fact]
    public void CopySliceSourceBoundaryFailureReleasesOwner()
    {
        // An over range slice must still release the rented destination.
        object budget = CreateBudget(1024L * 1024L);
        var outstanding = new PoolOutstandingArrays();
        PoolTracker.SetObservers(
            (array, _) =>
            {
                outstanding.NoteRent(array);
            },
            (array, _) =>
            {
                outstanding.NoteReturn(array);
            });
        try
        {
            Assert.Throws<TargetInvocationException>(() =>
            {
                InvokeCopySlice(new byte[1], 0, 16, budget, CancellationToken.None);
            });
            Assert.True(outstanding.IsEmpty);
            AssertBudgetBalanced(budget);
        }
        finally
        {
            PoolTracker.ClearObservers();
        }
    }

    private static async Task AssertDecodeCancellationBalancedAsync(byte[] bytes, int cancelAtSmallRent, int targetBatchRowCount)
    {
        var outstanding = new PoolOutstandingArrays();
        using CancellationTokenSource cancellation = new();
        int smallRents = 0;
        PoolTracker.SetObservers(
            (array, requested) =>
            {
                outstanding.NoteRent(array);

                if (array is byte[] && requested == 1)
                {
                    smallRents++;
                    if (smallRents == cancelAtSmallRent)
                    {
                        cancellation.Cancel();
                    }
                }
            },
            (array, _) =>
            {
                outstanding.NoteReturn(array);
            });
        object? budget = null;
        try
        {
            await using var file = await ParquetFile.OpenAsync(new MemoryStream(bytes, writable: false));
            budget = ScanBudgetProperty.GetValue(file);
            await using var scan = file.ScanAsync(new([file.Metadata.Schema.Columns[0]], null, null, targetBatchRowCount), cancellation.Token).GetAsyncEnumerator();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await scan.MoveNextAsync());
            Assert.True(cancellation.IsCancellationRequested);
        }
        finally
        {
            PoolTracker.ClearObservers();
        }

        Assert.Equal(cancelAtSmallRent, smallRents);
        Assert.True(outstanding.IsEmpty);
        AssertBudgetBalanced(budget ?? throw new InvalidOperationException("The scan budget was not captured."));
    }

    private static void InvokeCopySlice(byte[] sourceBits, int sourceOffset, int count, object budget, CancellationToken cancellationToken)
    {
        Type? slicerType = typeof(ParquetFile).Assembly.GetType("Lokad.Parquet.Internal.ValidityBitmap");
        if (slicerType is null)
        {
            throw new InvalidOperationException("The shared validity slicer was not found.");
        }

        MethodInfo? method = slicerType.GetMethod("CopySlice", BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Static);
        if (method is null)
        {
            throw new InvalidOperationException("The shared validity slice method was not found.");
        }

        object?[] args = new object?[] { new ReadOnlyMemory<byte>(sourceBits), sourceOffset, count, budget, cancellationToken, null, null };
        method.Invoke(null, args);
    }

    private static object CreateBudget(long maximumBytes)
    {
        Type? budgetType = typeof(ParquetFile).Assembly.GetType("Lokad.Parquet.Internal.ParquetScanMemoryBudget");
        if (budgetType is null)
        {
            throw new InvalidOperationException("The scan memory budget was not found.");
        }

        object? budget = Activator.CreateInstance(budgetType, maximumBytes);
        if (budget is null)
        {
            throw new InvalidOperationException("The scan memory budget could not be created.");
        }

        return budget;
    }

    private static void AssertBudgetBalanced(object budget)
    {
        FieldInfo? retainedField = budget.GetType().GetField("_retainedBytes", BindingFlags.NonPublic | BindingFlags.Instance);
        if (retainedField is null)
        {
            throw new InvalidOperationException("The scan budget retention counter was not found.");
        }

        Assert.Equal(0L, Assert.IsType<long>(retainedField.GetValue(budget)));
    }

}
