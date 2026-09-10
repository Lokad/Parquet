using System.Reflection;

namespace Lokad.Parquet.Tests;

// Coverage for the typed page/dictionary storage boundaries: every primitive
// physical type round-trips through both the full-page transfer path (the batch
// target covers the page) and the partial-copy path (a small target), a
// dictionary page exercises the dictionary lease, and the column-cache contract
// preserves its inconsistent-type defense with disposal-safe leases.
public sealed class TypedStorageLeaseTests
{
    [Fact]
    public async Task BooleanPagesTransferAndCopy()
    {
        var values = new bool[64];
        for (var row = 0; row < values.Length; row++)
            values[row] = (row & 3) != 0;
        var bytes = ParquetFixtureBuilder.CreateInt32(new()
        {
            PhysicalTypeCode = (int)ParquetPhysicalType.Boolean,
            PhysicalValues = values,
        });
        await AssertTransferAndCopy(bytes, values);
    }

    [Fact]
    public async Task Int32PagesTransferAndCopy()
    {
        var values = new int[64];
        for (var row = 0; row < values.Length; row++)
            values[row] = row * 3 + 1;
        var bytes = ParquetFixtureBuilder.CreateInt32(new() { Values = values });
        await AssertTransferAndCopy(bytes, values);
    }

    [Fact]
    public async Task Int64PagesTransferAndCopy()
    {
        var values = new long[64];
        for (var row = 0; row < values.Length; row++)
            values[row] = row * 1_000_003L + 7;
        var bytes = ParquetFixtureBuilder.CreateInt32(new()
        {
            PhysicalTypeCode = (int)ParquetPhysicalType.Int64,
            PhysicalValues = values,
        });
        await AssertTransferAndCopy(bytes, values);
    }

    [Fact]
    public async Task FloatPagesTransferAndCopy()
    {
        var values = new float[64];
        for (var row = 0; row < values.Length; row++)
            values[row] = row * 0.5f + 1f;
        var bytes = ParquetFixtureBuilder.CreateInt32(new()
        {
            PhysicalTypeCode = (int)ParquetPhysicalType.Float,
            PhysicalValues = values,
        });
        await AssertTransferAndCopy(bytes, values);
    }

    [Fact]
    public async Task DoublePagesTransferAndCopy()
    {
        var values = new double[64];
        for (var row = 0; row < values.Length; row++)
            values[row] = row * 0.5 + 1.0;
        var bytes = ParquetFixtureBuilder.CreateInt32(new()
        {
            PhysicalTypeCode = (int)ParquetPhysicalType.Double,
            PhysicalValues = values,
        });
        await AssertTransferAndCopy(bytes, values);
    }

    [Fact]
    public async Task DictionaryPagesTransferAndCopy()
    {
        var values = new int[64];
        for (var row = 0; row < values.Length; row++)
            values[row] = (row * 7) % 8;
        var dictionary = new int[8];
        for (var value = 0; value < dictionary.Length; value++)
            dictionary[value] = value;
        var bytes = ParquetFixtureBuilder.CreateInt32(new()
        {
            Values = values,
            DictionaryValues = dictionary,
            DictionaryIndices = values,
        });
        await AssertTransferAndCopy(bytes, values);
    }

    [Fact]
    public async Task DictionaryIndexOutsideDictionaryFails()
    {
        var dictionary = new int[65];
        for (var value = 0; value < dictionary.Length; value++)
            dictionary[value] = value;
        var bytes = ParquetFixtureBuilder.CreateInt32(new()
        {
            Values = [5, 6],
            DictionaryValues = dictionary,
            DictionaryIndices = [0, 99],
        });
        await using var file = await ParquetFile.OpenAsync(bytes, new ParquetReaderOptions(), CancellationToken.None);
        var exception = await Assert.ThrowsAsync<ParquetFormatException>(async () =>
        {
            await foreach (var batch in file.ScanAsync(new([file.Metadata.Schema.Columns[0]])))
            {
                batch.Dispose();
            }
        });
        Assert.Contains("outside the dictionary", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ColumnCacheRejectsMixedPhysicalTypes()
    {
        var bytes = ParquetFixtureBuilder.CreateInt32(new() { Values = [1, 2, 3] });
        await using var file = await ParquetFile.OpenAsync(bytes, new ParquetReaderOptions(), CancellationToken.None);
        var rent = typeof(ParquetFile).GetMethod("RentColumnValues", BindingFlags.NonPublic | BindingFlags.Instance) ??
            throw new InvalidOperationException("The column value cache accessor was not found.");
        var column = file.Metadata.Schema.Columns[0];
        using (rent.MakeGenericMethod(typeof(int)).Invoke(file, [column, 3]) as IDisposable ??
            throw new InvalidOperationException("The typed column rent returned nothing."))
        {
        }

        var thrown = Assert.Throws<TargetInvocationException>(() =>
            rent.MakeGenericMethod(typeof(long)).Invoke(file, [column, 3]));
        var inner = Assert.IsType<InvalidOperationException>(thrown.InnerException);
        Assert.Contains("inconsistent physical type", inner.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void EmptyLeaseExposesNothingAndDisposesSafely()
    {
        var assembly = typeof(ParquetFile).Assembly;
        var leaseType = assembly.GetType("Lokad.Parquet.Internal.PooledValueLease") ??
            throw new InvalidOperationException("The pooled value lease was not found.");
        var lease = Activator.CreateInstance(leaseType) ??
            throw new InvalidOperationException("The pooled value lease could not be created.");
        Assert.False(HasLeaseValues(leaseType, lease));
        var take = leaseType.GetMethod("TryTake", BindingFlags.Public | BindingFlags.Instance)?.MakeGenericMethod(typeof(int)) ??
            throw new InvalidOperationException("The lease transfer was not found.");
        Assert.False(Assert.IsType<bool>(take.Invoke(lease, [null])));
        var get = leaseType.GetMethod("TryGetValues", BindingFlags.Public | BindingFlags.Instance)?.MakeGenericMethod(typeof(int)) ??
            throw new InvalidOperationException("The lease access was not found.");
        Assert.False(Assert.IsType<bool>(get.Invoke(lease, [null])));
        var dispose = leaseType.GetMethod("Dispose", BindingFlags.Public | BindingFlags.Instance) ??
            throw new InvalidOperationException("The lease disposal was not found.");
        dispose.Invoke(lease, []);
        Assert.False(HasLeaseValues(leaseType, lease));
    }

    [Fact]
    public async Task ValueLeaseBindsOwnerUntilTakeOrDispose()
    {
        var bytes = ParquetFixtureBuilder.CreateInt32(new() { Values = [1, 2, 3] });
        await using var file = await ParquetFile.OpenAsync(bytes, new ParquetReaderOptions(), CancellationToken.None);
        var rent = typeof(ParquetFile).GetMethod("RentColumnValues", BindingFlags.NonPublic | BindingFlags.Instance) ??
            throw new InvalidOperationException("The column value cache accessor was not found.");
        var column = file.Metadata.Schema.Columns[0];
        var owner = rent.MakeGenericMethod(typeof(int)).Invoke(file, [column, 3]) ??
            throw new InvalidOperationException("The typed column rent returned nothing.");
        var assembly = typeof(ParquetFile).Assembly;
        var leaseType = assembly.GetType("Lokad.Parquet.Internal.PooledValueLease") ??
            throw new InvalidOperationException("The pooled value lease was not found.");
        var lease = Activator.CreateInstance(leaseType) ??
            throw new InvalidOperationException("The pooled value lease could not be created.");
        Assert.False(HasLeaseValues(leaseType, lease));
        var take = leaseType.GetMethod("TryTake", BindingFlags.Public | BindingFlags.Instance)?.MakeGenericMethod(typeof(int)) ??
            throw new InvalidOperationException("The lease transfer was not found.");
        Assert.False(Assert.IsType<bool>(take.Invoke(lease, [null])));
        var set = leaseType.GetMethod("Set", BindingFlags.Public | BindingFlags.Instance)?.MakeGenericMethod(typeof(int)) ??
            throw new InvalidOperationException("The lease assignment was not found.");
        set.Invoke(lease, [owner]);
        Assert.True(HasLeaseValues(leaseType, lease));
        Assert.Throws<TargetInvocationException>(() => set.Invoke(lease, [owner]));
        var get = leaseType.GetMethod("TryGetValues", BindingFlags.Public | BindingFlags.Instance)?.MakeGenericMethod(typeof(int)) ??
            throw new InvalidOperationException("The lease access was not found.");
        var peeked = new object?[] { null };
        Assert.True(Assert.IsType<bool>(get.Invoke(lease, peeked)));
        Assert.True(Assert.IsType<int[]>(peeked[0]).Length >= 3);
        var taken = new object?[] { null };
        Assert.True(Assert.IsType<bool>(take.Invoke(lease, taken)));
        Assert.False(HasLeaseValues(leaseType, lease));
        set.Invoke(lease, [owner]);
        Assert.True(HasLeaseValues(leaseType, lease));
        var dispose = leaseType.GetMethod("Dispose", BindingFlags.Public | BindingFlags.Instance) ??
            throw new InvalidOperationException("The lease disposal was not found.");
        dispose.Invoke(lease, []);
        Assert.False(HasLeaseValues(leaseType, lease));
        (taken[0] as IDisposable)?.Dispose();
    }

    private static async Task AssertTransferAndCopy<T>(byte[] bytes, T[] expected)
        where T : unmanaged
    {
        await using (var transferred = await ParquetFile.OpenAsync(bytes, new ParquetReaderOptions(), CancellationToken.None))
        {
            var collected = new List<T>();
            await foreach (var batch in transferred.ScanAsync(new([transferred.Metadata.Schema.Columns[0]], null, null, expected.Length)))
            {
                collected.AddRange(Assert.IsType<ParquetPrimitiveColumnBatch<T>>(batch.Columns[0]).Values.ToArray());
                batch.Dispose();
            }

            Assert.Equal(expected, collected);
        }

        await using (var copied = await ParquetFile.OpenAsync(bytes, new ParquetReaderOptions(), CancellationToken.None))
        {
            var collected = new List<T>();
            await foreach (var batch in copied.ScanAsync(new([copied.Metadata.Schema.Columns[0]], null, null, 3)))
            {
                collected.AddRange(Assert.IsType<ParquetPrimitiveColumnBatch<T>>(batch.Columns[0]).Values.ToArray());
                batch.Dispose();
            }

            Assert.Equal(expected, collected);
        }
    }

    private static bool HasLeaseValues(Type leaseType, object lease) =>
        Assert.IsType<bool>(leaseType.GetProperty("HasValues")?.GetValue(lease));
}


