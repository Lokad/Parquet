namespace Lokad.Parquet.Tests;

public sealed class ScanCancellationTests
{
    [Theory]
    [InlineData((int)ParquetPhysicalType.Int32)]
    [InlineData((int)ParquetPhysicalType.ByteArray)]
    public async Task CancelledScanDoesNotYieldBatchAfterPayloadRead(int physicalTypeCode)
    {
        using var cancellation = new CancellationTokenSource();
        var physicalType = (ParquetPhysicalType)physicalTypeCode;
        byte[] bytes = physicalType == ParquetPhysicalType.ByteArray
            ? ParquetFixtureBuilder.CreateInt32(new() { PhysicalTypeCode = (int)ParquetPhysicalType.ByteArray, PhysicalValues = new byte[][] { [1, 2, 3] } })
            : ParquetFixtureBuilder.CreateInt32(new() { Values = [1, 2, 3] });
        var source = new CancelAfterPayloadSource(bytes, cancellation, 5);
        await using var file = await ParquetFile.OpenAsync(source);
        await using var scan = file.ScanAsync(new([file.Metadata.Schema.Columns[0]]), cancellation.Token).GetAsyncEnumerator();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await scan.MoveNextAsync());
    }

    [Fact]
    public async Task CancelledProjectedScanDoesNotYieldBatch()
    {
        using var cancellation = new CancellationTokenSource();
        var bytes = ParquetFixtureBuilder.CreateRequiredInt32Columns(
        [
            new RequiredInt32FixtureColumn { Name = "a", Pages = [[1, 2, 3]] },
            new RequiredInt32FixtureColumn { Name = "b", Pages = [[4, 5, 6]] },
        ]);
        var source = new CancelAfterPayloadSource(bytes, cancellation, 5);
        await using var file = await ParquetFile.OpenAsync(source);
        var options = new ParquetScanOptions([file.Metadata.Schema.Columns[0], file.Metadata.Schema.Columns[1]]);
        await using var scan = file.ScanAsync(options, cancellation.Token).GetAsyncEnumerator();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await scan.MoveNextAsync());
    }

    [Fact]
    public async Task CancelledFooterOpenThrows()
    {
        var bytes = ParquetFixtureBuilder.CreateInt32(new() { Values = [1] });
        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
        {
            await ParquetFile.OpenAsync(new MemoryStream(bytes, writable: false), ParquetSourceOwnership.Caller, ParquetReaderOptions.Default, cancellation.Token);
        });
    }

    [Fact]
    public void CancelledCrcComputationThrowsBeforeScanningPayload()
    {
        // The CRC loop observes cancellation every 4096 bytes, so a
        // pre-cancelled token must surface immediately even for a megabyte
        // input instead of running unbounded bit-by-bit work after cancel.
        var input = new byte[1024 * 1024];
        new Random(42).NextBytes(input);
        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();
        Assert.ThrowsAny<OperationCanceledException>(() => ComputeCrc32(input, cancellation.Token));
    }

    [Fact]
    public void Crc32MatchesStandardCheckValue()
    {
        // IEEE CRC-32 check value for "123456789".
        var input = System.Text.Encoding.ASCII.GetBytes("123456789");
        Assert.Equal(0xCBF43926u, ComputeCrc32(input, CancellationToken.None));
    }

    private static readonly Func<ReadOnlySpan<byte>, CancellationToken, uint> ComputeCrc32 = GetCrc32Method();

    private static Func<ReadOnlySpan<byte>, CancellationToken, uint> GetCrc32Method()
    {
        var type = typeof(ParquetFile).Assembly.GetType("Lokad.Parquet.Internal.ScanPageReader") ??
            throw new InvalidOperationException("Internal type ScanPageReader was not found.");
        var method = type.GetMethod("ComputeCrc32", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static) ??
            throw new InvalidOperationException("Internal method ScanPageReader.ComputeCrc32 was not found.");
        return (Func<ReadOnlySpan<byte>, CancellationToken, uint>)method.CreateDelegate(typeof(Func<ReadOnlySpan<byte>, CancellationToken, uint>));
    }

    private sealed class CancelAfterPayloadSource : IParquetRandomAccessSource
    {
        private readonly byte[] _bytes;
        private readonly CancellationTokenSource _cancellation;
        private readonly int _cancelAfter;
        private int _reads;
        public CancelAfterPayloadSource(byte[] bytes, CancellationTokenSource cancellation, int cancelAfter)
        {
            _bytes = bytes;
            _cancellation = cancellation;
            _cancelAfter = cancelAfter;
        }

        public long Length => _bytes.Length;
        public ValueTask ReadExactlyAsync(long offset, ArraySegment<byte> destination, CancellationToken cancellationToken)
        {
            _bytes.AsMemory((int)offset, destination.Count).CopyTo(destination.AsMemory());
            if (++_reads == _cancelAfter)
            {
                _cancellation.Cancel();
            }

            return ValueTask.CompletedTask;
        }

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }
}
