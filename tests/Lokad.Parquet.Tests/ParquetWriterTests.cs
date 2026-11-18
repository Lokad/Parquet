using System.Reflection;
using System.Text;

namespace Lokad.Parquet.Tests;

public sealed class ParquetWriterTests
{
    private static readonly ParquetWriteColumn[] MixedSchema =
    [
        new("flag", ParquetPhysicalType.Boolean),
        new("Flag", ParquetPhysicalType.Boolean),
        new("ratio", ParquetPhysicalType.Float),
        new("quantity", ParquetPhysicalType.Int64),
        new("label", ParquetPhysicalType.ByteArray),
        new("day", ParquetPhysicalType.Int32),
    ];

    [Fact]
    public async Task ProductionFilesRoundTripAcrossSlicesPagesAndRowGroups()
    {
        using var pool = new PoolTracker();
        using var output = new SequentialDestination();
        using var writer = await ParquetWriter.CreateAsync(output, MixedSchema,
            new() { TargetPageBytes = 24, TargetPageValues = 9 });

        const int totalRows = 23;
        var rows = Enumerable.Range(0, totalRows).ToArray();
        var flags = rows.Select(i => i % 3 == 0).ToArray();
        var otherFlags = rows.Select(i => i % 2 == 0).ToArray();
        var floatBits = rows.Select(i => (i % 4) switch
        {
            0 => 0,
            1 => int.MinValue,
            2 => 0x7fc01234 + i,
            _ => BitConverter.SingleToInt32Bits(i + 0.25f),
        }).ToArray();
        var ratios = floatBits.Select(BitConverter.Int32BitsToSingle).ToArray();
        var quantities = rows.Select(i => (i % 3) switch
        {
            0 => long.MinValue,
            1 => long.MaxValue,
            _ => i * -17L,
        }).ToArray();
        string[] labels = ["", "café", "مرحبا世界", "über😀", "a value larger than the page target"];
        var days = rows.Select(i => i % 7 - 3).ToArray();
        var offset = 0;
        foreach (var rowCount in new[] { 11, 12 })
        {
            writer.BeginRowGroup(rowCount);
            var callerSlice = flags.AsSpan(offset, 3).ToArray();
            await writer.WriteBooleansAsync(callerSlice);
            Array.Fill(callerSlice, false);
            await writer.WriteBooleansAsync(flags.AsMemory(offset + 3, rowCount - 3));
            await writer.CompleteColumnAsync();
            await writer.WriteBooleansAsync(otherFlags.AsMemory(offset, rowCount));
            await writer.CompleteColumnAsync();
            await writer.WriteFloatsAsync(ratios.AsMemory(offset, 2));
            await writer.WriteFloatsAsync(ratios.AsMemory(offset + 2, rowCount - 2));
            await writer.CompleteColumnAsync();
            await writer.WriteInt64Async(quantities.AsMemory(offset, rowCount));
            await writer.CompleteColumnAsync();

            var (payload, offsets) = EncodeUtf8(rows.AsSpan(offset, rowCount).ToArray().Select(i => labels[i % labels.Length]));
            await writer.WriteUtf8Async(payload, offsets.AsMemory(0, 3));
            await writer.WriteUtf8Async(payload, offsets.AsMemory(2));
            Array.Clear(payload);
            Array.Clear(offsets);
            await writer.CompleteColumnAsync();
            await writer.WriteDatesAsync(days.AsMemory(offset, rowCount));
            await writer.CompleteColumnAsync();
            offset += rowCount;
        }
        await writer.CompleteAsync();
        Assert.Equal(0, pool.OutstandingBytes);
        writer.Dispose();
        Assert.True(output.CanWrite);

        var bytes = output.ToArray();
        await using var file = await ParquetFile.OpenAsync((ReadOnlyMemory<byte>)bytes);
        AssertSchema(file);
        Assert.Equal(totalRows, file.Metadata.RowCount);
        Assert.Equal(new long[] { 11, 12 }, file.Metadata.RowGroups.Select(group => group.RowCount));
        var nextChunkOffset = 4L;
        foreach (var group in file.Metadata.RowGroups)
        {
            foreach (var column in group.Columns)
            {
                Assert.Equal(ParquetCompressionCodec.Snappy, column.CompressionCodec);
                Assert.Equal(nextChunkOffset, column.DataPageOffset);
                Assert.Equal(group.RowCount, column.ValueCount);
                nextChunkOffset += column.TotalCompressedSize;
            }
        }

        offset = 0;
        await foreach (var batch in file.ScanAsync(new(file.Metadata.Schema.Columns)))
        {
            using (batch)
            {
                Assert.Equal(offset, batch.RowOffset);
                Assert.All(batch.Columns, column => Assert.True(column.Validity.IsAllValid));
                var binary = Assert.IsType<ParquetBinaryColumnBatch>(batch.Columns[4]);
                for (var i = 0; i < batch.RowCount; i++, offset++)
                {
                    Assert.Equal(flags[offset], Value<bool>(batch, 0, i));
                    Assert.Equal(otherFlags[offset], Value<bool>(batch, 1, i));
                    Assert.Equal(floatBits[offset], BitConverter.SingleToInt32Bits(Value<float>(batch, 2, i)));
                    Assert.Equal(quantities[offset], Value<long>(batch, 3, i));
                    Assert.Equal(days[offset], Value<int>(batch, 5, i));
                    var valueStart = binary.Offsets.Span[i];
                    var valueEnd = binary.Offsets.Span[i + 1];
                    Assert.Equal(labels[offset % labels.Length], Encoding.UTF8.GetString(binary.Payload.Span[valueStart..valueEnd]));
                }
            }
        }
        Assert.Equal(totalRows, offset);
        await ExportForPyArrowAsync("mixed.parquet", bytes);

        static T Value<T>(ParquetBatch batch, int ordinal, int index) where T : unmanaged =>
            Assert.IsType<ParquetPrimitiveColumnBatch<T>>(batch.Columns[ordinal]).Values.Span[index];

        static (byte[] Payload, int[] Offsets) EncodeUtf8(IEnumerable<string> values)
        {
            using var payload = new MemoryStream();
            var offsets = new List<int> { 0 };
            foreach (var value in values)
            {
                payload.Write(Encoding.UTF8.GetBytes(value));
                offsets.Add((int)payload.Length);
            }
            return (payload.ToArray(), offsets.ToArray());
        }
    }

    [Fact]
    public async Task EmptyFilePreservesTheEntireSchema()
    {
        using var output = new MemoryStream();
        using var writer = await ParquetWriter.CreateAsync(output, MixedSchema);
        await writer.CompleteAsync();

        var bytes = output.ToArray();
        await using var file = await ParquetFile.OpenAsync((ReadOnlyMemory<byte>)bytes);
        AssertSchema(file);
        Assert.Empty(file.Metadata.RowGroups);
        Assert.Equal(0, file.Metadata.RowCount);
        await ExportForPyArrowAsync("empty.parquet", bytes);
    }

    [Fact]
    public async Task ValidatesAllSchemaAndOptionInputsBeforeWriting()
    {
        using var output = new MemoryStream();
        await Assert.ThrowsAsync<ArgumentException>(async () => await ParquetWriter.CreateAsync(output, []));
        await Assert.ThrowsAsync<ArgumentException>(async () => await ParquetWriter.CreateAsync(output,
            [new("", ParquetPhysicalType.Boolean)]));
        await Assert.ThrowsAsync<ArgumentException>(async () => await ParquetWriter.CreateAsync(output,
            [new("a", ParquetPhysicalType.Boolean), new("a", ParquetPhysicalType.Float)]));
        await Assert.ThrowsAsync<ArgumentException>(async () => await ParquetWriter.CreateAsync(output,
            [new("a", ParquetPhysicalType.Int96)]));
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(async () => await ParquetWriter.CreateAsync(
            output, MixedSchema, new() { TargetPageBytes = 7 }));
        Assert.Equal(0, output.Length);

        output.WriteByte(1);
        output.Position = 0;
        await Assert.ThrowsAsync<ArgumentException>(async () => await ParquetWriter.CreateAsync(output, MixedSchema));
        Assert.Equal(new byte[] { 1 }, output.ToArray());
    }

    [Fact]
    public async Task InvalidSlicesAndIncompleteColumnsRemainCorrectable()
    {
        using var output = new SequentialDestination();
        using var writer = await ParquetWriter.CreateAsync(output, [new("label", ParquetPhysicalType.ByteArray)]);
        Assert.Throws<ArgumentOutOfRangeException>(() => writer.BeginRowGroup(0));
        Assert.Throws<ArgumentOutOfRangeException>(() => writer.BeginRowGroup(1_048_577));
        writer.BeginRowGroup(2);

        Assert.Contains("Column 'label' in row group 0", (await Assert.ThrowsAsync<ArgumentException>(
            async () => await writer.WriteUtf8Async(new byte[] { 65, 0xc3 }, new int[] { 0, 1, 2 }))).Message);
        await Assert.ThrowsAsync<ArgumentException>(async () => await writer.WriteUtf8Async("abc"u8.ToArray(), new int[] { 1, 0 }));
        await Assert.ThrowsAsync<ArgumentException>(async () => await writer.WriteUtf8Async("abc"u8.ToArray(), new int[] { 0, 4 }));
        await Assert.ThrowsAsync<ArgumentException>(async () => await writer.WriteUtf8Async(default, default));
        await Assert.ThrowsAsync<ArgumentException>(async () => await writer.WriteUtf8Async(default, new int[] { 0, 0, 0, 0 }));
        await Assert.ThrowsAsync<ArgumentException>(async () => await writer.WriteDatesAsync(new int[] { 0 }));
        await writer.WriteUtf8Async("a"u8.ToArray(), new int[] { 0, 1 });
        Assert.Contains("Column 'label' in row group 0", (await Assert.ThrowsAsync<InvalidOperationException>(
            async () => await writer.CompleteColumnAsync())).Message);
        await Assert.ThrowsAsync<InvalidOperationException>(async () => await writer.CompleteAsync());
        Assert.Throws<InvalidOperationException>(() => writer.BeginRowGroup(1));
        Assert.Equal(4, output.ToArray().Length);

        await writer.WriteUtf8Async(default, new int[] { 0, 0 });
        await writer.CompleteColumnAsync();
        await writer.CompleteAsync();
        await Assert.ThrowsAsync<InvalidOperationException>(async () => await writer.CompleteAsync());
    }

    [Fact]
    public async Task AValueCanFillTheHardPageLimit()
    {
        const int limit = 16 * 1024 * 1024 - sizeof(int);
        var payload = new byte[limit + 1];
        Array.Fill(payload, (byte)'x');
        using var output = new MemoryStream();
        using var writer = await ParquetWriter.CreateAsync(output, [new("label", ParquetPhysicalType.ByteArray)]);
        writer.BeginRowGroup(1);
        await Assert.ThrowsAsync<ArgumentException>(async () => await writer.WriteUtf8Async(payload, new int[] { 0, limit + 1 }));
        await writer.WriteUtf8Async(payload, new int[] { 1, limit + 1 });
        await writer.CompleteColumnAsync();
        await writer.CompleteAsync();

        await using var file = await ParquetFile.OpenAsync((ReadOnlyMemory<byte>)output.ToArray());
        await foreach (var batch in file.ScanAsync(new(file.Metadata.Schema.Columns)))
        {
            using (batch)
            {
                var column = Assert.IsType<ParquetBinaryColumnBatch>(Assert.Single(batch.Columns));
                Assert.Equal(new[] { 0, limit }, column.Offsets.ToArray());
                Assert.True(column.Payload.Span.SequenceEqual(payload.AsSpan(1)));
            }
        }
    }

    [Theory]
    [InlineData(3)] // Snappy page payload, after its header
    [InlineData(4)] // footer and trailer
    public async Task OutputFailurePreservesErrorFaultsWriterAndReturnsBuffers(int failOnWrite)
    {
        using var pool = new PoolTracker();
        using var output = new SequentialDestination { FailOnWrite = failOnWrite };
        using var writer = await ParquetWriter.CreateAsync(output, [new("value", ParquetPhysicalType.Int64)]);
        writer.BeginRowGroup(2);
        await writer.WriteInt64Async(new long[] { 1, 2 });
        var failure = await Assert.ThrowsAsync<IOException>(async () =>
        {
            await writer.CompleteColumnAsync();
            await writer.CompleteAsync();
        });
        Assert.Same(output.Failure, failure);
        Assert.True(output.CanWrite);
        Assert.True(pool.RentCount > 0);
        Assert.Equal(0, pool.OutstandingBytes);
        await Assert.ThrowsAsync<InvalidOperationException>(async () => await writer.CompleteAsync());
        await Assert.ThrowsAsync<InvalidOperationException>(async () => await writer.WriteInt64Async(default));
    }

    [Fact]
    public async Task OutputCancellationRejectsConcurrentOperationsAndReturnsBuffers()
    {
        using var pool = new PoolTracker();
        using var cancellation = new CancellationTokenSource();
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var output = new SequentialDestination
        {
            BeforeWrite = async write =>
            {
                if (write != 3)
                    return;
                started.SetResult();
                await release.Task;
                cancellation.Cancel(); // This destination ignores the token.
            },
        };
        using var writer = await ParquetWriter.CreateAsync(output, [new("v", ParquetPhysicalType.Boolean)]);
        writer.BeginRowGroup(1);
        await writer.WriteBooleansAsync(new bool[] { true });
        var pending = writer.CompleteColumnAsync(cancellation.Token).AsTask();
        try
        {
            await started.Task.WaitAsync(TimeSpan.FromSeconds(10));
            Assert.Throws<InvalidOperationException>(() => writer.BeginRowGroup(1));
            Assert.Throws<InvalidOperationException>(() => writer.Dispose());
            await Assert.ThrowsAsync<InvalidOperationException>(async () => await writer.CompleteAsync());
        }
        finally
        {
            release.SetResult();
        }
        var failure = await Assert.ThrowsAnyAsync<OperationCanceledException>(() => pending);
        Assert.Equal(cancellation.Token, failure.CancellationToken);
        Assert.Equal(0, pool.OutstandingBytes);
        await Assert.ThrowsAsync<InvalidOperationException>(async () => await writer.CompleteAsync());
    }

    [Fact]
    public async Task DisposingAnIncompleteWriterReleasesBuffersWithoutWritingAFooter()
    {
        using var pool = new PoolTracker();
        using var output = new SequentialDestination();
        using var writer = await ParquetWriter.CreateAsync(output, [new("v", ParquetPhysicalType.Int64)]);
        writer.BeginRowGroup(2);
        await writer.WriteInt64Async(new long[] { 1 });
        Assert.True(pool.OutstandingBytes > 0);
        writer.Dispose();
        Assert.Equal(0, pool.OutstandingBytes);
        Assert.True(output.CanWrite);
        Assert.Equal("PAR1"u8.ToArray(), output.ToArray());
        await Assert.ThrowsAsync<ObjectDisposedException>(async () => await writer.CompleteAsync());
    }

    private delegate int Compress(ReadOnlySpan<byte> input, Span<byte> output, Span<int> positions, CancellationToken cancellationToken);
    private delegate void Decompress(ReadOnlySpan<byte> input, Span<byte> output, CancellationToken cancellationToken);

    [Fact]
    public void SnappyBlocksFitTheBoundAndDecodeLiteralAndCopyTags()
    {
        var assembly = typeof(ParquetWriter).Assembly;
        var encoder = assembly.GetType("Lokad.Parquet.Internal.SnappyBlockEncoder") ??
            throw new InvalidOperationException("Snappy encoder type is missing.");
        var decoder = assembly.GetType("Lokad.Parquet.Internal.SnappyBlockDecoder") ??
            throw new InvalidOperationException("Snappy decoder type is missing.");
        var compress = RequireMethod(encoder, "Compress").CreateDelegate<Compress>();
        var maximumLength = RequireMethod(encoder, "MaximumCompressedLength").CreateDelegate<Func<int, int>>();
        var decompress = RequireMethod(decoder, "Decompress").CreateDelegate<Decompress>();
        var positions = new int[Assert.IsType<int>(encoder.GetField("HashEntries", BindingFlags.Public | BindingFlags.Static)?.GetRawConstantValue())];
        var random = new Random(11);
        foreach (var size in new[] { 0, 1, 3, 4, 5, 32, 33, 59, 60, 61, 255, 256, 257, 65_535, 65_536, 262_144 })
        {
            var input = new byte[size];
            random.NextBytes(input);
            var length = RoundTrip(input);
            if (size > 0)
                Assert.True(length > size);

            Array.Fill(input, (byte)0xAB);
            length = RoundTrip(input);
            if (size >= 59)
                Assert.True(length < size);
        }

        // After the first copy of "abcd", the next four-byte match starts
        // at the final "d" of that copy, so that window must be indexed.
        var adjacentMatches = "abcd!wabcdxyz?dxyz#"u8;
        Assert.InRange(RoundTrip(adjacentMatches), 1, adjacentMatches.Length);

        int RoundTrip(ReadOnlySpan<byte> input)
        {
            var compressed = new byte[maximumLength(input.Length)];
            var actual = new byte[input.Length];
            var length = compress(input, compressed, positions, default);
            decompress(compressed.AsSpan(0, length), actual, default);
            Assert.True(input.SequenceEqual(actual));
            return length;
        }

        static MethodInfo RequireMethod(Type type, string name) =>
            type.GetMethod(name) ?? throw new InvalidOperationException($"Snappy method '{name}' is missing.");
    }

    private static void AssertSchema(ParquetFile file)
    {
        var columns = file.Metadata.Schema.Columns;
        Assert.Equal(MixedSchema.Select(column => column.Name), columns.Select(column => column.Name));
        Assert.Equal(MixedSchema.Select(column => (ParquetPhysicalType?)column.PhysicalType),
            columns.Select(column => column.SchemaElement.PhysicalType));
        Assert.All(columns, column => Assert.Equal(ParquetRepetition.Required, column.SchemaElement.Repetition));
        Assert.Equal(ParquetConvertedType.Utf8, columns[4].SchemaElement.ConvertedType);
        Assert.Equal(ParquetSemanticTypeKind.String, columns[4].SchemaElement.SemanticAnnotation?.Kind);
        Assert.Equal(ParquetConvertedType.Date, columns[5].SchemaElement.ConvertedType);
        Assert.Equal(ParquetSemanticTypeKind.Date, columns[5].SchemaElement.SemanticAnnotation?.Kind);
    }

    private static async Task ExportForPyArrowAsync(string name, byte[] bytes)
    {
        if (Environment.GetEnvironmentVariable("LOKAD_PARQUET_WRITER_ARROW_OUTPUT") is { Length: > 0 } directory)
        {
            Directory.CreateDirectory(directory);
            await File.WriteAllBytesAsync(Path.Combine(directory, name), bytes);
        }
    }

    private sealed class SequentialDestination : Stream
    {
        private readonly MemoryStream _bytes = new();
        private int _writeCount;

        public IOException Failure { get; } = new("Injected partial write.");
        public int FailOnWrite { get; init; }
        public Func<int, ValueTask>? BeforeWrite { get; init; }
        public byte[] ToArray() => _bytes.ToArray();
        public override bool CanRead => false;
        public override bool CanSeek => false;
        public override bool CanWrite => _bytes.CanWrite;
        public override long Length => throw new NotSupportedException();
        public override long Position { get => throw new NotSupportedException(); set => throw new NotSupportedException(); }
        public override void Flush() => throw new NotSupportedException();
        public override int Read(byte[] buffer, int offset, int count) => throw new NotSupportedException();
        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => throw new NotSupportedException();
        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();

        public override async ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken)
        {
            _writeCount++;
            if (BeforeWrite is { } beforeWrite)
                await beforeWrite(_writeCount);
            if (_writeCount == FailOnWrite)
            {
                _bytes.Write(buffer.Span[..(buffer.Length / 2)]);
                throw Failure;
            }
            _bytes.Write(buffer.Span);
        }

        protected override void Dispose(bool disposing)
        {
            if (disposing)
                _bytes.Dispose();
            base.Dispose(disposing);
        }
    }
}
