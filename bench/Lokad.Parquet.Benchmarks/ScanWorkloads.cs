using Parquet;
using Parquet.Schema;
using System.Text;
using BaselineParquetSchema = Parquet.Schema.ParquetSchema;
using BaselineParquetWriter = Parquet.ParquetWriter;

namespace Lokad.Parquet.Benchmarks;

public enum ScanWorkload
{
    RequiredInt32Plain,
    NullableInt32Plain,
    RequiredInt32Snappy,
    RequiredStringPlain,
    RequiredStringSnappy,
    RequiredStringDictionary,
    RequiredStringDictionarySnappy,
    TwoRequiredInt32Plain,
    EightRequiredInt32Plain,
}

internal static class ScanWorkloadCatalog
{
    internal static IReadOnlyList<ScanWorkload> ParityWorkloads { get; } =
    [
        ScanWorkload.RequiredInt32Plain,
        ScanWorkload.NullableInt32Plain,
        ScanWorkload.RequiredInt32Snappy,
        ScanWorkload.RequiredStringPlain,
        ScanWorkload.RequiredStringSnappy,
        ScanWorkload.RequiredStringDictionary,
        ScanWorkload.RequiredStringDictionarySnappy,
        ScanWorkload.TwoRequiredInt32Plain,
        ScanWorkload.EightRequiredInt32Plain,
    ];

    internal static int GetColumnCount(ScanWorkload workload) => workload switch
    {
        ScanWorkload.TwoRequiredInt32Plain => 2,
        ScanWorkload.EightRequiredInt32Plain => 8,
        _ => 1,
    };

    // Single ownership for the reconciler's workload names and report labels.
    internal static IReadOnlyDictionary<ScanWorkload, string> Labels { get; } = new Dictionary<ScanWorkload, string>
    {
        [ScanWorkload.RequiredInt32Plain] = "Required INT32, PLAIN",
        [ScanWorkload.NullableInt32Plain] = "Nullable INT32, PLAIN",
        [ScanWorkload.RequiredInt32Snappy] = "Required INT32, Snappy",
        [ScanWorkload.RequiredStringPlain] = "Required UTF-8, PLAIN",
        [ScanWorkload.RequiredStringSnappy] = "Required UTF-8, PLAIN + Snappy",
        [ScanWorkload.RequiredStringDictionary] = "Required UTF-8, dictionary",
        [ScanWorkload.RequiredStringDictionarySnappy] = "Required UTF-8, dictionary + Snappy",
        [ScanWorkload.TwoRequiredInt32Plain] = "Two required INT32, PLAIN",
        [ScanWorkload.EightRequiredInt32Plain] = "Eight required INT32, PLAIN",
    };

    internal static bool IsString(ScanWorkload workload) => workload is
        ScanWorkload.RequiredStringPlain or
        ScanWorkload.RequiredStringSnappy or
        ScanWorkload.RequiredStringDictionary or
        ScanWorkload.RequiredStringDictionarySnappy;

    internal static int GetUtf8PayloadByteCount(ScanWorkload workload, int rowCount)
    {
        if (!IsString(workload))
            return 0;
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(rowCount);
        var byteCount = 0;
        for (var row = 0; row < rowCount; row++)
            byteCount = checked(byteCount + GetUtf8Bytes(row).Length);
        return byteCount;
    }

    internal static ReadOnlySpan<byte> GetUtf8Bytes(int row) => Utf8EncodedValues[row & 15];

    internal static IReadOnlyList<string> Utf8Values { get; } =
    [
        "",
        "ASCII text",
        "café",
        "naïve façade",
        "e\u0301",
        "Ελληνικά",
        "Москва",
        "東京",
        "数据工程",
        "안녕하세요",
        "مرحبا بالعالم",
        "नमस्ते दुनिया",
        "🙂",
        "🚀🌍✨",
        "Crème brûlée — déjà vu.",
        "UTF-8 stays bytes: café, 東京, مرحبا, 🙂.",
    ];
    private static readonly byte[][] Utf8EncodedValues =
        Utf8Values.Select(static value => Encoding.UTF8.GetBytes(value)).ToArray();
}

internal static class ScanChecksum
{
    internal const long Seed = 1_469_598_103_934_665_603L;
    internal const int ValueSeparator = 0x100;

    internal static int CreateInt32(int value) => unchecked((value * 1_000_003) ^ (value >> 3));

    internal static long Mix(long checksum, int value) =>
        unchecked((checksum * 1_099_511_628_211L) ^ value);

    internal static long ConsumeRequired(long checksum, ReadOnlySpan<int> values)
    {
        foreach (var value in values)
            checksum = Mix(checksum, value);
        return checksum;
    }

    // Canonical truth contract: every consumer accumulates one Mix chain per column
    // in row order, independent of batch partitioning, over non-null values only.
    // Null row ordinals accumulate in a separate chain, so a null can never equal
    // any value; CombineColumn folds the two chains in a fixed order. Folded
    // columns then combine in scan order, so swapped, duplicated, omitted, or
    // reordered columns change the result. A single column stands alone without
    // cross-column order. Only the per-value mapping and this combination are
    // shared; the reader consumers keep independent decode logic.
    internal static long CombineColumn(long valueChain, long nullChain) =>
        Mix(Mix(valueChain, (int)nullChain), (int)(nullChain >> 32));

    internal static long CombineColumns(ReadOnlySpan<long> columnHashes)
    {
        if (columnHashes.Length == 1)
            return columnHashes[0];
        var combined = Seed;
        foreach (var columnHash in columnHashes)
        {
            combined = Mix(combined, (int)columnHash);
            combined = Mix(combined, (int)(columnHash >> 32));
        }
        return combined;
    }

    internal static long CombineColumns(ReadOnlySpan<long> valueChains, ReadOnlySpan<long> nullChains)
    {
        if (valueChains.Length != nullChains.Length)
            throw new ArgumentException("Value and null chains must cover the same columns.", nameof(nullChains));
        if (valueChains.Length == 1)
            return CombineColumn(valueChains[0], nullChains[0]);
        var folded = new long[valueChains.Length];
        for (var column = 0; column < folded.Length; column++)
            folded[column] = CombineColumn(valueChains[column], nullChains[column]);
        return CombineColumns(folded);
    }
}

internal sealed record ScanFixture(
    ScanWorkload Workload,
    int RowCount,
    int ColumnCount,
    int Utf8PayloadBytes,
    byte[] Bytes,
    long Checksum,
    long[] ColumnChecksums,
    int[] NullCounts)
{
    internal static async Task<ScanFixture> CreateAsync(ScanWorkload workload, int rowCount)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(rowCount);
        var columnCount = ScanWorkloadCatalog.GetColumnCount(workload);
        var usesDictionary = workload is ScanWorkload.RequiredStringDictionary or
            ScanWorkload.RequiredStringDictionarySnappy;
        var options = new ParquetOptions
        {
            CompressionMethod = workload is ScanWorkload.RequiredInt32Snappy or
                ScanWorkload.RequiredStringSnappy or
                ScanWorkload.RequiredStringDictionarySnappy
                    ? CompressionMethod.Snappy
                    : CompressionMethod.None,
            DictionaryEncodingThreshold = usesDictionary
                    ? 1
                    : 0,
        };
        using var stream = new MemoryStream();
        var checksum = ScanChecksum.Seed;
        var perColumnChecksums = new long[columnCount];
        var nullCounts = new int[columnCount];
        for (var column = 0; column < columnCount; column++)
            perColumnChecksums[column] = ScanChecksum.Seed;

        if (workload == ScanWorkload.NullableInt32Plain)
        {
            var field = new DataField<int?>("value");
            var values = new int?[rowCount];
            var valueChain = ScanChecksum.Seed;
            var nullChain = ScanChecksum.Seed;
            for (var row = 0; row < values.Length; row++)
            {
                values[row] = (row & 7) == 0 ? null : ScanChecksum.CreateInt32(row);
                if (values[row] is int value)
                    valueChain = ScanChecksum.Mix(valueChain, value);
                else
                {
                    nullChain = ScanChecksum.Mix(nullChain, row);
                    nullCounts[0]++;
                }
            }
            checksum = ScanChecksum.CombineColumn(valueChain, nullChain);
            perColumnChecksums[0] = checksum;
            await WriteAsync(
                new BaselineParquetSchema(field),
                async rowGroup => await rowGroup.WriteAsync<int>(field, values));
        }
        else if (ScanWorkloadCatalog.IsString(workload))
        {
            var field = new DataField<string>("value", nullable: false);
            var encodedValues = new ReadOnlyMemory<char>[rowCount];
            for (var row = 0; row < encodedValues.Length; row++)
            {
                encodedValues[row] = ScanWorkloadCatalog.Utf8Values[row & 15].AsMemory();
                foreach (var value in ScanWorkloadCatalog.GetUtf8Bytes(row))
                    checksum = ScanChecksum.Mix(checksum, value);
                checksum = ScanChecksum.Mix(checksum, ScanChecksum.ValueSeparator);
            }
            if (usesDictionary)
                options.ColumnEncodingHints[field.Path.ToString()] = EncodingHint.Dictionary;
            await WriteAsync(
                new BaselineParquetSchema(field),
                async rowGroup => await rowGroup.WriteAsync<ReadOnlyMemory<char>>(field, encodedValues));
        }
        else
        {
            var fields = Enumerable.Range(0, columnCount)
                .Select(column => new DataField<int>($"value-{column}", nullable: false))
                .ToArray();
            var values = Enumerable.Range(0, columnCount)
                .Select(column => CreateInt32Values(rowCount, checked(column * 17)))
                .ToArray();
            for (var column = 0; column < columnCount; column++)
                perColumnChecksums[column] = ScanChecksum.CombineColumn(
                    ScanChecksum.ConsumeRequired(ScanChecksum.Seed, values[column]),
                    ScanChecksum.Seed);
            checksum = columnCount == 1
                ? perColumnChecksums[0]
                : ScanChecksum.CombineColumns(perColumnChecksums);
            await WriteAsync(
                new BaselineParquetSchema(fields),
                async rowGroup =>
                {
                    for (var column = 0; column < fields.Length; column++)
                        await rowGroup.WriteAsync<int>(fields[column], values[column]);
                });
        }

        var bytes = stream.ToArray();
        if (ScanWorkloadCatalog.IsString(workload))
        {
            await using var inspection = await ParquetFile.OpenAsync((ReadOnlyMemory<byte>)bytes);
            var chunk = inspection.Metadata.RowGroups[0].Columns[0];
            var hasDictionaryEncoding = chunk.EncodingCodes.Contains((int)ParquetEncoding.PlainDictionary) ||
                chunk.EncodingCodes.Contains((int)ParquetEncoding.RunLengthDictionary);
            if (hasDictionaryEncoding != usesDictionary)
                throw new InvalidOperationException(
                    $"The {workload} UTF-8 fixture does not use its declared encoding: " +
                    $"encodings {string.Join(',', chunk.EncodingCodes)}.");
        }
        return new ScanFixture(
            workload,
            rowCount,
            columnCount,
            ScanWorkloadCatalog.GetUtf8PayloadByteCount(workload, rowCount),
            bytes,
            checksum,
            perColumnChecksums,
            nullCounts);

        async Task WriteAsync(
            BaselineParquetSchema schema,
            Func<ParquetRowGroupWriter, Task> writeColumns)
        {
            await using var writer = await BaselineParquetWriter.CreateAsync(schema, stream, options);
            using var rowGroup = writer.CreateRowGroup();
            await writeColumns(rowGroup);
            rowGroup.CompleteValidate();
        }

        static int[] CreateInt32Values(int count, int salt)
        {
            var values = new int[count];
            for (var row = 0; row < values.Length; row++)
                values[row] = ScanChecksum.CreateInt32(row + salt);
            return values;
        }
    }

    // Setup-time truth gate: re-scans the fixture through the Lokad reader and
    // asserts row counts, per-column folded hashes, and null counts before any
    // timing runs. Integer lanes only; UTF-8 lanes are covered by their sink
    // checksum comparison at each call site.
    internal async Task AssertLokadScanAsync()
    {
        if (ScanWorkloadCatalog.IsString(Workload))
            throw new InvalidOperationException("The scan truth gate covers integer lanes only.");
        await using var file = await ParquetFile.OpenAsync((ReadOnlyMemory<byte>)Bytes);
        if (file.Metadata.RowCount != RowCount || file.Metadata.Schema.Columns.Count != ColumnCount)
            throw new InvalidOperationException("The scan truth gate found unexpected metadata.");
        var valueChains = new long[ColumnCount];
        var nullChains = new long[ColumnCount];
        var nullCounts = new int[ColumnCount];
        var consumed = new int[ColumnCount];
        Array.Fill(valueChains, ScanChecksum.Seed);
        Array.Fill(nullChains, ScanChecksum.Seed);
        var rows = 0;
        await foreach (var batch in file.ScanAsync(new ParquetScanOptions(file.Metadata.Schema.Columns)))
        {
            using (batch)
            {
                rows += batch.RowCount;
                for (var columnIndex = 0; columnIndex < batch.Columns.Count; columnIndex++)
                {
                    if (batch.Columns[columnIndex] is not ParquetPrimitiveColumnBatch<int> integers)
                        throw new InvalidOperationException("The scan truth gate decoded an unexpected batch type.");
                    for (var row = 0; row < integers.RowCount; row++)
                    {
                        if (integers.Validity.IsValid(row))
                            valueChains[columnIndex] = ScanChecksum.Mix(valueChains[columnIndex], integers.Values.Span[row]);
                        else
                        {
                            nullChains[columnIndex] = ScanChecksum.Mix(nullChains[columnIndex], consumed[columnIndex] + row);
                            nullCounts[columnIndex]++;
                        }
                    }
                    consumed[columnIndex] += integers.RowCount;
                }
            }
        }
        if (rows != RowCount)
            throw new InvalidOperationException($"The scan truth gate saw {rows} rows instead of {RowCount}.");
        for (var column = 0; column < ColumnCount; column++)
        {
            if (nullCounts[column] != NullCounts[column])
                throw new InvalidOperationException($"The scan truth gate saw {nullCounts[column]} nulls instead of {NullCounts[column]} in column {column}.");
            var folded = ScanChecksum.CombineColumn(valueChains[column], nullChains[column]);
            if (folded != ColumnChecksums[column])
                throw new InvalidOperationException($"The scan truth gate hash mismatch in column {column}.");
        }
    }
}

internal sealed class Utf8ScanSink
{
    private static readonly UTF8Encoding StrictUtf8 = new(false, true);
    private readonly byte[] _payload;
    private readonly int[] _offsets;
    private int _byteCount;
    private long _checksum;
    private int _rowCount;

    internal Utf8ScanSink(int rowCount, int payloadByteCount)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(rowCount);
        ArgumentOutOfRangeException.ThrowIfNegative(payloadByteCount);
        _payload = new byte[payloadByteCount];
        _offsets = new int[checked(rowCount + 1)];
        Reset();
    }

    internal void Reset()
    {
        _byteCount = 0;
        _checksum = ScanChecksum.Seed;
        _rowCount = 0;
        _offsets[0] = 0;
    }

    internal void AppendUtf8(ReadOnlySpan<byte> value)
    {
        _ = StrictUtf8.GetCharCount(value);
        var destination = _payload.AsSpan(_byteCount);
        value.CopyTo(destination);
        AppendBytes(destination[..value.Length]);
    }

    internal void AppendString(string value)
    {
        var written = StrictUtf8.GetBytes(value, _payload.AsSpan(_byteCount));
        AppendBytes(_payload.AsSpan(_byteCount, written));
    }

    internal long Complete(int expectedRowCount, int expectedPayloadByteCount)
    {
        if (_rowCount != expectedRowCount || _byteCount != expectedPayloadByteCount ||
            _offsets[_rowCount] != expectedPayloadByteCount)
            throw new InvalidOperationException("The UTF-8 benchmark sink produced an unexpected layout.");
        return _checksum;
    }

    private void AppendBytes(ReadOnlySpan<byte> bytes)
    {
        foreach (var value in bytes)
            _checksum = ScanChecksum.Mix(_checksum, value);
        _checksum = ScanChecksum.Mix(_checksum, ScanChecksum.ValueSeparator);
        _byteCount = checked(_byteCount + bytes.Length);
        _rowCount++;
        _offsets[_rowCount] = _byteCount;
    }
}
