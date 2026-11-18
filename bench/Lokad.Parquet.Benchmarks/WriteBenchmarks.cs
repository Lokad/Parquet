using System.Reflection;
using System.Security.Cryptography;
using System.Text;
using BenchmarkDotNet.Attributes;
using Parquet;
using Parquet.Schema;
using BaselineSchema = Parquet.Schema.ParquetSchema;
using BaselineWriter = Parquet.ParquetWriter;
using LokadWriter = Lokad.Parquet.ParquetWriter;

namespace Lokad.Parquet.Benchmarks;

public enum WriteWorkload
{
    FixedWidth,
    FixedWidthRuns,
    RepeatedUtf8,
    HighCardinalityUtf8,
}

[MemoryDiagnoser]
public class WriteBenchmarks
{
    internal const int GroupRows = 16_384;
    internal const int Groups = 4;
    internal const int Rows = GroupRows * Groups;
    private static readonly UTF8Encoding Utf8 = new(false, true);
    private static readonly ParquetOptions BaselineOptions = new()
    {
        CompressionMethod = CompressionMethod.Snappy,
        DictionaryEncodingThreshold = 0,
    };

    private readonly MemoryStream _destination = new(capacity: 4 * 1024 * 1024);
    private ParquetWriteColumn[] _columns = [];
    private BaselineSchema? _baselineSchema;
    private bool[] _flags = [];
    private float[] _measurements = [];
    private long[] _integers = [];
    private byte[] _payload = [];
    private int[] _offsets = [];

    [ParamsAllValues]
    public WriteWorkload Workload { get; set; }

    internal int ColumnCount => _columns.Length;
    private bool IsFixedWidth => Workload is WriteWorkload.FixedWidth or WriteWorkload.FixedWidthRuns;

    [GlobalSetup]
    public async Task Setup()
    {
        BenchmarkHostPolicy.AssertWorkerEnvironment();
        await PrepareAsync();
    }

    internal async Task PrepareAsync()
    {
        PrepareInput();
        var lokadBytes = await LokadWrite();
        var lokadHash = await VerifyOutputAsync(lokadBytes, "Lokad.Parquet");
        var baselineBytes = await ParquetNetWrite();
        var baselineHash = await VerifyOutputAsync(baselineBytes, "Parquet.NET");
        new PublicationSetupEvidence(Workload.ToString(), Rows, ColumnCount)
        {
            LokadFileBytes = lokadBytes,
            BaselineFileBytes = baselineBytes,
            LokadFileSha256 = lokadHash,
            BaselineFileSha256 = baselineHash,
        }.Log();
    }

    internal void PrepareInput()
    {
        if (IsFixedWidth)
        {
            _columns =
            [
                new("flag", ParquetPhysicalType.Boolean),
                new("measurement", ParquetPhysicalType.Float),
                new("quantity", ParquetPhysicalType.Int64),
            ];
            _baselineSchema = new BaselineSchema(new DataField<bool>("flag"),
                new DataField<float>("measurement"), new DataField<long>("quantity"));
            _flags = new bool[Rows];
            _measurements = new float[Rows];
            _integers = new long[Rows];
            var usesRuns = Workload == WriteWorkload.FixedWidthRuns;
            var secondWordOffset = usesRuns ? 31 : 1;
            for (var row = 0; row < Rows; row++)
            {
                var sample = usesRuns ? row / 7 : row;
                var integer = ScanChecksum.CreateInt32(sample);
                _flags[row] = (sample & 7) == 0;
                _measurements[row] = sample * 0.25f;
                _integers[row] = ((long)integer << 32) | (uint)ScanChecksum.CreateInt32(sample + secondWordOffset);
            }
        }
        else
        {
            _columns = [new("label", ParquetPhysicalType.ByteArray)];
            _baselineSchema = new BaselineSchema(new DataField<string>("label", nullable: false));
            using var bytes = new MemoryStream();
            _offsets = new int[Rows + 1];
            string[] repeated = ["", "café", "e\u0301", "سلام", "漢字😀", "Référence 123"];
            for (var row = 0; row < Rows; row++)
            {
                var label = Workload == WriteWorkload.RepeatedUtf8
                    ? repeated[row % repeated.Length]
                    : FormattableString.Invariant($"{row:x8}-{ScanChecksum.CreateInt32(row):x8}-é😀");
                bytes.Write(Utf8.GetBytes(label));
                _offsets[row + 1] = (int)bytes.Length;
            }
            _payload = bytes.ToArray();
        }
    }

    [GlobalCleanup]
    public void Cleanup() => _destination.Dispose();

    [Benchmark]
    public async Task<int> LokadWrite()
    {
        _destination.SetLength(0);
        using var writer = await LokadWriter.CreateAsync(_destination, _columns);
        await WriteLokadGroupsAsync(writer, Groups);
        await writer.CompleteAsync();
        return (int)_destination.Length;
    }

    [Benchmark(Baseline = true)]
    public async Task<int> ParquetNetWrite()
    {
        _destination.SetLength(0);
        var schema = _baselineSchema ?? throw new InvalidOperationException("The baseline schema is unavailable.");
        await using (var writer = await BaselineWriter.CreateAsync(schema, _destination, BaselineOptions))
        {
            var fields = schema.DataFields;
            for (var group = 0; group < Groups; group++)
            {
                using var rowGroup = writer.CreateRowGroup();
                var start = group * GroupRows;
                if (IsFixedWidth)
                {
                    await rowGroup.WriteAsync<bool>(fields[0], _flags.AsMemory(start, GroupRows));
                    await rowGroup.WriteAsync<float>(fields[1], _measurements.AsMemory(start, GroupRows));
                    await rowGroup.WriteAsync<long>(fields[2], _integers.AsMemory(start, GroupRows));
                }
                else
                {
                    var strings = new string[GroupRows];
                    for (var i = 0; i < strings.Length; i++)
                    {
                        var row = start + i;
                        strings[i] = Utf8.GetString(_payload.AsSpan(_offsets[row].._offsets[row + 1]));
                    }
                    await rowGroup.WriteAsync(fields[0], strings);
                }
                rowGroup.CompleteValidate();
            }
        }
        return (int)_destination.Length;
    }

    internal async Task<WriterMemorySample> MeasureWorkingBuffersAsync(int groups)
    {
        // Use the writer's actual pool capacities in a separate, untimed pass.
        var pool = typeof(LokadWriter).Assembly.GetType("Lokad.Parquet.Internal.ParquetArrayPool") ??
            throw new InvalidOperationException("The writer pool is unavailable.");
        var rent = pool.GetProperty("RentObserver") ?? throw new InvalidOperationException("Pool rent observer is unavailable.");
        var giveBack = pool.GetProperty("ReturnObserver") ?? throw new InvalidOperationException("Pool return observer is unavailable.");
        var live = 0L;
        var peak = 0L;
        rent.SetValue(null, (Action<Array, int>)((array, _) =>
        {
            live += Buffer.ByteLength(array);
            peak = Math.Max(peak, live);
        }));
        giveBack.SetValue(null, (Action<Array, int>)((array, _) => live -= Buffer.ByteLength(array)));
        try
        {
            // The same input groups are reused for the scaling probe.
            using var writer = await LokadWriter.CreateAsync(Stream.Null, _columns);
            await WriteLokadGroupsAsync(writer, groups);
            var pagePeak = peak;
            if (typeof(LokadWriter).GetField("_footerBound", BindingFlags.Instance | BindingFlags.NonPublic)
                ?.GetValue(writer) is not int retainedFooterBound)
                throw new InvalidOperationException("The writer footer capacity bound is unavailable.");
            await writer.CompleteAsync();
            if (live != 0)
                throw new InvalidOperationException("The writer left working buffers rented.");
            return new WriterMemorySample(groups, pagePeak, peak, groups * ColumnCount, retainedFooterBound);
        }
        finally
        {
            rent.SetValue(null, null);
            giveBack.SetValue(null, null);
        }
    }

    private async Task WriteLokadGroupsAsync(LokadWriter writer, int groups)
    {
        for (var group = 0; group < groups; group++)
        {
            writer.BeginRowGroup(GroupRows);
            var start = (group % Groups) * GroupRows;
            if (IsFixedWidth)
            {
                await writer.WriteBooleansAsync(_flags.AsMemory(start, GroupRows));
                await writer.CompleteColumnAsync();
                await writer.WriteFloatsAsync(_measurements.AsMemory(start, GroupRows));
                await writer.CompleteColumnAsync();
                await writer.WriteInt64Async(_integers.AsMemory(start, GroupRows));
                await writer.CompleteColumnAsync();
            }
            else
            {
                await writer.WriteUtf8Async(_payload, _offsets.AsMemory(start, GroupRows + 1));
                await writer.CompleteColumnAsync();
            }
        }
    }

    private async Task<string> VerifyOutputAsync(int length, string writer)
    {
        var bytes = (ReadOnlyMemory<byte>)_destination.GetBuffer().AsMemory(0, length);
        await using var file = await ParquetFile.OpenAsync(bytes);
        var columns = file.Metadata.Schema.Columns;
        if (file.Metadata.RowCount != Rows || file.Metadata.RowGroups.Count != Groups ||
            columns.Count != _columns.Length)
            throw new InvalidOperationException($"{writer} write benchmark produced an unexpected table shape.");

        for (var i = 0; i < columns.Count; i++)
        {
            var column = columns[i];
            var isUtf8 = _columns[i].PhysicalType == ParquetPhysicalType.ByteArray;
            if (column.Name != _columns[i].Name || column.SchemaElement.PhysicalType != _columns[i].PhysicalType ||
                column.SchemaElement.Repetition != ParquetRepetition.Required ||
                (isUtf8 && column.SchemaElement.SemanticAnnotation?.Kind != ParquetSemanticTypeKind.String))
                throw new InvalidOperationException($"{writer} write benchmark produced an unexpected schema.");
        }
        foreach (var group in file.Metadata.RowGroups)
        {
            if (group.RowCount != GroupRows || group.Columns.Any(chunk =>
                chunk.CompressionCodec != ParquetCompressionCodec.Snappy ||
                !chunk.EncodingCodes.Contains((int)ParquetEncoding.Plain) ||
                chunk.EncodingCodes.Any(code => code != (int)ParquetEncoding.Plain &&
                    code != (int)ParquetEncoding.RunLength && code != (int)ParquetEncoding.BitPacked)))
                throw new InvalidOperationException($"{writer} write benchmark did not produce PLAIN Snappy chunks with matching row-group sizes.");
        }

        var start = 0;
        await foreach (var batch in file.ScanAsync(new(columns)))
        {
            using (batch)
            {
                var rows = batch.RowCount;
                var matches = IsFixedWidth
                    ? ((ParquetPrimitiveColumnBatch<bool>)batch.Columns[0]).Values.Span.SequenceEqual(_flags.AsSpan(start, rows)) &&
                      ((ParquetPrimitiveColumnBatch<float>)batch.Columns[1]).Values.Span.SequenceEqual(_measurements.AsSpan(start, rows)) &&
                      ((ParquetPrimitiveColumnBatch<long>)batch.Columns[2]).Values.Span.SequenceEqual(_integers.AsSpan(start, rows))
                    : MatchesUtf8((ParquetBinaryColumnBatch)batch.Columns[0], start);
                if (!matches)
                    throw new InvalidOperationException($"{writer} write benchmark decoded unexpected values.");
                start += rows;
            }
        }
        if (start != Rows)
            throw new InvalidOperationException($"{writer} write benchmark decoded {start} rows instead of {Rows}.");
        return Convert.ToHexStringLower(SHA256.HashData(bytes.Span));

        bool MatchesUtf8(ParquetBinaryColumnBatch batch, int row)
        {
            var expectedStart = _offsets[row];
            var end = _offsets[row + batch.RowCount];
            if (!batch.Payload.Span.SequenceEqual(_payload.AsSpan(expectedStart..end)))
                return false;
            for (var i = 0; i <= batch.RowCount; i++)
            {
                if (batch.Offsets.Span[i] != _offsets[row + i] - expectedStart)
                    return false;
            }
            return true;
        }
    }
}

internal sealed record WriterMemorySample(
    int RowGroups, long PageBuffersPeakBytes, long AllPooledPeakBytes, int ChunkDescriptors, int FooterCapacityBoundBytes);
