using System.Globalization;
using BenchmarkDotNet.Columns;
using BenchmarkDotNet.Reports;
using BenchmarkDotNet.Running;

namespace Lokad.Parquet.Benchmarks;

internal enum NormalizedScanMetric
{
    NanosecondsPerCell,
    MillionCellsPerSecond,
    NanosecondsPerUtf8Byte,
    Utf8GigabytesPerSecond,
}

internal sealed class NormalizedScanColumn(NormalizedScanMetric metric) : IColumn
{
    public string Id => nameof(NormalizedScanColumn) + "." + metric;
    public string ColumnName => metric switch
    {
        NormalizedScanMetric.NanosecondsPerCell => "ns/cell",
        NormalizedScanMetric.MillionCellsPerSecond => "M cells/s",
        NormalizedScanMetric.NanosecondsPerUtf8Byte => "ns/UTF8 B",
        NormalizedScanMetric.Utf8GigabytesPerSecond => "UTF8 GB/s",
        _ => throw new InvalidOperationException("The normalized scan metric is unknown."),
    };
    public bool AlwaysShow => true;
    public ColumnCategory Category => ColumnCategory.Metric;
    public int PriorityInCategory => (int)metric;
    public bool IsNumeric => true;
    public UnitType UnitType => UnitType.Dimensionless;
    public string Legend => metric switch
    {
        NormalizedScanMetric.NanosecondsPerCell => "Mean nanoseconds per decoded projected cell.",
        NormalizedScanMetric.MillionCellsPerSecond => "Mean decoded projected cells per second, in millions.",
        NormalizedScanMetric.NanosecondsPerUtf8Byte =>
            "Mean nanoseconds per validated UTF-8 payload byte delivered to the consumer sink.",
        NormalizedScanMetric.Utf8GigabytesPerSecond =>
            "Validated UTF-8 payload delivered to the consumer sink in decimal gigabytes per second.",
        _ => throw new InvalidOperationException("The normalized scan metric is unknown."),
    };

    public string GetValue(Summary summary, BenchmarkCase benchmarkCase) =>
        GetValue(summary, benchmarkCase, summary.Style);

    public string GetValue(Summary summary, BenchmarkCase benchmarkCase, SummaryStyle style)
    {
        var report = summary[benchmarkCase];
        var statistics = report?.ResultStatistics;
        var unitCount = metric is NormalizedScanMetric.NanosecondsPerUtf8Byte or
            NormalizedScanMetric.Utf8GigabytesPerSecond
                ? GetUtf8ByteCount(benchmarkCase)
                : GetCellCount(benchmarkCase);
        if (statistics is null || unitCount == 0)
            return "-";
        var value = metric switch
        {
            NormalizedScanMetric.NanosecondsPerCell => statistics.Mean / unitCount,
            NormalizedScanMetric.MillionCellsPerSecond => unitCount * 1000d / statistics.Mean,
            NormalizedScanMetric.NanosecondsPerUtf8Byte => statistics.Mean / unitCount,
            NormalizedScanMetric.Utf8GigabytesPerSecond => unitCount / statistics.Mean,
            _ => throw new InvalidOperationException("The normalized scan metric is unknown."),
        };
        return value.ToString("N3", CultureInfo.InvariantCulture);

        static long GetCellCount(BenchmarkCase benchmarkCase)
        {
            if (benchmarkCase.Descriptor.Type == typeof(RequiredInt32Benchmarks))
                return (int)benchmarkCase.Parameters["RowCount"];
            if (benchmarkCase.Descriptor.Type == typeof(SteadyStateScanBenchmarks) ||
                benchmarkCase.Descriptor.Type == typeof(SourceScanBenchmarks))
                return (int)benchmarkCase.Parameters["RowCount"];
            if (benchmarkCase.Descriptor.Type == typeof(PreopenedScanBenchmarks) ||
                benchmarkCase.Descriptor.Type == typeof(PreopenedUtf8ScanBenchmarks) ||
                benchmarkCase.Descriptor.Type == typeof(PreopenedMaterializationBenchmarks))
            {
                var parityRows = (int)benchmarkCase.Parameters["RowCount"];
                var parityWorkload = (ScanWorkload)benchmarkCase.Parameters["Workload"];
                var parityColumns = ScanWorkloadCatalog.GetColumnCount(parityWorkload);
                return checked((long)parityRows * parityColumns);
            }
            if (benchmarkCase.Descriptor.Type != typeof(CoreScanBenchmarks))
                return 0;

            var rows = (int)benchmarkCase.Parameters["RowCount"];
            var workload = (ScanWorkload)benchmarkCase.Parameters["Workload"];
            var columns = ScanWorkloadCatalog.GetColumnCount(workload);
            return checked((long)rows * columns);
        }

        static long GetUtf8ByteCount(BenchmarkCase benchmarkCase)
        {
            if (benchmarkCase.Descriptor.Type != typeof(CoreScanBenchmarks) &&
                benchmarkCase.Descriptor.Type != typeof(PreopenedUtf8ScanBenchmarks))
                return 0;
            var rows = (int)benchmarkCase.Parameters["RowCount"];
            var workload = (ScanWorkload)benchmarkCase.Parameters["Workload"];
            return ScanWorkloadCatalog.GetUtf8PayloadByteCount(workload, rows);
        }
    }

    public bool IsDefault(Summary summary, BenchmarkCase benchmarkCase) => false;

    public bool IsAvailable(Summary summary) => true;
}
