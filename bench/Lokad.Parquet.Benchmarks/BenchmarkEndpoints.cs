namespace Lokad.Parquet.Benchmarks;

// Frozen benchmark-endpoint catalog (B05). Every BenchmarkDotNet benchmark
// method belongs to exactly one endpoint, and BenchmarkEndpointTests pins that
// coverage in both directions, so a new or relabeled benchmark cannot silently
// join a claim. PipelineEvidence marks full read pipelines; checksum-only
// probes, sentinel materializations, kernel/codec micro-benchmarks, open-only
// timings, and the public-accessor diagnostic stay false so they can never be
// substituted for pipeline evidence.
public static class BenchmarkEndpoints
{
    public sealed record Endpoint(string Id, string Label, string BenchmarkType, string BenchmarkMethod, bool PipelineEvidence);

    public static readonly Endpoint[] All =
    [
        new("open", "Metadata open", nameof(MetadataOpenBenchmarks), nameof(MetadataOpenBenchmarks.LokadOpen), false),
        new("open", "Metadata open", nameof(MetadataOpenBenchmarks), nameof(MetadataOpenBenchmarks.ParquetNetOpen), false),
        new("preopened-scan", "Pre-opened projected scan", nameof(PreopenedScanBenchmarks), nameof(PreopenedScanBenchmarks.LokadProjectedScan), true),
        new("preopened-scan", "Pre-opened projected scan", nameof(PreopenedScanBenchmarks), nameof(PreopenedScanBenchmarks.ParquetNetProjectedScan), true),
        new("preopened-scan", "Pre-opened UTF-8 pipeline", nameof(PreopenedUtf8ScanBenchmarks), nameof(PreopenedUtf8ScanBenchmarks.LokadUtf8Pipeline), true),
        new("preopened-scan", "Pre-opened UTF-8 pipeline", nameof(PreopenedUtf8ScanBenchmarks), nameof(PreopenedUtf8ScanBenchmarks.ParquetNetUtf8Pipeline), true),
        new("open-scan-required", "Required INT32 open and scan", nameof(RequiredInt32Benchmarks), nameof(RequiredInt32Benchmarks.LokadProjectedScan), true),
        new("open-scan-required", "Required INT32 open and scan", nameof(RequiredInt32Benchmarks), nameof(RequiredInt32Benchmarks.ParquetNetProjectedScan), true),
        new("open-scan-required", "Required INT32 public-accessor diagnostic", nameof(RequiredInt32Benchmarks), nameof(RequiredInt32Benchmarks.LokadPublicAccessorDiagnostic), false),
        new("open-scan-workloads", "Core open and scan", nameof(CoreScanBenchmarks), nameof(CoreScanBenchmarks.LokadProjectedScan), true),
        new("open-scan-workloads", "Core open and scan", nameof(CoreScanBenchmarks), nameof(CoreScanBenchmarks.ParquetNetProjectedScan), true),
        new("consumer-only", "Scan on an open file", nameof(SteadyStateScanBenchmarks), nameof(SteadyStateScanBenchmarks.Scan), false),
        new("consumer-only", "Scan on an open file", nameof(PlainInt32KernelBenchmarks), nameof(PlainInt32KernelBenchmarks.EightColumnScan), false),
        new("consumer-only", "Checksum loop without decoding", nameof(PlainInt32KernelBenchmarks), nameof(PlainInt32KernelBenchmarks.ArrayChecksum), false),
        new("consumer-only", "Checksum loop without decoding", nameof(PlainInt32KernelBenchmarks), nameof(PlainInt32KernelBenchmarks.MemorySpanChecksum), false),
        new("materialization", "Pre-opened materialization sentinel", nameof(PreopenedMaterializationBenchmarks), nameof(PreopenedMaterializationBenchmarks.LokadMaterialize), false),
        new("materialization", "Pre-opened materialization sentinel", nameof(PreopenedMaterializationBenchmarks), nameof(PreopenedMaterializationBenchmarks.ParquetNetMaterialize), false),
        new("source-io", "Open and scan from a source", nameof(SourceScanBenchmarks), nameof(SourceScanBenchmarks.Memory), true),
        new("source-io", "Open and scan from a source", nameof(SourceScanBenchmarks), nameof(SourceScanBenchmarks.MemoryStream), true),
        new("source-io", "Open and scan from a source", nameof(SourceScanBenchmarks), nameof(SourceScanBenchmarks.LocalFile), true),
        new("source-io", "Open and scan from a source", nameof(SourceScanBenchmarks), nameof(SourceScanBenchmarks.CustomSource), true),
        new("decoder-codec", "Decoder and codec kernels", nameof(PlainInt32KernelBenchmarks), nameof(PlainInt32KernelBenchmarks.Decode), false),
        new("decoder-codec", "Decoder and codec kernels", nameof(SnappyCodecBenchmarks), nameof(SnappyCodecBenchmarks.Decode), false),
    ];
}
