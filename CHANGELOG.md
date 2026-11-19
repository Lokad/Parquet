# Changelog

## 0.2.0 - 2026-11-19

- Add streaming writes for flat, required BOOLEAN, FLOAT, INT64, UTF-8 STRING,
  and DATE columns using PLAIN Data Page V1 and Snappy.
- Ship .NET 10 and .NET 11 assets in the same dependency-free NuGet package.
- Fix row-range slicing for all-valid optional INT32 and required
  FIXED_LEN_BYTE_ARRAY pages, and preserve absent statistics extrema.
- Reject unsupported column chunks before scanning. Tighten metadata and page
  validation, memory-budget enforcement, and error locations.
- Improve cancellation and cleanup during reads so failed or canceled scans
  release their buffers and owned resources.
- Reduce scan allocations and copying by reusing buffers and transferring
  decoded binary data directly into batches.

## 0.1.0 - 2026-09-02

- Initial dependency-free .NET 10 reader with bounded random access from files,
  streams, memory, or custom sources.
- Read flat required and optional columns with Data Page V1/V2, PLAIN or
  dictionary encoding, and uncompressed or Snappy payloads. Support all Parquet
  physical types except INT96 and expose validated logical annotations.
- Expose immutable footer metadata and asynchronous projected scans with
  row-group and row-range selection.
