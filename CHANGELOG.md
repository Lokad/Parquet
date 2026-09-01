# Changelog

## Unreleased

### Added

- Memory-safe managed `.NET 10` reader with bounded path, stream, and custom
  random-access sources; immutable footer metadata; projected asynchronous
  scans; row-group/range selection; and disposable aligned column batches.
- Direct, borrowable `ReadOnlyMemory<byte>` input without a whole-file copy.
- Core 0.1 support for flat required/optional columns, every physical type
  except `INT96`, Data Page V1/V2, PLAIN/dictionary encodings, and
  UNCOMPRESSED/SNAPPY payloads. Other profiles remain explicitly unsupported.
- Normalized semantic annotations across modern and legacy schema metadata,
  exposed only after internal-consistency and physical-type validation.
- Hostile-input, deterministic mutation, pool-ownership, independent
  cross-producer, API-snapshot, and allocation/performance qualification.
- NuGet package metadata, Release-only packaging, symbol packages, and the
  `artifacts/nuget` output convention.
- MIT license and package icon artwork.
