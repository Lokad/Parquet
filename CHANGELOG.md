# Changelog

## Unreleased

### Fixed

- Correct slicing of all-valid optional INT32 pages and validate batch-size
  limits consistently for single-column and multi-column scans.
- Prevent disposal of a completed projected enumerator from unregistering a
  later scan.
- Return page-header buffers after synchronous parsing failures and protect
  earlier buffer rentals when a subsequent rental fails.
- Consume custom-source `ValueTask` reads exactly once and respect asynchronous
  disposal overrides on `MemoryStream` subclasses.
- Enforce inline Thrift collection limits, add depth checks for known metadata
  structures, validate individual metadata offsets, and check decoded
  dictionary sizes.
- Observe cancellation after page reads and in additional decoding and CRC
  loops; improve malformed-input classification and error locations.
- Release idle column-cache buffers when changing projections so they do not
  prevent scans within the configured memory budget.
- Release shared validity bitmaps when cancellation or invalid slicing fails during
  bitmap construction, keeping pool and budget ownership balanced.
- Release every scan resource and registration when an early cleanup step throws, and
  preserve the primary decode or cancellation error over teardown diagnostics.
- Check page counts against their row group, minimum rent bytes against the remaining
  scan budget, and footer container counts against the remaining input before renting
  or allocating; transient peak accounting now includes already retained bytes.
- Observe cancellation in dictionary, footer, and page-header loops and check it before
  publishing each batch, releasing the built batch instead of yielding it; disposal
  during a projected scan now surfaces a file-disposal error like a single scan.
- Validate row-group descriptors and the row range before empty-selection fast exits,
  and acquire the single scan lane before evicting idle column caches.
- Give Thrift depth one meaning: skip entry points take the enclosing struct depth
  so skipped subtrees share the depth cap with known structures; document the model.
- Annotate malformed Snappy and page-header failures with page identity while keeping
  the offending offset and inner error; document that the dictionary byte limit caps
  serialized page bytes as well as the decoded layout.
- Share one lifetime and owner array between decoded and public single-column batches
  and derive scan budgets and caches from the file instead of passing them alongside it.
- Share V1 section splitting, required V2 validation, null counting, and batch owner
  disposal through single checked helpers instead of repeating them per path.

### Changed

- Transfer complete decoded BYTE_ARRAY and FIXED_LEN_BYTE_ARRAY pages into
  batches without an additional payload copy; reuse uncompressed V2 payloads.
- Avoid boxing in enum-backed metadata accessors and reduce page-header and
  binary-batch bookkeeping allocations.
- Reject redundant final bit-packed groups; only the minimum final group
  padding needed for the declared value count is accepted.
- Clarify Core 0.1 support: Boolean values require PLAIN or dictionary encoding,
  and page boundaries may produce batches smaller than the requested target.
- Expand ownership, cancellation, malformed-input, partitioning, and public API
  contract regression coverage.
- Cover public fields, events, generic method constraints, by-reference
  parameter kinds, and array-level nullability in the public API snapshot, and
  document the policy comment/string stripper interpolation blind spot.
- Clarify borrowed payload versus rented header ownership, drop unused page-header
  retry parameters, describe current page-state invariants, and correct release
  and benchmark documentation wording.
- Bind the benchmark truth checksum to column order and separated null chains so
  swapped, duplicated, omitted, or reordered columns and null-marker values fail
  loudly; qualify the actual Core, Parity, and Census consumers with
  `--verify-truth` before timing.
- Measure work-census source reads on the exercised async path with per-pass
  peaks, an uneven multi-row-group case, and warmed competitor retention in the
  snapshot.

## 0.1.0 - 2026-09-02

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
