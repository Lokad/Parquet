# Changelog

## Unreleased

### Fixed

- Correct slicing of all-valid optional INT32 pages and validate batch-size
  limits consistently for single-column and multi-column scans.
- Reject metadata-known unsupported column chunks during scan planning instead of
  mid-scan, for single-column as well as projected scans.
- Dispose owned streams exactly once when adapter construction fails, preserving
  the primary error; caller-owned streams stay untouched. Ownership transfers after
  argument validation.
- Preserve absent statistics extrema as absent instead of present-empty values,
  keeping counts-only and explicitly empty statistics distinguishable.
- Decode row-range slices of required FIXED_LEN_BYTE_ARRAY pages in slice
  coordinates, validating the full declared page size before bounded reads.
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
- Attach page identity to dictionary decoding failures, preserving inner errors
  and any more precise byte offset.

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
- Recompute paired-report point estimates, Student-t intervals, and log ratios
  from raw timings in `benchmark-report.ps1`, rejecting raw-time, log, bound,
  identity, and protocol tampering; schema-8 snapshots use a tested
  Cornish-Fisher Student-t quantile instead of the flat 1.645 fallback.
- Bind every benchmark worker to one logical processor from an explicit
  affinity handoff, resolve workspace links against the Linux mount table
  instead of /mnt prefixes, and persist host tuning plus output storage in
  schema-9 paired and schema-4 census snapshots.
- Freeze explicit benchmark endpoints: equivalent bulk consumers for engine
  comparisons with a labeled public-accessor diagnostic, a custom-source
  source-I/O lane, labeled consumer-only probes, and a pinned endpoint catalog
  documenting rows, nulls, output bytes, and ownership per endpoint. Fold the
  single-column required checksum so open-file, source, and steady-state scans
  compare against the same fixture checksum as the Core consumer.
- Attribute warm metadata open per stage and record the small-footer deficit as an
  accepted parity limitation: footer parsing dominates, the pristine control fails
  the same lane, and wider schemas already favor the validated reader.
- Extend the work census with narrow-projection, small-row-range, many-row-group,
  low-cardinality dictionary, compressible Snappy, and nullable-boolean lanes,
  recording per-pass time, batch counts, managed allocation, and GC counts in
  schema-4 snapshots. CRC-bearing pages are measured through committed producer
  fixtures and dedicated dictionary lanes with page-CRC validation.
- Verify live-session retention probes against the first-pass projected checksum instead of
  the full-case checksum, name the census case in probe diagnostics, and combine reordered
  projections in scan order; the stored full checksum now stands only for identity-ordered
  full projections, with narrow, reordered, and range projection coverage.
- Correct census truth details exposed by the first completed run: pass the raw UTF-8 byte
  chain as the high-cardinality string oracle, select baseline nullable-INT32 destinations
  by schema nullability, and re-pin the committed fixed-width fixture hash to the value
  both readers mutually verify.
- Retain one baseline live-session destination buffer per projected column, selected from
  the field type and nullability, instead of twelve parallel layouts per column.

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
