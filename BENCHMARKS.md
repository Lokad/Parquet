# Lokad.Parquet versus Parquet.NET 6.1.0

Snapshot date: 2026-08-31. This report covers the fixed set of overlapping
Core 0.1 operations declared before qualification. It is a non-inferiority
claim for that catalog, not a feature-parity or universal-performance claim.

Direct `ReadOnlyMemory<byte>` input is covered by the separate Source suite,
not by this fixed parity table. Its repository-local Dry run is a truth and
allocation smoke check only; no quantitative direct-memory claim is made from
that run.

The current harness extends this recorded snapshot with UTF-8-preserving scan
cases. Those cases must complete the same four-session Windows/Linux protocol
before they are folded into the generated result tables below.

## UTF-8 pipeline contract

String qualification models the intended application pipeline: Parquet UTF-8
is strictly validated, processed, and retained as UTF-8. The measured endpoint
is an equivalent reusable payload-and-offset sink for both readers.

- Lokad.Parquet validates each public UTF-8 byte span and copies it directly
  into the sink.
- Parquet.NET materializes .NET strings and strictly re-encodes them into the
  sink. Its UTF-8-to-UTF-16-to-UTF-8 interlude and per-value allocations are
  measured as Parquet.NET overhead; the benchmark does not normalize them
  away.
- Both paths must reproduce the same UTF-8 bytes, value boundaries, ordering,
  row count, and checksum before timing.
- The fixture mixes empty and short-to-medium ASCII, composed and decomposed
  Latin text, Greek, Cyrillic, CJK, Korean, Arabic, Devanagari, and emoji.
- PLAIN, PLAIN with Snappy, dictionary, and dictionary with Snappy are separate
  cases over the same values, so encoding and compression costs are not
  conflated.
- Results report time per value and per UTF-8 payload byte, UTF-8 GB/s, managed
  allocation, and GC activity. Payload bytes exclude offsets and Parquet
  framing.

This is application-pipeline parity, not a claim that the libraries expose the
same intermediate representation. UTF-16 conversion would be artificial work
for Lokad.Parquet and is therefore not added to its path.

## Endpoint catalog

Every BenchmarkDotNet benchmark belongs to exactly one frozen endpoint
(see `BenchmarkEndpoints`; `BenchmarkEndpointTests` pins the coverage both
ways). Pipeline evidence comes only from full read pipelines; checksum-only
loops, materialization sentinels, decoder/codec kernels, open-only timings,
and the public-accessor diagnostic are labeled non-pipeline and can never be
substituted for pipeline claims.

| Endpoint | Benchmarks | Measures | Excludes | Ownership |
|---|---|---|---|---|
| open | Metadata open and open-stage diagnostics (both readers) | Footer open on a MemoryStream; stages split reads, parse, open-only, dispose-only, and large schemas | Scan and consume | Stream disposed per invocation (or in iteration cleanup for the no-dispose split) |
| preopened-scan | Pre-opened projected scans and UTF-8 pipelines | Scan and consume on pre-opened readers | Open | Preallocated baseline destinations and pooled Lokad batches; sinks reset per invocation |
| open-scan-required | Required INT32 open and scan, plus the public-accessor diagnostic | Open, scan, and consume of one required INT32 column | Shared state across invocations | Fresh stream per invocation; the diagnostic uses per-row accessors and is not a parity endpoint |
| open-scan-workloads | Core open and scan across workloads | Open, scan, and consume across the workload catalog | Cross-workload pooling | Fresh stream per invocation |
| consumer-only | Steady-state and eight-column scans, array and span checksums | Consume work with the open cost excluded | Open; decoding for the pure checksum loops | Pre-opened files or plain arrays |
| materialization | Pre-opened materialization sentinels | First/last value probes per column | Full row coverage | Pre-opened readers and destinations |
| source-io | Open and scan from memory, stream, file, or a custom source | The full pipeline with the source as the varied axis | Source-specific warmup beyond the stated state | Borrowed memory, caller streams, warmed files, custom adapters |
| decoder-codec | PLAIN INT32 and Snappy kernels | Codec kernels without any reader | File and page structure | Caller arrays |

Equivalence invariants: every pipeline endpoint reads the same rows, columns,
and nulls from identical fixture bytes and must reproduce the same output
bytes and checksum before timing (setup truth checks throw otherwise); the
work census counts retained pooled capacity per workload, and retention probes
hold one pre-opened reader per side with reused destinations and sink alive
through the observation, verifying the same emitted rows, columns and ranges
on every repetition. The baseline UTF-8 re-encoding (UTF-16 interlude plus
per-value allocations) is part of the measured UTF-8 endpoint, not a universal
string-reading claim; no UTF-16-output endpoint is measured. Baseline scan
destinations stay preallocated and Lokad pool reuse stays intentional so
allocation differences reflect steady-state retention, not per-invocation setup.

## Result

For the recorded pre-expansion catalog, Lokad.Parquet reached the declared
performance-parity objective in two
independent Windows x64 sessions and two independent Linux x64 sessions. Every
one-sided 95% upper bound for `Lokad / Parquet.NET` is at most 1.05. The fixed
catalog comprises warm metadata open and pre-opened scans of required,
nullable, Snappy, two-column, and eight-column INT32 data.

<!-- BEGIN GENERATED PARITY REPORT -->
Generated from four ignored paired snapshots and one ignored work-census snapshot.

- Source fingerprint: `working-tree-fa767aa0a64822989817902d99679f04aa16292e74edeeae7dea4956167d6a2e`
- Parquet.NET lock fingerprint: `330ec880242d9f567e45b8602d57eb24e71f0c9c3171b67ff08294eb5f008dfb`
- Windows: .NET 10.0.11, Intel64 Family 6 Model 183 Stepping 1, GenuineIntel, affinity 0x1, two distinct high-priority processes
- Linux: .NET 10.0.10, unrecorded, WSL2 native ext4, affinity 0x1, two distinct processes
- Protocol: 400 balanced randomized AB/BA observations per workload; every observation retained; one-sided 95% Student-t bound over paired log ratios

| Workload | Windows 1 | Windows 2 | Linux 1 | Linux 2 | Gate |
|---|---:|---:|---:|---:|---:|
| Required INT32, PLAIN | 1.001 / 1.009 | 0.996 / 1.003 | 0.370 / 0.373 | 0.336 / 0.340 | pass |
| Nullable INT32, PLAIN | 0.798 / 0.806 | 0.787 / 0.792 | 0.509 / 0.513 | 0.503 / 0.507 | pass |
| Required INT32, Snappy | 0.259 / 0.263 | 0.262 / 0.265 | 0.136 / 0.138 | 0.171 / 0.172 | pass |
| Two required INT32, PLAIN | 1.003 / 1.009 | 1.017 / 1.024 | 0.371 / 0.375 | 0.357 / 0.362 | pass |
| Eight required INT32, PLAIN | 1.000 / 1.005 | 1.004 / 1.019 | 0.366 / 0.371 | 0.359 / 0.363 | pass |
| Warm metadata open | 0.926 / 0.933 | 0.945 / 0.956 | 0.904 / 0.911 | 0.898 / 0.906 | pass |

Each result is point estimate / upper 95% bound for Lokad / Parquet.NET; lower is better and the declared gate is an upper bound no greater than 1.05.

| Workload | Reads / bytes | Pool rents | Peak / output | Bytes cleared | End-scan retained | Retained |
|---|---:|---:|---:|---:|---:|---:|
| Required INT32, PLAIN | 2 / 262400 | 3 | 524288 B / 262144 B (2.000x) | 524544 | unrecorded | 0 |
| Nullable INT32, PLAIN | 2 / 262404 | 4 | 794624 B / 262144 B (3.031x) | 794880 | unrecorded | 0 |
| Required INT32, Snappy | 2 / 262415 | 4 | 1048576 B / 262144 B (4.000x) | 1048832 | unrecorded | 0 |
| Two required INT32, PLAIN | 4 / 524800 | 4 | 786432 B / 524288 B (1.500x) | 786688 | unrecorded | 0 |
| Eight required INT32, PLAIN | 16 / 2099200 | 10 | 2359296 B / 2097152 B (1.125x) | 2359552 | unrecorded | 0 |
<!-- END GENERATED PARITY REPORT -->

## Warm metadata open

Warm metadata open is an accepted parity limitation for small footers: the
paired lane reports about 1.21 / 1.23 (point / upper 95%) at revision
d854233 (B06) and 1.26 / 1.29 on the pristine 0.1.0 control under matching process,
runtime, CPU, and warmup conditions, so the deficit predates the review fixes
and current code does not regress it. The gap is about 0.35 microseconds per
open at this size. (An older recorded table passed the gate; it comes from a
different protocol revision and machine state and is not relabeled.)

Stage attribution on the required-INT32 fixture (203-byte footer) puts the cost
in footer parsing: byte-range reads about 0.6 microseconds, Thrift decode with
validation and immutable metadata construction about 3.2 microseconds, file
teardown about 0.3 microseconds (in-process medians; the `MetadataOpenBenchmarks`
stage benchmarks keep each stage reproducible). These medians locate cost but do
not establish the gap: both readers validate, and allocation-reducing variants
have left the paired WarmMetadataOpen endpoint unchanged or regressed it, so
neither a validation-overhead nor a scaling story is established. All validation
is preserved; closing the
remaining gap would mean micro-optimizing validated parsing, which stays open
as future work rather than a silent relaxation. The B08 campaign re-measures
this lane from fresh sessions on both platforms.

## Published throughput and allocation cross-check

BenchmarkDotNet uses one launch, five warmups, 15 measured iterations, and a
250 ms minimum iteration time. The paired measurements above are the
authoritative statistical gate; this separate Windows run is the published
throughput and managed-allocation cross-check.

| Workload | Lokad mean | Parquet.NET mean | Lokad / Parquet.NET | Lokad allocation | Parquet.NET allocation |
|---|---:|---:|---:|---:|---:|
| Required INT32, PLAIN | 85.55 us | 75.48 us | 1.13 | 1.52 KiB | 2.75 KiB |
| Nullable INT32, PLAIN | 223.28 us | 314.32 us | 0.71 | 1.60 KiB | 2.84 KiB |
| Required INT32, Snappy | 91.95 us | 328.64 us | 0.28 | 1.59 KiB | 1,164.52 KiB |
| Two required INT32, PLAIN | 191.13 us | 169.27 us | 1.13 | 3.25 KiB | 5.49 KiB |
| Eight required INT32, PLAIN | 631.24 us | 777.81 us | 0.81 | 11.17 KiB | 22.07 KiB |

All Lokad lanes report zero measured Gen0, Gen1, and Gen2 collections. Managed
allocation is 0.022–0.025 B per decoded fixed-width cell, below the declared
0.10 B/cell ceiling. The work census records peak pooled capacity separately
because rented arrays are not managed allocations in BenchmarkDotNet's table.
The work census runs two truth-checked passes per workload: the full projection
at the full-row target, then the second half of the columns at a 4,096-row
target, sharing one pool-balanced case. The first pass is labeled
cold-instrumented and later passes warm-instrumented; instrumented time covers
consumer, checksum and sink work, so differently shaped passes are never a
comparative timing sample. Its table reports end-of-scan retained
bytes (file-cache storage still held after the scans) separately from the
zero-after-disposal check, and consumer UTF-8 bytes are measured across both
passes. Each pass records its own peak pooled bytes against its own decoded layout,
counting file-cache storage carried into the pass, so each pass carries its own budget
envelope. Live owned storage (session held open), warmed pool retention and
post-disposal growth are recorded as separate snapshot fields. Two recorded budget failures are resolved on the current
revision, not hidden. The short required-INT32 row range once peaked at
278,528 pooled bytes for 16,384 emitted bytes (17.0x) because page loading
materialized whole payloads; bounded buffering for required uncompressed
pages cut that lane to 32,768 bytes (2.0x), pinned by range-peak ceilings for
1-row, short-range, and full-range selections. The nullable-boolean lane once
peaked at 58,368 pooled bytes against a corrected 9,216-byte layout (6.33x,
previously hidden behind an INT32-sized denominator); bitmap definition
levels cut it to 9,472 bytes (1.03x) with headroom under the enforced 6x
gate. Compressed, CRC-bearing, and dictionary pages can still require
full-page work, so short-range peaks on those paths are judged against their
page sizes, not against the required-uncompressed selection ratio.

The parity objective and the Core budgets are separate gates evaluated from
different evidence. Parity means an upper one-sided 95% ratio no greater than
1.05 on every declared paired lane. The Core budgets are: managed allocation
below 0.10 fixed-width bytes per cell with zero measured GC collections after
stabilization; peak pooled bytes within 6x the decoded layout per census case
and pass; and no Core workload above 3x pinned-baseline CPU on the paired
ratios. Every recorded scan ratio sits far below that CPU ceiling; the
campaign review checks it from the same paired tables rather than rerunning
until a lane happens to pass, and any lane that fails any gate is retained in
the evidence with its failure visible. An uneven two-column multi-row-group case covers uneven batch partitioning
with a fully known oracle. Source reads are measured on the stream path with exactly
one read in flight; exact-MemoryStream and direct-memory borrows bypass reads and sit
outside these figures. Each case also records warmed Lokad and competitor retention
measured with the same yardstick. The retired composite-copy gate assumed zero copies; copies remain
legitimate on slicing and binary paths, which the UTF-8 and multi-batch lanes
exercise under truth and pool-balance checks. Cross-column page misalignment cannot come from the single-page baseline writer;
the partitioning tests cover it against synthetic uneven fixtures, and a real
multi-page census lane splits two columns at different rows within one row group.
Named diagnostic lanes extend the census beyond the frozen parity catalog without
joining the parity claim: wider required primitives (INT64/FLOAT/DOUBLE), nullable
non-INT32 and variable-width binary lanes, hand-built Data Page V2 layouts the
baseline writer cannot produce, high-cardinality and dense-null dictionary shapes,
and committed producer fixtures carrying page CRCs. Committed-fixture truth comes from
pinned independent column hashes plus Lokad/baseline agreement before timing. The pinned
baseline never reads or validates page CRCs, while Lokad validates every present CRC
before trusting payload bytes, so the CRC lanes measure validation-inclusive decode on
one side against unchecked decode on the other. The census table above
regenerates with fresh snapshots on the next qualification.

## What changed

The measured gains came from removing the second multi-column value copy,
transferring typed page storage directly into public batches, specializing the
maximum-definition-level-one validity path, scattering nullable PLAIN INT32
directly into final storage, and eliminating avoidable asynchronous and
allocation overhead in pre-opened memory scans and warm metadata open. The
single-scan design also removed redundant stream serialization and multi-scan
bookkeeping. File-owned byte and typed-array caches reuse bounded pooled
storage between sequential scans; it remains charged to the file budget and
is cleared before return.

The implementation remains safe managed C#: no `unsafe`, pointers, pinning,
`Unsafe`, `MemoryMarshal`, native code, or runtime package dependency was
introduced. Scalar decoding remains the semantic oracle for the portable
vector path.

## Interpretation

- Both readers consume the same generated Parquet bytes and must produce the
  same truth-checked row, null, order, and checksum results before timing.
- Numeric truth is canonical per column: each consumer accumulates one Mix chain
  per column in row order over non-null values, independent of batch
  partitioning, plus a separate chain over null row ordinals, and folds the two
  in a fixed order, so a null can never equal a value. Folded columns combine
  in scan order; a single column stands alone. Swapped, duplicated, omitted, or
  reordered columns therefore fail the truth comparison. `--verify-truth` runs
  these scheme and consumer checks against the actual Core, Parity, and Census
  consumers before any timing run.
- The authoritative scan operations start from pre-opened readers and consume
  public batches. Materialization and decoder kernels are diagnostic only.
- UTF-8 qualification uses the application-pipeline contract above. The
  generated tables still describe the earlier fixed-width snapshot until the
  expanded catalog is requalified on both operating systems.
- The catalog does not cover nested values, writing, additional codecs or
  encodings, object serialization, or broad Parquet compatibility.
- Windows and Linux results remain separate; no cross-machine average is used.
- Every qualifying process is restricted to one logical processor. Linux
  sessions run from a WSL-native ext4 workspace, never a Windows-backed mount,
  so host-filesystem mediation cannot distort the result. On Linux the benchmark
  entry point resolves links to a final path and rejects Windows-backed mounts
  wherever they appear (mount-table verdict, longest-prefix match); a native mount
  is accepted even under `/mnt`. The CPU model is collected from `/proc/cpuinfo`,
  and `bench.ps1` records the CPU scaling governor instead of a Windows power scheme.

## Scope difference

Lokad.Parquet is a dependency-free, `net10.0`, read-only projected batch
scanner for the deliberately narrow Core 0.1 profile. Parquet.NET is a mature,
general-purpose read/write library with broader schemas, encodings, codecs,
logical conversions, and framework support. Choose based on required format
coverage and API shape; the table above supports only the declared overlapping
operations.

## Reproduction

The benchmark package lock pins Parquet.NET 6.1.0. From PowerShell:

```powershell
.\test.ps1 -Configuration Release
.\bench.ps1 -Suite Utf8
.\bench.ps1 -Suite Parity
.\bench.ps1 -Suite Paired -EnforceParity
.\bench.ps1 -Suite Census
```

Run two paired processes on each qualifying OS. For Linux, copy the source to a
native Linux filesystem (for example, under `/home` in WSL), build, execute,
and write benchmark artifacts there; do not run from `/mnt/c` or another
Windows-backed mount. Raw snapshots are intentionally ignored. Reconcile four
chosen paired snapshots and one census into this file, or verify an existing
reconciliation, without measuring:

The reconciler takes the workload catalog exported by the benchmark binary, so
the catalog lives in exactly one place. It recomputes each point estimate and each
upper 95% bound from the retained raw observations and rejects any stored value
that differs bit-for-bit, re-derives the gate outcomes, and checks fixture and
dimension identity across sessions and the census; only the raw observations
themselves stay with the runner. Every session must report Windows or Linux.

```powershell
.\bench.ps1 -Suite Catalog
.\benchmark-report.ps1 -PairedSnapshot <four-paths> `
    -CensusSnapshot <census-path> -Catalog <catalog-path>
.\benchmark-report.ps1 -PairedSnapshot <four-paths> `
    -CensusSnapshot <census-path> -Catalog <catalog-path> -Verify
```

