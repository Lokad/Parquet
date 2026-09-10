# Lokad.Parquet Specification

Status: Draft for the 0.1 preview

## 1. Purpose

`Lokad.Parquet` is a high-performance, read-only Apache Parquet library for
.NET 10. It is intended for trusted and untrusted Parquet inputs where column
projection, bounded memory, low garbage-collector pressure, predictable
failure behavior, and exact data semantics matter.

The library is deliberately narrower than the full Parquet ecosystem. Its
primary job is to expose selected columns from large, flat Parquet tables as
typed, column-oriented batches. It is not a general object serializer, query
engine, dataframe, compatibility layer for every historical producer, or
writer.

The words MUST, MUST NOT, SHOULD, SHOULD NOT, and MAY in this document describe
requirements on the implementation. A support table marked **Core 0.1** is a
requirement for the first public preview. **Expansion** identifies a candidate
for a later preview or stable release and is not a compatibility promise.

## 2. Design principles

### 2.1 Security before compatibility

- Every input byte is untrusted.
- Malformed files MUST fail deterministically with work and memory proportional
  to validated, configured bounds.
- No performance fast path may perform fewer structural, bounds, or semantic
  checks than its scalar counterpart.
- Unsupported features MUST produce an explicit unsupported-feature error.
  They MUST NOT be approximated, ignored when doing so changes values, or
  decoded as a similar-looking feature.
- Corruption MUST NOT be reported as an unsupported feature, and unsupported
  features MUST NOT be reported as corruption.

### 2.2 Memory-safe managed implementation and dependencies

- The shipped package MUST depend only on the .NET 10 standard libraries.
- The shipped package MUST NOT use native libraries, P/Invoke, COM, external
  processes, runtime-downloaded codecs, or direct unmanaged heap allocation.
- Every shipped source file MUST compile with `AllowUnsafeBlocks=false`.
- C# `unsafe`, pointer types, `fixed`, pinning, `GCHandle`,
  `System.Runtime.CompilerServices.Unsafe`, `MemoryMarshal`,
  `CollectionsMarshal`, `Marshal`, and `NativeMemory` MUST NOT be used by the
  shipped library.
- `Span<T>`, `Memory<T>`, `BinaryPrimitives`, `BitOperations`, portable
  `Vector<T>`, and hardware-vector arithmetic MAY be used only through APIs
  that preserve managed type and bounds safety.
- Safe `stackalloc` into a `Span<T>` MAY be used only for small, explicitly
  bounded buffers whose size is independent of an unchecked input value.
- Test and benchmark dependencies MUST remain confined to their respective
  projects.

This document uses “safe” to mean memory-safe managed C# without unverifiable
memory access or APIs that bypass runtime type and bounds checks. Performance
must come from bounded algorithms, pooling, spans, and safe vector APIs rather
than pointer access.

### 2.3 Columnar and batch-oriented

- Column projection MUST happen before column-chunk payloads are fetched.
- Primitive values MUST be decoded in batches, not materialized as per-row
  objects.
- UTF-8 and arbitrary binary values SHOULD remain bytes until a caller
  explicitly requests decoding.
- Primitive scans MUST NOT allocate one managed object per row or per value.
- Memory use MUST be proportional to selected pages, selected dictionaries,
  and the configured output batch, not to total file or row-group size.

### 2.4 Lean public surface

- Public types MUST describe file access, immutable metadata, projections,
  scans, batches, options, and errors.
- Thrift-generated or wire-format implementation types MUST remain internal.
- Encoding, compression, pooling, SIMD, and page-planning strategies MUST
  remain implementation details.
- Convenience APIs that require reflection, dynamic code generation,
  attribute-driven mapping, or row-object materialization are out of scope.
- New public abstractions require a demonstrated use that cannot be expressed
  efficiently through the existing column-batch model.

### 2.5 Exact semantics

- A supported physical value MUST be exposed without loss.
- Logical conversions MUST either be exact or fail explicitly.
- Timestamps, decimals, unsigned integers, and binary strings MUST NOT be
  silently narrowed, rounded, reinterpreted, or decoded with replacement
  characters.
- Nulls MUST remain distinguishable from default values.
- Raw physical access and logical annotation metadata MUST remain available
  independently of convenience conversions.

## 3. Normative format basis

The metadata vocabulary is pinned to Apache `parquet-format`
`parquet.thrift` revision
`e94a5d090b324a0c0ee1adbb8ea6b099852dc3cc`, serialized with Thrift Compact
Protocol. The revision pin makes tests and enum interpretation reproducible; it
does not imply support for every feature present in that schema. This document
is the authority on the supported subset.

A supported ordinary file has:

1. the four-byte `PAR1` magic at offset zero;
2. zero or more column chunks organized into zero or more row groups;
3. a Thrift Compact Protocol file footer;
4. a four-byte little-endian footer length; and
5. the four-byte `PAR1` magic at the end.

A schema-only file with zero row groups is valid and MUST open and scan as an
empty table.

The reader MUST locate and validate the footer before planning any column data
reads. Encrypted-footer magic and modular encryption are unsupported.

### 3.1 Forward compatibility

- Unknown Thrift fields MUST be skipped within configured limits.
- Unknown enum values and format features MUST remain distinguishable from
  malformed encodings of known values.
- For an unknown logical-type union member, metadata MUST retain its field
  discriminator and unsupported status while safely skipping its content. It
  MUST NOT alter raw physical decoding.
- A feature added after the pinned schema revision is unsupported until its
  interpretation, limits, tests, and support-table entry are added here.

## 4. Release profiles

### 4.1 Core 0.1 preview

The first preview MUST support:

- ordinary `PAR1` files with in-file metadata;
- zero or more row groups;
- projection of flat, top-level primitive leaves;
- `REQUIRED` and `OPTIONAL` leaves;
- projection by leaf ordinal and unambiguous top-level name;
- Data Page V1 and Data Page V2;
- PLAIN values, definition levels, dictionary pages, and mixed
  dictionary/plain column chunks;
- uncompressed and Snappy-compressed pages;
- caller-selected row groups and a global contiguous row range;
- aligned, column-oriented disposable batches;
- asynchronous bounded random-access I/O with synchronous CPU decoding;
- page CRC validation when a CRC is present; and
- immutable schema, row-group, column-chunk, column-order, raw-statistics, and
  custom-metadata views.

The core profile is intentionally sufficient to validate the public API,
ownership model, projected scan architecture, and common flat-file path without
making broader compatibility a prerequisite.

### 4.2 Compatibility expansion

After Core 0.1, support MAY expand based on real interoperable files,
independent fixtures, security analysis, and benchmark evidence. Candidate
expansions include:

- INT96 physical values and explicitly named legacy timestamp conventions;
- delta encodings and byte-stream split;
- GZIP, Brotli, Zstandard, and LZ4_RAW;
- exact convenience views for temporal, decimal, UUID, and FLOAT16 logical
  types;
- typed statistics; and
- selected page-index uses that preserve correctness without statistics.

An expansion becomes supported only when its table entry changes in this
document. Presence in this list is not a schedule or compatibility guarantee.

### 4.3 Explicitly outside the initial contract

- Writing, appending, updating, or repairing Parquet files.
- Nested groups, structs, lists, maps, repeated primitive columns, or any
  column with a maximum repetition level greater than zero.
- Object serialization or deserialization.
- Schema inference across multiple files or schema merging.
- Predicate expression evaluation.
- Bloom-filter probing.
- Page-index-based predicate pruning.
- Parquet modular encryption.
- External footer or column metadata files.
- Variant shredding, geospatial types, and embedded file values.
- Recovery of partially written files or corrupt pages.
- Returning a partial batch after a structural or data error.

Opening metadata MAY succeed for a file containing unsupported columns so that
callers can inspect its schema. Attempting to project an unsupported column
MUST fail before reading its column-chunk payload.

## 5. Schema and type support

### 5.1 Repetition and nesting

The first supported schema profile consists of primitive leaves directly under
the root message:

- `REQUIRED` leaves have maximum definition level zero.
- `OPTIONAL` leaves have maximum definition level one.
- Maximum repetition level MUST be zero.

The metadata model MUST preserve the complete schema tree even when some nodes
are unsupported. Every leaf descriptor MUST report whether it is readable by
the current implementation and, if not, the unsupported reason.

The root repetition field may be absent as specified or carry the legacy
`REQUIRED` marker used by some producers. That marker is retained as metadata
but does not contribute a definition or repetition level. Other root
repetition values are malformed.

### 5.2 Physical types

| Parquet physical type | Lossless physical view | Status |
|---|---|---|
| `BOOLEAN` | `bool` | Core 0.1 |
| `INT32` | `int` | Core 0.1 |
| `INT64` | `long` | Core 0.1 |
| `FLOAT` | `float` preserving IEEE bits | Core 0.1 |
| `DOUBLE` | `double` preserving IEEE bits | Core 0.1 |
| `BYTE_ARRAY` | 32-bit offsets plus one contiguous byte payload | Core 0.1 |
| `FIXED_LEN_BYTE_ARRAY` | fixed-width slices over one contiguous byte payload | Core 0.1 |
| `INT96` | exact 12-byte value | Expansion |

Core physical types MUST be readable without applying a logical annotation.
An INT96 column MUST be classified as unsupported by Core 0.1 before reading
its payload. A future INT96 implementation MUST expose the raw 12-byte value
independently of any producer-specific timestamp conversion.

### 5.3 Logical annotations

Modern `LogicalType`, legacy `ConvertedType`, schema-element `scale` and
`precision`, and the discriminator of an unknown logical-type union member MUST
be exposed independently in immutable metadata whether or not a convenience
view exists. Neither annotation representation may erase the other, and a
known legacy annotation needs no duplicate modern annotation. When both are
present, a logical interpretation is supported only when they are semantically
consistent; otherwise the descriptor MUST report the conflict while
structurally valid physical decoding remains available.

For an annotation supported by Core 0.1, immutable metadata MUST expose a
normalized semantic annotation only when the annotation is valid for the
physical storage type. An invalid physical pairing MUST be reported without
blocking physical decoding. Unsupported and unknown future annotations remain
available through their raw metadata and do not acquire a guessed semantic
interpretation.

Core 0.1 returns physical values. The normalized semantic annotation describes
those values but does not convert them or promise a separate public
logical-conversion layer. In particular:

- `STRING` and `ENUM` remain byte-array values with their annotation;
- integer annotations retain signedness and declared bit width in metadata;
- `DATE`, `TIME`, and `TIMESTAMP` retain their unit and
  adjusted-to-UTC metadata;
- `DECIMAL` retains precision, scale, and its exact physical bytes or
  integer;
- `UUID` and `FLOAT16` retain their annotations; and
- `JSON` and `BSON` remain annotated binary and are not parsed or
  semantically validated.

Any later convenience conversion MUST follow these rules:

- UTF-8 conversion MUST reject malformed UTF-8. Replacement fallback is not
  permitted.
- Decimal conversion to `decimal` MUST reject values whose precision or scale
  cannot be represented exactly. A raw or wider exact representation MUST
  remain available.
- Nanosecond time or timestamp values MUST NOT be rounded to .NET ticks. A
  tick-based conversion may succeed only when exact.
- Instant timestamps and local timestamps MUST remain distinct.
- Unsigned values MUST NOT flow through signed values when that changes the
  represented range.
- UUID byte order MUST be specified and tested against the format definition.

Nested logical types, `VARIANT`, `GEOMETRY`, `GEOGRAPHY`, and `FILE`
are unsupported by the initial scanner.

### 5.4 Names and paths

- Column identity is its schema path and leaf ordinal.
- Ordinal projection MUST always be available.
- Name projection MUST use ordinal, case-sensitive comparison.
- A duplicate top-level name MUST make name lookup ambiguous; it MUST NOT pick
  one silently.
- Names and key/value metadata MUST be strictly decoded from their Thrift
  strings and bounded by the configured metadata limits.

## 6. Page, encoding, and codec support

### 6.1 Page types

| Page type | Status |
|---|---|
| Dictionary page | Core 0.1 |
| Data Page V1 | Core 0.1 |
| Data Page V2 | Core 0.1 |
| Index page | Recognized, range-validated, not read in Core 0.1 |
| Unknown future page type | Unsupported |

A column chunk may contain at most one dictionary page, and it must precede
data pages that reference it. Dictionary indices MUST be checked against the
decoded dictionary count. When no dictionary-page offset is advertised but the
leading page exactly at the advertised data offset is a dictionary page (as
emitted by some producers), the reader admits that exact shape and requires
data pages at the computed page end; every other offset mismatch is malformed.

### 6.2 Encodings

| Encoding | Supported use | Status |
|---|---|---|
| `PLAIN` | All Core 0.1 physical types | Core 0.1 |
| `PLAIN_DICTIONARY` | Legacy dictionary marker | Core 0.1 |
| `RLE` / bit-packing hybrid | Definition levels and dictionary indices (boolean values are `PLAIN` only in Core 0.1) | Core 0.1 |
| `RLE_DICTIONARY` | Dictionary indices | Core 0.1 |
| `DELTA_BINARY_PACKED` | `INT32`, `INT64` | Expansion |
| `DELTA_LENGTH_BYTE_ARRAY` | `BYTE_ARRAY` | Expansion |
| `DELTA_BYTE_ARRAY` | Byte-array physical types | Expansion |
| `BYTE_STREAM_SPLIT` | Legal fixed-width physical types | Expansion |
| Deprecated `BIT_PACKED` | Historical levels | Expansion only with evidence |
| `ALP` | None | Unsupported |

In a data-page value-encoding field, `PLAIN_DICTIONARY` is the legacy synonym
of `RLE_DICTIONARY`. In `DictionaryPageHeader.encoding`, both `PLAIN` and the
legacy `PLAIN_DICTIONARY` marker mean PLAIN dictionary values.

`ColumnMetaData.encodings` is an untrusted capability hint, not decoder
selection or a completeness guarantee. Actual page headers and the
format-defined level encoding are authoritative. An omitted advertised level
encoding MUST NOT reject a valid page; an unsupported encoding MUST be rejected
if and when a page actually selects it.

Every decoder MUST verify that it consumes a structurally valid payload and
produces exactly the expected number of physical values. It MUST reject:

- truncated varints, runs, mini-blocks, lengths, or values;
- zero or impossible block sizes;
- integer overflow while accumulating deltas or lengths;
- RLE runs that exceed the remaining logical value count; bit-packed runs that exceed it except for the final run with the minimal groups covering the remainder (at most seven padding values);
- out-of-range dictionary indices;
- binary lengths that exceed the page, batch, or configured value limit; and
- trailing state that contradicts the page header.

A final bit-packed run is padded to a multiple of eight values as defined by parquet-format; its padding values MUST be ignored. Any wider overlong run, any overlong intermediate run, and any final run that is not minimal or does not consume the payload exactly MUST be rejected.

Definition levels MUST produce exactly the page's logical row count. The
number of physical values MUST equal the number of defined values.

### 6.3 Compression codecs

| Codec | Implementation rule | Status |
|---|---|---|
| `UNCOMPRESSED` | Checked copy or view | Core 0.1 |
| `SNAPPY` | Managed bounded block decoder | Core 0.1 |
| `GZIP` | BCL; concatenated-member behavior must be explicit | Expansion |
| `BROTLI` | BCL | Expansion |
| `ZSTD` | Managed bounded decoder required | Expansion |
| `LZ4_RAW` | Managed bounded block decoder required | Expansion |
| `LZO` | None | Unsupported |
| Deprecated Hadoop `LZ4` | None | Unsupported |

No codec dependency may be added to the shipped project. A codec decoder MUST
receive an exact expected output length, MUST stay within that destination,
and MUST fail if the decoded length differs. Compressed and uncompressed page
sizes MUST be validated before renting or allocating a buffer.

Corpus frequency may change the order of Expansion work, but it does not
silently change this public support table.

## 7. Input and ownership model

### 7.1 Random-access source

The core I/O abstraction is a bounded random-access source with this semantic
contract:

- an immutable non-negative `Length`;
- immutable file content for the lifetime of an open `ParquetFile`;
- `ValueTask ReadExactlyAsync(long offset, ArraySegment<byte> destination,
  CancellationToken)` semantics;
- rejection of negative offsets and reads past `Length`;
- exact completion or `EndOfStreamException`; and
- safe sequential calls from one open reader.

The source need not support concurrent calls. A source that changes length or
content while open violates the contract; the reader MUST remain memory-safe
but cannot promise snapshot consistency for such a source.

The public API SHOULD provide:

- a source interface for range-capable storage adapters;
- a file-path helper optimized with `RandomAccess` where available; and
- a `ReadOnlyMemory<byte>` helper that retains and borrows immutable managed
  content without a whole-input copy; and
- a readable, seekable `Stream` adapter.

The caller MUST keep the backing owner of memory input alive and MUST NOT
modify its content until the resulting `ParquetFile` is disposed. The reader
retains the `ReadOnlyMemory<byte>` value but does not assume ownership of any
separately disposable backing owner.

Caller-supplied sources and streams are left open by default. The
`ParquetSourceOwnership` argument explicitly assigns disposal to the caller or
to `ParquetFile`; Boolean ownership flags are not part of the public contract.
A helper that opens a path owns its file handle. `ParquetFile.DisposeAsync`
MUST finish or cancel its internal reads before disposing an owned source.

### 7.2 I/O planning

- Footer discovery MUST use a small tail read before renting the full footer
  buffer.
- Only selected column chunks may be read.
- Adjacent small ranges MAY be coalesced within a bounded configured distance.
- One scan MUST have at most one source read in flight.
- Cancellation MUST be observed between I/O operations, pages, and bounded
  decode blocks.
- The implementation MUST NOT issue one I/O operation per value or per small
  metadata field.
- A row range does not imply page-index pruning. The reader MAY decode and
  discard preceding values inside a boundary row group when no trusted offset
  index is used.

## 8. Public API semantics

The reviewed API snapshot defines the exact public names for Core 0.1.
The behavior in this section is normative for any chosen names.

### 8.1 File and metadata

`ParquetFile` is the entry point. Opening reads and validates the footer and
exposes immutable:

- input length and `ParquetFileMetadata`, including declared file row count;
- `ParquetSchema` and `ParquetColumn` descriptors, including leaf count;
- row-group descriptors, including row counts and declared size fields;
- column-chunk locations, codecs, advertised encodings, counts, and declared
  compressed and uncompressed sizes;
- raw column-order and statistics fields with their presence flags; and
- custom key/value metadata.

Metadata types MUST NOT expose mutable collections or internal Thrift objects.
Core 0.1 MUST NOT present typed statistics as trusted decoded values. Raw
minimum/maximum byte representations, null count, distinct count, and metadata
flags MAY be exposed after structural validation.

These descriptors MUST suffice for a page-free structural summary of file
length, rows, columns, row groups, and per-column chunk counts and declared
sizes. Value-derived profiles such as examples, constancy, histograms,
averages, exact truth counts, or text-shape metrics require a projected scan;
Core 0.1 does not add analyzer-specific public types for them.

### 8.2 Scan selection

The primary data API is an asynchronous projected scan:

```csharp
await using var file = await ParquetFile.OpenAsync(
    source,
    ParquetSourceOwnership.Caller,
    options,
    cancel);

var columns = new[]
{
    file.Metadata.Schema.GetColumn("quantity"),
    file.Metadata.Schema.Columns[3],
};
var scan = new ParquetScanOptions(columns);

await foreach (var batch in file.ScanAsync(scan, cancel))
{
    using (batch)
    {
        // Consume selected, row-aligned column buffers.
    }
}
```

The scan contract is:

- At least one column MUST be projected.
- A scan is constructed from an ordered, eagerly snapshotted list of column
  descriptors. A leaf ordinal resolves through `ParquetSchema.Columns`; an
  exact top-level name resolves through `ParquetSchema.GetColumn` and fails
  when absent or ambiguous. Duplicate descriptors are rejected.
- Batch columns appear in caller projection order.
- Omitted row-group selection means all row groups. An explicit empty
  selection produces no batches.
- Explicit row-group descriptors form a set, must belong to the open file, and
  are scanned in source order. Duplicates are rejected.
- The optional row range is a zero-based, half-open global file-row interval.
  It is intersected with the selected row groups.
- Range start and count use checked 64-bit arithmetic. An interval outside the
  file is rejected rather than silently clamped.
- A zero-length interval produces no batches.
- A target batch row count is a preference bounded by the configured maximum,
  not a guarantee.

The scanner MUST preserve source row order. All columns in one batch MUST
describe the same global row interval and row count. A batch MUST NOT cross a
row-group boundary. In Core 0.1 a batch holds rows from at most one page per column; page boundaries, row-group ends, the target row count, and byte or memory limits MAY force a shorter batch. Page-boundary coalescing is Expansion.
No empty batch may be yielded.

Dictionary-encoded input is expanded into the ordinary physical batch view in
Core 0.1. A future dictionary-preserving view must be additive and explicit.

### 8.3 Batch representation

A batch owns rented memory, MUST be disposable, and MUST make `Dispose`
idempotent. Its views become invalid immediately after disposal.

An enumerator permits at most one undisposed yielded batch. Calling
`MoveNextAsync` before disposing the preceding batch MUST fail without
renting another output batch. This rule bounds caller-visible output memory per
scan.

Every batch reports:

- a positive `RowCount`;
- its zero-based global `RowOffset`;
- its row-group ordinal and row offset within that row group; and
- one column view per projection, in projection order.

Fixed-width primitive columns expose:

- a contiguous `ReadOnlyMemory<T>` whose length equals `RowCount`; and
- a validity bitmap for optional values.

For a fixed-width null, the corresponding value slot is unspecified and MUST
not be read without checking validity. Required columns expose an implicit
all-valid state and MUST NOT allocate a validity bitmap.

`BYTE_ARRAY` columns expose:

- one contiguous byte payload;
- `ReadOnlyMemory<int>` offsets of length `RowCount + 1`;
- `offsets[0] == 0`, monotonically non-decreasing offsets, and
  `offsets[RowCount] == payload.Length`; and
- the same validity representation.

A null binary value has equal adjacent offsets. An empty non-null value may
also have equal offsets and is distinguished by validity.

`FIXED_LEN_BYTE_ARRAY` exposes one contiguous payload plus its positive type
width. Its payload length is exactly `checked(RowCount * TypeWidth)`.
Bytes belonging to null rows are unspecified.

Validity uses one bit per row, least-significant bit first within each byte.
Bit zero of byte zero describes row zero. Padding bits in the final byte MUST
be zero.

The public batch surface MUST avoid:

- one object per value;
- one array per binary value;
- one `string` per UTF-8 value;
- boxing primitive values; and
- retaining unbounded compressed or decoded page storage.

An open file MAY retain bounded reusable pooled storage between its sequential
scans. Retained storage remains charged to the file memory budget and MUST be
cleared when it is returned to the shared pool. Before reading payloads, a new
scan MAY release idle retained storage for columns outside its projection to
fit the shared budget.

### 8.4 Lifetime

- Disposing a scan enumerator stops further I/O and returns its internal
  buffers. A batch already yielded remains owned by the caller until disposed.
- Disposing `ParquetFile` prevents new scans and causes active enumerators to
  terminate deterministically after returning internal buffers.
- A cancellation request is reported as `OperationCanceledException`.
  Disposal invalidation is reported as `ObjectDisposedException`.
- Holding one batch may retain up to the configured batch and scan budgets;
  holding views after batch disposal is invalid.

### 8.5 Options

`ParquetReaderOptions` MUST be immutable after opening and MUST contain
metadata, page, codec, and memory limits. `ParquetScanOptions` MUST be
immutable after scan creation and contain projection, row selection, target
batch size.

Defaults MUST be safe for untrusted data. Raising a configurable limit requires
an explicit caller action and cannot exceed the implementation ceiling in
Section 9.2.

### 8.6 Errors

The public taxonomy consists of:

- `ParquetFormatException` for malformed, inconsistent, truncated, or corrupt
  file data;
- `ParquetUnsupportedFeatureException` for well-formed but unsupported format
  features;
- `ParquetLimitExceededException` when otherwise processable input exceeds a
  configured safety limit;
- standard `ArgumentException` variants for invalid caller projections,
  ranges, options, or duplicate selections;
- `InvalidOperationException` for a typed accessor incompatible with its
  column descriptor, advancing with an undisposed batch, or an overlapping
  scan on one open file;
- standard `IOException`-family failures for source failures unrelated to
  file truncation;
- `OperationCanceledException` for cancellation; and
- `ObjectDisposedException` for invalidated owners.

`EndOfStreamException` while reading a range that was valid against the
advertised immutable length is translated to `ParquetFormatException`.

Parquet exceptions SHOULD carry structured byte offset, row-group ordinal,
column ordinal/path, and page ordinal when known. Messages MUST NOT include
decoded data values or arbitrarily large input fragments.

## 9. Security requirements

### 9.1 Threat model

The reader defends process memory safety, availability within configured
resource bounds, semantic integrity of decoded values, and deterministic
cleanup against malicious file bytes.

The reader does not provide:

- authenticity or confidentiality;
- protection from a caller that deliberately configures excessive limits,
  or creates excessive reader instances;
- snapshot semantics for a source that violates the immutable-content
  contract; or
- recovery of useful rows from malformed input.

CRC32 detects accidental corruption only. Applications requiring authenticity
must authenticate the containing object separately.

### 9.2 Configurable hard limits

Core 0.1 uses the following defaults and non-negotiable implementation
ceilings. “MiB” means 1,048,576 bytes. A caller may lower any configurable
limit. Raising one above its default is explicit; raising it above the ceiling
is rejected.

| Limit | Default | Ceiling |
|---|---:|---:|
| Footer bytes | 64 MiB | 1 GiB |
| Thrift nesting depth | 64 | 256 |
| Elements in one Thrift container | 1,048,576 | 16,777,216 |
| Schema elements | 16,384 | 1,048,576 |
| Primitive leaf columns | 4,096 | 65,536 |
| Row groups | 65,536 | 1,048,576 |
| Key/value metadata entries | 16,384 | 1,048,576 |
| Aggregate decoded metadata string bytes | 16 MiB | 256 MiB |
| One page header | 1 MiB | 16 MiB |
| One compressed page payload | 64 MiB | 1 GiB |
| One uncompressed page payload | 256 MiB | 1 GiB |
| Pages in one column chunk | 1,048,576 | 16,777,216 |
| Dictionary entries | 16,777,216 | 134,217,728 |
| Dictionary page and decoded bytes | 256 MiB | 1 GiB |
| Logical values in one page | 16,777,216 | 134,217,728 |
| One binary value | 64 MiB | 1 GiB |
| Rows in one output batch | 1,048,576 | 16,777,216 |
| Binary payload in one output batch | 256 MiB | 1 GiB |
| Total pooled bytes owned by one open file | 512 MiB | 8 GiB |

The default target batch size is 65,536 rows and is independently capped by
the rows-per-batch and memory limits. Output buffers, compressed buffers,
decompressed buffers, dictionaries, validity maps, and in-flight read buffers
all count toward the scan budget. Immutable footer metadata is governed by the
metadata-specific limits and does not count toward a scan.

An implementation MAY introduce a lower runtime ceiling when a requested
single managed buffer cannot be represented on the current runtime. It MUST
fail with `ParquetLimitExceededException`, not overflow or partially decode.

No allocation or pool rent may be based on an on-disk length before the
corresponding limit and enclosing range have been checked.

### 9.3 Checked arithmetic and ranges

- All on-disk signed lengths and counts MUST be checked for negativity.
- Addition, multiplication, narrowing, and offset calculation MUST use checked
  arithmetic or equivalent preconditions.
- Every footer, column chunk, index, bloom filter, page header, compressed page,
  and referenced range MUST lie fully inside the immutable input length.
- Cumulative page counts and sizes MUST not exceed their enclosing column
  chunk.
- Row-group `total_byte_size` and optional `total_compressed_size` are advisory
  metadata. They MUST NOT define an enclosing range, drive an allocation, or
  cause rejection merely because they differ from child-column aggregates.
- A row group's supported projected column chunks MUST agree with the row-group
  row count.
- File-level row-count sums and global range calculations MUST use checked
  64-bit arithmetic.

### 9.4 Thrift Compact Protocol

The internal Thrift reader MUST:

- parse directly from bounded spans or sequences;
- reject invalid field types and illegal field-id deltas;
- bound recursion, list/map/set sizes, strings, and skipped unknown fields;
- reject unterminated and overflowing varints;
- support safely skipping unknown fields needed for forward compatibility; and
- never instantiate a general-purpose object graph for the footer.

Only metadata needed for validation, planning, supported semantics, and the
public metadata view should be retained.

### 9.5 Decompression and decode bombs

- Declared uncompressed size is a hard upper bound, not a hint.
- A decoder MUST write into a pre-bounded destination.
- Excess output, insufficient output, invalid back-references, or impossible
  distances MUST fail.
- Highly compressed but valid pages remain bounded by the uncompressed-page
  and scan-memory limits.
- Dictionary size, binary lengths, value counts, page counts, and level runs
  MUST be bounded independently of compressed size.
- Decoder loops MUST observe cancellation at bounded intervals when processing
  large permitted buffers.

### 9.6 Checksums, indexes, and statistics

- When a page CRC is present, it MUST be validated against the serialized page
  payload before its values are trusted.
- Statistics, column indexes, and offset indexes are untrusted hints. They MUST
  be structurally validated before any use and MUST never override decoded
  truth.
- Declared column order MUST be retained with raw statistics; absent, unknown,
  or unsupported order MUST remain distinguishable and MUST NOT be guessed.
- Core 0.1 MUST NOT use statistics to skip data.
- Core 0.1 MUST remain correct when all optional indexes and statistics are
  absent.

### 9.7 Memory-safe low-level code

- The build and release checks MUST enforce Section 2.2 and reject forbidden
  constructs or API references in the shipped assembly.
- Bounds, lengths, overlap rules, and output capacity MUST be checked before a
  scalar or vectorized kernel runs.
- Vectorized kernels MUST use safe managed loads and stores; `LoadUnsafe`,
  `StoreUnsafe`, and equivalent bounds-bypassing APIs are prohibited.

### 9.8 Failure atomicity

- A batch is yielded only after every selected column for that batch has been
  decoded and validated successfully.
- On failure, every rented buffer MUST be returned exactly once.
- A failed scan cannot be resumed.
- Disposal and cancellation MUST be safe at every await boundary.
- Malformed input MUST never produce a partial batch or partially trusted
  metadata object.

## 10. Performance requirements

### 10.1 Complexity

- Footer parsing is linear in the bounded footer size.
- Page decoding is linear in compressed input plus produced values.
- Name lookup may be indexed once but MUST preserve duplicate-name detection.
- Dictionary lookup is constant-time per value after dictionary decode.
- No supported decoder may exhibit quadratic behavior on adversarial input.

### 10.2 Allocation and memory

- Footer metadata may allocate immutable descriptors, subject to limits.
- After warm-up and pool stabilization, a fixed-width scan MUST allocate
  managed objects in proportion to batches and selected columns, not rows or
  values.
- Binary scans MUST use bounded payload and offset buffers, not per-value
  arrays.
- Pooling MUST be bounded. A pool is not permission for unbounded retention.
- Measured open-file live buffer bytes MUST not exceed the configured memory
  budget. Small owner objects and immutable metadata are reported separately.
- Buffers containing managed references MUST be cleared before pool return when
  required for correctness or confidentiality.

### 10.3 SIMD and intrinsics

Scalar implementations are the semantic reference. Hardware-specific paths
MAY use `Vector128`, `Vector256`, `Vector512`, or portable `Vector<T>`
for:

- bit unpacking and level expansion;
- byte transposition;
- byte-stream-split reconstruction;
- boolean expansion;
- endian conversion;
- UTF-8 validation; and
- selected codec primitives.

Every optimized path MUST comply with Section 2.2 and match forced scalar
execution for valid, boundary, tail, and malformed inputs. Unsupported hardware
MUST use the scalar path with identical output and error classification.

An optimized path is retained only when a focused Release benchmark shows a
repeatable benefit on at least one representative supported workload and no
material regression on the others.

### 10.4 Sequential execution

- One open `ParquetFile` permits at most one active scan.
- Page decoding, projected columns, and source reads execute sequentially; the
  reader MUST NOT create parallel CPU work or overlap source reads.
- Asynchronous I/O is permitted, so execution MAY resume on another OS thread;
  the API does not promise thread affinity.
- Callers MAY schedule separate `ParquetFile` instances independently.

## 11. Benchmarking

Benchmarks run only in Release and may depend on Parquet.NET as a comparison
baseline. Such dependencies MUST remain private to the benchmark project and
MUST NOT become package dependencies.

Benchmark correctness MUST be checked before timing. Comparison lanes must
decode the same rows, columns, physical values, and nulls. End-to-end scans and
isolated decoder microbenchmarks MUST be reported separately.

Release evidence MUST report throughput, allocation and GC activity, peak scan
memory, relevant input dimensions, runtime, processor, instruction-set mode,
operating system, source revision, and fixture identity. Budgets are established
on pinned workloads and machines. A comparison library provides context; the
reader is not required to win every case.

Comparative qualification MUST bind the complete benchmark process, including
both readers, to one logical processor and record the applied affinity.
Linux qualification MUST build, execute, and write artifacts on a native Linux
filesystem; a Windows-backed WSL mount such as `/mnt/c` is not qualifying.

## 12. Testing and conformance

Conformance coverage MUST include:

- unit and hand-verifiable golden tests for bounded parsers and decoders;
- file-level and differential tests against an independent reader for the
  overlapping supported profile;
- malformed, mutation, fuzz, configured-limit, and complexity tests;
- allocation, memory-budget, sequential-I/O, overlapping-scan rejection,
  cancellation, short-read, ownership, and disposal tests; and
- forced-scalar equivalence for every optimized path.

### 12.1 Fixtures and provenance

- Public CI MUST NOT depend on ignored reference checkouts or private corpora.
- Small binary fixtures committed to the repository MUST be redistributable and
  listed in a tracked provenance manifest with origin, license, generator or
  upstream revision, and cryptographic hash.
- Generated fixtures SHOULD be preferred when they can exercise the same
  behavior independently.
- Third-party fixture licenses and notices MUST accompany copied fixtures as
  required.
- Large or restricted corpora MAY be used locally but cannot be required to
  validate the public package.

### 12.2 Compatibility policy

A new compatibility feature requires a reproducible file, a normative
interpretation, bounded-memory design, provenance, conformance coverage, a hot-
path benchmark where applicable, and a support-table update. Compatibility
quirks MUST be isolated and named; broad permissive modes that weaken structural
validation are prohibited.

## 13. Thread safety and lifetime

- Immutable metadata objects are thread-safe.
- A random-access source is not required to be thread-safe.
- One open file permits one active scan; an overlapping scan is rejected with
  `InvalidOperationException`.
- A scan enumerator and an individual batch are not thread-safe.
- One enumerator has at most one outstanding batch.
- Disposing the file invalidates outstanding scans in the manner defined in
  Section 8.4.
- Buffers and views MUST never be used after their owning batch is disposed.

## 14. Platforms, packaging, and compatibility

- Target framework: `net10.0`.
- Core 0.1 release qualification targets Windows x64 and Linux x64.
- Correctness MUST NOT depend on an optional instruction set.
- CI MUST exercise scalar execution and every optimized lane present on each
  supported platform.
- Package ID and assembly name: `Lokad.Parquet`.
- License: MIT.
- The package is produced only from Release builds.
- Semantic versioning applies to the public API and documented behavior.
- The shipped package has no runtime package dependencies.
- Generated, benchmark, test, and reference-repository artifacts MUST not be
  included in the NuGet package.

The 0.x previews may revise API names and contracts while the batch model is
qualified. Version 1.0 requires an explicitly documented stable support
profile, a frozen public API snapshot, and production evidence for that
profile. After 1.0, additions are preferred over changes, and support broadening
must not weaken security or bounded-memory guarantees.

## 15. Definition of done for Core 0.1

Core 0.1 is complete when:

- every Core 0.1 support-table entry is implemented;
- Sections 7–9 API, lifetime, security, and failure contracts pass Section 12
  conformance coverage;
- Section 10 performance requirements and release budgets pass on the Section
  14 platform matrix;
- the package and compiled assembly satisfy Sections 2.2 and 14;
- Release build, test, package-content, API-snapshot, formatting, and benchmark
  checks pass with zero warnings; and
- README documents the exact supported subset without implying full Parquet
  compatibility.

Version 1.0 will require a later SPEC revision that freezes an evidence-backed
support profile, public API, performance budgets, and platform matrix.
