# Lokad.Parquet

`Lokad.Parquet` is a dependency-free Parquet library for .NET 10 and .NET 11.
The library uses memory-safe managed C# for projected reads and streaming writes.

The Core 0.1 surface includes bounded random-access input, immutable footer
metadata, projected asynchronous scans, row-group/range selection, and
disposable aligned column batches. It reads flat required or optional columns
of every Parquet physical type except `INT96`, using Data Page V1/V2, PLAIN or
dictionary encoding, and UNCOMPRESSED or SNAPPY payloads. Nested/repeated
schemas and other codecs remain outside the reader profile. The full support
contract is in the [specification](https://github.com/Lokad/Parquet/blob/master/SPEC.md).

The primary scan API uses descriptors resolved once from immutable metadata:

```csharp
await using var file = await ParquetFile.OpenAsync("data.parquet");
var columns = new[]
{
    file.Metadata.Schema.GetColumn("quantity"),
    file.Metadata.Schema.Columns[3],
};

await foreach (var batch in file.ScanAsync(new ParquetScanOptions(columns)))
{
    using (batch)
    {
        // Consume row-aligned physical column buffers.
    }
}
```

One `ParquetFile` permits one active scan. Dispose each current batch before
advancing; its memory views become invalid at disposal. Disposing the file ends
an active enumerator, while an already-yielded batch remains valid until that
batch is separately disposed.

In-memory content can be opened directly without wrapping or copying the whole
input. If the memory has a separately disposable backing owner, the caller must
keep that owner alive; the bytes must remain unchanged until the file is
disposed:

```csharp
ReadOnlyMemory<byte> content = GetParquetContent();
await using var file = await ParquetFile.OpenAsync(content);
```

Custom range sources fill an `ArraySegment<byte>` and can therefore bridge
managed array-plus-offset storage APIs without unsafe memory access. Streams
and custom sources remain caller-owned unless ownership is explicitly
transferred with `ParquetSourceOwnership.ParquetFile`.

## Writing a flat table

The writer produces required BOOLEAN, FLOAT, INT64, STRING, and DATE columns
using PLAIN Data Page V1 and Snappy. Declare each row group's size and supply
its columns in schema order. Each column can arrive in several typed slices.
STRING takes UTF-8 payload bytes and offsets. DATE takes signed days since
1970-01-01.

```csharp
await using var output = File.Create("data.parquet");
using var writer = await ParquetWriter.CreateAsync(output,
[
    new("quantity", ParquetPhysicalType.Int64),
    new("label", ParquetPhysicalType.ByteArray),
]);

writer.BeginRowGroup(2);
await writer.WriteInt64Async(new long[] { 14, 27 });
await writer.CompleteColumnAsync();
await writer.WriteUtf8Async("café"u8.ToArray(), new int[] { 0, 5, 5 });
await writer.CompleteColumnAsync();
await writer.CompleteAsync();
```

The second label is an empty string. Call `CompleteAsync` to write the footer,
including for a schema-only file. The writer leaves the destination open and
accepts streams that support only sequential writes.

## Repository layout

- `src/Lokad.Parquet`: the dependency-free library, published as the `Lokad.Parquet` NuGet package.
- `tests/Lokad.Parquet.Tests`: the xUnit test project.
- `bench/Lokad.Parquet.Benchmarks`: the Release-only BenchmarkDotNet project.
- `artifacts/nuget`: generated packages (ignored by Git).

## Build and test

Use the .NET 11 SDK selected by `global.json` and the .NET 10 runtime. The
Release checks exercise both target frameworks.

```powershell
dotnet restore Lokad.Parquet.slnx --tl:off -v minimal
./test.ps1
```

## Package

Release builds emit the package to `artifacts/nuget`.

```powershell
./pack.ps1
```

The bundled changelog describes the packaged changes. Keep the assembly
version stable across compatible releases. The file version follows the
numeric package version.

## Benchmarks

Run Release benchmarks on a native Linux filesystem. The harness pins the
process to one logical processor.

```powershell
./bench.ps1 -Suite Publication
```

The [benchmark report](https://github.com/Lokad/Parquet/blob/master/BENCHMARKS.md)
shows representative read and write comparisons.

## License

`Lokad.Parquet` is licensed under the [MIT License](LICENSE.txt).
