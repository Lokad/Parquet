# Lokad.Parquet

`Lokad.Parquet` is a high-performance, read-only Parquet library for
`.NET 10`, designed for analytics workloads and a small dependency footprint.
The shipped reader is restricted to memory-safe managed C#: it has no unsafe
blocks, pinning, native interop, or APIs that bypass runtime memory safety.

The Core 0.1 surface includes bounded random-access input, immutable footer
metadata, projected asynchronous scans, row-group/range selection, and
disposable aligned column batches. It reads flat required or optional columns
of every Parquet physical type except `INT96`, using Data Page V1/V2, PLAIN or
dictionary encoding, and UNCOMPRESSED or SNAPPY payloads. Nested/repeated
schemas and other codecs remain intentionally unsupported; [SPEC.md](SPEC.md)
is the exact support contract.

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

## Repository layout

- `src/Lokad.Parquet`: the dependency-free library, published as the `Lokad.Parquet` NuGet package.
- `tests/Lokad.Parquet.Tests`: the xUnit test project.
- `bench/Lokad.Parquet.Benchmarks`: the Release-only BenchmarkDotNet project.
- `assets`: source artwork for the NuGet icon.
- `artifacts/nuget`: generated packages (ignored by Git).

## Build and test

The .NET 10 SDK is required.

```powershell
dotnet restore Lokad.Parquet.slnx --tl:off -v minimal
./test.ps1
```

## Package

Packages are intentionally restricted to `Release` builds and are emitted to
`artifacts/nuget`:

```powershell
./pack.ps1
```

## Benchmarks

Benchmark execution is restricted to `Release` builds:

```powershell
./bench.ps1
```

The harness restricts each benchmark process to one logical processor. Linux
measurements must be built, run, and written on a native Linux filesystem, not
on a Windows-backed WSL path such as `/mnt/c`; see [BENCHMARKS.md](BENCHMARKS.md).

## License

`Lokad.Parquet` is licensed under the [MIT License](LICENSE.txt).
