# Lokad.Parquet

`Lokad.Parquet` is a dependency-free, read-only Parquet library targeting
`.NET 10`. The implemented Core 0.1 reader covers bounded footer parsing and
projected scans of flat required/optional physical columns with PLAIN or
dictionary encoding and UNCOMPRESSED or SNAPPY pages. `SPEC.md` is the exact
support contract; do not imply full Parquet compatibility.

## Standard validation

Run commands from the repository root and disable the dynamic terminal logger:

```powershell
dotnet restore Lokad.Parquet.slnx --tl:off -v minimal
dotnet build Lokad.Parquet.slnx -c Release --tl:off --nologo -v minimal
dotnet test Lokad.Parquet.slnx -c Release --tl:off --nologo -v minimal --no-build
dotnet format Lokad.Parquet.slnx --verify-no-changes --no-restore
```

`./test.ps1` provides a Debug build-and-test loop for quick iteration (use `./test.ps1 -Configuration Release` for a Release loop); it does not run the formatter. The four commands above remain the complete Release qualification.

Before considering a reader change complete, add positive and malformed-input
coverage, verify cancellation and pool ownership where applicable, preserve
the reviewed public API snapshot deliberately, and benchmark any changed hot
path against the pinned baseline. Scalar behavior remains the oracle for any
optimized path.

## Dependencies

Keep the shipped `Lokad.Parquet` project on the .NET standard libraries only.
xUnit dependencies belong only in the test project, and BenchmarkDotNet belongs
only in the benchmark project.

Keep the shipped library in memory-safe managed C#. Do not use C# `unsafe`,
pointers, `fixed`, pinning, `System.Runtime.CompilerServices.Unsafe`,
`MemoryMarshal`, `CollectionsMarshal`, `Marshal`, or `NativeMemory`.
`AllowUnsafeBlocks` must remain `false`.

## Packages and benchmarks

NuGet packages must be produced in `Release` and are written to
`artifacts/nuget`. Use `./pack.ps1` for the standard package command.

Benchmarks must also run in `Release`; use `./bench.ps1` rather than a Debug
invocation. Qualification is single-logical-processor. Linux evidence must be
built and run from a native Linux filesystem, never a Windows-backed WSL path.
