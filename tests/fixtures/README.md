# Parquet fixtures

Public tests generate their minimal Parquet inputs in memory with
`ParquetFixtureBuilder`. The generator is original MIT-licensed test code in
this repository.

The `apache-parquet-testing` directory contains six unmodified, small
cross-producer fixtures from the pinned Apache Parquet testing repository.
They remain under Apache-2.0; its bundled `LICENSE.txt` applies. No private data
is present. `manifest.json` records and tests every origin, license, revision,
reproduction model, and SHA-256.

## Reproduce the writer interoperability check

The writer tests can export synthetic files for an independent PyArrow check.
From the repository root, use PowerShell and a Python environment with
`pyarrow==22.0.0`.

```powershell
$env:LOKAD_PARQUET_WRITER_ARROW_OUTPUT = Join-Path ([IO.Path]::GetTempPath()) "parquet-writer-check"
dotnet test tests/Lokad.Parquet.Tests -c Release -f net10.0 --tl:off --filter FullyQualifiedName~ParquetWriterTests
python tests/verify-writer-pyarrow.py $env:LOKAD_PARQUET_WRITER_ARROW_OUTPUT
```
