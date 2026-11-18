# Benchmarks

Release source-build medians from 2026-11-18 UTC using .NET 10.0.15 and Parquet.NET 6.1.0.
Linux on Intel(R) Xeon(R) W-2223 CPU @ 3.60GHz, with one logical processor (affinity 0x1).
Source `6f8f8f33e4c72ae30e377835a283e68d1a4d41ea-dirty-4744445b7507aeb94e7dc38002692c91ccc9217f2aade081e32d0b9ba2ead965`.

| Case | Rows | File KiB | Lokad ms | Parquet.NET ms | Lokad / Parquet.NET | Lokad allocated KiB | Parquet.NET allocated KiB |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Read INT32, PLAIN | 65,536 | 256.26 | 0.143 | 0.367 | 0.39× | 1.35 | 2.81 |
| Read INT32, PLAIN + Snappy | 65,536 | 256.27 | 0.165 | 0.523 | 0.32× | 1.41 | 1153.78 |
| Read UTF-8, PLAIN + Snappy | 65,536 | 65.19 | 6.463 | 14.788 | 0.44× | 1.50 | 5731.41 |
| Write BOOLEAN, FLOAT, INT64 | 65,536 | 705.29 / 653.50 | 5.081 | 2.171 | 2.34× | 1.18 | 39.16 |
| Write repeated UTF-8 | 65,536 | 33.12 / 33.94 | 3.208 | 19.362 | 0.17× | 0.88 | 4112.82 |
| Write high-cardinality UTF-8 | 65,536 | 862.20 / 864.33 | 11.310 | 27.225 | 0.42× | 0.88 | 6675.43 |

Read cases start with open readers over the same bytes and consume all projected values.
Both UTF-8 readers deliver bytes and offsets to reusable sinks. Write cases
begin with typed buffers or UTF-8 bytes and offsets and finish at footer emission.
Both writers use PLAIN Snappy, four equal row groups, and a preallocated
memory stream. Parquet.NET's UTF-8 input conversion is timed. File KiB shows
the shared input for reads and Lokad / Parquet.NET output for writes.

BenchmarkDotNet verifies the workload outputs during setup. Run from a native
Linux checkout. Raw measurements, GC counts, file identities, and writer
working-buffer and footer scaling probes land in `artifacts/benchmarks/publication`.

```powershell
./bench.ps1 -Suite Publication
./benchmark-report.ps1
```
