# Benchmarks

Release source-build medians from 2026-11-18 UTC using .NET 10.0.15 and Parquet.NET 6.1.0.
Linux on Intel(R) Xeon(R) W-2223 CPU @ 3.60GHz, with one logical processor (affinity 0x1).
Source `b11ef87ca09332761f719af3cb3090aab827c48f-dirty-542c4bd89360027ffed5e251dfc98256f673a5068d6b9ba1244c32f973180023`.

| Case | Rows | File KiB | Lokad ms | Parquet.NET ms | Lokad / Parquet.NET | Lokad allocated KiB | Parquet.NET allocated KiB |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Read INT32, PLAIN | 65,536 | 256.26 | 0.131 | 0.144 | 0.91× | 1.35 | 2.81 |
| Read INT32, PLAIN + Snappy | 65,536 | 256.27 | 0.162 | 0.461 | 0.35× | 1.41 | 1153.79 |
| Read UTF-8, PLAIN + Snappy | 65,536 | 65.19 | 6.540 | 14.977 | 0.44× | 1.50 | 5731.41 |
| Write BOOLEAN, FLOAT, INT64 | 65,536 | 652.77 / 653.50 | 3.356 | 1.977 | 1.70× | 1.18 | 39.16 |
| Write repeated UTF-8 | 65,536 | 33.13 / 33.94 | 2.587 | 17.331 | 0.15× | 0.88 | 4112.78 |
| Write high-cardinality UTF-8 | 65,536 | 859.24 / 864.33 | 11.019 | 30.743 | 0.36× | 0.88 | 6676.12 |

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
