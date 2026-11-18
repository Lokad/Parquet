# Benchmarks

Release source-build medians from 2026-11-18 UTC using .NET 10.0.15 and Parquet.NET 6.1.0.
Linux on Intel(R) Xeon(R) W-2223 CPU @ 3.60GHz, one logical processor (affinity 0x1), instruction set: Auto.
Source `e3f87b0071cff851e6283325055b9c213a049ba1`.

| Case | Rows | File KiB | Lokad ms | Parquet.NET ms | Lokad / Parquet.NET | Lokad allocated KiB | Parquet.NET allocated KiB |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Read INT32, PLAIN | 65,536 | 256.26 | 0.148 | 0.142 | 1.04× | 1.32 | 2.81 |
| Read INT32, PLAIN + Snappy | 65,536 | 256.27 | 0.181 | 0.508 | 0.36× | 1.38 | 1153.82 |
| Read UTF-8, PLAIN + Snappy | 65,536 | 65.19 | 4.813 | 19.474 | 0.25× | 1.47 | 5731.41 |
| Write BOOLEAN, FLOAT, INT64 | 65,536 | 652.74 / 653.50 | 3.044 | 2.115 | 1.44× | 1.18 | 39.16 |
| Write fixed-width runs | 65,536 | 157.01 / 157.89 | 1.650 | 1.804 | 0.91× | 1.18 | 38.84 |
| Write repeated UTF-8 | 65,536 | 33.13 / 33.94 | 2.394 | 19.166 | 0.12× | 0.88 | 4112.33 |
| Write high-cardinality UTF-8 | 65,536 | 859.24 / 864.33 | 10.683 | 30.409 | 0.35× | 0.88 | 6675.24 |

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
