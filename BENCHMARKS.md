# Benchmarks

Release source-build medians from 2026-11-19 UTC using .NET 10.0.15 and Parquet.NET 6.1.0.
Linux on Intel(R) Xeon(R) W-2223 CPU @ 3.60GHz, one logical processor (affinity 0x1), instruction set: Auto.
Source `33deb9e2ae11aa2e710f8212348c8912799ae074-dirty-381ad6b864487b8348d1b45eb4b30fe6f24956edf9c12a2f148bb0bb38c0c11d`.

| Case | Rows | File KiB | Lokad ms | Parquet.NET ms | Lokad / Parquet.NET | Lokad allocated KiB | Parquet.NET allocated KiB |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Read INT32, PLAIN | 65,536 | 256.26 | 0.129 | 0.132 | 0.98× | 1.32 | 2.81 |
| Read INT32, PLAIN + Snappy | 65,536 | 256.27 | 0.160 | 0.458 | 0.35× | 1.38 | 1153.86 |
| Read UTF-8, PLAIN + Snappy | 65,536 | 65.19 | 7.265 | 14.336 | 0.51× | 1.47 | 5731.41 |
| Write BOOLEAN, FLOAT, INT64 | 65,536 | 652.74 / 653.50 | 1.748 | 1.977 | 0.88× | 1.18 | 39.16 |
| Write fixed-width runs | 65,536 | 157.01 / 157.89 | 1.451 | 1.924 | 0.75× | 1.18 | 39.16 |
| Write repeated UTF-8 | 65,536 | 33.13 / 33.94 | 2.391 | 15.765 | 0.15× | 0.88 | 4112.68 |
| Write high-cardinality UTF-8 | 65,536 | 859.24 / 864.33 | 10.000 | 24.770 | 0.40× | 0.88 | 6675.75 |

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
