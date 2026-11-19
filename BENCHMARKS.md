# Benchmarks

Release source-build medians from 2026-11-19 UTC using .NET 10.0.15 and Parquet.NET 6.1.0.
Linux on Intel(R) Xeon(R) W-2223 CPU @ 3.60GHz, one logical processor (affinity 0x1), instruction set: Auto.
Source `2586c9eae3db6f527870e9ff740925d22bd99a51-dirty-1b5380db4c0d84d4fd03c7c4f60ac9fc809fd8b8242f8dd7f2079f2c67c5c61a`.

| Case | Rows | File KiB | Lokad ms | Parquet.NET ms | Lokad / Parquet.NET | Lokad allocated KiB | Parquet.NET allocated KiB |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Read INT32, PLAIN | 65,536 | 256.26 | 0.125 | 0.122 | 1.03× | 1.32 | 2.81 |
| Read INT32, PLAIN + Snappy | 65,536 | 256.27 | 0.109 | 0.483 | 0.23× | 1.38 | 1153.80 |
| Read UTF-8, PLAIN + Snappy | 65,536 | 65.19 | 6.024 | 14.803 | 0.41× | 1.47 | 5731.41 |
| Write BOOLEAN, FLOAT, INT64 | 65,536 | 652.74 / 653.50 | 1.651 | 2.071 | 0.80× | 1.18 | 39.16 |
| Write fixed-width runs | 65,536 | 157.01 / 157.89 | 1.612 | 1.780 | 0.91× | 1.18 | 38.84 |
| Write repeated UTF-8 | 65,536 | 33.13 / 33.94 | 3.023 | 18.783 | 0.16× | 0.88 | 4112.33 |
| Write high-cardinality UTF-8 | 65,536 | 859.24 / 864.33 | 9.270 | 29.700 | 0.31× | 0.88 | 6676.26 |

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
