# Benchmarks

Release source-build medians from 2026-11-18 UTC using .NET 10.0.15 and Parquet.NET 6.1.0.
Linux on Intel(R) Xeon(R) W-2223 CPU @ 3.60GHz, with one logical processor (affinity 0x1).
Source `c223bfae34410f085ebe024bee2e2dd478ef7f9c-dirty-5a82de6afe6a471e5396062033e7d0a7047c709c5b76ca98fc862866ce986472`.

| Case | Rows | File KiB | Lokad ms | Parquet.NET ms | Lokad / Parquet.NET | Lokad allocated KiB | Parquet.NET allocated KiB |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Read INT32, PLAIN | 65,536 | 256.26 | 0.122 | 0.135 | 0.90× | 1.35 | 2.81 |
| Read INT32, PLAIN + Snappy | 65,536 | 256.27 | 0.162 | 0.472 | 0.34× | 1.41 | 1153.83 |
| Read UTF-8, PLAIN + Snappy | 65,536 | 65.19 | 5.043 | 18.682 | 0.27× | 1.50 | 5731.41 |
| Write BOOLEAN, FLOAT, INT64 | 65,536 | 652.74 / 653.50 | 2.280 | 2.393 | 0.95× | 1.18 | 38.84 |
| Write fixed-width runs | 65,536 | 157.01 / 157.89 | 1.657 | 1.678 | 0.99× | 1.18 | 38.84 |
| Write repeated UTF-8 | 65,536 | 33.13 / 33.94 | 3.165 | 16.714 | 0.19× | 0.88 | 4113.20 |
| Write high-cardinality UTF-8 | 65,536 | 859.24 / 864.33 | 10.974 | 31.030 | 0.35× | 0.88 | 6675.22 |

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
