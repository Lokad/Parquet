<#
.SYNOPSIS
Publishes the representative Linux comparison from a completed benchmark snapshot.
.DESCRIPTION
Run bench.ps1 -Suite Publication first. Use -Verify to compare the snapshot
with the saved document and -VerifyCurrentSource to check the checkout and
benchmark package lock against the recorded fingerprints.
#>
[CmdletBinding()]
param(
    [string] $PublicationSnapshot = (Join-Path $PSScriptRoot "artifacts/benchmarks/publication/snapshot.json"),
    [string] $Document = (Join-Path $PSScriptRoot "BENCHMARKS.md"),
    [switch] $Verify,
    [switch] $VerifyCurrentSource
)

$ErrorActionPreference = "Stop"

function Test-Integer($value, [long] $minimum) {
    return ($value -is [int] -or $value -is [long]) -and $value -ge $minimum
}

function Test-Timing($timing) {
    $median = $timing.MedianNanoseconds
    return ($median -is [double] -or (Test-Integer $median 1)) -and
        [double]::IsFinite($median) -and $median -gt 0 -and
        (Test-Integer $timing.AllocatedBytes 0)
}

$snapshot = Get-Content -LiteralPath $PublicationSnapshot -Raw | ConvertFrom-Json
$catalog = Get-Content -LiteralPath (Join-Path $PSScriptRoot "bench/Lokad.Parquet.Benchmarks/PublicationCatalog.json") -Raw |
    ConvertFrom-Json
$culture = [Globalization.CultureInfo]::InvariantCulture
$sha256 = '^[0-9a-f]{64}$'
$revision = '^(?:[0-9a-f]{40}(?:-dirty-[0-9a-f]{64})?|working-tree-[0-9a-f]{64})$'
$date = [datetime]::MinValue
if ((-not (Test-Integer $snapshot.Schema 1)) -or $snapshot.Schema -gt 2 -or
    $snapshot.OperatingSystem -cnotmatch '^Linux \S' -or
    $snapshot.Affinity -cnotmatch '^0x[1248]0*$' -or
    -not [datetime]::TryParseExact($snapshot.Date, "yyyy-MM-dd", $culture, [Globalization.DateTimeStyles]::None, [ref] $date) -or
    $snapshot.Source -cnotmatch $revision -or
    $snapshot.BaselineVersion -cnotmatch '^\d+\.\d+\.\d+(?:-[0-9A-Za-z.-]+)?$' -or
    [string]::IsNullOrWhiteSpace($snapshot.Processor) -or [string]::IsNullOrWhiteSpace($snapshot.Runtime)) {
    throw "The snapshot must describe a completed single-processor Linux publication run."
}
if ($snapshot.PackageLock -cnotmatch $sha256) {
    throw "Invalid package lock hash in the publication snapshot."
}
if ($snapshot.InstructionSet -cnotin @("Auto", "Forced scalar")) {
    throw "Invalid instruction-set mode in the publication snapshot."
}

$sourceFingerprint = if ($snapshot.Source -cmatch '-([0-9a-f]{64})$') { $Matches[1] }
if ($snapshot.Schema -eq 2) {
    if ($snapshot.SourceFingerprint -cnotmatch $sha256 -or
        ($sourceFingerprint -and $sourceFingerprint -cne $snapshot.SourceFingerprint)) {
        throw "The publication snapshot has an invalid source fingerprint."
    }
    $sourceFingerprint = $snapshot.SourceFingerprint
}

if ($VerifyCurrentSource) {
    . (Join-Path $PSScriptRoot "bench/benchmark-identity.ps1")
    if (-not $sourceFingerprint -or
        (Get-BenchmarkSourceFingerprint -RepositoryRoot $PSScriptRoot) -cne $sourceFingerprint) {
        throw "The benchmark source fingerprint does not match the current checkout."
    }
    $lock = Join-Path $PSScriptRoot "bench/Lokad.Parquet.Benchmarks/packages.lock.json"
    if ((Get-FileHash -LiteralPath $lock -Algorithm SHA256).Hash.ToLowerInvariant() -cne $snapshot.PackageLock) {
        throw "The benchmark package lock does not match the current checkout."
    }
}

$measurements = [Collections.Generic.Dictionary[string, object]]::new([StringComparer]::Ordinal)
foreach ($measurement in $snapshot.Measurements) {
    $key = if ($snapshot.Schema -eq 1) { $measurement.Case } else { $measurement.Id }
    if ($key -isnot [string] -or -not $key -or -not $measurements.TryAdd($key, $measurement)) {
        throw "The publication contains a missing or duplicate case identifier."
    }
}
if ($measurements.Count -ne $catalog.Count) {
    throw "The publication must contain all $($catalog.Count) catalog cases."
}

function Format-KiB([double] $bytes) {
    return ($bytes / 1KB).ToString("0.00", $culture)
}

$rows = foreach ($case in $catalog) {
    $key = if ($snapshot.Schema -eq 1) { $case.Case } else { $case.Id }
    if (-not $measurements.ContainsKey($key)) {
        throw "The publication is missing case '$key'."
    }
    $measurement = $measurements[$key]
    $lokad = $measurement.Lokad
    $baseline = $measurement.Baseline
    if ($measurement.Case -cne $case.Case -or
        -not (Test-Integer $measurement.Rows 1) -or $measurement.Rows -ne $case.Rows -or
        -not (Test-Integer $measurement.Columns 1) -or $measurement.Columns -ne $case.Columns -or
        -not (Test-Timing $lokad) -or -not (Test-Timing $baseline)) {
        throw "The publication contains an invalid measurement for '$key'."
    }
    $size = if ($case.Benchmark -ceq "WriteBenchmarks") {
        if (-not (Test-Integer $measurement.LokadFileBytes 1) -or
            -not (Test-Integer $measurement.BaselineFileBytes 1) -or
            $null -ne $measurement.InputBytes -or $null -ne $measurement.FixtureSha256) {
            throw "The publication requires both output sizes for '$key'."
        }
        if ($snapshot.Schema -eq 2 -and ($measurement.LokadFileSha256 -cnotmatch $sha256 -or
            $measurement.BaselineFileSha256 -cnotmatch $sha256)) {
            throw "The publication requires both output digests for '$key'."
        }
        "$(Format-KiB $measurement.LokadFileBytes) / $(Format-KiB $measurement.BaselineFileBytes)"
    } else {
        if (-not (Test-Integer $measurement.InputBytes 1) -or
            $measurement.FixtureSha256 -cnotmatch $sha256 -or
            $null -ne $measurement.LokadFileBytes -or $null -ne $measurement.BaselineFileBytes) {
            throw "The publication requires an input size and fixture digest for '$key'."
        }
        Format-KiB $measurement.InputBytes
    }
    $rowCount = $measurement.Rows.ToString("N0", $culture)
    $lokadMs = ($lokad.MedianNanoseconds / 1e6).ToString("0.000", $culture)
    $baselineMs = ($baseline.MedianNanoseconds / 1e6).ToString("0.000", $culture)
    $ratio = ($lokad.MedianNanoseconds / $baseline.MedianNanoseconds).ToString("0.00", $culture)
    "| $($case.Case) | $rowCount | $size | $lokadMs | $baselineMs | ${ratio}× | $(Format-KiB $lokad.AllocatedBytes) | $(Format-KiB $baseline.AllocatedBytes) |"
}

$hardware = "Linux on $($snapshot.Processor), with one logical processor (affinity $($snapshot.Affinity))."
if ($snapshot.Schema -eq 2) {
    $hardware = "Linux on $($snapshot.Processor), one logical processor (affinity $($snapshot.Affinity)), instruction set: $($snapshot.InstructionSet)."
}

$documentText = @(
    "# Benchmarks"
    ""
    "Release source-build medians from $($snapshot.Date) UTC using $($snapshot.Runtime) and Parquet.NET $($snapshot.BaselineVersion)."
    $hardware
    "Source ``$($snapshot.Source)``."
    ""
    "| Case | Rows | File KiB | Lokad ms | Parquet.NET ms | Lokad / Parquet.NET | Lokad allocated KiB | Parquet.NET allocated KiB |"
    "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |"
    $rows
    ""
    "Read cases start with open readers over the same bytes and consume all projected values."
    "Both UTF-8 readers deliver bytes and offsets to reusable sinks. Write cases"
    "begin with typed buffers or UTF-8 bytes and offsets and finish at footer emission."
    "Both writers use PLAIN Snappy, four equal row groups, and a preallocated"
    "memory stream. Parquet.NET's UTF-8 input conversion is timed. File KiB shows"
    "the shared input for reads and Lokad / Parquet.NET output for writes."
    ""
    "BenchmarkDotNet verifies the workload outputs during setup. Run from a native"
    "Linux checkout. Raw measurements, GC counts, file identities, and writer"
    "working-buffer and footer scaling probes land in ``artifacts/benchmarks/publication``."
    ""
    '```powershell'
    './bench.ps1 -Suite Publication'
    './benchmark-report.ps1'
    '```'
) -join "`n"
$documentText += "`n"
if ($Verify) {
    $current = (Get-Content -LiteralPath $Document -Raw).Replace("`r`n", "`n")
    if ($current -cne $documentText) { throw "The benchmark report differs from the publication snapshot." }
} else {
    [IO.File]::WriteAllText($ExecutionContext.SessionState.Path.GetUnresolvedProviderPathFromPSPath($Document), $documentText)
}
