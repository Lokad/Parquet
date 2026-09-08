param(
    [Parameter(Mandatory)]
    [string[]] $PairedSnapshot,
    [Parameter(Mandatory)]
    [string] $CensusSnapshot,
    [Parameter(Mandatory)]
    [string] $Catalog,
    [string] $Document = (Join-Path $PSScriptRoot "BENCHMARKS.md"),
    [switch] $Verify
)

$ErrorActionPreference = "Stop"
$beginMarker = "<!-- BEGIN GENERATED PARITY REPORT -->"
$endMarker = "<!-- END GENERATED PARITY REPORT -->"
$legacyCaseNames = @(
    "PreopenedScan/RequiredInt32Plain",
    "PreopenedScan/NullableInt32Plain",
    "PreopenedScan/RequiredInt32Snappy",
    "PreopenedScan/TwoRequiredInt32Plain",
    "PreopenedScan/EightRequiredInt32Plain",
    "WarmMetadataOpen"
)
# Frozen legacy catalog (paired schema 6): historical runs only. Do not extend.
$legacyCaseLabels = @{
    "PreopenedScan/RequiredInt32Plain" = "Required INT32, PLAIN"
    "PreopenedScan/NullableInt32Plain" = "Nullable INT32, PLAIN"
    "PreopenedScan/RequiredInt32Snappy" = "Required INT32, Snappy"
    "PreopenedScan/TwoRequiredInt32Plain" = "Two required INT32, PLAIN"
    "PreopenedScan/EightRequiredInt32Plain" = "Eight required INT32, PLAIN"
    "WarmMetadataOpen" = "Warm metadata open"
}

function Assert-ReportCondition([bool] $Condition, [string] $Message) {
    if (-not $Condition) {
        throw $Message
    }
}

Assert-ReportCondition ($PairedSnapshot.Count -eq 4) `
    "Exactly four paired snapshots are required."
$runs = @($PairedSnapshot | ForEach-Object {
    Get-Content -LiteralPath $_ -Raw | ConvertFrom-Json
})
$census = Get-Content -LiteralPath $CensusSnapshot -Raw | ConvertFrom-Json
$catalogDoc = Get-Content -LiteralPath $Catalog -Raw | ConvertFrom-Json
$schemaVersions = @($runs.schemaVersion | Sort-Object -Unique)
Assert-ReportCondition ($schemaVersions.Count -eq 1) `
    "Paired snapshots have different schemas."
$pairedSchema = $schemaVersions[0]
if ($pairedSchema -eq 6) {
    $caseNames = $legacyCaseNames
    $caseLabels = $legacyCaseLabels
    $censusSchema = 1
}
elseif ($pairedSchema -eq 7) {
    Assert-ReportCondition ($catalogDoc.pairedSchemaVersion -eq $pairedSchema) `
        "The catalog does not match the paired snapshot schema."
    $caseNames = @($catalogDoc.cases.name)
    $caseLabels = @{}
    foreach ($entry in $catalogDoc.cases) { $caseLabels[$entry.name] = $entry.label }
    $censusSchema = 2
}
else {
    throw "Unsupported paired snapshot schema."
}

$sourceRevisions = @($runs.sourceRevision | Sort-Object -Unique)
$packageLocks = @($runs.packageLockHash | Sort-Object -Unique)
Assert-ReportCondition ($sourceRevisions.Count -eq 1) `
    "Paired snapshots have different source revisions."
Assert-ReportCondition ($packageLocks.Count -eq 1) `
    "Paired snapshots have different package-lock hashes."
Assert-ReportCondition ($sourceRevisions[0] -ne "unrecorded") `
    "Paired snapshots must record a source revision."
Assert-ReportCondition ($packageLocks[0] -ne "unrecorded") `
    "Paired snapshots must record a package-lock hash."
Assert-ReportCondition ($census.sourceRevision -eq $sourceRevisions[0]) `
    "The work census has a different source revision."
Assert-ReportCondition ((@($runs.sessionId | Sort-Object -Unique)).Count -eq 4) `
    "Paired snapshots must come from four distinct sessions."

foreach ($run in $runs) {
    # Note: $IsWindows/$IsLinux are read-only automatic variables; use other names.
    $onWindows = $run.operatingSystem -like "*Windows*"
    $onLinux = $run.operatingSystem -like "*Linux*"
    if ($pairedSchema -eq 6 -and -not $onWindows -and -not $onLinux) {
        # Frozen schema-6 snapshots predate OS-family recording and name the
        # distro only; the old rule classified every non-Windows session as Linux.
        $run.operatingSystem = "Linux " + $run.operatingSystem
        $onLinux = $true
    }
    Assert-ReportCondition ($onWindows -xor $onLinux) `
        "Every qualifying session must run on Windows or Linux."
}
$windowsRuns = @($runs | Where-Object operatingSystem -Like "*Windows*" |
    Sort-Object recordedAtUtc)
$linuxRuns = @($runs | Where-Object operatingSystem -Like "*Linux*" |
    Sort-Object recordedAtUtc)
Assert-ReportCondition ($windowsRuns.Count -eq 2) `
    "Two Windows sessions are required."
Assert-ReportCondition ($linuxRuns.Count -eq 2) `
    "Two Linux sessions are required."
Assert-ReportCondition ((@($windowsRuns.runnerFingerprint | Sort-Object -Unique)).Count -eq 1) `
    "Windows sessions have different runner fingerprints."
Assert-ReportCondition ((@($linuxRuns.runnerFingerprint | Sort-Object -Unique)).Count -eq 1) `
    "Linux sessions have different runner fingerprints."
# Native-filesystem qualification is enforced at collection time: the benchmark
# refuses Windows-backed mounts on Linux. powerMode stays informational only.

foreach ($run in $runs) {
    Assert-ReportCondition ($run.schemaVersion -eq $pairedSchema) `
        "Unsupported paired snapshot schema."
    Assert-ReportCondition ($run.architecture -eq "X64") `
        "Every qualifying snapshot must be x64."
    Assert-ReportCondition ($run.sampleCount -eq 400) `
        "Every qualifying snapshot must contain 400 paired observations per case."
    Assert-ReportCondition ($run.processorAffinity -match '^0x[0-9a-f]+$') `
        "Every qualifying snapshot must record a hexadecimal processor affinity."
    $affinity = [Convert]::ToInt64($run.processorAffinity.Substring(2), 16)
    Assert-ReportCondition ($affinity -ne 0 -and ($affinity -band ($affinity - 1)) -eq 0) `
        "Every qualifying process must be bound to exactly one logical processor."
    Assert-ReportCondition ($run.outlierRule -Like "none; retain every*") `
        "The qualifying protocol must retain every observation."
    Assert-ReportCondition ($run.cases.Count -eq $caseNames.Count) `
        "A qualifying snapshot has the wrong catalog size."
    foreach ($caseName in $caseNames) {
        $case = @($run.cases | Where-Object name -EQ $caseName)
        Assert-ReportCondition ($case.Count -eq 1) `
            "A qualifying snapshot is missing $caseName."
        Assert-ReportCondition ($case[0].observations.Count -eq 400) `
            "$caseName does not contain 400 observations."
        # Recompute the point estimate from the retained observations; the
        # interval width itself stays with the runner (Student-t), but the
        # point, the gate outcome, the counts, and the identities below are
        # re-derived here rather than trusted.
        $logs = @($case[0].observations | ForEach-Object { $_.logRatio })
        # [double]::IsFinite is unavailable on Windows PowerShell, so finiteness
        # is expressed through IsNaN/IsInfinity instead.
        $sum = 0.0
        foreach ($log in $logs) {
            Assert-ReportCondition (-not ([double]::IsNaN($log) -or [double]::IsInfinity($log))) `
                "$caseName has a non-finite observation."
            $sum += $log
        }
        $point = [Math]::Exp($sum / $logs.Count)
        Assert-ReportCondition (-not ([double]::IsNaN($point) -or [double]::IsInfinity($point))) `
            "$caseName has a non-finite recomputed point ratio."
        Assert-ReportCondition ((-not ([double]::IsNaN($case[0].pointRatio) -or [double]::IsInfinity($case[0].pointRatio))) -and (-not ([double]::IsNaN($case[0].upper95Ratio) -or [double]::IsInfinity($case[0].upper95Ratio)))) `
            "$caseName has a non-finite recorded ratio."
        Assert-ReportCondition ($point -eq $case[0].pointRatio) `
            "$caseName point ratio does not match its observations."
        Assert-ReportCondition ($case[0].passed -eq ($case[0].upper95Ratio -le 1.05)) `
            "$caseName recorded gate is inconsistent."
        Assert-ReportCondition ($case[0].upper95Ratio -ge $point) `
            "$caseName upper bound is below its recomputed point."
        Assert-ReportCondition $case[0].passed `
            "$caseName does not pass its recorded non-inferiority gate."
        Assert-ReportCondition ($case[0].upper95Ratio -le 1.05) `
            "$caseName exceeds the 1.05 upper-bound gate."
        if ($caseName -like "PreopenedScan/RequiredString*") {
            Assert-ReportCondition ($case[0].utf8PayloadBytes -gt 0) `
                "$caseName does not record its UTF-8 payload size."
        }
    }
}

Assert-ReportCondition ($census.schemaVersion -eq $censusSchema) `
    "Unsupported work-census schema."
foreach ($caseName in $caseNames) {
    $hashes = @($runs | ForEach-Object { (@($_.cases | Where-Object name -EQ $caseName))[0].fixtureHash })
    Assert-ReportCondition ((@($hashes | Sort-Object -Unique)).Count -eq 1) `
        "$caseName fixture hashes differ across sessions."
    $identityFields = @("rowCount", "columnCount")
    if ($pairedSchema -eq 7) {
        # utf8PayloadBytes exists only on post-UTF-8 snapshots; the frozen
        # schema-6 evidence predates the field and pins identity by fixture hash.
        $identityFields += "utf8PayloadBytes"
    }
    foreach ($field in $identityFields) {
        $values = @($runs | ForEach-Object { (@($_.cases | Where-Object name -EQ $caseName))[0].$field })
        Assert-ReportCondition ((@($values | Sort-Object -Unique)).Count -eq 1) `
            "$caseName $field differs across sessions."
    }
    if ($caseName -like "PreopenedScan/*") {
        $workload = $caseName.Substring(14)
        $censusEntry = @($census.cases | Where-Object name -EQ $workload)
        Assert-ReportCondition ($censusEntry.Count -eq 1) `
            "The work census is missing $workload."
        Assert-ReportCondition ($censusEntry[0].fixtureHash -eq $hashes[0]) `
            "The $workload census fixture differs from the paired sessions."
    }
}
foreach ($entry in $census.cases) {
    Assert-ReportCondition ($entry.poolRents -eq $entry.poolReturns) `
        "The $($entry.name) work census has unbalanced pool activity."
    Assert-ReportCondition ($entry.retainedPoolBytes -eq 0) `
        "The $($entry.name) work census retained pooled storage."

    Assert-ReportCondition ($entry.maximumConcurrentReads -eq 1) `
        "The $($entry.name) work census overlapped source reads."
    Assert-ReportCondition ($entry.peakPooledBytes -le (6 * $entry.logicalOutputBytes)) `
        "The $($entry.name) work census exceeds the 6x pooled-memory gate."
    if ($null -ne $entry.passPeaks) {
        foreach ($pass in $entry.passPeaks) {
            Assert-ReportCondition ($pass.peakPooledBytes -le (6 * $pass.logicalOutputBytes)) `
                "The $($entry.name) work census exceeds the 6x pooled-memory gate in one pass."
        }
    }
    if ($censusSchema -eq 2) {
        Assert-ReportCondition ($entry.consumerUtf8CopiedBytes -eq (2 * $entry.utf8PayloadBytes)) `
            "The $($entry.name) work census has inconsistent UTF-8 consumer-copy accounting."
    }
}

$lines = [Collections.Generic.List[string]]::new()
$lines.Add("Generated from four ignored paired snapshots and one ignored work-census snapshot.")
$lines.Add("")
$lines.Add("- Source fingerprint: ``$($sourceRevisions[0])``")
$lines.Add("- Parquet.NET lock fingerprint: ``$($packageLocks[0])``")
$lines.Add("- Windows: $($windowsRuns[0].runtime), $($windowsRuns[0].processor), affinity $($windowsRuns[0].processorAffinity), two distinct high-priority processes")
$lines.Add("- Linux: $($linuxRuns[0].runtime), $($linuxRuns[0].processor), $($linuxRuns[0].powerMode), affinity $($linuxRuns[0].processorAffinity), two distinct processes")
$lines.Add("- Protocol: 400 balanced randomized AB/BA observations per workload; every observation retained; one-sided 95% Student-t bound over paired log ratios")
$lines.Add("")
$lines.Add("| Workload | Windows 1 | Windows 2 | Linux 1 | Linux 2 | Gate |")
$lines.Add("|---|---:|---:|---:|---:|---:|")
foreach ($caseName in $caseNames) {
    $cells = foreach ($run in @($windowsRuns + $linuxRuns)) {
        $case = $run.cases | Where-Object name -EQ $caseName
        "{0:F3} / {1:F3}" -f $case.pointRatio, $case.upper95Ratio
    }
    $lines.Add("| $($caseLabels[$caseName]) | $($cells[0]) | $($cells[1]) | $($cells[2]) | $($cells[3]) | pass |")
}
$lines.Add("")
$lines.Add("Each result is `point estimate / upper 95% bound` for `Lokad / Parquet.NET`; lower is better and the declared gate is an upper bound no greater than 1.05.")
$lines.Add("")
$lines.Add("| Workload | Reads / bytes | Pool rents | Peak / output | Bytes cleared | End-scan retained | Retained |")
$lines.Add("|---|---:|---:|---:|---:|---:|---:|")
foreach ($entry in $census.cases) {
    $peakRatio = $entry.peakPooledBytes / $entry.logicalOutputBytes
    $label = $caseLabels["PreopenedScan/$($entry.name)"]
    if ($null -eq $label) { $label = $entry.name }
    $lines.Add("| $label | $($entry.sourceReadCalls) / $($entry.sourceBytesRead) | $($entry.poolRents) | $($entry.peakPooledBytes) B / $($entry.logicalOutputBytes) B ($($peakRatio.ToString('F3', [Globalization.CultureInfo]::InvariantCulture))x) | $($entry.pooledBytesCleared) | $(if ($null -eq $entry.endOfScanRetainedPoolBytes) { 'unrecorded' } else { $entry.endOfScanRetainedPoolBytes }) | $($entry.retainedPoolBytes) |")
}

$documentText = [IO.File]::ReadAllText($Document).Replace("`r`n", "`n")
$beginIndex = $documentText.IndexOf($beginMarker, [StringComparison]::Ordinal)
$endIndex = $documentText.IndexOf($endMarker, [StringComparison]::Ordinal)
Assert-ReportCondition ($beginIndex -ge 0 -and $endIndex -gt $beginIndex) `
    "The benchmark document does not contain one ordered generated-report region."
$before = $documentText.Substring(0, $beginIndex + $beginMarker.Length)
$after = $documentText.Substring($endIndex)
$expected = "$before`n$($lines -join "`n")`n$after"

if ($Verify) {
    Assert-ReportCondition ($documentText -eq $expected) `
        "BENCHMARKS.md does not match the supplied snapshots."
    Write-Output "BENCHMARKS.md matches the supplied snapshots."
}
else {
    [IO.File]::WriteAllText($Document, $expected, [Text.UTF8Encoding]::new($false))
    Write-Output "Rewrote the generated parity report in $Document."
}
