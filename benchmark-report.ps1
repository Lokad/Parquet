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

# One-sided 95% Student-t quantile shared with the paired runner: exact textbook
# values to df 30, then the same bounded Cornish-Fisher expansion the schema-8
# runner uses. Snapshots before schema 8 used a flat 1.645 fallback past df 30.
function Get-StudentT95Quantile([int] $DegreesOfFreedom, [bool] $LegacyFallback) {
    $exact = @(6.314, 2.920, 2.353, 2.132, 2.015, 1.943, 1.895, 1.860, 1.833, 1.812, 1.796, 1.782, 1.771, 1.761, 1.753, 1.746, 1.740, 1.734, 1.729, 1.725, 1.721, 1.717, 1.714, 1.711, 1.708, 1.706, 1.703, 1.701, 1.699, 1.697)
    if ($DegreesOfFreedom -le 0) { throw "Student-t degrees of freedom must be positive." }
    if (-not $LegacyFallback -and $DegreesOfFreedom -gt $exact.Count) {
        $z = 1.6448536269514722
        $inverse = 1.0 / $DegreesOfFreedom
        return $z + ((($z * $z) + 1.0) * $z) / 4.0 * $inverse + ((((5.0 * $z * $z) + 16.0) * $z * $z * $z) + (3.0 * $z)) / 96.0 * $inverse * $inverse
    }
    if ($DegreesOfFreedom -le $exact.Count) { return $exact[$DegreesOfFreedom - 1] }
    return 1.645
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
elseif ($pairedSchema -eq 7 -or $pairedSchema -eq 8) {
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
$useLegacyInterval = ($pairedSchema -ne 8)

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
    Assert-ReportCondition ($null -ne ($run.sessionId -as [guid])) `
        "A paired snapshot has an unparseable session identity."
    Assert-ReportCondition (-not [string]::IsNullOrEmpty($run.runnerFingerprint)) `
        "A paired snapshot is missing its runner fingerprint."
    foreach ($field in @("runtime", "processor", "architecture", "instructionMode")) {
        Assert-ReportCondition (-not [string]::IsNullOrEmpty($run.$field)) `
            "A paired snapshot is missing its runtime environment evidence."
    }
    Assert-ReportCondition ($null -ne $run.powerMode) `
        "A paired snapshot is missing its power mode."
    Assert-ReportCondition ($run.stopwatchFrequency -gt 0) `
        "A paired snapshot is missing its stopwatch frequency."
}

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
        Assert-ReportCondition ($null -ne $case[0].fixtureHash -and $case[0].fixtureHash -ne "") `
            "$caseName is missing its fixture identity."
        Assert-ReportCondition ($case[0].operationsPerBlock -gt 0) `
            "$caseName is missing its operation count."
        # Recompute every log ratio from finite positive raw timings; neither the
        # stored ratios nor the stored bound are trusted. Means, deviations, and
        # the interval below mirror the runner operation for operation.
        $logs = @()
        $abSum = 0.0
        $abCount = 0
        $baSum = 0.0
        foreach ($observation in $case[0].observations) {
            Assert-ReportCondition ($observation.order -eq "AB" -or $observation.order -eq "BA") `
                "$caseName has an observation without a balanced AB/BA order."
            Assert-ReportCondition ($null -ne $observation.lokadNanoseconds -and $null -ne $observation.parquetNetNanoseconds) `
                "$caseName has an observation with missing timings."
            $lokad = $observation.lokadNanoseconds
            $baseline = $observation.parquetNetNanoseconds
            Assert-ReportCondition ((-not ([double]::IsNaN($lokad) -or [double]::IsInfinity($lokad))) -and (-not ([double]::IsNaN($baseline) -or [double]::IsInfinity($baseline)))) `
                "$caseName has an observation with non-finite timings."
            Assert-ReportCondition ($lokad -gt 0 -and $baseline -gt 0) `
                "$caseName has an observation with non-positive timings."
            foreach ($field in @("lokadAllocatedBytes", "parquetNetAllocatedBytes")) {
                Assert-ReportCondition ($null -ne $observation.$field -and $observation.$field -ge 0) `
                    "$caseName has an observation with missing allocation counts."
            }
            foreach ($field in @("lokadGen0Collections", "parquetNetGen0Collections", "lokadGen1Collections", "parquetNetGen1Collections", "lokadGen2Collections", "parquetNetGen2Collections")) {
                Assert-ReportCondition ($null -ne $observation.$field -and $observation.$field -ge 0) `
                    "$caseName has an observation with missing GC counts."
            }
            $recomputed = [Math]::Log($lokad / $baseline)
            Assert-ReportCondition (-not ([double]::IsNaN($recomputed) -or [double]::IsInfinity($recomputed))) `
                "$caseName has an observation with a non-finite recomputed log ratio."
            Assert-ReportCondition ($null -ne $observation.logRatio -and $recomputed -eq $observation.logRatio) `
                "$caseName observation log ratio does not match its raw timings."
            if ($observation.order -eq "AB") {
                $abSum += $recomputed
                $abCount++
            }
            else {
                $baSum += $recomputed
            }
            $logs += $recomputed
        }
        Assert-ReportCondition ($abCount -eq 200 -and ($logs.Count - $abCount) -eq 200) `
            "$caseName does not contain balanced AB/BA observations."
        # [double]::IsFinite is unavailable on Windows PowerShell, so finiteness
        # is expressed through IsNaN/IsInfinity instead.
        $sum = 0.0
        foreach ($log in $logs) {
            $sum += $log
        }
        $meanLog = $sum / $logs.Count
        $point = [Math]::Exp($meanLog)
        $sumSquaredDeviation = 0.0
        foreach ($log in $logs) { $sumSquaredDeviation += [Math]::Pow($log - $meanLog, 2) }
        $standardError = [Math]::Sqrt($sumSquaredDeviation / ($logs.Count - 1)) / [Math]::Sqrt($logs.Count)
        $upper = [Math]::Exp($meanLog + (Get-StudentT95Quantile ($logs.Count - 1) $useLegacyInterval) * $standardError)
        Assert-ReportCondition ($upper -eq $case[0].upper95Ratio) `
            "$caseName upper bound does not match recomputed evidence."
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
        # Order and drift diagnostics for human examination at qualification time:
        # AB versus BA means expose order effects, halves expose time drift, and the
        # block-SD ratio exposes correlated blocks (near 1.0 under independence).
        # These print but never gate; protocol randomness makes hard gates flaky.
        $halfCount = [int]($logs.Count / 2)
        $firstSum = 0.0
        for ($i = 0; $i -lt $halfCount; $i++) { $firstSum += $logs[$i] }
        $secondSum = 0.0
        for ($i = $halfCount; $i -lt $logs.Count; $i++) { $secondSum += $logs[$i] }
        $blockSize = [int]($logs.Count / 10)
        $blockMeans = @()
        for ($block = 0; $block -lt 10; $block++) {
            $blockSum = 0.0
            for ($i = 0; $i -lt $blockSize; $i++) { $blockSum += $logs[$block * $blockSize + $i] }
            $blockMeans += $blockSum / $blockSize
        }
        $blockAverageSum = 0.0
        foreach ($value in $blockMeans) { $blockAverageSum += $value }
        $blockAverage = $blockAverageSum / $blockMeans.Count
        $blockSsd = 0.0
        foreach ($value in $blockMeans) { $blockSsd += [Math]::Pow($value - $blockAverage, 2) }
        $blockSd = [Math]::Sqrt($blockSsd / ($blockMeans.Count - 1))
        $blockRatioText = "undefined (zero variance)"
        if ($standardError -ne 0) {
            $blockRatioText = "{0:F2}" -f ($blockSd / ($standardError * [Math]::Sqrt($logs.Count / $blockSize)))
        }
        Write-Verbose ("$caseName order/drift: AB mean {0:E4} (n={1}), BA mean {2:E4} (n={3}), first-half {4:E4}, second-half {5:E4}, block-SD ratio {6}" -f ($abSum / $abCount), $abCount, ($baSum / ($logs.Count - $abCount)), ($logs.Count - $abCount), ($firstSum / $halfCount), ($secondSum / ($logs.Count - $halfCount)), $blockRatioText)
    }
}

Assert-ReportCondition ($census.schemaVersion -eq $censusSchema) `
    "Unsupported work-census schema."
Assert-ReportCondition ($null -ne $census.recordedAtUtc -and "$($census.recordedAtUtc)" -ne "") `
    "The work census is missing its recording timestamp."
foreach ($caseName in $caseNames) {
    $hashes = @($runs | ForEach-Object { (@($_.cases | Where-Object name -EQ $caseName))[0].fixtureHash })
    Assert-ReportCondition ((@($hashes | Sort-Object -Unique)).Count -eq 1) `
        "$caseName fixture hashes differ across sessions."
    $identityFields = @("rowCount", "columnCount")
    if ($pairedSchema -eq 7 -or $pairedSchema -eq 8) {
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
