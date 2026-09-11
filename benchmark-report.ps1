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
elseif ($pairedSchema -eq 7 -or $pairedSchema -eq 8 -or $pairedSchema -eq 9) {
    Assert-ReportCondition ($catalogDoc.pairedSchemaVersion -eq $pairedSchema) `
        "The catalog does not match the paired snapshot schema."
    $caseNames = @($catalogDoc.cases.name)
    $caseLabels = @{}
    foreach ($entry in $catalogDoc.cases) { $caseLabels[$entry.name] = $entry.label }
    $censusSchema = 2
    if ($pairedSchema -eq 9) {
        $censusSchema = 4
    }
}
else {
    throw "Unsupported paired snapshot schema."
}
$useLegacyInterval = ($pairedSchema -ne 8 -and $pairedSchema -ne 9)

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
if ($pairedSchema -eq 9) {
    Assert-ReportCondition ((-not [string]::IsNullOrEmpty($catalogDoc.sourceRevision)) -and ($catalogDoc.sourceRevision -ne "unrecorded")) `
        "The catalog does not record its source identity."
    Assert-ReportCondition ($catalogDoc.sourceRevision -eq $sourceRevisions[0]) `
        "The catalog has a different source revision."
    Assert-ReportCondition ((-not [string]::IsNullOrEmpty($catalogDoc.packageLockHash)) -and ($catalogDoc.packageLockHash -ne "unrecorded")) `
        "The catalog does not record its package identity."
    Assert-ReportCondition ($catalogDoc.packageLockHash -eq $packageLocks[0]) `
        "The catalog has a different package-lock hash."
}
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


if ($pairedSchema -eq 9) {
    # Schema 9 records the bound worker host tuning and output storage, so
    # the report gates the same CPU evidence the runners persist. Earlier
    # schemas predate these fields and stay frozen.
    foreach ($run in $runs) {
        Assert-ReportCondition ($null -ne $run.logicalProcessor -and $run.logicalProcessor -ge 0) `
            "A paired snapshot is missing its bound logical processor."
        Assert-ReportCondition ($null -ne $run.serverGarbageCollection) `
            "A paired snapshot is missing its server GC evidence."
        foreach ($field in @("gcLatencyMode", "tieredCompilation", "tieredPgo")) {
            Assert-ReportCondition (-not [string]::IsNullOrEmpty($run.$field)) `
                "A paired snapshot is missing its runtime tuning evidence."
        }
        foreach ($field in @("resolvedOutputPath", "outputFileSystem")) {
            Assert-ReportCondition (-not [string]::IsNullOrEmpty($run.$field)) `
                "A paired snapshot is missing its output storage evidence."
        }
    }
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
# Lane outcomes below only record evidence validity; the strict parity gate is
# evaluated once, explicitly, after the diagnostic report renders.

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

if ($censusSchema -eq 3 -or $censusSchema -eq 4) {
    # Schema 3 carries the census worker affinity, host tuning, and output
    # storage alongside the pool evidence.
    Assert-ReportCondition ($census.processorAffinity -match "^0x[0-9a-f]+$") `
        "The work census must record a hexadecimal processor affinity."
    $censusAffinity = [Convert]::ToInt64($census.processorAffinity.Substring(2), 16)
    Assert-ReportCondition ($censusAffinity -ne 0 -and ($censusAffinity -band ($censusAffinity - 1)) -eq 0) `
        "The work-census process must be bound to exactly one logical processor."
    Assert-ReportCondition ($null -ne $census.logicalProcessor -and $census.logicalProcessor -ge 0) `
        "The work census is missing its bound logical processor."
    Assert-ReportCondition ($null -ne $census.serverGarbageCollection) `
        "The work census is missing its server GC evidence."
    foreach ($field in @("gcLatencyMode", "tieredCompilation", "tieredPgo")) {
        Assert-ReportCondition (-not [string]::IsNullOrEmpty($census.$field)) `
            "The work census is missing its runtime tuning evidence."
    }
    foreach ($field in @("resolvedOutputPath", "outputFileSystem")) {
        Assert-ReportCondition (-not [string]::IsNullOrEmpty($census.$field)) `
            "The work census is missing its output storage evidence."
    }
}
foreach ($caseName in $caseNames) {
    $hashes = @($runs | ForEach-Object { (@($_.cases | Where-Object name -EQ $caseName))[0].fixtureHash })
    Assert-ReportCondition ((@($hashes | Sort-Object -Unique)).Count -eq 1) `
        "$caseName fixture hashes differ across sessions."
    $identityFields = @("rowCount", "columnCount")
    if ($pairedSchema -eq 7 -or $pairedSchema -eq 8 -or $pairedSchema -eq 9) {
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
        Assert-ReportCondition ($censusEntry.Count -ge 1) `
            "The work census is missing $workload."
        Assert-ReportCondition ($censusEntry.Count -le 1) `
            "The work census duplicates $workload."
        Assert-ReportCondition ($censusEntry[0].fixtureHash -eq $hashes[0]) `
            "The $workload census fixture differs from the paired sessions."
    }
}
$frozenCensusPhysicalTypes = @("boolean", "int32", "int64", "float", "double", "fixed", "utf8", "binary")
$frozenCensusConsumers = @("int32", "nullable-int32", "multi-int32", "boolean", "utf8", "int64", "float", "double", "fixed", "nullable-int64", "binary", "nullable-binary")

function Get-CensusSlotWidth([object] $PhysicalType, [object] $ValueWidthBytes, [string] $CaseName) {
    switch ($PhysicalType) {
        "boolean" {
            Assert-ReportCondition ($ValueWidthBytes -eq 1) "$CaseName census layout changed its boolean width."
            return 1
        }
        "int32" {
            Assert-ReportCondition ($ValueWidthBytes -eq 4) "$CaseName census layout changed its INT32 width."
            return 4
        }
        "int64" {
            Assert-ReportCondition ($ValueWidthBytes -eq 8) "$CaseName census layout changed its INT64 width."
            return 8
        }
        "float" {
            Assert-ReportCondition ($ValueWidthBytes -eq 4) "$CaseName census layout changed its FLOAT width."
            return 4
        }
        "double" {
            Assert-ReportCondition ($ValueWidthBytes -eq 8) "$CaseName census layout changed its DOUBLE width."
            return 8
        }
        "fixed" {
            Assert-ReportCondition (($null -ne $ValueWidthBytes) -and ($ValueWidthBytes -gt 0)) "$CaseName census layout changed its fixed width."
            return [long]$ValueWidthBytes
        }
        "utf8" {
            Assert-ReportCondition ($ValueWidthBytes -eq 0) "$CaseName census layout changed its UTF-8 width."
            return 0
        }
        "binary" {
            Assert-ReportCondition ($ValueWidthBytes -eq 0) "$CaseName census layout changed its binary width."
            return 0
        }
    }
    throw "$CaseName census layout carries an unknown physical type."
}
function Get-CensusLogicalBytes([object] $Entry, [int] $ColumnCount, [string] $CaseName) {
    $width = Get-CensusSlotWidth $Entry.physicalType $Entry.valueWidthBytes $CaseName
    if ($Entry.physicalType -eq "utf8") {
        return [long]$Entry.utf8PayloadBytes + [long]$ColumnCount * ([long]$Entry.rowCount + 1) * 4
    }
    if ($Entry.physicalType -eq "binary") {
        Assert-ReportCondition (($null -ne $Entry.binaryPayloadBytes) -and ($Entry.binaryPayloadBytes -ge 0)) "$CaseName census layout is missing its binary payload."
        $binaryTotal = [long]$Entry.binaryPayloadBytes + [long]$ColumnCount * ([long]$Entry.rowCount + 1) * 4
        if ($Entry.nullable) {
            $binaryTotal += [long]$ColumnCount * [long][Math]::Floor(([long]$Entry.rowCount + 7) / 8)
        }
        return $binaryTotal
    }
    $total = [long]$Entry.rowCount * [long]$ColumnCount * $width
    if ($Entry.nullable) {
        $total += [long]$ColumnCount * [long][Math]::Floor(([long]$Entry.rowCount + 7) / 8)
    }
    return $total
}
function Get-PoolBudgetOutcome([object] $PeakBytes, [object] $LogicalBytes) {
    if ([long]$PeakBytes -le (6 * [long]$LogicalBytes)) { return "pass" }
    return "FAIL"
}
function Assert-ReportCounter([object] $Value, [string] $Message) {
    Assert-ReportCondition (($null -ne $Value) -and ($Value -ge 0)) $Message
}
$catalogCensusEntries = @{}
$catalogCensusPassCounts = @{}
if ($censusSchema -eq 4) {
    $mappedCensusNames = @($caseNames | Where-Object { $_ -like "PreopenedScan/*" } | ForEach-Object { $_.Substring(14) })
    Assert-ReportCondition ($null -ne $catalogDoc.censusCases) `
        "The catalog does not record its census cases."
    foreach ($catalogEntry in $catalogDoc.censusCases) {
        Assert-ReportCondition (-not [string]::IsNullOrEmpty($catalogEntry.name)) `
            "The catalog carries an unnamed census case."
        Assert-ReportCondition (-not $catalogCensusEntries.ContainsKey($catalogEntry.name)) `
            "The catalog duplicates census case $($catalogEntry.name)."
        Assert-ReportCondition ($frozenCensusConsumers -contains $catalogEntry.consumer) `
            "The catalog census case $($catalogEntry.name) carries an unknown consumer identity."
        $catalogSlotWidth = Get-CensusSlotWidth $catalogEntry.physicalType $catalogEntry.typeWidthBytes $catalogEntry.name
        Assert-ReportCondition ($null -ne $catalogEntry.nullable) `
            "The catalog census case $($catalogEntry.name) is missing its nullability evidence."
        Assert-ReportCondition (($null -ne $catalogEntry.passes) -and (@($catalogEntry.passes).Count -gt 0)) `
            "The catalog census case $($catalogEntry.name) carries no expected passes."
        foreach ($catalogPass in $catalogEntry.passes) {
            Assert-ReportCondition (($null -ne $catalogPass.ordinals) -and (@($catalogPass.ordinals).Count -gt 0)) `
                "The catalog census case $($catalogEntry.name) carries an empty expected projection."
            foreach ($ordinal in $catalogPass.ordinals) {
                Assert-ReportCondition (($null -ne $ordinal) -and ($ordinal -ge 0)) `
                    "The catalog census case $($catalogEntry.name) carries an invalid ordinal."
            }
        }
        $catalogCensusEntries[$catalogEntry.name] = $catalogEntry
        $catalogCensusPassCounts[$catalogEntry.name] = @($catalogEntry.passes).Count
    }
    $expectedCensusNames = @($mappedCensusNames + @($catalogCensusEntries.Keys) | Sort-Object -Unique)
    $actualCensusNames = @($census.cases | ForEach-Object { $_.name } | Sort-Object -Unique)
    Assert-ReportCondition ($census.cases.Count -eq $expectedCensusNames.Count) `
        "The work census does not match the frozen case set."
    Assert-ReportCondition (@(Compare-Object $expectedCensusNames $actualCensusNames).Count -eq 0) `
        "The work census does not match the frozen case set."
}

$censusBudgetFailures = @()
foreach ($entry in $census.cases) {
    Assert-ReportCondition ($entry.poolRents -eq $entry.poolReturns) `
        "The $($entry.name) work census has unbalanced pool activity."
    Assert-ReportCondition ($entry.retainedPoolBytes -eq 0) `
        "The $($entry.name) work census retained pooled storage."

    Assert-ReportCondition ($entry.maximumConcurrentReads -eq 1) `
        "The $($entry.name) work census overlapped source reads."
    $caseBudget = Get-PoolBudgetOutcome $entry.peakPooledBytes $entry.logicalOutputBytes
    if ($caseBudget -eq "FAIL") {
        $censusBudgetFailures += "$($entry.name) case peaks at $($entry.peakPooledBytes) B against $($entry.logicalOutputBytes) B layout"
    }
    if ($null -ne $entry.passPeaks) {
        $passBudgetNumber = 0
        foreach ($pass in $entry.passPeaks) {
            $passBudgetNumber++
            $passBudget = Get-PoolBudgetOutcome $pass.peakPooledBytes $pass.logicalOutputBytes
            if ($passBudget -eq "FAIL") {
                $censusBudgetFailures += "$($entry.name) pass $passBudgetNumber peaks at $($pass.peakPooledBytes) B against $($pass.logicalOutputBytes) B layout"
            }
            if ($censusSchema -eq 4) {
                Assert-ReportCondition (($null -ne $pass.elapsedMilliseconds) -and (-not ([double]::IsNaN($pass.elapsedMilliseconds) -or [double]::IsInfinity($pass.elapsedMilliseconds))) -and ($pass.elapsedMilliseconds -ge 0)) `
                    "The $($entry.name) work census is missing its pass timing evidence."
                Assert-ReportCondition (($null -ne $pass.batchCount) -and ($pass.batchCount -gt 0)) `
                    "The $($entry.name) work census is missing its pass batch count."
                Assert-ReportCondition (($null -ne $pass.allocatedBytes) -and ($pass.allocatedBytes -ge 0)) `
                    "The $($entry.name) work census is missing its pass allocation evidence."
                Assert-ReportCondition (($null -ne $pass.gen0Collections) -and ($pass.gen0Collections -ge 0) -and ($null -ne $pass.gen1Collections) -and ($pass.gen1Collections -ge 0) -and ($null -ne $pass.gen2Collections) -and ($pass.gen2Collections -ge 0)) `
                    "The $($entry.name) work census is missing its pass GC evidence."
            }
        }
    }
    if (($censusSchema -eq 2 -or $censusSchema -eq 4) -and (($null -eq $entry.physicalType) -or ($entry.physicalType -eq "utf8"))) {
        Assert-ReportCondition ($entry.consumerUtf8CopiedBytes -eq (2 * $entry.utf8PayloadBytes)) `
            "The $($entry.name) work census has inconsistent UTF-8 consumer-copy accounting."
    }
    if ($censusSchema -eq 4) {
        Assert-ReportCondition (($entry.sourceReadCalls -gt 0) -and ($entry.sourceBytesRead -gt 0)) `
            "The $($entry.name) work census is missing its source read evidence."
        foreach ($field in @("fixtureBytes", "rowCount", "columnCount", "logicalOutputBytes")) {
            Assert-ReportCondition (($null -ne $entry.$field) -and ($entry.$field -gt 0)) `
                "The $($entry.name) work census carries invalid $field evidence."
        }
        foreach ($field in @("utf8PayloadBytes", "maximumConcurrentReads", "publicBatches", "decodedColumnBatches", "totalMoves", "synchronousMoves", "poolRents", "poolReturns", "requestedPoolBytes", "rentedPoolCapacityBytes", "peakPooledBytes", "returnedPoolCapacityBytes", "sourceCopiedBytes", "endOfScanRetainedPoolBytes", "consumerUtf8CopiedBytes", "pooledBytesCleared", "retainedPoolBytes")) {
            Assert-ReportCounter $entry.$field "The $($entry.name) work census carries invalid $field evidence."
        }
        Assert-ReportCondition ($entry.synchronousMoves -le $entry.totalMoves) `
            "The $($entry.name) work census has inconsistent move accounting."
        Assert-ReportCondition ($entry.requestedPoolBytes -le $entry.rentedPoolCapacityBytes) `
            "The $($entry.name) work census has inconsistent pool budget accounting."
        Assert-ReportCondition ($entry.rentedPoolCapacityBytes -eq $entry.returnedPoolCapacityBytes) `
            "The $($entry.name) work census has inconsistent pool budget accounting."
        Assert-ReportCondition ($entry.pooledBytesCleared -eq $entry.returnedPoolCapacityBytes) `
            "The $($entry.name) work census has inconsistent pool budget accounting."
        Assert-ReportCondition ($entry.peakPooledBytes -le $entry.rentedPoolCapacityBytes) `
            "The $($entry.name) work census has inconsistent pool budget accounting."
        Assert-ReportCondition ($entry.endOfScanRetainedPoolBytes -le $entry.peakPooledBytes) `
            "The $($entry.name) work census has inconsistent pool budget accounting."
        Assert-ReportCondition ($frozenCensusPhysicalTypes -contains $entry.physicalType) `
            "The $($entry.name) work census carries an unknown layout identity."
        Assert-ReportCondition ($null -ne $entry.nullable) `
            "The $($entry.name) work census is missing its nullability evidence."
        Assert-ReportCondition ($frozenCensusConsumers -contains $entry.consumer) `
            "The $($entry.name) work census carries an unknown consumer identity."
        Assert-ReportCondition ((($null -eq $entry.rowRangeStart) -and ($null -eq $entry.rowRangeCount)) -or (($null -ne $entry.rowRangeStart) -and ($null -ne $entry.rowRangeCount))) `
            "The $($entry.name) work census carries a partial row-range identity."
        if (($null -ne $entry.rowRangeStart) -and ($null -ne $entry.rowRangeCount)) {
            Assert-ReportCondition (($entry.rowRangeStart -ge 0) -and ($entry.rowRangeCount -gt 0)) `
                "The $($entry.name) work census carries an invalid row range."
            Assert-ReportCondition ($entry.rowCount -eq $entry.rowRangeCount) `
                "The $($entry.name) work census row count does not match its range."
        }
        Assert-ReportCondition ((Get-CensusLogicalBytes $entry $entry.columnCount $entry.name) -eq $entry.logicalOutputBytes) `
            "The $($entry.name) work census denominator does not match its recorded layout."
        foreach ($field in @("lokadLiveOwnedBytes", "baselineLiveOwnedBytes")) {
            Assert-ReportCondition ($null -ne $entry.$field) `
                "The $($entry.name) work census is missing its $field evidence."
        }
        $expectedPassCount = 2
        if ($catalogCensusPassCounts.ContainsKey($entry.name)) {
            $expectedPassCount = $catalogCensusPassCounts[$entry.name]
        }
        if ($catalogCensusEntries.ContainsKey($entry.name)) {
            $catalogEntry = $catalogCensusEntries[$entry.name]
            Assert-ReportCondition (($entry.physicalType -eq $catalogEntry.physicalType) -and ($entry.valueWidthBytes -eq $catalogEntry.typeWidthBytes) -and ($entry.nullable -eq $catalogEntry.nullable) -and ($entry.consumer -eq $catalogEntry.consumer)) `
                "The $($entry.name) work census case does not match its catalog layout."
        }
        Assert-ReportCondition (($null -ne $entry.passPeaks) -and (@($entry.passPeaks).Count -eq $expectedPassCount)) `
            "The $($entry.name) work census does not carry its expected pass set."
        $passIndex = 0
        foreach ($pass in $entry.passPeaks) {
            Assert-ReportCondition (($null -ne $pass.projection) -and (@($pass.projection).Count -gt 0)) `
                "The $($entry.name) work census pass is missing its projection."
            foreach ($ordinal in $pass.projection) {
                Assert-ReportCondition (($null -ne $ordinal) -and ($ordinal -ge 0)) `
                    "The $($entry.name) work census pass carries an invalid ordinal."
            }
            Assert-ReportCondition (($null -ne $pass.target) -and ($pass.target -gt 0)) `
                "The $($entry.name) work census pass is missing its target."
            $expectedRole = "warm-instrumented"
            if ($passIndex -eq 0) { $expectedRole = "cold-instrumented" }
            Assert-ReportCondition ($pass.role -eq $expectedRole) `
                "The $($entry.name) work census pass carries an unexpected role."
            Assert-ReportCondition ((Get-CensusLogicalBytes $entry @($pass.projection).Count $entry.name) -eq $pass.logicalOutputBytes) `
                "The $($entry.name) work census pass denominator does not match its recorded layout."
            if ($catalogCensusEntries.ContainsKey($entry.name)) {
                $catalogOrdinals = @($catalogCensusEntries[$entry.name].passes[$passIndex].ordinals)
                Assert-ReportCondition ((@($pass.projection) -join ",") -eq ($catalogOrdinals -join ",")) `
                    "The $($entry.name) work census pass does not match its catalog passes."
            }
            $passIndex++
        }
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
    $laneFailed = $false
    foreach ($run in @($windowsRuns + $linuxRuns)) {
        $gateCase = $run.cases | Where-Object name -EQ $caseName
        if (-not $gateCase.passed) { $laneFailed = $true }
    }
    $laneGate = "pass"
    if ($laneFailed) { $laneGate = "FAIL" }
    $lines.Add("| $($caseLabels[$caseName]) | $($cells[0]) | $($cells[1]) | $($cells[2]) | $($cells[3]) | $laneGate |")
}
$lines.Add("")
$lines.Add("Each result is `point estimate / upper 95% bound` for `Lokad / Parquet.NET`; lower is better and the declared gate is an upper bound no greater than 1.05.")
$failedScanLanes = @()
foreach ($scanLane in $caseNames) {
    if ($scanLane -notlike "PreopenedScan/*") { continue }
    foreach ($run in @($windowsRuns + $linuxRuns)) {
        if (-not ($run.cases | Where-Object name -EQ $scanLane).passed) {
            $failedScanLanes += $scanLane
            break
        }
    }
}
$failedScanLanes = @($failedScanLanes | Sort-Object -Unique)
$parityClaim = "PASS"
if ($failedScanLanes.Count -gt 0) { $parityClaim = "FAIL ($($failedScanLanes -join ", "))" }
$lines.Add("- Parity claim (upper 95% bound no greater than 1.05 on every pre-opened scan lane): $parityClaim")
$lines.Add("- Warm metadata open is an accepted parity limitation for small footers and does not join the parity claim.")
$lines.Add("")
$lines.Add("| Workload | Reads / bytes | Pool rents | Peak / output | Bytes cleared | End-scan retained | Retained | Budget |")
$lines.Add("|---|---:|---:|---:|---:|---:|---:|---:|")
foreach ($entry in $census.cases) {
    $peakRatio = $entry.peakPooledBytes / $entry.logicalOutputBytes
    $label = $caseLabels["PreopenedScan/$($entry.name)"]
    if ($null -eq $label) { $label = $entry.name }
    $lines.Add("| $label | $($entry.sourceReadCalls) / $($entry.sourceBytesRead) | $($entry.poolRents) | $($entry.peakPooledBytes) B / $($entry.logicalOutputBytes) B ($($peakRatio.ToString('F3', [Globalization.CultureInfo]::InvariantCulture))x) | $($entry.pooledBytesCleared) | $(if ($null -eq $entry.endOfScanRetainedPoolBytes) { 'unrecorded' } else { $entry.endOfScanRetainedPoolBytes }) | $($entry.retainedPoolBytes) | $(Get-PoolBudgetOutcome $entry.peakPooledBytes $entry.logicalOutputBytes) |")
}

$lines.Add("")
$lines.Add("Per-session allocation normalizes each session by its own operation, row and column counts: operations per block vary across sessions, so pooled bytes per observation would mix denominators. The allocation, GC and CPU budgets below gate only the Lokad scan lanes; string lanes report variable-width bytes without a fixed-width cell gate, and the parity claim stays a separate gate on the paired ratios.")
$lines.Add("")
$lines.Add("| Workload | Session | Lokad B/op | Lokad B/cell | Parquet.NET B/op | Parquet.NET B/cell | Lokad GC 0/1/2 | Parquet.NET GC 0/1/2 | Alloc | GC | CPU |")
$lines.Add("|---|---|---|---|---|---|---|---|---|---|---|")
$pairedBudgetFailures = @()
foreach ($caseName in $caseNames) {
    if ($caseName -notlike "PreopenedScan/*") { continue }
    $isFixedWidth = $caseName -notlike "PreopenedScan/RequiredString*"
    foreach ($osSessions in @(@{ Label = "Windows"; Runs = $windowsRuns }, @{ Label = "Linux"; Runs = $linuxRuns })) {
        $sessionNumber = 0
        foreach ($run in $osSessions.Runs) {
            $sessionNumber++
            $session = "$($osSessions.Label) $sessionNumber"
            $allocationCase = $run.cases | Where-Object name -EQ $caseName
            Assert-ReportCondition (($allocationCase.rowCount -gt 0) -and ($allocationCase.columnCount -gt 0)) `
                "$caseName has no decoded cells in one session."
            $sessionOperations = [long]$allocationCase.operationsPerBlock * $allocationCase.observations.Count
            $sessionCells = [long]$allocationCase.rowCount * [long]$allocationCase.columnCount
            $lokadBytes = 0.0
            $baselineBytes = 0.0
            $lokadGc = @(0, 0, 0)
            $baselineGc = @(0, 0, 0)
            foreach ($observation in $allocationCase.observations) {
                $lokadBytes += $observation.lokadAllocatedBytes
                $baselineBytes += $observation.parquetNetAllocatedBytes
                $lokadGc[0] += $observation.lokadGen0Collections
                $lokadGc[1] += $observation.lokadGen1Collections
                $lokadGc[2] += $observation.lokadGen2Collections
                $baselineGc[0] += $observation.parquetNetGen0Collections
                $baselineGc[1] += $observation.parquetNetGen1Collections
                $baselineGc[2] += $observation.parquetNetGen2Collections
            }
            $lokadBOp = $lokadBytes / $sessionOperations
            $baselineBOp = $baselineBytes / $sessionOperations
            $lokadBCell = ($lokadBytes / $allocationCase.observations.Count) / $sessionCells
            $baselineBCell = ($baselineBytes / $allocationCase.observations.Count) / $sessionCells
            $allocOutcome = "n/a"
            if ($isFixedWidth) {
                $allocOutcome = "pass"
                if (-not ($lokadBCell -lt 0.10)) {
                    $allocOutcome = "FAIL"
                    $pairedBudgetFailures += "$caseName ($session) allocates $($lokadBCell.ToString('F4', [Globalization.CultureInfo]::InvariantCulture)) fixed-width B/cell, above the 0.10 budget"
                }
            }
            $gcOutcome = "pass"
            if (($lokadGc[0] + $lokadGc[1] + $lokadGc[2]) -gt 0) {
                $gcOutcome = "FAIL"
                $pairedBudgetFailures += "$caseName ($session) collects $($lokadGc[0])/$($lokadGc[1])/$($lokadGc[2]) against the zero-GC budget"
            }
            $cpuOutcome = "pass"
            if (-not ($allocationCase.pointRatio -le 3)) {
                $cpuOutcome = "FAIL"
                $pairedBudgetFailures += "$caseName ($session) runs at $($allocationCase.pointRatio) against the 3x CPU budget"
            }
            $lines.Add("| $($caseLabels[$caseName]) | $session | $($lokadBOp.ToString('F3', [Globalization.CultureInfo]::InvariantCulture)) | $($lokadBCell.ToString('F4', [Globalization.CultureInfo]::InvariantCulture)) | $($baselineBOp.ToString('F3', [Globalization.CultureInfo]::InvariantCulture)) | $($baselineBCell.ToString('F4', [Globalization.CultureInfo]::InvariantCulture)) | $($lokadGc[0])/$($lokadGc[1])/$($lokadGc[2]) | $($baselineGc[0])/$($baselineGc[1])/$($baselineGc[2]) | $allocOutcome | $gcOutcome | $cpuOutcome |")
        }
    }
}
$lines.Add("")
$lines.Add("Census pass dimensions record the projection, target, role and budget envelope of every pass; roles distinguish cold-instrumented first passes from warm-instrumented later passes.")
$lines.Add("")
$lines.Add("| Case | Pass | Projection | Target | Role | Batches | ms | Allocated B | GC 0/1/2 | Peak / output | Budget |")
$lines.Add("|---|---|---|---|---|---|---:|---:|---:|---:|---:|")
foreach ($entry in $census.cases) {
    $label = $caseLabels["PreopenedScan/$($entry.name)"]
    if ($null -eq $label) { $label = $entry.name }
    $passNumber = 0
    foreach ($pass in $entry.passPeaks) {
        $passNumber++
        $passDenominator = "unrecorded"
        if (($null -ne $pass.logicalOutputBytes) -and ($pass.logicalOutputBytes -gt 0)) {
            $passRatio = $pass.peakPooledBytes / $pass.logicalOutputBytes
            $passDenominator = "$($pass.peakPooledBytes) B / $($pass.logicalOutputBytes) B (" + $passRatio.ToString('F3', [Globalization.CultureInfo]::InvariantCulture) + "x)"
        }
        $projectionText = "unrecorded"
        if ($null -ne $pass.projection) { $projectionText = ($pass.projection -join ",") }
        $elapsedText = "unrecorded"
        if ($null -ne $pass.elapsedMilliseconds) { $elapsedText = $pass.elapsedMilliseconds.ToString('F3', [Globalization.CultureInfo]::InvariantCulture) }
        $lines.Add("| $label | $passNumber | $projectionText | $($pass.target) | $($pass.role) | $($pass.batchCount) | $elapsedText | $($pass.allocatedBytes) | $($pass.gen0Collections)/$($pass.gen1Collections)/$($pass.gen2Collections) | $passDenominator | $(Get-PoolBudgetOutcome $pass.peakPooledBytes $pass.logicalOutputBytes) |")
    }
}
$lines.Add("")
$lines.Add("Census live memory separates session-held storage from post-disposal growth for both readers.")
$lines.Add("")
$lines.Add("| Case | Layout | Nullable | Consumer | Range | Lokad live B | Baseline live B | Lokad retained | Baseline retained | End-scan retained | Retained |")
$lines.Add("|---|---|---|---|---|---|---:|---:|---:|---:|---:|---:|")
foreach ($entry in $census.cases) {
    $label = $caseLabels["PreopenedScan/$($entry.name)"]
    if ($null -eq $label) { $label = $entry.name }
    if ($null -eq $entry.physicalType) {
        $lines.Add("| $label | unrecorded | unrecorded | unrecorded | unrecorded | unrecorded | unrecorded | unrecorded | unrecorded | unrecorded | unrecorded |")
        continue
    }
    $rangeText = "-"
    if (($null -ne $entry.rowRangeStart) -and ($null -ne $entry.rowRangeCount)) { $rangeText = "$($entry.rowRangeStart)+$($entry.rowRangeCount)" }
    $lines.Add("| $label | $($entry.physicalType)/$($entry.valueWidthBytes) | $($entry.nullable) | $($entry.consumer) | $rangeText | $($entry.lokadLiveOwnedBytes) | $($entry.baselineLiveOwnedBytes) | $($entry.lokadRetainedManagedBytes)/$($entry.lokadRetainedProcessPrivateBytes) | $($entry.baselineRetainedManagedBytes)/$($entry.baselineRetainedProcessPrivateBytes) | $($entry.endOfScanRetainedPoolBytes) | $($entry.retainedPoolBytes) |")
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
    $gateFailures = @()
    if ($failedScanLanes.Count -gt 0) {
        $gateFailures += "The parity claim fails on $($failedScanLanes -join ", ")."
    }
    if ($pairedBudgetFailures.Count -gt 0) {
        $gateFailures += "The paired scan budget fails on " + ($pairedBudgetFailures -join "; ") + "."
    }
    if ($censusBudgetFailures.Count -gt 0) {
        $gateFailures += "The work census fails its 6x pooled-memory budget on " + ($censusBudgetFailures -join "; ") + "."
    }
    Assert-ReportCondition ($gateFailures.Count -eq 0) ($gateFailures -join " ")
}
else {
    [IO.File]::WriteAllText($Document, $expected, [Text.UTF8Encoding]::new($false))
    Write-Output "Rewrote the generated parity report in $Document."
    $gateFailures = @()
    if ($failedScanLanes.Count -gt 0) {
        $gateFailures += "The parity claim fails on $($failedScanLanes -join ", ")."
    }
    if ($pairedBudgetFailures.Count -gt 0) {
        $gateFailures += "The paired scan budget fails on " + ($pairedBudgetFailures -join "; ") + "."
    }
    if ($censusBudgetFailures.Count -gt 0) {
        $gateFailures += "The work census fails its 6x pooled-memory budget on " + ($censusBudgetFailures -join "; ") + "."
    }
    Assert-ReportCondition ($gateFailures.Count -eq 0) ($gateFailures -join " ")
}

