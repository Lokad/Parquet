<#
.SYNOPSIS
Builds and runs Release benchmarks on one logical processor on Linux.
.DESCRIPTION
Run from a native Linux filesystem with PowerShell and taskset. Select a suite
or filter. Use -SkipBuild to run an existing Release build.
Publication runs the representative read and write comparison and records
timings, allocations, and diagnostics under artifacts/benchmarks/publication.
#>
param(
    [ValidateSet("Publication", "Core", "Utf8", "Parity", "Paired", "Census", "CensusSmoke", "Catalog", "Materialization", "ColdOpen", "WarmOpen", "Kernel", "Codec", "SteadyState", "Source", "All")]
    [string] $Suite = "Core",
    [string] $Filter = "",
    [string] $PairedCase = "",
    [switch] $EnforceParity,
    [switch] $ForceScalar,
    [switch] $SkipBuild
)

$ErrorActionPreference = "Stop"
if (-not $IsLinux) { throw "Benchmarks require Linux and a native Linux checkout." }
$project = Join-Path $PSScriptRoot "bench/Lokad.Parquet.Benchmarks/Lokad.Parquet.Benchmarks.csproj"
$benchmarkDll = Join-Path $PSScriptRoot "bench/Lokad.Parquet.Benchmarks/bin/Release/net10.0/Lokad.Parquet.Benchmarks.dll"
$cpuList = Get-Content -LiteralPath "/proc/self/status" | Select-String '^Cpus_allowed_list:\s*(\d+)'
$processor = $cpuList.Matches[0].Groups[1].Value

function Invoke-Benchmark {
    & taskset --cpu-list $processor dotnet $benchmarkDll @args
}

Push-Location $PSScriptRoot
. (Join-Path $PSScriptRoot "bench/benchmark-identity.ps1")
try {
    $headRevision = (& git rev-parse --verify HEAD 2>$null)
    $hasHeadRevision = $LASTEXITCODE -eq 0 -and $headRevision
    $workingTreeChanges = @(& git status --porcelain --untracked-files=all 2>$null)
    $fingerprint = Get-BenchmarkSourceFingerprint -RepositoryRoot $PSScriptRoot
    if ($hasHeadRevision -and $workingTreeChanges.Count -eq 0) {
        $sourceRevision = $headRevision
    }
    else {
        $sourceRevision = if ($hasHeadRevision) {
            "$headRevision-dirty-$fingerprint"
        }
        else {
            "working-tree-$fingerprint"
        }
    }
    $env:LOKAD_PARQUET_SOURCE_REVISION = $sourceRevision
    $env:LOKAD_PARQUET_SOURCE_FINGERPRINT = $fingerprint
    $lockPath = Join-Path $PSScriptRoot "bench/Lokad.Parquet.Benchmarks/packages.lock.json"
    $env:LOKAD_PARQUET_PACKAGE_LOCK_HASH =
        (Get-FileHash -LiteralPath $lockPath -Algorithm SHA256).Hash.ToLowerInvariant()
    $env:LOKAD_PARQUET_POWER_MODE = & {
        $governor = Get-Content -LiteralPath "/sys/devices/system/cpu/cpu0/cpufreq/scaling_governor" -ErrorAction SilentlyContinue
        if ($governor) { "scaling_governor: $($governor -join " ")" } else { "power management unavailable" }
    }
    $env:LOKAD_PARQUET_BENCHMARK_MODE = if ($Suite -eq "ColdOpen") { "ColdOpen" } else { "Qualification" }

    if (-not $Filter) {
        $Filter = switch ($Suite) {
            "Core" { "*CoreScanBenchmarks*" }
            "Utf8" { "*PreopenedUtf8ScanBenchmarks*" }
            "Parity" { "*Preopened*ScanBenchmarks*" }
            "Materialization" { "*PreopenedMaterializationBenchmarks*" }
            "ColdOpen" { "*MetadataOpenBenchmarks.LokadOpen* *MetadataOpenBenchmarks.ParquetNetOpen*" }
            "WarmOpen" { "*MetadataOpenBenchmarks*" }
            "Kernel" { "*PlainInt32KernelBenchmarks*" }
            "Codec" { "*SnappyCodecBenchmarks*" }
            "SteadyState" { "*SteadyStateScanBenchmarks*" }
            "Source" { "*SourceScanBenchmarks*" }
            default { "*" }
        }
    }

    if (-not $SkipBuild) {
        & dotnet restore $project --locked-mode --tl:off -v minimal
        if ($LASTEXITCODE -ne 0) {
            exit $LASTEXITCODE
        }

        & dotnet build $project --configuration Release --tl:off --nologo -v minimal --no-restore
        if ($LASTEXITCODE -ne 0) {
            exit $LASTEXITCODE
        }
    } elseif (-not (Test-Path -LiteralPath $benchmarkDll -PathType Leaf)) {
        throw "The Release benchmark assembly is missing. Run without -SkipBuild to build it."
    }

    # Filesystem qualification is decided once in C#: links resolve to a final
    # path and the Linux mount table decides whether storage is Windows-backed
    # (BenchmarkHostPolicy.CheckNativeWorkspacePath, also enforced at collection
    # time). The entry point only forwards the workspace paths after the build.
    foreach ($workspacePath in @(
        @{ Path = $PSScriptRoot; Role = "Repository" },
        @{ Path = (Split-Path -Parent $benchmarkDll); Role = "Build output" },
        @{ Path = (Join-Path $PSScriptRoot "artifacts/benchmarks"); Role = "Artifacts output" })) {
        & dotnet $benchmarkDll --check-path $workspacePath.Path --check-role $workspacePath.Role
        if ($LASTEXITCODE -ne 0) {
            exit $LASTEXITCODE
        }
    }

    # The scalar lane is an explicit host switch: the paired and census lanes
    # run in this process and read it, while BenchmarkDotNet workers establish
    # their own lane in-process.
    $scalarArguments = @()
    if ($ForceScalar) {
        $scalarArguments += "--force-scalar"
    }

    if ($Suite -eq "Publication") {
        Invoke-Benchmark --publication @scalarArguments
        exit $LASTEXITCODE
    }

    if ($Suite -eq "Paired") {
        $pairedArguments = @("--paired")
        if ($PairedCase) {
            $pairedArguments += @("--paired-case", $PairedCase)
        }
        if ($EnforceParity) {
            $pairedArguments += "--paired-enforce"
        }
        Invoke-Benchmark @pairedArguments @scalarArguments
        exit $LASTEXITCODE
    }

    if ($Suite -eq "Census") {
        Invoke-Benchmark --census @scalarArguments
        exit $LASTEXITCODE
    }

    if ($Suite -eq "CensusSmoke") {
        Invoke-Benchmark --census-smoke
        exit $LASTEXITCODE
    }

    if ($Suite -eq "Catalog") {
        Invoke-Benchmark --catalog
        exit $LASTEXITCODE
    }

    # The entry point accepts one --filter flag per invocation, so
    # multi-pattern suites like ColdOpen run once per pattern and keep
    # excluding diagnostic benchmarks while sharing the class.
    foreach ($pattern in $Filter.Split(' ', [StringSplitOptions]::RemoveEmptyEntries)) {
        Invoke-Benchmark --filter $pattern @scalarArguments
        if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
    }
    exit 0
}
finally {
    Pop-Location
}
