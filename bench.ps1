param(
    [ValidateSet("Core", "Utf8", "Parity", "Paired", "Census", "Catalog", "Materialization", "ColdOpen", "WarmOpen", "Kernel", "Codec", "SteadyState", "Source", "All")]
    [string] $Suite = "Core",
    [string] $Filter = "",
    [string] $PairedCase = "",
    [switch] $EnforceParity
)

$ErrorActionPreference = "Stop"
$project = Join-Path $PSScriptRoot "bench/Lokad.Parquet.Benchmarks/Lokad.Parquet.Benchmarks.csproj"
$benchmarkDll = Join-Path $PSScriptRoot "bench/Lokad.Parquet.Benchmarks/bin/Release/net10.0/Lokad.Parquet.Benchmarks.dll"

Push-Location $PSScriptRoot
try {
    $headRevision = (& git rev-parse --verify HEAD 2>$null)
    $hasHeadRevision = $LASTEXITCODE -eq 0 -and $headRevision
    $workingTreeChanges = @(& git status --porcelain --untracked-files=all 2>$null)
    if ($hasHeadRevision -and $workingTreeChanges.Count -eq 0) {
        $sourceRevision = $headRevision
    }
    else {
        $identityPaths = @(& git ls-files --cached --others --exclude-standard 2>$null) |
            Where-Object {
                $_ -eq "Directory.Build.props" -or
                $_ -eq "global.json" -or
                $_ -eq "bench.ps1" -or
                $_.StartsWith("src/", [StringComparison]::Ordinal) -or
                $_.StartsWith("bench/", [StringComparison]::Ordinal)
            } |
            Sort-Object
        $hasher = [Security.Cryptography.IncrementalHash]::CreateHash(
            [Security.Cryptography.HashAlgorithmName]::SHA256)
        try {
            foreach ($relativePath in $identityPaths) {
                $normalizedPath = $relativePath.Replace('\', '/')
                $hasher.AppendData([Text.Encoding]::UTF8.GetBytes($normalizedPath))
                $hasher.AppendData([byte[]] @(0))
                $fullPath = Join-Path $PSScriptRoot $relativePath
                if (Test-Path -LiteralPath $fullPath -PathType Leaf) {
                    $hasher.AppendData([byte[]] @(1))
                    $hasher.AppendData([IO.File]::ReadAllBytes($fullPath))
                }
                else {
                    $hasher.AppendData([byte[]] @(0))
                }
                $hasher.AppendData([byte[]] @(0))
            }
            $fingerprint = [Convert]::ToHexString($hasher.GetHashAndReset()).ToLowerInvariant()
        }
        finally {
            $hasher.Dispose()
        }
        $sourceRevision = if ($hasHeadRevision) {
            "$headRevision-dirty-$fingerprint"
        }
        else {
            "working-tree-$fingerprint"
        }
    }
    $env:LOKAD_PARQUET_SOURCE_REVISION = $sourceRevision
    $lockPath = Join-Path $PSScriptRoot "bench/Lokad.Parquet.Benchmarks/packages.lock.json"
    $env:LOKAD_PARQUET_PACKAGE_LOCK_HASH =
        (Get-FileHash -LiteralPath $lockPath -Algorithm SHA256).Hash.ToLowerInvariant()
    # powercfg exists only on Windows; collect best-effort evidence per OS without failing.
    $env:LOKAD_PARQUET_POWER_MODE = if ($IsWindows) {
        (& powercfg /getactivescheme) -join " "
    }
    else {
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
            "ColdOpen" { "*MetadataOpenBenchmarks*" }
            "WarmOpen" { "*MetadataOpenBenchmarks*" }
            "Kernel" { "*PlainInt32KernelBenchmarks*" }
            "Codec" { "*SnappyCodecBenchmarks*" }
            "SteadyState" { "*SteadyStateScanBenchmarks*" }
            "Source" { "*SourceScanBenchmarks*" }
            default { "*" }
        }
    }

    & dotnet restore $project --tl:off -v minimal
    if ($LASTEXITCODE -ne 0) {
        exit $LASTEXITCODE
    }

    & dotnet build $project --configuration Release --tl:off --nologo -v minimal --no-restore
    if ($LASTEXITCODE -ne 0) {
        exit $LASTEXITCODE
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

    if ($Suite -eq "Paired") {
        $pairedArguments = @("--paired")
        if ($PairedCase) {
            $pairedArguments += @("--paired-case", $PairedCase)
        }
        if ($EnforceParity) {
            $pairedArguments += "--paired-enforce"
        }
        & dotnet $benchmarkDll @pairedArguments
        exit $LASTEXITCODE
    }

    if ($Suite -eq "Census") {
        & dotnet $benchmarkDll --census
        exit $LASTEXITCODE
    }

    if ($Suite -eq "Catalog") {
        & dotnet $benchmarkDll --catalog
        exit $LASTEXITCODE
    }

    & dotnet $benchmarkDll --filter $Filter
    exit $LASTEXITCODE
}
finally {
    Pop-Location
}
