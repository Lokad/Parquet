<#
.SYNOPSIS
Defines helpers that identify a benchmark build from its source and fixture files.
.DESCRIPTION
Dot-source this file and pass the repository root. The helpers list benchmark
inputs and hash their paths and contents for the recorded source fingerprint.
#>
function Get-BenchmarkIdentityPaths {
    param([Parameter(Mandatory)][string]$RepositoryRoot)
    $identityPaths = @(& git -C $RepositoryRoot ls-files --cached --others --exclude-standard 2>$null) |
        Where-Object {
            $_ -eq "Directory.Build.props" -or
            $_ -eq "global.json" -or
            $_ -eq "bench.ps1" -or
            $_.StartsWith("src/", [StringComparison]::Ordinal) -or
            $_.StartsWith("bench/", [StringComparison]::Ordinal) -or
            $_ -eq "tests/Lokad.Parquet.Tests/ParquetFixtureBuilder.cs" -or
            $_.StartsWith("tests/fixtures/", [StringComparison]::Ordinal)
        } |
        Sort-Object
    return @($identityPaths)
}

function Get-BenchmarkSourceFingerprint {
    param([Parameter(Mandatory)][string]$RepositoryRoot)
    $hasher = [Security.Cryptography.IncrementalHash]::CreateHash(
        [Security.Cryptography.HashAlgorithmName]::SHA256)
    try {
        foreach ($relativePath in (Get-BenchmarkIdentityPaths -RepositoryRoot $RepositoryRoot)) {
            $normalizedPath = $relativePath.Replace('\', '/')
            $hasher.AppendData([Text.Encoding]::UTF8.GetBytes($normalizedPath))
            $hasher.AppendData([byte[]] @(0))
            $fullPath = Join-Path $RepositoryRoot $relativePath
            if (Test-Path -LiteralPath $fullPath -PathType Leaf) {
                $hasher.AppendData([byte[]] @(1))
                $hasher.AppendData([IO.File]::ReadAllBytes($fullPath))
            }
            else {
                $hasher.AppendData([byte[]] @(0))
            }
            $hasher.AppendData([byte[]] @(0))
        }
        return [Convert]::ToHexString($hasher.GetHashAndReset()).ToLowerInvariant()
    }
    finally {
        $hasher.Dispose()
    }
}
