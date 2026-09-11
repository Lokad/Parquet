param(
    [ValidateSet("Debug", "Release")]
    [string] $Configuration = "Debug",
    [switch] $SkipBuild,
    [string] $Filter,
    [switch] $ForceScalar
)

$ErrorActionPreference = "Stop"
$solution = Join-Path $PSScriptRoot "Lokad.Parquet.slnx"
$scalarState = $env:LOKAD_PARQUET_FORCE_SCALAR

Push-Location $PSScriptRoot
try {
    if (-not $SkipBuild) {
        & dotnet restore $solution --tl:off -v minimal
        if ($LASTEXITCODE -ne 0) {
            exit $LASTEXITCODE
        }

        & dotnet build $solution --configuration $Configuration --tl:off --nologo -v minimal --no-restore
        if ($LASTEXITCODE -ne 0) {
            exit $LASTEXITCODE
        }
    }

    if ($ForceScalar) {
        $env:LOKAD_PARQUET_FORCE_SCALAR = "1"
        Write-Output "Forced-scalar mode: every decoder takes its scalar lane."
    }

    $testArguments = @(
        "test",
        $solution,
        "--configuration", $Configuration,
        "--tl:off",
        "--nologo",
        "-v", "minimal",
        "--no-build",
        "--no-restore"
    )

    if ($Filter) {
        $testArguments += @("--filter", $Filter)
    }

    & dotnet @testArguments
    exit $LASTEXITCODE
}
finally {
    if ($null -eq $scalarState) {
        Remove-Item Env:\LOKAD_PARQUET_FORCE_SCALAR -ErrorAction SilentlyContinue
    }
    else {
        $env:LOKAD_PARQUET_FORCE_SCALAR = $scalarState
    }
    Pop-Location
}
