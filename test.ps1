param(
    [ValidateSet("Debug", "Release")]
    [string] $Configuration = "Debug",
    [switch] $SkipBuild,
    [string] $Filter
)

$ErrorActionPreference = "Stop"
$solution = Join-Path $PSScriptRoot "Lokad.Parquet.slnx"

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
    Pop-Location
}
