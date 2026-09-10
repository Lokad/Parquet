$ErrorActionPreference = "Stop"
$project = Join-Path $PSScriptRoot "src/Lokad.Parquet/Lokad.Parquet.csproj"

Push-Location $PSScriptRoot
try {
    & dotnet restore $project --tl:off -v minimal
    if ($LASTEXITCODE -ne 0) {
        exit $LASTEXITCODE
    }

    # Release-only packaging: a Release build creates the package through
    # GeneratePackageOnBuild into artifacts/nuget.
    & dotnet build $project --configuration Release --tl:off --nologo -v minimal --no-restore
    exit $LASTEXITCODE
}
finally {
    Pop-Location
}
