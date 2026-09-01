using System.Security.Cryptography;
using System.Text.Json;

namespace Lokad.Parquet.Tests;

public sealed class FixtureManifestTests
{
    [Fact]
    public void EveryFixtureEntryHasProvenanceAndCurrentGeneratorHash()
    {
        var manifestPath = Path.Combine(RepositoryTestPaths.Root, "tests", "fixtures", "manifest.json");
        using var document = JsonDocument.Parse(File.ReadAllBytes(manifestPath));
        var fixtures = document.RootElement.GetProperty("fixtures");
        Assert.NotEqual(0, fixtures.GetArrayLength());

        foreach (var fixture in fixtures.EnumerateArray())
        {
            Assert.False(string.IsNullOrWhiteSpace(fixture.GetProperty("origin").GetString()));
            Assert.False(string.IsNullOrWhiteSpace(fixture.GetProperty("license").GetString()));
            Assert.False(string.IsNullOrWhiteSpace(fixture.GetProperty("generation").GetString()));
            var generator = fixture.GetProperty("generator").GetString() ??
                throw new InvalidOperationException("A fixture manifest generator path is null.");
            var expectedHash = fixture.GetProperty("generatorSha256").GetString();
            var bytes = File.ReadAllBytes(Path.Combine(RepositoryTestPaths.Root, generator.Replace('/', Path.DirectorySeparatorChar)));
            Assert.Equal(expectedHash, Convert.ToHexStringLower(SHA256.HashData(bytes)));
        }
    }
}
