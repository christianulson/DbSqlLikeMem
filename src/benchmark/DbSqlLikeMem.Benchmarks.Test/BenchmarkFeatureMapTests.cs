namespace DbSqlLikeMem.Benchmarks.Test;

/// <summary>
/// EN: Verifies the published benchmark feature maps keep a valid structured shape.
/// PT-br: Verifica se os mapas publicados de features do benchmark mantem um formato estruturado valido.
/// </summary>
public sealed class BenchmarkFeatureMapTests
{
    /// <summary>
    /// EN: Verifies the comparative benchmark feature map exposes the expected contract fields.
    /// PT-br: Verifica se o mapa comparativo de features do benchmark expõe os campos de contrato esperados.
    /// </summary>
    [Fact]
    [Trait("Category", "Core")]
    public void BenchmarkFeatureMap_ShouldExposeStructuredComparativeContract()
        => AssertFeatureMap("src/benchmark/DbSqlLikeMem.Benchmarks/benchmark-feature-map.json", "supportsMockFeatures");

    /// <summary>
    /// EN: Verifies the app-specific benchmark feature map exposes the expected contract fields.
    /// PT-br: Verifica se o mapa app-specific de features do benchmark expõe os campos de contrato esperados.
    /// </summary>
    [Fact]
    [Trait("Category", "Core")]
    public void BenchmarkAppSpecificFeatureMap_ShouldExposeStructuredAppSpecificContract()
        => AssertFeatureMap("src/benchmark/DbSqlLikeMem.Benchmarks/benchmark-feature-map.app-specific.json", "supportsAppFeatures");

    private static void AssertFeatureMap(string relativePath, string providerSupportPropertyName)
    {
        using var document = JsonDocument.Parse(File.ReadAllText(Path.Combine(FindRepoRoot(), relativePath.Replace('/', Path.DirectorySeparatorChar))));
        var root = document.RootElement;
        var expectedProvidersById = ProviderCatalog.All.ToDictionary(static provider => provider.Id.ToString(), StringComparer.Ordinal);
        var comparableFeatures = FeatureCatalog.All
            .Where(static feature => feature.Comparable)
            .Select(static feature => feature.Id.ToString())
            .ToHashSet(StringComparer.Ordinal);
        var appSpecificFeatures = FeatureCatalog.All
            .Where(static feature => !feature.Comparable)
            .Select(static feature => feature.Id.ToString())
            .ToHashSet(StringComparer.Ordinal);

        Assert.True(root.TryGetProperty("resultSchema", out var resultSchema));
        Assert.Equal("./benchmark-result.schema.json", resultSchema.GetString());

        Assert.True(root.TryGetProperty("organization", out var organization));
        var expectedOrganization = providerSupportPropertyName == "supportsMockFeatures"
            ? new[] { "Core", "Transactions", "Batch", "Json", "Temporal", "Dialect", "AdvancedQuery", "Setup", "Diagnostics" }
            : ["Diagnostics", "Snapshot", "Setup", "Transactions"];
        var actualOrganization = organization
            .EnumerateArray()
            .Select(static value => value.GetString() ?? string.Empty)
            .ToArray();

        Assert.Equal(expectedOrganization, actualOrganization);

        Assert.True(root.TryGetProperty("providers", out var providers));

        var expectedProviderIds = ProviderCatalog.All
            .Select(static provider => provider.Id.ToString())
            .ToHashSet(StringComparer.Ordinal);
        var actualProviderIds = new HashSet<string>(StringComparer.Ordinal);
        var publishedFeatureIds = new HashSet<string>(StringComparer.Ordinal);

        foreach (var provider in providers.EnumerateArray())
        {
            Assert.True(provider.TryGetProperty("id", out var id));
            var providerId = id.GetString() ?? string.Empty;
            actualProviderIds.Add(providerId);

            Assert.True(expectedProvidersById.TryGetValue(providerId, out var expectedProvider));

            Assert.True(provider.TryGetProperty("displayName", out var displayName));
            Assert.Equal(expectedProvider.DisplayName, displayName.GetString());

            Assert.True(provider.TryGetProperty("latestSimulatedVersion", out var latestSimulatedVersion));
            Assert.Equal(expectedProvider.LatestSimulatedVersion, latestSimulatedVersion.GetString());

            Assert.True(provider.TryGetProperty("externalEngine", out var externalEngine));
            Assert.Equal(expectedProvider.ExternalEngine.ToString(), externalEngine.GetString());

            if (provider.TryGetProperty("externalImage", out var externalImage))
            {
                Assert.Equal(expectedProvider.ExternalImage, externalImage.ValueKind == JsonValueKind.Null ? null : externalImage.GetString());
            }

            Assert.True(provider.TryGetProperty(providerSupportPropertyName, out var supportedFeatures));
            var supportedFeatureIds = supportedFeatures
                .EnumerateArray()
                .Select(static feature => feature.GetString() ?? string.Empty)
                .ToArray();

            Assert.NotEmpty(supportedFeatureIds);

            CheckSuportedFeatures(relativePath, providerSupportPropertyName, comparableFeatures, appSpecificFeatures, publishedFeatureIds, supportedFeatureIds);

            if (providerSupportPropertyName == "supportsMockFeatures" && !expectedProvider.SupportsComparableBenchmarks)
            {
                Assert.Equal(ProviderId.SqlAzure.ToString(), providerId);
            }
        }

        Assert.Equal(expectedProviderIds, actualProviderIds);

        if (providerSupportPropertyName == "supportsMockFeatures")
        {
            Assert.True(comparableFeatures.IsSubsetOf(publishedFeatureIds), "Every comparable feature should appear in at least one comparative provider entry.");
        }
        else
        {
            Assert.True(appSpecificFeatures.SetEquals(publishedFeatureIds));
        }
    }

    private static void CheckSuportedFeatures(
        string relativePath,
        string providerSupportPropertyName,
        HashSet<string> comparableFeatures,
        HashSet<string> appSpecificFeatures,
        HashSet<string> publishedFeatureIds,
        string[] supportedFeatureIds)
    {
        foreach (var featureId in supportedFeatureIds)
        {
            Assert.True(Enum.TryParse<BenchmarkFeatureId>(featureId, ignoreCase: false, out var parsedFeatureId), $"Unknown feature id '{featureId}' in {relativePath}.");
            var catalogFeature = FeatureCatalog.All.FirstOrDefault(feature => feature.Id == parsedFeatureId);
            Assert.NotNull(catalogFeature);

            if (providerSupportPropertyName == "supportsMockFeatures")
            {
                Assert.Contains(featureId, comparableFeatures);
            }
            else
            {
                Assert.Contains(featureId, appSpecificFeatures);
            }

            publishedFeatureIds.Add(featureId);
        }
    }

    private static string FindRepoRoot()
    {
        var directory = new DirectoryInfo(AppContext.BaseDirectory);
        while (directory is not null)
        {
            if (File.Exists(Path.Combine(directory.FullName, "AGENTS.md")))
                return directory.FullName;

            directory = directory.Parent;
        }

        throw new DirectoryNotFoundException("Unable to locate the repository root for the benchmark feature map tests.");
    }
}
