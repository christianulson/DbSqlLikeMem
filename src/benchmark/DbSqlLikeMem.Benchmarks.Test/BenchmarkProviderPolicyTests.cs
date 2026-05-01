namespace DbSqlLikeMem.Benchmarks.Test;

/// <summary>
/// EN: Verifies the benchmark provider catalog keeps the current execution policy explicit.
/// PT-br: Verifica se o catalogo de provedores do benchmark mantem explicita a politica atual de execucao.
/// </summary>
public sealed class BenchmarkProviderPolicyTests
{
    /// <summary>
    /// EN: Verifies SQL Azure remains the only provider excluded from comparable benchmark runs.
    /// PT-br: Verifica se SQL Azure continua sendo o unico provedor excluido de execucoes comparaveis de benchmark.
    /// </summary>
    [Fact]
    public void ProviderCatalog_ShouldKeepSqlAzureAsTheOnlyNonComparableProvider()
    {
        var nonComparableProviders = ProviderCatalog.All
            .Where(static provider => !provider.SupportsComparableBenchmarks)
            .ToArray();

        Assert.Single(nonComparableProviders);
        Assert.Equal(ProviderId.SqlAzure, nonComparableProviders[0].Id);
        Assert.Equal(BenchmarkEngine.NotAvailable, nonComparableProviders[0].ExternalEngine);
    }

    /// <summary>
    /// EN: Verifies comparable providers keep a concrete execution engine configured.
    /// PT-br: Verifica se os provedores comparaveis mantem um mecanismo de execucao concreto configurado.
    /// </summary>
    [Fact]
    public void ComparableProviders_ShouldNotUseTheNotAvailableEngine()
    {
        Assert.All(
            ProviderCatalog.All.Where(static provider => provider.SupportsComparableBenchmarks),
            provider => Assert.NotEqual(BenchmarkEngine.NotAvailable, provider.ExternalEngine));
    }
}
