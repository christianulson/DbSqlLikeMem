using DbSqlLikeMem.Benchmarks.Core;
using DbSqlLikeMem.TestTools;
using Xunit;

namespace DbSqlLikeMem.Benchmarks.Test;

/// <summary>
/// EN: Covers the benchmark automation catalog and its command-line validation flow.
/// PT: Cobre o catalogo de automacao de benchmarks e seu fluxo de validacao por linha de comando.
/// </summary>
public sealed class BenchmarkCatalogValidationTests(
        ITestOutputHelper helper
    ) : XUnitTestBase(helper)
{
    /// <summary>
    /// EN: Ensures the benchmark catalog stays aligned with the public benchmark suite surface.
    /// PT: Garante que o catalogo de benchmarks permaneça alinhado com a superficie publica da suite.
    /// </summary>
    [Fact]
    [Trait("Category", "Core")]
    public void ValidateCatalog_ShouldHaveNoIssues()
    {
        var report = BenchmarkCatalogValidator.Validate();

        Assert.True(report.IsValid, report.Format());
    }

    /// <summary>
    /// EN: Ensures the benchmark run options parse the catalog validation flag without affecting benchmark arguments.
    /// PT: Garante que as opcoes de execucao de benchmark analisem a flag de validacao do catalogo sem afetar os argumentos do benchmark.
    /// </summary>
    [Fact]
    [Trait("Category", "Core")]
    public void Parse_ShouldRecognizeValidateCatalogFlag()
    {
        var options = BenchmarkRunOptions.Parse(["--validate-catalog", "--inprocess", "--filter", "*Insert*"]);

        Assert.True(options.ValidateCatalog);
        Assert.True(options.UseInProcess);
        Assert.False(options.IsTest);
        Assert.Equal(["--filter", "*Insert*"], options.BenchmarkDotNetArgs);
    }
}
