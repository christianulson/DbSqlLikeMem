namespace DbSqlLikeMem.Benchmarks.Test;

/// <summary>
/// EN: Verifies the published regression summary page keeps the expected placeholder snapshot.
/// PT-br: Verifica se a pagina publicada de resumo de regressao mantem o snapshot esperado do placeholder.
/// </summary>
public sealed class BenchmarkRegressionSummarySnapshotTests
{
    /// <summary>
    /// EN: Verifies the regression summary landing page matches the published snapshot.
    /// PT-br: Verifica se a pagina de resumo de regressao corresponde ao snapshot publicado.
    /// </summary>
    [Fact]
    [Trait("Category", "Core")]
    public void BenchmarkRegressionSummary_ShouldMatchSnapshot()
        => SnapshotTestHelper.AssertFileMatchesSnapshot(
            "docs/Wiki/BenchmarkResults/benchmark-regression-summary.md",
            "Fixtures/benchmark-regression-summary.snapshot.md");
}
