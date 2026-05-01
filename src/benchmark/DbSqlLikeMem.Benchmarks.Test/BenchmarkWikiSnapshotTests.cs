namespace DbSqlLikeMem.Benchmarks.Test;

/// <summary>
/// EN: Verifies the published benchmark wiki landing pages keep the expected snapshot shape.
/// PT-br: Verifica se as paginas de entrada publicadas da wiki de benchmark mantem o formato esperado do snapshot.
/// </summary>
public sealed class BenchmarkWikiSnapshotTests
{
    /// <summary>
    /// EN: Verifies the English benchmark landing page matches the published snapshot.
    /// PT-br: Verifica se a pagina inicial de benchmark em ingles corresponde ao snapshot publicado.
    /// </summary>
    [Fact]
    [Trait("Category", "Core")]
    public void BenchmarkResultsHome_ShouldMatchSnapshot()
        => SnapshotTestHelper.AssertFileMatchesSnapshot("docs/Wiki/BenchmarkResults/Home.md", "Fixtures/benchmark-results-home.en.snapshot.md");

    /// <summary>
    /// EN: Verifies the Brazilian Portuguese benchmark landing page matches the published snapshot.
    /// PT-br: Verifica se a pagina inicial de benchmark em portugues brasileiro corresponde ao snapshot publicado.
    /// </summary>
    [Fact]
    [Trait("Category", "Core")]
    public void BenchmarkResultsHomePtBr_ShouldMatchSnapshot()
        => SnapshotTestHelper.AssertFileMatchesSnapshot("docs/Wiki/BenchmarkResults/Home.pt-BR.md", "Fixtures/benchmark-results-home.pt-BR.snapshot.md");
}
