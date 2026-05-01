namespace DbSqlLikeMem.Benchmarks.Test;

/// <summary>
/// EN: Verifies the public benchmark support docs keep the expected snapshot shape.
/// PT-br: Verifica se os documentos publicos de apoio ao benchmark mantem o formato esperado do snapshot.
/// </summary>
public sealed class BenchmarkPublicDocsSnapshotTests
{
    /// <summary>
    /// EN: Verifies the observability guide matches the published snapshot.
    /// PT-br: Verifica se o guia de observabilidade corresponde ao snapshot publicado.
    /// </summary>
    [Fact]
    [Trait("Category", "Core")]
    public void BenchmarkObservabilityGuide_ShouldMatchSnapshot()
        => SnapshotTestHelper.AssertFileMatchesSnapshot(
            "docs/features-backlog/benchmarks/benchmark-observability.md",
            "Fixtures/benchmark-observability.snapshot.md");

    /// <summary>
    /// EN: Verifies the benchmark README matches the published snapshot.
    /// PT-br: Verifica se o README do benchmark corresponde ao snapshot publicado.
    /// </summary>
    [Fact]
    [Trait("Category", "Core")]
    public void BenchmarkReadme_ShouldMatchSnapshot()
        => SnapshotTestHelper.AssertFileMatchesSnapshot(
            "src/benchmark/DbSqlLikeMem.Benchmarks/README.Benchmarks.md",
            "Fixtures/benchmark-readme.snapshot.md");
}
