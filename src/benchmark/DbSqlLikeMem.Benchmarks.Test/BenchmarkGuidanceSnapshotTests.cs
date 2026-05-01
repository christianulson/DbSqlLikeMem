namespace DbSqlLikeMem.Benchmarks.Test;

/// <summary>
/// EN: Verifies the published benchmark guidance pages keep the expected snapshot shape.
/// PT-br: Verifica se as paginas publicadas de guia de benchmark mantem o formato esperado do snapshot.
/// </summary>
public sealed class BenchmarkGuidanceSnapshotTests
{
    /// <summary>
    /// EN: Verifies the benchmark environment guide matches the published snapshot.
    /// PT-br: Verifica se o guia de ambiente de benchmark corresponde ao snapshot publicado.
    /// </summary>
    [Fact]
    [Trait("Category", "Core")]
    public void BenchmarkEnvironmentGuide_ShouldMatchSnapshot()
        => SnapshotTestHelper.AssertFileMatchesSnapshot(
            "docs/features-backlog/benchmarks/benchmark-environment.md",
            "Fixtures/benchmark-environment.snapshot.md");

    /// <summary>
    /// EN: Verifies the benchmark baseline guide matches the published snapshot.
    /// PT-br: Verifica se o guia de baseline de benchmark corresponde ao snapshot publicado.
    /// </summary>
    [Fact]
    [Trait("Category", "Core")]
    public void BenchmarkBaselineGuide_ShouldMatchSnapshot()
        => SnapshotTestHelper.AssertFileMatchesSnapshot(
            "docs/features-backlog/benchmarks/benchmark-baseline.md",
            "Fixtures/benchmark-baseline.snapshot.md");
}
