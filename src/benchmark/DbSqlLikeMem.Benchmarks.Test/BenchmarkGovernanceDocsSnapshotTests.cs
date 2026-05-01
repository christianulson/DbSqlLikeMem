namespace DbSqlLikeMem.Benchmarks.Test;

/// <summary>
/// EN: Verifies the published benchmark governance docs keep the expected snapshot shape.
/// PT-br: Verifica se os documentos publicos de governanca do benchmark mantem o formato esperado do snapshot.
/// </summary>
public sealed class BenchmarkGovernanceDocsSnapshotTests
{
    /// <summary>
    /// EN: Verifies the execution status guide matches the published snapshot.
    /// PT-br: Verifica se o guia de status de execucao corresponde ao snapshot publicado.
    /// </summary>
    [Fact]
    [Trait("Category", "Core")]
    public void BenchmarkExecutionStatusGuide_ShouldMatchSnapshot()
        => SnapshotTestHelper.AssertFileMatchesSnapshot(
            "docs/features-backlog/benchmarks/benchmark-execution-status.md",
            "Fixtures/benchmark-execution-status.snapshot.md");

    /// <summary>
    /// EN: Verifies the feature history guide matches the published snapshot.
    /// PT-br: Verifica se o guia de historico de features corresponde ao snapshot publicado.
    /// </summary>
    [Fact]
    [Trait("Category", "Core")]
    public void BenchmarkFeatureHistoryGuide_ShouldMatchSnapshot()
        => SnapshotTestHelper.AssertFileMatchesSnapshot(
            "docs/features-backlog/benchmarks/benchmark-feature-history.md",
            "Fixtures/benchmark-feature-history.snapshot.md");

    /// <summary>
    /// EN: Verifies the manual runbook guide matches the published snapshot.
    /// PT-br: Verifica se o guia manual de execucao corresponde ao snapshot publicado.
    /// </summary>
    [Fact]
    [Trait("Category", "Core")]
    public void BenchmarkManualRunbookGuide_ShouldMatchSnapshot()
        => SnapshotTestHelper.AssertFileMatchesSnapshot(
            "docs/features-backlog/benchmarks/benchmark-manual-runbook.md",
            "Fixtures/benchmark-manual-runbook.snapshot.md");
}
