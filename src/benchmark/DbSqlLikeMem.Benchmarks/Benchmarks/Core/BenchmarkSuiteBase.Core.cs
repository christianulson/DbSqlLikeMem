namespace DbSqlLikeMem.Benchmarks.Core;

public abstract partial class BenchmarkSuiteBase
{
    /// <summary>
    /// EN: Executes a row-count benchmark after an insert operation.
    /// PT-br: Executa um benchmark de contagem de linhas apos uma operacao de insert.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("core")]
    public void RowCountAfterInsert() => Run(BenchmarkFeatureId.RowCountAfterInsert);

    /// <summary>
    /// EN: Executes a row-count benchmark after an update operation.
    /// PT-br: Executa um benchmark de contagem de linhas apos uma operacao de update.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("core")]
    public void RowCountAfterUpdate() => Run(BenchmarkFeatureId.RowCountAfterUpdate);

    /// <summary>
    /// EN: Executes a row-count benchmark after a select operation.
    /// PT-br: Executa um benchmark de contagem de linhas apos uma operacao de select.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("core")]
    public void RowCountAfterSelect() => Run(BenchmarkFeatureId.RowCountAfterSelect);

    /// <summary>
    /// EN: Executes a simple common table expression benchmark.
    /// PT-br: Executa um benchmark simples de expressao de tabela comum.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("advanced")]
    public void CteSimple() => Run(BenchmarkFeatureId.CteSimple);

    /// <summary>
    /// EN: Executes the select variant of the simple common table expression benchmark.
    /// PT-br: Executa a variante select do benchmark simples de expressao de tabela comum.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("advanced")]
    public void SelectCteSimple() => Run(BenchmarkFeatureId.CteSimple);
}
