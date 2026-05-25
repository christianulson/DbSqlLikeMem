namespace DbSqlLikeMem.Benchmarks.Core;

public abstract partial class BenchmarkSuiteBase
{
    /// <summary>
    /// EN: Executes a ROW_NUMBER window benchmark query.
    /// PT-br: Executa uma consulta de benchmark de janela com ROW_NUMBER.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("advanced")]
    public void WindowRowNumber() => Run(BenchmarkFeatureId.WindowRowNumber);

    /// <summary>
    /// EN: Executes a LAG window benchmark query.
    /// PT-br: Executa uma consulta de benchmark de janela com LAG.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("advanced")]
    public void WindowLag() => Run(BenchmarkFeatureId.WindowLag);

    /// <summary>
    /// EN: Executes a LEAD window benchmark query.
    /// PT-br: Executa uma consulta de benchmark de janela com LEAD.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void WindowLead() => Run(BenchmarkFeatureId.WindowLead);

    /// <summary>
    /// EN: Executes a RANK and DENSE_RANK window benchmark query.
    /// PT-br: Executa uma consulta de benchmark de janela com RANK e DENSE_RANK.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void WindowRankDenseRank() => Run(BenchmarkFeatureId.WindowRankDenseRank);

    /// <summary>
    /// EN: Executes a FIRST_VALUE and LAST_VALUE window benchmark query.
    /// PT-br: Executa uma consulta de benchmark de janela com FIRST_VALUE e LAST_VALUE.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void WindowFirstLastValue() => Run(BenchmarkFeatureId.WindowFirstLastValue);

    /// <summary>
    /// EN: Executes an NTILE window benchmark query.
    /// PT-br: Executa uma consulta de benchmark de janela com NTILE.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void WindowNtile() => Run(BenchmarkFeatureId.WindowNtile);

    /// <summary>
    /// EN: Executes a PERCENT_RANK and CUME_DIST window benchmark query.
    /// PT-br: Executa uma consulta de benchmark de janela com PERCENT_RANK e CUME_DIST.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void WindowPercentRankCumeDist() => Run(BenchmarkFeatureId.WindowPercentRankCumeDist);

    /// <summary>
    /// EN: Executes an NTH_VALUE window benchmark query.
    /// PT-br: Executa uma consulta de benchmark de janela com NTH_VALUE.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void WindowNthValue() => Run(BenchmarkFeatureId.WindowNthValue);

    /// <summary>
    /// EN: Executes the select variant of the RANK and DENSE_RANK window benchmark query.
    /// PT-br: Executa a variante select da consulta de benchmark de janela com RANK e DENSE_RANK.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void SelectWindowRankDenseRank() => Run(BenchmarkFeatureId.WindowRankDenseRank);

    /// <summary>
    /// EN: Executes the select variant of the FIRST_VALUE and LAST_VALUE window benchmark query.
    /// PT-br: Executa a variante select da consulta de benchmark de janela com FIRST_VALUE e LAST_VALUE.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void SelectWindowFirstLastValue() => Run(BenchmarkFeatureId.WindowFirstLastValue);

    /// <summary>
    /// EN: Executes the select variant of the NTILE window benchmark query.
    /// PT-br: Executa a variante select da consulta de benchmark de janela com NTILE.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void SelectWindowNtile() => Run(BenchmarkFeatureId.WindowNtile);

    /// <summary>
    /// EN: Executes the select variant of the PERCENT_RANK and CUME_DIST window benchmark query.
    /// PT-br: Executa a variante select da consulta de benchmark de janela com PERCENT_RANK e CUME_DIST.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void SelectWindowPercentRankCumeDist() => Run(BenchmarkFeatureId.WindowPercentRankCumeDist);

    /// <summary>
    /// EN: Executes the select variant of the NTH_VALUE window benchmark query.
    /// PT-br: Executa a variante select da consulta de benchmark de janela com NTH_VALUE.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void SelectWindowNthValue() => Run(BenchmarkFeatureId.WindowNthValue);
}
