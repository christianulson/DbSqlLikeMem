namespace DbSqlLikeMem.Benchmarks.Core;

public abstract partial class BenchmarkSuiteBase
{
    /// <summary>
    /// EN: Executes an EXISTS predicate benchmark query.
    /// PT: Executa uma consulta de benchmark com predicado EXISTS.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void SelectExistsPredicate() => Run(BenchmarkFeatureId.SelectExistsPredicate);

    /// <summary>
    /// EN: Executes a NOT EXISTS predicate benchmark query.
    /// PT: Executa uma consulta de benchmark com predicado NOT EXISTS.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void SelectNotExistsPredicate() => Run(BenchmarkFeatureId.SelectNotExistsPredicate);

    /// <summary>
    /// EN: Executes a LEFT JOIN anti-join benchmark query.
    /// PT: Executa uma consulta de benchmark com anti-join via LEFT JOIN.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void SelectLeftJoinAntiJoin() => Run(BenchmarkFeatureId.SelectLeftJoinAntiJoin);

    /// <summary>
    /// EN: Executes a correlated COUNT subquery benchmark.
    /// PT: Executa um benchmark com subconsulta correlacionada de COUNT.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void SelectCorrelatedCount() => Run(BenchmarkFeatureId.SelectCorrelatedCount);

    /// <summary>
    /// EN: Executes a scalar subquery and CASE matrix benchmark.
    /// PT: Executa um benchmark com subconsulta escalar e matriz CASE.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void SelectScalarCaseMatrix() => Run(BenchmarkFeatureId.SelectScalarCaseMatrix);

    /// <summary>
    /// EN: Executes a GROUP BY HAVING benchmark query.
    /// PT: Executa uma consulta de benchmark com GROUP BY HAVING.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void GroupByHaving() => Run(BenchmarkFeatureId.GroupByHaving);

    /// <summary>
    /// EN: Executes the select variant of the GROUP BY HAVING benchmark query.
    /// PT: Executa a variante select da consulta de benchmark com GROUP BY HAVING.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void SelectGroupByHaving() => Run(BenchmarkFeatureId.GroupByHaving);

    /// <summary>
    /// EN: Executes a UNION ALL projection benchmark query.
    /// PT: Executa uma consulta de benchmark com projeccao UNION ALL.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void UnionAllProjection() => Run(BenchmarkFeatureId.UnionAllProjection);

    /// <summary>
    /// EN: Executes the select variant of the UNION ALL projection benchmark query.
    /// PT: Executa a variante select da consulta de benchmark com projeccao UNION ALL.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void SelectUnionAllProjection() => Run(BenchmarkFeatureId.UnionAllProjection);

    /// <summary>
    /// EN: Executes a UNION projection benchmark query.
    /// PT: Executa uma consulta de benchmark com projeção UNION.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void UnionDistinctProjection() => Run(BenchmarkFeatureId.UnionDistinctProjection);

    /// <summary>
    /// EN: Executes the select variant of the UNION projection benchmark query.
    /// PT: Executa a variante select da consulta de benchmark com projeção UNION.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void SelectUnionDistinctProjection() => Run(BenchmarkFeatureId.UnionDistinctProjection);

    /// <summary>
    /// EN: Executes a DISTINCT projection benchmark query.
    /// PT: Executa uma consulta de benchmark com projeccao DISTINCT.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void DistinctProjection() => Run(BenchmarkFeatureId.DistinctProjection);

    /// <summary>
    /// EN: Executes the select variant of the DISTINCT projection benchmark query.
    /// PT: Executa a variante select da consulta de benchmark com projeccao DISTINCT.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void SelectDistinctProjection() => Run(BenchmarkFeatureId.DistinctProjection);

    /// <summary>
    /// EN: Executes a multi-join aggregate benchmark query.
    /// PT: Executa uma consulta de benchmark com agregacao e multiplos joins.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void MultiJoinAggregate() => Run(BenchmarkFeatureId.MultiJoinAggregate);

    /// <summary>
    /// EN: Executes the select variant of the multi-join aggregate benchmark query.
    /// PT: Executa a variante select da consulta de benchmark com agregacao e multiplos joins.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void SelectMultiJoinAggregate() => Run(BenchmarkFeatureId.MultiJoinAggregate);

    /// <summary>
    /// EN: Executes a scalar subquery benchmark query.
    /// PT: Executa uma consulta de benchmark com subconsulta escalar.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void SelectScalarSubquery() => Run(BenchmarkFeatureId.SelectScalarSubquery);

    /// <summary>
    /// EN: Executes an IN subquery benchmark query.
    /// PT: Executa uma consulta de benchmark com subconsulta IN.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void SelectInSubquery() => Run(BenchmarkFeatureId.SelectInSubquery);

    /// <summary>
    /// EN: Executes a NOT IN subquery benchmark query.
    /// PT: Executa uma consulta de benchmark com subconsulta NOT IN.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void SelectNotInSubquery() => Run(BenchmarkFeatureId.SelectNotInSubquery);

    /// <summary>
    /// EN: Executes a combined BETWEEN, LIKE, and ORDER BY benchmark query.
    /// PT: Executa uma consulta de benchmark combinada com BETWEEN, LIKE e ORDER BY.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void SelectBetweenLikeOrderByMatrix() => Run(BenchmarkFeatureId.SelectBetweenLikeOrderByMatrix);

    /// <summary>
    /// EN: Executes a CROSS APPLY benchmark query.
    /// PT: Executa uma consulta de benchmark com CROSS APPLY.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void CrossApplyProjection() => Run(BenchmarkFeatureId.CrossApplyProjection);

    /// <summary>
    /// EN: Executes the select variant of the CROSS APPLY benchmark query.
    /// PT: Executa a variante select da consulta de benchmark com CROSS APPLY.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void SelectCrossApplyProjection() => Run(BenchmarkFeatureId.CrossApplyProjection);

    /// <summary>
    /// EN: Executes an OUTER APPLY benchmark query.
    /// PT: Executa uma consulta de benchmark com OUTER APPLY.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void OuterApplyProjection() => Run(BenchmarkFeatureId.OuterApplyProjection);

    /// <summary>
    /// EN: Executes the select variant of the OUTER APPLY benchmark query.
    /// PT: Executa a variante select da consulta de benchmark com OUTER APPLY.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void SelectOuterApplyProjection() => Run(BenchmarkFeatureId.OuterApplyProjection);

    /// <summary>
    /// EN: Executes a paged name projection benchmark query.
    /// PT: Executa uma consulta de benchmark com projeção paginada de nomes.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void PagedNameProjection() => Run(BenchmarkFeatureId.PagedNameProjection);

    /// <summary>
    /// EN: Executes the select variant of the paged name projection benchmark query.
    /// PT: Executa a variante select da consulta de benchmark com projeção paginada de nomes.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("advancedquery")]
    public void SelectPagedNameProjection() => Run(BenchmarkFeatureId.PagedNameProjection);
}
