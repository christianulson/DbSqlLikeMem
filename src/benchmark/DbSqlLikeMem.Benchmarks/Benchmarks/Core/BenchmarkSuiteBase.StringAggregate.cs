namespace DbSqlLikeMem.Benchmarks.Core;

public abstract partial class BenchmarkSuiteBase
{
    /// <summary>
    /// EN: Executes the ordered string aggregate benchmark.
    /// PT: Executa o benchmark de agregacao de strings ordenada.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("dialect")]
    public void StringAggregateOrdered() => Run(BenchmarkFeatureId.StringAggregateOrdered);

    /// <summary>
    /// EN: Executes the distinct string aggregate benchmark.
    /// PT: Executa o benchmark de agregacao de strings distinta.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("dialect")]
    public void StringAggregateDistinct() => Run(BenchmarkFeatureId.StringAggregateDistinct);

    /// <summary>
    /// EN: Executes the custom-separator string aggregate benchmark.
    /// PT: Executa o benchmark de agregacao de strings com separador personalizado.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("dialect")]
    public void StringAggregateCustomSeparator() => Run(BenchmarkFeatureId.StringAggregateCustomSeparator);

    /// <summary>
    /// EN: Executes the large-group string aggregate benchmark.
    /// PT: Executa o benchmark de agregacao de strings para grupo grande.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("dialect")]
    public void StringAggregateLargeGroup() => Run(BenchmarkFeatureId.StringAggregateLargeGroup);

    /// <summary>
    /// EN: Executes the string aggregate summary matrix benchmark.
    /// PT: Executa o benchmark da matriz resumo de agregacao de strings.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("dialect")]
    public void StringAggregateSummaryMatrix() => Run(BenchmarkFeatureId.StringAggregateSummaryMatrix);

    /// <summary>
    /// EN: Executes the grouped string aggregate matrix benchmark.
    /// PT: Executa o benchmark da matriz agrupada de agregacao de strings.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("dialect")]
    public void StringAggregateGroupCaseMatrix() => Run(BenchmarkFeatureId.StringAggregateGroupCaseMatrix);

    /// <summary>
    /// EN: Executes the string aggregation summary matrix alias benchmark.
    /// PT: Executa o benchmark alias da matriz resumo de agregacao de strings.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("dialect")]
    public void StringAggregationSummaryMatrix() => Run(BenchmarkFeatureId.StringAggregationSummaryMatrix);

    /// <summary>
    /// EN: Executes the grouped string aggregation alias benchmark.
    /// PT: Executa o benchmark alias da matriz agrupada de agregacao de strings.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("dialect")]
    public void StringAggregationGroupCaseMatrix() => Run(BenchmarkFeatureId.StringAggregationGroupCaseMatrix);

    /// <summary>
    /// EN: Executes the string aggregation variants benchmark.
    /// PT: Executa o benchmark das variantes de agregacao de strings.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("dialect")]
    public void StringAggregationVariants() => Run(BenchmarkFeatureId.StringAggregationVariants);
}
