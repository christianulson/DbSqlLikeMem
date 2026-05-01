namespace DbSqlLikeMem.Benchmarks.Core;

/// <summary>
/// EN: Provides the sequence benchmark set for provider suites that support sequence features.
/// PT: Fornece o conjunto de benchmarks de sequence para suites de provedores que suportam sequence.
/// </summary>
public abstract class SequenceBenchmarkSuiteBase : BenchmarkSuiteBase
{
    /// <summary>
    /// EN: Executes a sequence next-value benchmark.
    /// PT: Executa um benchmark de proximo valor de sequencia.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("core")]
    public void SequenceNextValue() => Run(BenchmarkFeatureId.SequenceNextValue);

    [Benchmark]
    [BenchmarkCategory("core")]
    public void SequenceNextValues() => Run(BenchmarkFeatureId.SequenceNextValue);

    /// <summary>
    /// EN: Executes a sequence current-value benchmark.
    /// PT: Executa um benchmark de valor atual de sequencia.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("core")]
    public void SequenceCurrentValue() => Run(BenchmarkFeatureId.SequenceCurrentValue);

    /// <summary>
    /// EN: Executes a sequence insert round-trip benchmark.
    /// PT: Executa um benchmark de round-trip de insert com sequence.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("core")]
    public void SequenceInsertRoundTrip() => Run(BenchmarkFeatureId.SequenceInsertRoundTrip);

    /// <summary>
    /// EN: Executes a sequence insert-expression benchmark.
    /// PT: Executa um benchmark de insert com expressao de sequence.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("core")]
    public void SequenceInsertExpression() => Run(BenchmarkFeatureId.SequenceInsertExpression);

    /// <summary>
    /// EN: Executes a sequence select-projection benchmark.
    /// PT: Executa um benchmark de projecao select com sequence.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("core")]
    public void SequenceSelectProjection() => Run(BenchmarkFeatureId.SequenceSelectProjection);

    /// <summary>
    /// EN: Executes a sequence expression-filter benchmark.
    /// PT: Executa um benchmark de filtro com expressao de sequence.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("core")]
    public void SequenceExpressionFilter() => Run(BenchmarkFeatureId.SequenceExpressionFilter);

    /// <summary>
    /// EN: Executes a sequence CASE/WHERE matrix benchmark.
    /// PT: Executa um benchmark de matriz CASE/WHERE com sequence.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("core")]
    public void SequenceCaseWhereMatrix() => Run(BenchmarkFeatureId.SequenceCaseWhereMatrix);

    /// <summary>
    /// EN: Executes a sequence temporal matrix benchmark.
    /// PT: Executa um benchmark de matriz temporal com sequence.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("core")]
    public void SequenceTemporalMatrix() => Run(BenchmarkFeatureId.SequenceTemporalMatrix);

    /// <summary>
    /// EN: Executes a sequence join aggregate benchmark.
    /// PT: Executa um benchmark de join agregado com sequence.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("core")]
    public void SequenceJoinAggregate() => Run(BenchmarkFeatureId.SequenceJoinAggregate);
}
