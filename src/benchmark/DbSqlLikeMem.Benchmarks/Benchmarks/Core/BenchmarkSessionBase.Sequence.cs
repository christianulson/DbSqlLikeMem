namespace DbSqlLikeMem.Benchmarks.Core;

public abstract partial class BenchmarkSessionBase
{
    /// <summary>
    /// EN: Creates a temporary sequence and reads its next value.
    /// PT-br: Cria uma sequência temporária e lê o seu próximo valor.
    /// </summary>
    /// <exception cref="NotSupportedException"></exception>
    [BenchmarkFeature(BenchmarkFeatureId.SequenceNextValue)]
    protected virtual void RunSequenceNextValue()
    {
        var state = GetPreparedSequenceState("SequenceNextValue");
        var value = state.RunSequenceNextValue();
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Reads the current value of a temporary sequence and keeps the result alive.
    /// PT-br: Lê o valor atual de uma sequência temporária e mantem o resultado vivo.
    /// </summary>
    [BenchmarkFeature(BenchmarkFeatureId.SequenceCurrentValue)]
    protected virtual void RunSequenceCurrentValue()
    {
        var state = GetPreparedSequenceState("SequenceCurrentValue");
        var value = state.RunSequenceCurrentValue();
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Executes the sequence insert round-trip benchmark and keeps the result alive.
    /// PT-br: Executa o benchmark de round-trip de insert com sequence e mantem o resultado vivo.
    /// </summary>
    [BenchmarkFeature(BenchmarkFeatureId.SequenceInsertRoundTrip)]
    protected virtual void RunSequenceInsertRoundTrip()
    {
        var state = GetPreparedSequenceUsersState("SequenceInsertRoundTrip");
        var value = state.RunSequenceInsertRoundTrip();
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Executes the sequence insert expression benchmark and keeps the result alive.
    /// PT-br: Executa o benchmark de expressao de insert com sequence e mantem o resultado vivo.
    /// </summary>
    [BenchmarkFeature(BenchmarkFeatureId.SequenceInsertExpression)]
    protected virtual void RunSequenceInsertExpression()
    {
        var state = GetPreparedSequenceUsersState("SequenceInsertExpression");
        var value = state.RunSequenceInsertExpression();
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Executes the sequence select projection benchmark and keeps the result alive.
    /// PT-br: Executa o benchmark de projeção select com sequence e mantem o resultado vivo.
    /// </summary>
    [BenchmarkFeature(BenchmarkFeatureId.SequenceSelectProjection)]
    protected virtual void RunSequenceSelectProjection()
    {
        var state = GetPreparedSequenceState("SequenceSelectProjection");
        var value = state.RunSequenceSelectProjection();
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Executes the sequence expression-filter benchmark and keeps the result alive.
    /// PT-br: Executa o benchmark de filtro por expressao com sequence e mantem o resultado vivo.
    /// </summary>
    [BenchmarkFeature(BenchmarkFeatureId.SequenceExpressionFilter)]
    protected virtual void RunSequenceExpressionFilter()
    {
        var state = GetPreparedSequenceExpressionFilterState("SequenceExpressionFilter");
        var value = state.RunSequenceExpressionFilter();
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Executes the sequence CASE/WHERE matrix benchmark and keeps the result alive.
    /// PT-br: Executa o benchmark de matriz CASE/WHERE com sequence e mantem o resultado vivo.
    /// </summary>
    [BenchmarkFeature(BenchmarkFeatureId.SequenceCaseWhereMatrix)]
    protected virtual void RunSequenceCaseWhereMatrix()
    {
        var state = GetPreparedSequenceState("SequenceCaseWhereMatrix");
        var value = state.RunSequenceCaseWhereMatrix();
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Executes the sequence temporal matrix benchmark and keeps the result alive.
    /// PT-br: Executa o benchmark de matriz temporal com sequence e mantem o resultado vivo.
    /// </summary>
    [BenchmarkFeature(BenchmarkFeatureId.SequenceTemporalMatrix)]
    protected virtual void RunSequenceTemporalMatrix()
    {
        var state = GetPreparedSequenceState("SequenceTemporalMatrix");
        var value = state.RunSequenceTemporalMatrix();
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Executes the sequence join-aggregate benchmark and keeps the result alive.
    /// PT-br: Executa o benchmark de join com agregacao e sequence e mantem o resultado vivo.
    /// </summary>
    [BenchmarkFeature(BenchmarkFeatureId.SequenceJoinAggregate)]
    protected virtual void RunSequenceJoinAggregate()
    {
        var state = GetPreparedSequenceState("SequenceJoinAggregate");
        var value = state.RunSequenceJoinAggregate();
        GC.KeepAlive(value);
    }
}
