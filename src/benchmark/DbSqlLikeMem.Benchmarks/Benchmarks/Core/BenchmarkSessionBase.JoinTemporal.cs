namespace DbSqlLikeMem.Benchmarks.Core;

public abstract partial class BenchmarkSessionBase
{
    /// <summary>
    /// EN: Executes the joined window and temporal matrix benchmark and keeps the projected snapshot alive.
    /// PT-br: Executa o benchmark da matriz com janela e temporal em join e mantem o snapshot projetado ativo.
    /// </summary>
    [BenchmarkFeature(BenchmarkFeatureId.JoinWindowTemporalMatrix)]
    protected virtual void RunJoinWindowTemporalMatrix()
    {
        var state = GetPreparedUsersOrdersQueryState(
            "JoinWindowTemporalMatrix",
            [(1, "Alice"), (2, "Bob"), (3, "Carla")],
            [(10, 1, "A"), (11, 1, "B"), (12, 2, "C")]);
        var value = state.Service.RunJoinWindowTemporalMatrixAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Executes the joined temporal matrix benchmark and keeps the projected snapshot alive.
    /// PT-br: Executa o benchmark da matriz temporal em join e mantem o snapshot projetado ativo.
    /// </summary>
    [BenchmarkFeature(BenchmarkFeatureId.JoinTemporalMatrix)]
    protected virtual void RunJoinTemporalMatrix()
    {
        var state = GetPreparedUsersOrdersQueryState(
            "JoinTemporalMatrix",
            [(1, "Alice"), (2, "Bob"), (3, "Carla")],
            [(10, 1, "A"), (11, 1, "B"), (12, 2, "C")]);
        var value = state.Service.RunJoinTemporalMatrixAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Executes the joined window, aggregate, and temporal matrix benchmark and keeps the projected snapshot alive.
    /// PT-br: Executa o benchmark da matriz com janela, agregacao e temporal em join e mantem o snapshot projetado ativo.
    /// </summary>
    [BenchmarkFeature(BenchmarkFeatureId.JoinWindowAggregateTemporalMatrix)]
    protected virtual void RunJoinWindowAggregateTemporalMatrix()
    {
        var state = GetPreparedUsersOrdersQueryState(
            "JoinWindowAggregateTemporalMatrix",
            [(1, "Alice"), (2, "Bob"), (3, "Carla")],
            [(10, 1, "A"), (11, 1, "B"), (12, 2, "C")]);
        var value = state.Service.RunJoinWindowAggregateTemporalMatrixAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Executes the APPLY and temporal composite benchmark by chaining the shared APPLY and temporal queries.
    /// PT-br: Executa o benchmark composto de APPLY e temporal encadeando as consultas compartilhadas de APPLY e temporal.
    /// </summary>
    [BenchmarkFeature(BenchmarkFeatureId.ApplyTemporalComposite)]
    protected virtual void RunApplyTemporalComposite()
    {
        RunCrossApplyProjection();
        RunOuterApplyProjection();
        RunJoinTemporalMatrix();
    }

    /// <summary>
    /// EN: Executes the APPLY and window-temporal composite benchmark by chaining the shared APPLY and window queries.
    /// PT-br: Executa o benchmark composto de APPLY e janela-temporal encadeando as consultas compartilhadas de APPLY e janela.
    /// </summary>
    [BenchmarkFeature(BenchmarkFeatureId.ApplyWindowTemporalComposite)]
    protected virtual void RunApplyWindowTemporalComposite()
    {
        RunCrossApplyProjection();
        RunOuterApplyProjection();
        RunJoinWindowMatrix();
        RunJoinWindowTemporalMatrix();
    }

    /// <summary>
    /// EN: Executes the joined window matrix benchmark and keeps the projected snapshot alive.
    /// PT-br: Executa o benchmark da matriz com janela em join e mantem o snapshot projetado ativo.
    /// </summary>
    [BenchmarkFeature(BenchmarkFeatureId.JoinWindowMatrix)]
    protected virtual void RunJoinWindowMatrix()
    {
        var state = GetPreparedUsersOrdersQueryState(
            "JoinWindowMatrix",
            [(1, "Alice"), (2, "Bob"), (3, "Carla")],
            [(10, 1, "A"), (11, 1, "B"), (12, 2, "C")]);
        var value = state.Service.RunJoinWindowMatrixAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }
}
