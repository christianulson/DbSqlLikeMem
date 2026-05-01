namespace DbSqlLikeMem.Benchmarks.Core;

public abstract partial class BenchmarkSessionBase
{
    /// <summary>
    /// EN: Executes the provider-specific string aggregation query over sample user names.
    /// PT-br: Executa a consulta de agregação de strings específica do provedor sobre nomes de usuários de exemplo.
    /// </summary>
    /// <exception cref="NotSupportedException"></exception>
    [BenchmarkFeature(BenchmarkFeatureId.StringAggregate)]
    protected virtual void RunStringAggregate()
    {
        var state = GetPreparedUsersQueryState(
            "StringAggregate",
            (1, "Charlie"), (2, "Alice"), (3, "Bob"));
        var value = state.Service.RunStringAggregateAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    [BenchmarkFeature(BenchmarkFeatureId.StringAggregateOrdered)]
    protected virtual void RunStringAggregateOrdered()
    {
        var state = GetPreparedUsersQueryState(
            "StringAggregateOrdered",
            (1, "Charlie"), (2, "Alice"), (3, "Bob"));
        var value = state.Service.RunStringAggregateOrderedAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    [BenchmarkFeature(BenchmarkFeatureId.StringAggregateDistinct)]
    protected virtual void RunStringAggregateDistinct()
    {
        var state = GetPreparedUsersQueryState(
            "StringAggregateDistinct",
            (1, "Charlie"), (2, "Alice"), (3, "Bob"));
        var value = state.Service.RunStringAggregateDistinctAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    [BenchmarkFeature(BenchmarkFeatureId.StringAggregateCustomSeparator)]
    protected virtual void RunStringAggregateCustomSeparator()
    {
        var state = GetPreparedUsersQueryState(
            "StringAggregateCustomSeparator",
            (1, "Charlie"), (2, "Alice"), (3, "Bob"));
        var value = state.Service.RunStringAggregateCustomSeparatorAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    [BenchmarkFeature(BenchmarkFeatureId.StringAggregateLargeGroup)]
    protected virtual void RunStringAggregateLargeGroup()
    {
        var seedRows = new[]
        {
            (1, "Charlie"),
            (2, "Alice"),
            (3, "Bob"),
            (4, "Delta"),
            (5, "Echo"),
        };
        var state = GetPreparedUsersQueryState("StringAggregateLargeGroup", seedRows);
        var value = state.Service.RunStringAggregateLargeGroupAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    [BenchmarkFeature(BenchmarkFeatureId.StringAggregateSummaryMatrix)]
    protected virtual void RunStringAggregateSummaryMatrix()
    {
        if (!Dialect.SupportsStringAggregate)
        {
            return;
        }

        var state = GetPreparedUsersQueryState(
            "StringAggregateSummaryMatrix",
            (1, "Charlie"), (2, "Alice"), (3, "Bob"));
        var snapshot = state.Service.RunStringAggregateSummaryMatrixAsync().GetAwaiter().GetResult();
        GC.KeepAlive(snapshot);
    }

    [BenchmarkFeature(BenchmarkFeatureId.StringAggregateGroupCaseMatrix)]
    protected virtual void RunStringAggregateGroupCaseMatrix()
    {
        if (!Dialect.SupportsStringAggregate)
        {
            return;
        }

        var state = GetPreparedUsersQueryState(
            "StringAggregateGroupCaseMatrix",
            (1, "Charlie"), (2, "Alice"), (3, "Bob"));
        var snapshot = state.Service.RunStringAggregateGroupCaseMatrixAsync().GetAwaiter().GetResult();
        GC.KeepAlive(snapshot);
    }

    [BenchmarkFeature(BenchmarkFeatureId.StringAggregationSummaryMatrix)]
    protected virtual void RunStringAggregationSummaryMatrix()
        => RunStringAggregateSummaryMatrix();

    [BenchmarkFeature(BenchmarkFeatureId.StringAggregationGroupCaseMatrix)]
    protected virtual void RunStringAggregationGroupCaseMatrix()
        => RunStringAggregateGroupCaseMatrix();

    /// <summary>
    /// EN: Executes the full string aggregation variants benchmark and keeps the provider results alive.
    /// PT-br: Executa o benchmark completo das variantes de agregacao de strings e mantem os resultados do provedor vivos.
    /// </summary>
    [BenchmarkFeature(BenchmarkFeatureId.StringAggregationVariants)]
    protected virtual void RunStringAggregationVariants()
    {
        RunStringAggregate();
        RunStringAggregateOrdered();
        RunStringAggregateDistinct();
        RunStringAggregateCustomSeparator();
        RunStringAggregateLargeGroup();
    }
}
