namespace DbSqlLikeMem.Benchmarks.Core;

public abstract partial class BenchmarkSessionBase
{
    /// <summary>
    /// EN: Executes the SQL Server metadata-functions benchmark and keeps the provider result alive.
    /// PT-br: Executa o benchmark de funcoes de metadados do SQL Server e mantem o resultado do provedor vivo.
    /// </summary>
    [BenchmarkFeature(BenchmarkFeatureId.SqlServerMetadataFunctions)]
    protected virtual void RunSqlServerMetadataFunctions()
    {
        var state = GetPreparedNoopQueryState("NoopQuery");
        var value = state.Service.RunSqlServerMetadataFunctionsAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Executes the SQL Server SCOPE_IDENTITY benchmark and keeps the provider result alive.
    /// PT-br: Executa o benchmark SCOPE_IDENTITY do SQL Server e mantem o resultado do provedor vivo.
    /// </summary>
    [BenchmarkFeature(BenchmarkFeatureId.ScopeIdentity)]
    protected virtual void RunScopeIdentity()
    {
        var state = GetPreparedNoopQueryState("NoopQuery");
        var value = state.Service.RunScopeIdentityAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Executes the SQL Server system-functions benchmark and keeps the provider result alive.
    /// PT-br: Executa o benchmark de funcoes de sistema do SQL Server e mantem o resultado do provedor vivo.
    /// </summary>
    [BenchmarkFeature(BenchmarkFeatureId.SqlServerSystemFunctions)]
    protected virtual void RunSqlServerSystemFunctions()
    {
        var state = GetPreparedNoopQueryState("NoopQuery");
        var value = state.Service.RunSqlServerSystemFunctionsAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Executes the SQL Server special-functions benchmark and keeps the provider result alive.
    /// PT-br: Executa o benchmark de funcoes especiais do SQL Server e mantem o resultado do provedor vivo.
    /// </summary>
    [BenchmarkFeature(BenchmarkFeatureId.SqlServerSpecialFunctions)]
    protected virtual void RunSqlServerSpecialFunctions()
    {
        var state = GetPreparedNoopQueryState("NoopQuery");
        var value = state.Service.RunSqlServerSpecialFunctionsAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Executes the SQL Server context-functions benchmark and keeps the provider result alive.
    /// PT-br: Executa o benchmark de funcoes de contexto do SQL Server e mantem o resultado do provedor vivo.
    /// </summary>
    [BenchmarkFeature(BenchmarkFeatureId.SqlServerContextFunctions)]
    protected virtual void RunSqlServerContextFunctions()
    {
        var state = GetPreparedNoopQueryState("NoopQuery");
        var value = state.Service.RunSqlServerContextFunctionsAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Executes the SQL Server transaction-state benchmark and keeps the provider result alive.
    /// PT-br: Executa o benchmark de estado de transacao do SQL Server e mantem o resultado do provedor vivo.
    /// </summary>
    [BenchmarkFeature(BenchmarkFeatureId.SqlServerTransactionStateFunctions)]
    protected virtual void RunSqlServerTransactionStateFunctions()
    {
        var state = GetPreparedNoopQueryState("NoopQuery");
        var value = state.Service.RunSqlServerTransactionStateFunctionsAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Executes the SQL Server session-functions benchmark and keeps the provider result alive.
    /// PT-br: Executa o benchmark de funcoes de sessao do SQL Server e mantem o resultado do provedor vivo.
    /// </summary>
    [BenchmarkFeature(BenchmarkFeatureId.SqlServerSessionFunctions)]
    protected virtual void RunSqlServerSessionFunctions()
    {
        var state = GetPreparedNoopQueryState("NoopQuery");
        var value = state.Service.RunSqlServerSessionFunctionsAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Executes the SQL Server aggregate-functions benchmark and keeps the provider result alive.
    /// PT-br: Executa o benchmark de funcoes agregadas do SQL Server e mantem o resultado do provedor vivo.
    /// </summary>
    [BenchmarkFeature(BenchmarkFeatureId.SqlServerAggregateFunctions)]
    protected virtual void RunSqlServerAggregateFunctions()
    {
        var state = GetPreparedNoopQueryState("NoopQuery");
        var value = state.Service.RunSqlServerAggregateFunctionsAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Executes the SQL Server percentile-aggregate benchmark and keeps the provider result alive.
    /// PT-br: Executa o benchmark de agregacao percentual do SQL Server e mantem o resultado do provedor vivo.
    /// </summary>
    [BenchmarkFeature(BenchmarkFeatureId.PercentileAggregateFunctions)]
    protected virtual void RunPercentileAggregateFunctions()
    {
        if (!Dialect.SupportsSqlServerAggregateFunction("PERCENTILE_CONT")
            || !Dialect.SupportsSqlServerAggregateFunction("PERCENTILE_DISC"))
        {
            return;
        }

        var state = GetPreparedUsersQueryState("PercentileAggregateFunctions", (1, "Ana"), (2, "Bob"));
        var value = state.Service.RunPercentileAggregatesAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }
}
