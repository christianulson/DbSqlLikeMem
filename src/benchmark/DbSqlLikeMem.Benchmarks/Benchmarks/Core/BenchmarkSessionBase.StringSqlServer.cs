namespace DbSqlLikeMem.Benchmarks.Core;

public abstract partial class BenchmarkSessionBase
{
    /// <summary>
    /// EN: Executes the SQL Server string utility benchmark and keeps the provider result alive.
    /// PT-br: Executa o benchmark de utilitarios de string do SQL Server e mantem o resultado do provedor vivo.
    /// </summary>
    [BenchmarkFeature(BenchmarkFeatureId.StringUtilityFunctions)]
    protected virtual void RunStringUtilityFunctions()
    {
        var state = GetPreparedNoopQueryState("NoopQuery");
        var value = state.Service.RunStringUtilityFunctionsAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Executes the SQL Server string metadata benchmark and keeps the provider result alive.
    /// PT-br: Executa o benchmark de metadados de string do SQL Server e mantem o resultado do provedor vivo.
    /// </summary>
    [BenchmarkFeature(BenchmarkFeatureId.StringMetadataFunctions)]
    protected virtual void RunStringMetadataFunctions()
    {
        var state = GetPreparedNoopQueryState("NoopQuery");
        var value = state.Service.RunStringMetadataFunctionsAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Executes the SQL Server STRING_ESCAPE benchmark and keeps the provider result alive.
    /// PT-br: Executa o benchmark STRING_ESCAPE do SQL Server e mantem o resultado do provedor vivo.
    /// </summary>
    [BenchmarkFeature(BenchmarkFeatureId.StringEscape)]
    protected virtual void RunStringEscape()
    {
        if (!Dialect.SupportsSqlServerScalarFunction("STRING_ESCAPE"))
        {
            return;
        }

        var state = GetPreparedNoopQueryState("NoopQuery");
        var value = state.Service.RunStringEscapeAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Executes the SQL Server TRANSLATE benchmark and keeps the provider result alive.
    /// PT-br: Executa o benchmark TRANSLATE do SQL Server e mantem o resultado do provedor vivo.
    /// </summary>
    [BenchmarkFeature(BenchmarkFeatureId.Translate)]
    protected virtual void RunTranslate()
    {
        if (!Dialect.SupportsSqlServerScalarFunction("TRANSLATE"))
        {
            return;
        }

        var state = GetPreparedNoopQueryState("NoopQuery");
        var value = state.Service.RunTranslateAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Executes the SQL Server FORMATMESSAGE benchmark and keeps the provider result alive.
    /// PT-br: Executa o benchmark FORMATMESSAGE do SQL Server e mantem o resultado do provedor vivo.
    /// </summary>
    [BenchmarkFeature(BenchmarkFeatureId.FormatMessage)]
    protected virtual void RunFormatMessage()
    {
        if (!Dialect.SupportsSqlServerScalarFunction("FORMATMESSAGE"))
        {
            return;
        }

        var state = GetPreparedNoopQueryState("NoopQuery");
        var value = state.Service.RunFormatMessageAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Executes the SQL Server FORMAT benchmark and keeps the provider result alive.
    /// PT-br: Executa o benchmark FORMAT do SQL Server e mantem o resultado do provedor vivo.
    /// </summary>
    [BenchmarkFeature(BenchmarkFeatureId.Format)]
    protected virtual void RunFormat()
    {
        if (!Dialect.SupportsSqlServerScalarFunction("FORMAT"))
        {
            return;
        }

        var state = GetPreparedNoopQueryState("NoopQuery");
        var value = state.Service.RunFormatAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }
}
