namespace DbSqlLikeMem.Benchmarks.Core;

public abstract partial class BenchmarkSessionBase
{
    /// <summary>
    /// EN: Executes the FOR JSON PATH projection benchmark and keeps the serialized payload alive.
    /// PT-br: Executa o benchmark de projecao FOR JSON PATH e mantem o payload serializado ativo.
    /// </summary>
    protected virtual void RunForJsonPathProjection()
    {
        if (!Dialect.SupportsForJsonClause)
        {
            return;
        }

        var state = GetPreparedSelectTableQueryState("ForJsonPathProjection");
        try
        {
            state.Repo.ExecuteNonQueryAsync($"INSERT INTO {state.Context.TbUsersFullName} (Id, Name) VALUES (2, 'Bob')").GetAwaiter().GetResult();
            var value = state.Service.RunForJsonPathProjectionAsync().GetAwaiter().GetResult();
            GC.KeepAlive(value);
        }
        finally
        {
            state.Repo.ExecuteNonQueryAsync($"DELETE FROM {state.Context.TbUsersFullName} WHERE Id = 2").GetAwaiter().GetResult();
        }
    }

    protected virtual void RunJsonInsertCast()
    {
        var state = GetPreparedNoopQueryState("NoopQuery");
        var service = state.Service;
        var value = service.RunJsonInsertCastAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Executes the JSON insert cast benchmark and keeps the provider result alive when it is null.
    /// PT: Executa o benchmark de insert e cast de JSON e mantem o resultado do provedor vivo quando ele eh nulo.
    /// </summary>
    protected virtual void RunJsonInsertCastReturnsNull()
        => RunJsonInsertCast();

    /// <summary>
    /// EN: Executes the SQL Server ISJSON benchmark and keeps the provider result alive.
    /// PT-br: Executa o benchmark ISJSON do SQL Server e mantem o resultado do provedor vivo.
    /// </summary>
    protected virtual void RunIsJson()
    {
        if (!Dialect.SupportsSqlServerScalarFunction("ISJSON"))
        {
            return;
        }

        var state = GetPreparedNoopQueryState("NoopQuery");
        var value = state.Service.RunIsJsonAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }
}
