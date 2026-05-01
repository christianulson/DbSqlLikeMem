namespace DbSqlLikeMem.Benchmarks.Core;

public abstract partial class BenchmarkSessionBase
{
    /// <summary>
    /// EN: Executes the current timestamp temporal benchmark and keeps the provider result alive.
    /// PT-br: Executa o benchmark temporal de timestamp atual e mantem o resultado do provedor vivo.
    /// </summary>
    protected virtual void RunTemporalCurrentTimestamp()
    {
        var state = GetPreparedNoopQueryState("NoopQuery");
        var service = state.Service;
        var value = service.RunTemporalCurrentTimestampAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Executes the date add temporal benchmark and keeps the provider result alive.
    /// PT-br: Executa o benchmark temporal de adicao de data e mantem o resultado do provedor vivo.
    /// </summary>
    protected virtual void RunTemporalDateAdd()
    {
        var state = GetPreparedNoopQueryState("NoopQuery");
        var service = state.Service;
        var value = service.RunTemporalDateAddAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Executes the current time filter temporal benchmark and keeps the provider result alive.
    /// PT-br: Executa o benchmark temporal de filtro por horario atual e mantem o resultado do provedor vivo.
    /// </summary>
    protected virtual void RunTemporalNowWhere()
    {
        var state = GetPreparedUsersQueryState("TemporalNowWhere", (1, "Alice"));
        var value = state.Service.RunTemporalNowWhereAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Executes the current time ordering temporal benchmark and keeps the provider result alive.
    /// PT-br: Executa o benchmark temporal de ordenacao por horario atual e mantem o resultado do provedor vivo.
    /// </summary>
    protected virtual void RunTemporalNowOrderBy()
    {
        var state = GetPreparedUsersQueryState("TemporalNowOrderBy", (1, "Bob"), (2, "Alice"));
        var value = state.Service.RunTemporalNowOrderByAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Executes the scalar temporal matrix benchmark and keeps the provider results alive.
    /// PT: Executa a matriz temporal escalar e mantem os resultados do provedor vivos.
    /// </summary>
    protected virtual void RunScalarTemporalMatrix()
    {
        RunDateScalar();
        RunTemporalCurrentTimestamp();
        RunTemporalDateAdd();
        RunTemporalNowWhere();
        RunTemporalNowOrderBy();
    }

    /// <summary>
    /// EN: Executes the temporal field matrix benchmark and keeps the validated snapshot alive.
    /// PT-br: Executa o benchmark da matriz de campos temporais e mantem o snapshot validado ativo.
    /// </summary>
    protected virtual void RunTemporalFieldMatrix()
    {
        var state = GetPreparedTypedFieldStorageMatrixState("TemporalFieldMatrix");
        var snapshot = state.RunTemporalFieldMatrix();
        GC.KeepAlive(snapshot);
    }

    /// <summary>
    /// EN: Executes the temporal comparison matrix benchmark and keeps the validated snapshot alive.
    /// PT-br: Executa o benchmark da matriz de comparacao temporal e mantem o snapshot validado ativo.
    /// </summary>
    protected virtual void RunTemporalComparisonMatrix()
    {
        var state = GetPreparedTypedFieldStorageMatrixState("TemporalComparisonMatrix");
        var snapshot = state.RunTemporalComparisonMatrix();
        GC.KeepAlive(snapshot);
    }

    /// <summary>
    /// EN: Executes the temporal arithmetic matrix benchmark and keeps the validated snapshot alive.
    /// PT-br: Executa o benchmark da matriz de aritmetica temporal e mantem o snapshot validado ativo.
    /// </summary>
    protected virtual void RunTemporalArithmeticMatrix()
    {
        var state = GetPreparedTypedFieldStorageMatrixState("TemporalArithmeticMatrix");
        var snapshot = state.RunTemporalArithmeticMatrix();
        GC.KeepAlive(snapshot);
    }

    /// <summary>
    /// EN: Executes the DATETRUNC temporal benchmark and keeps the provider result alive.
    /// PT-br: Executa o benchmark temporal DATETRUNC e mantem o resultado do provedor vivo.
    /// </summary>
    protected virtual void RunTemporalDateTrunc()
    {
        if (!Dialect.SupportsSqlServerDateFunction("DATETRUNC"))
        {
            return;
        }

        var state = GetPreparedNoopQueryState("NoopQuery");
        var value = state.Service.RunTemporalDateTruncAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Executes the time-zone offset temporal benchmark and keeps the provider result alive.
    /// PT-br: Executa o benchmark temporal de fuso horario e mantem o resultado do provedor vivo.
    /// </summary>
    protected virtual void RunTemporalTimeZoneOffset()
    {
        if (!Dialect.SupportsSqlServerScalarFunction("TODATETIMEOFFSET"))
        {
            return;
        }

        var state = GetPreparedNoopQueryState("NoopQuery");
        var value = state.Service.RunTemporalTimeZoneOffsetAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Executes the FROMPARTS temporal benchmark and keeps the provider result alive.
    /// PT-br: Executa o benchmark temporal FROMPARTS e mantem o resultado do provedor vivo.
    /// </summary>
    protected virtual void RunTemporalFromParts()
    {
        if (!Dialect.SupportsSqlServerScalarFunction("DATEFROMPARTS"))
        {
            return;
        }

        var state = GetPreparedNoopQueryState("NoopQuery");
        var value = state.Service.RunTemporalFromPartsAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Executes the end-of-month temporal benchmark and keeps the provider result alive.
    /// PT-br: Executa o benchmark temporal de fim de mes e mantem o resultado do provedor vivo.
    /// </summary>
    protected virtual void RunTemporalEndOfMonth()
    {
        if (!Dialect.SupportsSqlServerDateFunction("EOMONTH"))
        {
            return;
        }

        var state = GetPreparedNoopQueryState("NoopQuery");
        var value = state.Service.RunTemporalEndOfMonthAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Executes the DATEDIFF_BIG temporal benchmark and keeps the provider result alive.
    /// PT-br: Executa o benchmark temporal DATEDIFF_BIG e mantem o resultado do provedor vivo.
    /// </summary>
    protected virtual void RunTemporalDateDiffBig()
    {
        if (!Dialect.SupportsSqlServerDateFunction("DATEDIFF_BIG"))
        {
            return;
        }

        var state = GetPreparedNoopQueryState("NoopQuery");
        var value = state.Service.RunTemporalDateDiffBigAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }
}
