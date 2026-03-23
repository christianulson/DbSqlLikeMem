namespace DbSqlLikeMem.TestTools.Query;

public partial class QueryServiceTest<T>
{
    /// <summary>
    /// EN: Executes a scalar date query and keeps the provider result alive.
    /// PT: Executa uma consulta escalar de data e mantém o resultado do provedor vivo.
    /// </summary>
    public object? RunDateScalar()
    {
        var value = ExecuteScalar(Dialect.DateScalar());
        GC.KeepAlive(value);
        return value;
    }

    /// <summary>
    /// EN: Executes the JSON scalar benchmark when the provider supports it.
    /// PT: Executa o benchmark escalar de JSON quando o provedor suporta isso.
    /// </summary>
    public object? RunJsonScalarRead()
    {
        if (!Dialect.SupportsJsonScalarRead)
        {
            throw new NotSupportedException($"{Dialect.DisplayName} does not support the JSON scalar benchmark.");
        }

        var value = ExecuteScalar(Dialect.JsonScalarRead("{\"name\":\"Alice\"}"));
        GC.KeepAlive(value);
        return value;
    }

    /// <summary>
    /// EN: Executes the nested JSON path benchmark when the provider supports it.
    /// PT: Executa o benchmark de caminho JSON aninhado quando o provedor suporta isso.
    /// </summary>
    public object? RunJsonPathRead()
    {
        if (!Dialect.SupportsJsonScalarRead)
        {
            throw new NotSupportedException($"{Dialect.DisplayName} does not support the JSON path benchmark.");
        }

        var value = ExecuteScalar(Dialect.JsonPathRead("{\"user\":{\"name\":\"Alice\"}}"));
        GC.KeepAlive(value);
        return value;
    }

    /// <summary>
    /// EN: Executes the JSON insert and cast benchmark when the provider supports JSON reads.
    /// PT: Executa o benchmark de insert e cast de JSON quando o provedor suporta leituras JSON.
    /// </summary>
    public object? RunJsonInsertCast()
    {
        if (!Dialect.SupportsJsonScalarRead)
        {
            throw new NotSupportedException($"{Dialect.DisplayName} does not support the JSON insert/cast benchmark.");
        }

        var value = ExecuteScalar(Dialect.JsonScalarRead("{\"value\":42,\"text\":\"Alice\"}"));
        GC.KeepAlive(value);
        return value;
    }

    /// <summary>
    /// EN: Executes a current timestamp scalar query and keeps the provider result alive.
    /// PT: Executa uma consulta escalar de timestamp atual e mantém o resultado do provedor vivo.
    /// </summary>
    public object? RunTemporalCurrentTimestamp()
    {
        var value = ExecuteScalar(Dialect.TemporalCurrentTimestamp());
        GC.KeepAlive(value);
        return value;
    }

    /// <summary>
    /// EN: Executes a temporal date-add query and keeps the provider result alive.
    /// PT: Executa uma consulta temporal de soma de data e mantém o resultado do provedor vivo.
    /// </summary>
    public object? RunTemporalDateAdd()
    {
        var value = ExecuteScalar(Dialect.TemporalDateAdd());
        GC.KeepAlive(value);
        return value;
    }

    /// <summary>
    /// EN: Executes the provider string aggregation benchmark over sample user names.
    /// PT: Executa o benchmark de agregacao de strings do provedor sobre nomes de usuarios de exemplo.
    /// </summary>
    public string? RunStringAggregate(params object[] pars)
    {
        if (!Dialect.SupportsStringAggregate)
        {
            throw new NotSupportedException($"{Dialect.DisplayName} does not support the string aggregate benchmark.");
        }

        var users = (string)pars[0];
        var value = Convert.ToString(ExecuteScalar(Dialect.StringAggregate(users)), CultureInfo.InvariantCulture);
        GC.KeepAlive(value);
        return value;
    }

    /// <summary>
    /// EN: Executes the ordered string aggregation benchmark over sample user names.
    /// PT: Executa o benchmark de agregacao ordenada de strings sobre nomes de usuarios de exemplo.
    /// </summary>
    public string? RunStringAggregateOrdered(params object[] pars)
    {
        if (!Dialect.SupportsStringAggregate)
        {
            throw new NotSupportedException($"{Dialect.DisplayName} does not support the string aggregate benchmark.");
        }

        var users = (string)pars[0];
        var value = Convert.ToString(ExecuteScalar(Dialect.StringAggregateOrdered(users)), CultureInfo.InvariantCulture);
        GC.KeepAlive(value);
        return value;
    }

    /// <summary>
    /// EN: Executes the distinct string aggregation benchmark over sample user names.
    /// PT: Executa o benchmark de agregacao distinta de strings sobre nomes de usuarios de exemplo.
    /// </summary>
    public string? RunStringAggregateDistinct(params object[] pars)
    {
        if (!Dialect.SupportsStringAggregate)
        {
            throw new NotSupportedException($"{Dialect.DisplayName} does not support the string aggregate benchmark.");
        }

        var users = (string)pars[0];
        var value = Convert.ToString(ExecuteScalar(Dialect.StringAggregateDistinct(users)), CultureInfo.InvariantCulture);
        GC.KeepAlive(value);
        return value;
    }

    /// <summary>
    /// EN: Executes the custom-separator string aggregation benchmark over sample user names.
    /// PT: Executa o benchmark de agregacao com separador customizado sobre nomes de usuarios de exemplo.
    /// </summary>
    public string? RunStringAggregateCustomSeparator(params object[] pars)
    {
        if (!Dialect.SupportsStringAggregate)
        {
            throw new NotSupportedException($"{Dialect.DisplayName} does not support the string aggregate benchmark.");
        }

        var users = (string)pars[0];
        var value = Convert.ToString(ExecuteScalar(Dialect.StringAggregateCustomSeparator(users, ";")), CultureInfo.InvariantCulture);
        GC.KeepAlive(value);
        return value;
    }

    /// <summary>
    /// EN: Executes the large-group string aggregation benchmark over sample user names.
    /// PT: Executa o benchmark de agregacao de strings em grupo grande sobre nomes de usuarios de exemplo.
    /// </summary>
    public string? RunStringAggregateLargeGroup(params object[] pars)
    {
        if (!Dialect.SupportsStringAggregate)
        {
            throw new NotSupportedException($"{Dialect.DisplayName} does not support the string aggregate benchmark.");
        }

        var users = (string)pars[0];
        var value = Convert.ToString(ExecuteScalar(Dialect.StringAggregateLargeGroup(users)), CultureInfo.InvariantCulture);
        GC.KeepAlive(value);
        return value;
    }

    /// <summary>
    /// EN: Reads a current-time predicate query result from the configured users table.
    /// PT: Lê o resultado de uma consulta com predicado de tempo atual na tabela de usuarios configurada.
    /// </summary>
    public object? RunTemporalNowWhere(params object[] pars)
    {
        var users = (string)pars[0];
        var value = ExecuteScalar(Dialect.TemporalNowWhere(users));
        GC.KeepAlive(value);
        return value;
    }

    /// <summary>
    /// EN: Reads a current-time ordering query result from the configured users table.
    /// PT: Lê o resultado de uma consulta com ordenação por tempo atual na tabela de usuarios configurada.
    /// </summary>
    public object? RunTemporalNowOrderBy(params object[] pars)
    {
        var users = (string)pars[0];
        var value = ExecuteScalar(Dialect.TemporalNowOrderBy(users));
        GC.KeepAlive(value);
        return value;
    }
}
