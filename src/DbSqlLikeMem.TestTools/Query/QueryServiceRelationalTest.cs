namespace DbSqlLikeMem.TestTools.Query;

public partial class QueryServiceTest<T>
{
    /// <summary>
    /// EN: Counts the rows returned by a select over the configured users table.
    /// PT: Conta as linhas retornadas por um select na tabela de usuarios configurada.
    /// </summary>
    public int RunRowCountAfterSelect(params object[] pars)
    {
        var users = (string)pars[0];
        var count = CountReaderRows($"SELECT * FROM {users}");
        if (count != 2)
        {
            throw new InvalidOperationException($"Unexpected select rowcount for {Dialect.DisplayName}: {count}.");
        }

        return count;
    }

    /// <summary>
    /// EN: Executes a simple CTE query against the configured users table.
    /// PT: Executa uma consulta CTE simples na tabela de usuarios configurada.
    /// </summary>
    public int RunCteSimple(params object[] pars)
    {
        var users = (string)pars[0];
        var value = Convert.ToInt32(ExecuteScalar(Dialect.CteSimple(users)), CultureInfo.InvariantCulture);
        if (value != 1)
        {
            throw new InvalidOperationException($"Unexpected CTE result for {Dialect.DisplayName}: {value}.");
        }

        return value;
    }

    /// <summary>
    /// EN: Executes a ROW_NUMBER window query against the configured users table.
    /// PT: Executa uma consulta de janela ROW_NUMBER na tabela de usuarios configurada.
    /// </summary>
    public int RunWindowRowNumber(params object[] pars)
    {
        var users = (string)pars[0];
        var value = Convert.ToInt32(ExecuteScalar(Dialect.WindowRowNumber(users)), CultureInfo.InvariantCulture);
        GC.KeepAlive(value);
        return value;
    }

    /// <summary>
    /// EN: Executes a LAG window query against the configured users table.
    /// PT: Executa uma consulta de janela LAG na tabela de usuarios configurada.
    /// </summary>
    public int RunWindowLag(params object[] pars)
    {
        var users = (string)pars[0];
        var value = Convert.ToInt32(ExecuteScalar(Dialect.WindowLag(users)), CultureInfo.InvariantCulture);
        GC.KeepAlive(value);
        return value;
    }

    /// <summary>
    /// EN: Executes an EXISTS predicate query against the configured users and orders tables.
    /// PT: Executa uma consulta com predicado EXISTS nas tabelas de usuarios e pedidos configuradas.
    /// </summary>
    public int RunSelectExistsPredicate(params object[] pars)
    {
        var users = (string)pars[0];
        var orders = (string)pars[1];
        var value = Convert.ToInt32(ExecuteScalar(Dialect.SelectExistsPredicate(users, orders)), CultureInfo.InvariantCulture);
        GC.KeepAlive(value);
        return value;
    }

    /// <summary>
    /// EN: Executes a correlated count query against the configured users and orders tables.
    /// PT: Executa uma consulta de contagem correlacionada nas tabelas de usuarios e pedidos configuradas.
    /// </summary>
    public int RunSelectCorrelatedCount(params object[] pars)
    {
        var users = (string)pars[0];
        var orders = (string)pars[1];
        var value = Convert.ToInt32(ExecuteScalar(Dialect.SelectCorrelatedCount(users, orders)), CultureInfo.InvariantCulture);
        GC.KeepAlive(value);
        return value;
    }

    /// <summary>
    /// EN: Executes a GROUP BY HAVING query against the configured users and orders tables.
    /// PT: Executa uma consulta GROUP BY HAVING nas tabelas de usuarios e pedidos configuradas.
    /// </summary>
    public int RunGroupByHaving(params object[] pars)
    {
        var users = (string)pars[0];
        var orders = (string)pars[1];
        var value = Convert.ToInt32(ExecuteScalar(Dialect.GroupByHaving(users, orders)), CultureInfo.InvariantCulture);
        GC.KeepAlive(value);
        return value;
    }

    /// <summary>
    /// EN: Executes a UNION ALL projection query against the configured users table.
    /// PT: Executa uma consulta de projeção UNION ALL na tabela de usuarios configurada.
    /// </summary>
    public int RunUnionAllProjection(params object[] pars)
    {
        var users = (string)pars[0];
        var value = Convert.ToInt32(ExecuteScalar(Dialect.UnionAllProjection(users)), CultureInfo.InvariantCulture);
        GC.KeepAlive(value);
        return value;
    }

    /// <summary>
    /// EN: Executes a DISTINCT projection query against the configured users table.
    /// PT: Executa uma consulta de projeção DISTINCT na tabela de usuarios configurada.
    /// </summary>
    public int RunDistinctProjection(params object[] pars)
    {
        var users = (string)pars[0];
        var value = Convert.ToInt32(ExecuteScalar(Dialect.DistinctProjection(users)), CultureInfo.InvariantCulture);
        GC.KeepAlive(value);
        return value;
    }

    /// <summary>
    /// EN: Executes a multi-join aggregate query against the configured users and orders tables.
    /// PT: Executa uma consulta agregada com multiplos joins nas tabelas de usuarios e pedidos configuradas.
    /// </summary>
    public int RunMultiJoinAggregate(params object[] pars)
    {
        var users = (string)pars[0];
        var orders = (string)pars[1];
        var value = Convert.ToInt32(ExecuteScalar(Dialect.MultiJoinAggregate(users, orders)), CultureInfo.InvariantCulture);
        GC.KeepAlive(value);
        return value;
    }

    /// <summary>
    /// EN: Executes a scalar subquery projection against the configured users and orders tables.
    /// PT: Executa uma projeção com subconsulta escalar nas tabelas de usuarios e pedidos configuradas.
    /// </summary>
    public object? RunSelectScalarSubquery(params object[] pars)
    {
        var users = (string)pars[0];
        var orders = (string)pars[1];
        var value = ExecuteScalar(Dialect.SelectScalarSubquery(users, orders));
        GC.KeepAlive(value);
        return value;
    }

    /// <summary>
    /// EN: Executes an IN subquery predicate against the configured users and orders tables.
    /// PT: Executa um predicado IN com subconsulta nas tabelas de usuarios e pedidos configuradas.
    /// </summary>
    public int RunSelectInSubquery(params object[] pars)
    {
        var users = (string)pars[0];
        var orders = (string)pars[1];
        var value = Convert.ToInt32(ExecuteScalar(Dialect.SelectInSubquery(users, orders)), CultureInfo.InvariantCulture);
        GC.KeepAlive(value);
        return value;
    }

    /// <summary>
    /// EN: Executes a CROSS APPLY style projection against the configured users and orders tables.
    /// PT: Executa uma projeção no estilo CROSS APPLY nas tabelas de usuarios e pedidos configuradas.
    /// </summary>
    public int RunCrossApplyProjection(params object[] pars)
    {
        var users = (string)pars[0];
        var orders = (string)pars[1];
        var value = Convert.ToInt32(ExecuteScalar(Dialect.CrossApplyProjection(users, orders)), CultureInfo.InvariantCulture);
        GC.KeepAlive(value);
        return value;
    }

    /// <summary>
    /// EN: Executes an OUTER APPLY style projection against the configured users and orders tables.
    /// PT: Executa uma projeção no estilo OUTER APPLY nas tabelas de usuarios e pedidos configuradas.
    /// </summary>
    public int RunOuterApplyProjection(params object[] pars)
    {
        var users = (string)pars[0];
        var orders = (string)pars[1];
        var value = Convert.ToInt32(ExecuteScalar(Dialect.OuterApplyProjection(users, orders)), CultureInfo.InvariantCulture);
        GC.KeepAlive(value);
        return value;
    }

    /// <summary>
    /// EN: Executes a partition-pruning style select against the configured users table.
    /// PT: Executa um select no estilo partition pruning na tabela de usuarios configurada.
    /// </summary>
    public int RunPartitionPruningSelect(params object[] pars)
    {
        var users = (string)pars[0];
        var value = Convert.ToInt32(ExecuteScalar($"SELECT COUNT(*) FROM {users} WHERE Id BETWEEN 5 AND 10"), CultureInfo.InvariantCulture);
        GC.KeepAlive(value);
        return value;
    }

    /// <summary>
    /// EN: Executes a pivot-style count query against the configured users table.
    /// PT: Executa uma consulta de contagem no estilo pivot na tabela de usuarios configurada.
    /// </summary>
    public int RunPivotCount(params object[] pars)
    {
        var users = (string)pars[0];
        var sql = $"SELECT SUM(CASE WHEN Name LIKE 'A%' THEN 1 ELSE 0 END) + SUM(CASE WHEN Name LIKE 'B%' THEN 1 ELSE 0 END) FROM {users}";
        var value = Convert.ToInt32(ExecuteScalar(sql), CultureInfo.InvariantCulture);
        GC.KeepAlive(value);
        return value;
    }

    private int CountReaderRows(string sql, DbTransaction? transaction = null)
    {
        using var command = Connection.CreateCommand();
        command.CommandText = sql;
        if (transaction is not null)
        {
            command.Transaction = transaction;
        }

        using var reader = command.ExecuteReader();
        var count = 0;
        while (reader.Read())
        {
            count++;
        }

        return count;
    }
}
