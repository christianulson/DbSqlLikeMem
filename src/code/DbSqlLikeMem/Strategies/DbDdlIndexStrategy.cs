namespace DbSqlLikeMem;

internal static class DbDdlIndexStrategy
{
    /// <summary>
    /// EN: Implements ExecuteCreateIndex.
    /// PT-br: Implementa ExecuteCreateIndex.
    /// </summary>
    public static DmlExecutionResult ExecuteCreateIndex(
        this DbConnectionMockBase connection,
        SqlCreateIndexQuery query,
        DbParameterCollection pars,
        ISqlDialect dialect)
    {
        _ = pars;
        _ = dialect;
        DmlExecutionResult affected;
        affected = connection.Db.ExecuteWithLock(() => ExecuteCreateIndexImpl(connection, query));

        connection.SetLastFoundRows(affected.AffectedRows);
        return affected;
    }

    private static DmlExecutionResult ExecuteCreateIndexImpl(
        DbConnectionMockBase connection,
        SqlCreateIndexQuery query)
    {
        var tableName = query.Table?.Name;
        ArgumentExceptionCompatible.ThrowIfNullOrWhiteSpace(tableName, nameof(tableName));

        // Cria colunas computadas ocultas para índices funcionais (expression-based)
        if (query.KeyExpressions is { } expressions
            && expressions.Any(static e => e is not null))
        {
            var schemaName = query.Table?.DbName;
            var table = connection.Db.GetTable(tableName!, schemaName);

            for (var i = 0; i < query.KeyColumns.Count; i++)
            {
                var expr = expressions[i];
                if (expr is null)
                    continue;

                var colName = query.KeyColumns[i];
                var col = table.AddColumn(colName, DbType.Object, nullable: true);
                col.GetGenValue = FunctionalIndexExpressionEvaluator.CreateGetGenValue(
                    expr, connection.Db, connection.ExecutionDialect, table);
                col.PersistComputedValue = false;
            }
        }

        connection.CreateIndex(query.IndexName, tableName!, query.KeyColumns, query.Unique, query.Table?.DbName);
        return new DmlExecutionResult();
    }

    /// <summary>
    /// EN: Implements ExecuteDropIndex.
    /// PT-br: Implementa ExecuteDropIndex.
    /// </summary>
    public static DmlExecutionResult ExecuteDropIndex(
        this DbConnectionMockBase connection,
        SqlDropIndexQuery query,
        DbParameterCollection pars,
        ISqlDialect dialect)
    {
        _ = pars;
        _ = dialect;
        DmlExecutionResult affected;
        affected = connection.Db.ExecuteWithLock(() => ExecuteDropIndexImpl(connection, query));

        connection.SetLastFoundRows(affected.AffectedRows);
        return affected;
    }

    private static DmlExecutionResult ExecuteDropIndexImpl(
        DbConnectionMockBase connection,
        SqlDropIndexQuery query)
    {
        connection.DropIndex(query.IndexName, query.IfExists, query.Table?.Name, query.Table?.DbName);
        return new DmlExecutionResult();
    }
}