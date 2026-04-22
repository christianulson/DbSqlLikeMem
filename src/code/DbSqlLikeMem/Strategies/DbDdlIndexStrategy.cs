namespace DbSqlLikeMem;

internal static class DbDdlIndexStrategy
{
    /// <summary>
    /// EN: Implements ExecuteCreateIndex.
    /// PT: Implementa ExecuteCreateIndex.
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
        connection.CreateIndex(query.IndexName, tableName!, query.KeyColumns, query.Unique, query.Table?.DbName);
        return new DmlExecutionResult();
    }

    /// <summary>
    /// EN: Implements ExecuteDropIndex.
    /// PT: Implementa ExecuteDropIndex.
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