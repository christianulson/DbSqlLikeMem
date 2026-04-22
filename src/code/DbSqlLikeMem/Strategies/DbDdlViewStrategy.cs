namespace DbSqlLikeMem;

internal static class DbDdlViewStrategy
{
    /// <summary>
    /// EN: Implements ExecuteCreateView.
    /// PT: Implementa ExecuteCreateView.
    /// </summary>
    public static DmlExecutionResult ExecuteCreateView(
        this DbConnectionMockBase connection,
        SqlCreateViewQuery query,
        DbParameterCollection pars,
        ISqlDialect dialect)
    {
        _ = pars;
        _ = dialect;
        DmlExecutionResult affected;
        affected = connection.Db.ExecuteWithLock(() => ExecuteCreateViewImpl(connection, query));

        connection.SetLastFoundRows(affected.AffectedRows);
        return affected;
    }

    private static DmlExecutionResult ExecuteCreateViewImpl(
        DbConnectionMockBase connection,
        SqlCreateViewQuery query)
    {
        connection.AddView(query);
        return new DmlExecutionResult();
    }

    /// <summary>
    /// EN: Implements ExecuteDropView.
    /// PT: Implementa ExecuteDropView.
    /// </summary>
    public static DmlExecutionResult ExecuteDropView(
        this DbConnectionMockBase connection,
        SqlDropViewQuery query,
        DbParameterCollection pars,
        ISqlDialect dialect)
    {
        _ = pars;
        _ = dialect;
        DmlExecutionResult affected;
        affected = connection.Db.ExecuteWithLock(() => ExecuteDropViewImpl(connection, query));

        connection.SetLastFoundRows(affected.AffectedRows);
        return affected;
    }

    private static DmlExecutionResult ExecuteDropViewImpl(
        DbConnectionMockBase connection,
        SqlDropViewQuery query)
    {
        var viewName = query.Table?.Name;
        ArgumentExceptionCompatible.ThrowIfNullOrWhiteSpace(viewName, nameof(viewName));
        connection.DropView(viewName!, query.IfExists, query.Table?.DbName);
        return new DmlExecutionResult();
    }
}