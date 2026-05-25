namespace DbSqlLikeMem;

internal static partial class DbSelectIntoAndInsertSelectStrategies
{
    /// <summary>
    /// EN: Creates a scalar function definition in the current schema.
    /// PT-br: Cria uma definicao de funcao escalar no schema atual.
    /// </summary>
    public static DmlExecutionResult ExecuteCreateFunction(
        this DbConnectionMockBase connection,
        SqlCreateFunctionQuery query,
        DbParameterCollection pars,
        ISqlDialect dialect)
    {
        _ = pars;
        _ = dialect;
        DmlExecutionResult affected;
        affected = connection.Db.ExecuteWithLock(() => ExecuteCreateFunctionImpl(connection, query));

        connection.SetLastFoundRows(affected.AffectedRows);
        return affected;
    }

    private static DmlExecutionResult ExecuteCreateFunctionImpl(
        DbConnectionMockBase connection,
        SqlCreateFunctionQuery query)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(query.Definition, nameof(query.Definition));
        connection.CreateFunction(query.Definition, query.OrReplace, query.Table?.DbName);
        return new DmlExecutionResult();
    }

    /// <summary>
    /// EN: Creates a trigger definition on the target table.
    /// PT-br: Cria uma definicao de trigger na tabela de destino.
    /// </summary>
    public static DmlExecutionResult ExecuteCreateTrigger(
        this DbConnectionMockBase connection,
        SqlCreateTriggerQuery query)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(query, nameof(query));
        return connection.CreateTrigger(query);
    }

    /// <summary>
    /// EN: Implements ExecuteCreateProcedure.
    /// PT-br: Implementa ExecuteCreateProcedure.
    /// </summary>
    public static DmlExecutionResult ExecuteCreateProcedure(
        this DbConnectionMockBase connection,
        SqlCreateProcedureQuery query,
        DbParameterCollection pars,
        ISqlDialect dialect)
    {
        _ = pars;
        _ = dialect;
        DmlExecutionResult affected;
        affected = connection.Db.ExecuteWithLock(() => ExecuteCreateProcedureImpl(connection, query));

        connection.SetLastFoundRows(affected.AffectedRows);
        return affected;
    }

    private static DmlExecutionResult ExecuteCreateProcedureImpl(
        DbConnectionMockBase connection,
        SqlCreateProcedureQuery query)
    {
        var procedureName = query.Table?.Name;
        ArgumentExceptionCompatible.ThrowIfNullOrWhiteSpace(procedureName, nameof(procedureName));
        connection.CreateProcedure(procedureName!, query.Definition, query.OrReplace, query.Table?.DbName);
        return new DmlExecutionResult();
    }

    /// <summary>
    /// EN: Drops a scalar function definition from the current schema.
    /// PT-br: Remove uma definicao de funcao escalar do schema atual.
    /// </summary>
    public static DmlExecutionResult ExecuteDropFunction(
        this DbConnectionMockBase connection,
        SqlDropFunctionQuery query,
        DbParameterCollection pars,
        ISqlDialect dialect)
    {
        _ = pars;
        _ = dialect;
        DmlExecutionResult affected;
        affected = connection.Db.ExecuteWithLock(() => ExecuteDropFunctionImpl(connection, query));

        connection.SetLastFoundRows(affected.AffectedRows);
        return affected;
    }

    /// <summary>
    /// EN: Implements ExecuteDropProcedure.
    /// PT-br: Implementa ExecuteDropProcedure.
    /// </summary>
    public static DmlExecutionResult ExecuteDropProcedure(
        this DbConnectionMockBase connection,
        SqlDropProcedureQuery query,
        DbParameterCollection pars,
        ISqlDialect dialect)
    {
        _ = pars;
        _ = dialect;
        DmlExecutionResult affected;
        affected = connection.Db.ExecuteWithLock(() => ExecuteDropProcedureImpl(connection, query));

        connection.SetLastFoundRows(affected.AffectedRows);
        return affected;
    }

    private static DmlExecutionResult ExecuteDropProcedureImpl(
        DbConnectionMockBase connection,
        SqlDropProcedureQuery query)
    {
        var procedureName = query.Table?.Name;
        ArgumentExceptionCompatible.ThrowIfNullOrWhiteSpace(procedureName, nameof(procedureName));
        connection.DropProcedure(procedureName!, query.IfExists, query.Table?.DbName);
        return new DmlExecutionResult();
    }

    /// <summary>
    /// EN: Implements ExecuteDropTrigger.
    /// PT-br: Implementa ExecuteDropTrigger.
    /// </summary>
    public static DmlExecutionResult ExecuteDropTrigger(
        this DbConnectionMockBase connection,
        SqlDropTriggerQuery query,
        DbParameterCollection pars,
        ISqlDialect dialect)
    {
        _ = pars;
        _ = dialect;
        DmlExecutionResult affected;
        affected = connection.Db.ExecuteWithLock(() => ExecuteDropTriggerImpl(connection, query));

        connection.SetLastFoundRows(affected.AffectedRows);
        return affected;
    }

    private static DmlExecutionResult ExecuteDropTriggerImpl(
        DbConnectionMockBase connection,
        SqlDropTriggerQuery query)
    {
        var triggerName = query.Table?.Name;
        ArgumentExceptionCompatible.ThrowIfNullOrWhiteSpace(triggerName, nameof(triggerName));
        connection.DropTrigger(triggerName!, query.IfExists, query.Table?.DbName);
        return new DmlExecutionResult();
    }
}
