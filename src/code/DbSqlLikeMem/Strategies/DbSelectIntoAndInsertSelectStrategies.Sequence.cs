namespace DbSqlLikeMem;

internal static partial class DbSelectIntoAndInsertSelectStrategies
{
    /// <summary>
    /// EN: Implements ExecuteCreateSequence.
    /// PT: Implementa ExecuteCreateSequence.
    /// </summary>
    public static DmlExecutionResult ExecuteCreateSequence(
        this DbConnectionMockBase connection,
        SqlCreateSequenceQuery query,
        DbParameterCollection pars,
        ISqlDialect dialect)
    {
        _ = pars;
        _ = dialect;
        DmlExecutionResult affected;
        affected = connection.Db.ExecuteWithLock(() => ExecuteCreateSequenceImpl(connection, query));

        connection.SetLastFoundRows(affected.AffectedRows);
        return affected;
    }

    private static DmlExecutionResult ExecuteCreateSequenceImpl(
        DbConnectionMockBase connection,
        SqlCreateSequenceQuery query)
    {
        var sequenceName = query.Table?.Name;
        ArgumentExceptionCompatible.ThrowIfNullOrWhiteSpace(sequenceName, nameof(sequenceName));
        connection.CreateSequence(
            sequenceName!,
            query.IfNotExists,
            query.StartValue,
            query.IncrementBy,
            query.MinValue,
            query.MaxValue,
            query.IsCycle,
            query.IsOwnedByNone ? null : query.OwnedByTable?.DbName,
            query.IsOwnedByNone ? null : query.OwnedByTable?.Name,
            query.IsOwnedByNone ? null : query.OwnedByColumn,
            schemaName: query.Table?.DbName);
        return new DmlExecutionResult();
    }

    /// <summary>
    /// EN: Implements ExecuteAlterSequence.
    /// PT: Implementa ExecuteAlterSequence.
    /// </summary>
    public static DmlExecutionResult ExecuteAlterSequence(
        this DbConnectionMockBase connection,
        SqlAlterSequenceQuery query,
        DbParameterCollection pars,
        ISqlDialect dialect)
    {
        _ = pars;
        _ = dialect;
        DmlExecutionResult affected;
        affected = connection.Db.ExecuteWithLock(() => ExecuteAlterSequenceImpl(connection, query));

        connection.SetLastFoundRows(affected.AffectedRows);
        return affected;
    }

    private static DmlExecutionResult ExecuteAlterSequenceImpl(
        DbConnectionMockBase connection,
        SqlAlterSequenceQuery query)
    {
        var sequenceName = query.Table?.Name;
        ArgumentExceptionCompatible.ThrowIfNullOrWhiteSpace(sequenceName, nameof(sequenceName));
        var targetSchema = connection.ResolveSchemaName(query.Table?.DbName);

        if (!connection.Db.TryGetSequence(sequenceName!, out var sequence, targetSchema) || sequence is null)
            throw new InvalidOperationException($"Sequence '{sequenceName!.NormalizeName()}' does not exist.");

        connection.CaptureSequenceStateForRollback(sequenceName!, targetSchema);
        if (query.IncrementBy.HasValue)
        {
            sequence.SetIncrementBy(query.IncrementBy.Value);
            return new DmlExecutionResult();
        }

        if (query.IsOwnedByNone)
        {
            sequence.ClearOwnership();
            return new DmlExecutionResult();
        }

        if (query.OwnedByTable is not null)
        {
            var ownedSchema = query.OwnedByTable.DbName ?? targetSchema;
            sequence.SetOwnership(ownedSchema, query.OwnedByTable.Name, query.OwnedByColumn);
            return new DmlExecutionResult();
        }

        var restartWith = query.RestartWith ?? sequence.StartValue;
        sequence.SetValue(restartWith, false);
        connection.ClearSessionSequenceValue(sequenceName!, targetSchema);
        return new DmlExecutionResult();
    }

    /// <summary>
    /// EN: Implements ExecuteDropSequence.
    /// PT: Implementa ExecuteDropSequence.
    /// </summary>
    public static DmlExecutionResult ExecuteDropSequence(
        this DbConnectionMockBase connection,
        SqlDropSequenceQuery query,
        DbParameterCollection pars,
        ISqlDialect dialect)
    {
        _ = pars;
        _ = dialect;
        DmlExecutionResult affected;
        affected = connection.Db.ExecuteWithLock(() => ExecuteDropSequenceImpl(connection, query));

        connection.SetLastFoundRows(affected.AffectedRows);
        return affected;
    }

    private static DmlExecutionResult ExecuteDropSequenceImpl(
        DbConnectionMockBase connection,
        SqlDropSequenceQuery query)
    {
        var sequenceName = query.Table?.Name;
        ArgumentExceptionCompatible.ThrowIfNullOrWhiteSpace(sequenceName, nameof(sequenceName));
        connection.DropSequence(sequenceName!, query.IfExists, query.Table?.DbName);
        return new DmlExecutionResult();
    }
}
