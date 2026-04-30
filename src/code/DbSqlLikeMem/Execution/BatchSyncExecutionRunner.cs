namespace DbSqlLikeMem;

internal static class BatchSyncExecutionRunner
{
    public static int ExecuteNonQueryCommands<TBatchCommand>(
        DbConnectionMockBase connection,
        IReadOnlyList<TBatchCommand> commands,
        Func<TBatchCommand, DbCommand> commandFactory)
    {
        var commandCount = commands.Count;
        if (commandCount == 0)
        {
            if (connection.Metrics.Enabled)
                connection.Metrics.IncrementBatchEmptyNonQueryExecution();
            return 0;
        }

        BatchExecutionGuards.RequireOpenConnectionState(connection);

        var metricsEnabled = connection.Metrics.Enabled;

        if (commandCount == 1)
        {
            using var command = commandFactory(commands[0]);
            return BatchNonQueryExecutionRunner.ExecuteCommand(connection, command, metricsEnabled);
        }

        var affected = 0;
        for (var i = 0; i < commandCount; i++)
        {
            using var command = commandFactory(commands[i]);
            affected += BatchNonQueryExecutionRunner.ExecuteCommand(connection, command, metricsEnabled);
        }

        return affected;
    }

    public static List<TableResultMock> ExecuteReaderCommands<TBatchCommand>(
        DbConnectionMockBase connection,
        IReadOnlyList<TBatchCommand> commands,
        Func<TBatchCommand, DbCommand> commandFactory,
        CommandBehavior behavior)
    {
        var commandCount = commands.Count;
        if (commandCount == 0)
        {
            if (connection.Metrics.Enabled)
                connection.Metrics.IncrementBatchEmptyReaderExecution();
            return [];
        }

        BatchExecutionGuards.RequireOpenConnectionState(connection);

        var metricsEnabled = connection.Metrics.Enabled;

        if (commandCount == 1)
        {
            var readerTables = new List<TableResultMock>(1);
            using var command = commandFactory(commands[0]);
            BatchCommandExecutionRunner.ExecuteIntoTables(connection, command, readerTables, behavior, metricsEnabled);
            return readerTables;
        }

        var readerTablesMulti = new List<TableResultMock>(commandCount);
        for (var i = 0; i < commandCount; i++)
        {
            using var command = commandFactory(commands[i]);
            BatchCommandExecutionRunner.ExecuteIntoTables(connection, command, readerTablesMulti, behavior, metricsEnabled);
        }

        return readerTablesMulti;
    }

    public static TReader ExecuteReaderCommands<TBatchCommand, TReader>(
        DbConnectionMockBase connection,
        IReadOnlyList<TBatchCommand> commands,
        Func<TBatchCommand, DbCommand> commandFactory,
        CommandBehavior behavior,
        Func<List<TableResultMock>, TReader> readerFactory)
    {
        var readerTables = ExecuteReaderCommands(connection, commands, commandFactory, behavior);
        return readerFactory(readerTables);
    }
}
