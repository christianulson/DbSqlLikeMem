namespace DbSqlLikeMem;

internal static class BatchAsyncExecutionRunner
{
    public static async Task<int> ExecuteNonQueryCommandsAsync<TBatchCommand>(
        DbConnectionMockBase connection,
        IReadOnlyList<TBatchCommand> commands,
        Func<TBatchCommand, DbCommand> commandFactory,
        CancellationToken cancellationToken)
    {
        if (commands.Count == 0)
        {
            connection.Metrics.IncrementBatchEmptyNonQueryExecution();
            return 0;
        }

        BatchExecutionGuards.RequireOpenConnectionState(connection);

        var affected = 0;
        foreach (var batchCommand in commands)
        {
            using var command = commandFactory(batchCommand);
            affected += await BatchNonQueryExecutionRunner
                .ExecuteCommandAsync(connection, command, cancellationToken)
                .ConfigureAwait(false);
        }

        return affected;
    }

    public static async Task<List<TableResultMock>> ExecuteReaderCommandsAsync<TBatchCommand>(
        DbConnectionMockBase connection,
        IReadOnlyList<TBatchCommand> commands,
        Func<TBatchCommand, DbCommand> commandFactory,
        CommandBehavior behavior,
        CancellationToken cancellationToken)
    {
        if (commands.Count == 0)
        {
            connection.Metrics.IncrementBatchEmptyReaderExecution();
            return [];
        }

        BatchExecutionGuards.RequireOpenConnectionState(connection);

        var tables = new List<TableResultMock>(commands.Count);
        foreach (var batchCommand in commands)
        {
            using var command = commandFactory(batchCommand);
            await BatchCommandExecutionRunner
                .ExecuteIntoTablesAsync(
                    connection,
                    command,
                    tables,
                    ct => command.ExecuteReaderAsync(behavior, ct),
                    cancellationToken)
                .ConfigureAwait(false);
        }

        return tables;
    }

    public static async Task<TReader> ExecuteReaderCommandsAsync<TBatchCommand, TReader>(
        DbConnectionMockBase connection,
        IReadOnlyList<TBatchCommand> commands,
        Func<TBatchCommand, DbCommand> commandFactory,
        CommandBehavior behavior,
        Func<List<TableResultMock>, TReader> readerFactory,
        CancellationToken cancellationToken)
    {
        var tables = await ExecuteReaderCommandsAsync(
            connection,
            commands,
            commandFactory,
            behavior,
            cancellationToken).ConfigureAwait(false);
        return readerFactory(tables);
    }
}
