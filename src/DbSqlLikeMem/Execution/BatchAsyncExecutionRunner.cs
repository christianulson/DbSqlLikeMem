namespace DbSqlLikeMem;

internal static class BatchAsyncExecutionRunner
{
    public static Task<int> ExecuteNonQueryCommandsAsync<TBatchCommand>(
        DbConnectionMockBase connection,
        IReadOnlyList<TBatchCommand> commands,
        Func<TBatchCommand, DbCommand> commandFactory,
        CancellationToken cancellationToken)
    {
        var commandCount = commands.Count;
        if (commandCount == 0)
        {
            if (connection.Metrics.Enabled)
                connection.Metrics.IncrementBatchEmptyNonQueryExecution();
            return Task.FromResult(0);
        }

        BatchExecutionGuards.RequireOpenConnectionState(connection);

        var metricsEnabled = connection.Metrics.Enabled;

        if (commandCount == 1)
        {
            var command = commandFactory(commands[0]);
            var task = BatchNonQueryExecutionRunner.ExecuteCommandAsync(connection, command, cancellationToken, metricsEnabled);
            if (task.Status == TaskStatus.RanToCompletion)
            {
                using (command)
                {
                    return Task.FromResult(task.Result);
                }
            }

            return ExecuteSingleNonQueryCommandAsyncCore(command, task);
        }

        return ExecuteManyNonQueryCommandsAsync(
            connection,
            commands,
            commandFactory,
            cancellationToken,
            metricsEnabled);
    }

    public static Task<List<TableResultMock>> ExecuteReaderCommandsAsync<TBatchCommand>(
        DbConnectionMockBase connection,
        IReadOnlyList<TBatchCommand> commands,
        Func<TBatchCommand, DbCommand> commandFactory,
        CommandBehavior behavior,
        CancellationToken cancellationToken)
    {
        var commandCount = commands.Count;
        if (commandCount == 0)
        {
            if (connection.Metrics.Enabled)
                connection.Metrics.IncrementBatchEmptyReaderExecution();
            return Task.FromResult(new List<TableResultMock>());
        }

        BatchExecutionGuards.RequireOpenConnectionState(connection);

        var metricsEnabled = connection.Metrics.Enabled;

        if (commandCount == 1)
        {
            var tables = new List<TableResultMock>(1);
            var command = commandFactory(commands[0]);
            var task = BatchCommandExecutionRunner.ExecuteIntoTablesAsync(
                connection,
                command,
                tables,
                behavior,
                cancellationToken,
                metricsEnabled);

            if (task.Status == TaskStatus.RanToCompletion)
            {
                using (command)
                {
                    return Task.FromResult(tables);
                }
            }

            return ExecuteSingleReaderCommandAsync(command, task, tables);
        }

        return ExecuteManyReaderCommandsAsync(
            connection,
            commands,
            commandFactory,
            behavior,
            cancellationToken,
            metricsEnabled);
    }

    public static Task<TReader> ExecuteReaderCommandsAsync<TBatchCommand, TReader>(
        DbConnectionMockBase connection,
        IReadOnlyList<TBatchCommand> commands,
        Func<TBatchCommand, DbCommand> commandFactory,
        CommandBehavior behavior,
        Func<List<TableResultMock>, TReader> readerFactory,
        CancellationToken cancellationToken)
    {
        var tablesTask = ExecuteReaderCommandsAsync(
            connection,
            commands,
            commandFactory,
            behavior,
            cancellationToken);

        if (tablesTask.Status == TaskStatus.RanToCompletion)
        {
            return Task.FromResult(readerFactory(tablesTask.Result));
        }

        return ExecuteReaderCommandsAsyncCore(tablesTask, readerFactory);
    }

    private static async Task<int> ExecuteSingleNonQueryCommandAsyncCore(
        DbCommand command,
        Task<int> executionTask)
    {
        using (command)
        {
            return await executionTask.ConfigureAwait(false);
        }
    }

    private static async Task<int> ExecuteManyNonQueryCommandsAsync<TBatchCommand>(
        DbConnectionMockBase connection,
        IReadOnlyList<TBatchCommand> commands,
        Func<TBatchCommand, DbCommand> commandFactory,
        CancellationToken cancellationToken,
        bool metricsEnabled)
    {
        var affected = 0;
        var commandCount = commands.Count;
        for (var i = 0; i < commandCount; i++)
        {
            using var command = commandFactory(commands[i]);
            affected += await BatchNonQueryExecutionRunner
                .ExecuteCommandAsync(connection, command, cancellationToken, metricsEnabled)
                .ConfigureAwait(false);
        }

        return affected;
    }

    private static async Task<List<TableResultMock>> ExecuteSingleReaderCommandAsync(
        DbCommand command,
        Task executionTask,
        List<TableResultMock> tables)
    {
        using (command)
        {
            await executionTask.ConfigureAwait(false);
        }

        return tables;
    }

    private static async Task<List<TableResultMock>> ExecuteManyReaderCommandsAsync<TBatchCommand>(
        DbConnectionMockBase connection,
        IReadOnlyList<TBatchCommand> commands,
        Func<TBatchCommand, DbCommand> commandFactory,
        CommandBehavior behavior,
        CancellationToken cancellationToken,
        bool metricsEnabled)
    {
        var commandCount = commands.Count;
        var tables = new List<TableResultMock>(commandCount);
        for (var i = 0; i < commandCount; i++)
        {
            using var command = commandFactory(commands[i]);
            await BatchCommandExecutionRunner
                .ExecuteIntoTablesAsync(connection, command, tables, behavior, cancellationToken, metricsEnabled)
                .ConfigureAwait(false);
        }

        return tables;
    }

    private static async Task<TReader> ExecuteReaderCommandsAsyncCore<TReader>(
        Task<List<TableResultMock>> tablesTask,
        Func<List<TableResultMock>, TReader> readerFactory)
    {
        var tables = await tablesTask.ConfigureAwait(false);
        return readerFactory(tables);
    }
}
