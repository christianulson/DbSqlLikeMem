namespace DbSqlLikeMem;

internal static class BatchScalarExecutionRunner
{
    public static object? ExecuteFirstScalar<TBatchCommand>(
        DbConnectionMockBase connection,
        IReadOnlyList<TBatchCommand> commands,
        Func<TBatchCommand, DbCommand> commandFactory)
    {
        var commandCount = commands.Count;
        if (commandCount == 0)
        {
            if (connection.Metrics.Enabled)
                connection.Metrics.IncrementBatchEmptyScalarExecution();
            return null;
        }

        BatchExecutionGuards.RequireOpenConnectionState(connection);

        var metricsEnabled = connection.Metrics.Enabled;
        if (metricsEnabled)
            connection.Metrics.IncrementBatchScalarCommand();

        using var command = commandFactory(commands[0]);
        if (metricsEnabled)
            connection.Metrics.IncrementBatchCommandTypeHit($"{BatchMetricKeys.TypePrefixes.Scalar}{command.CommandType}");

        return metricsEnabled
            ? BatchPhaseExecutionTelemetry.Execute(
                connection,
                BatchMetricKeys.Phases.Scalar,
                command.ExecuteScalar)
            : command.ExecuteScalar();
    }

    public static Task<object?> ExecuteFirstScalarAsync<TBatchCommand>(
        DbConnectionMockBase connection,
        IReadOnlyList<TBatchCommand> commands,
        Func<TBatchCommand, DbCommand> commandFactory,
        CancellationToken cancellationToken)
    {
        var commandCount = commands.Count;
        if (commandCount == 0)
        {
            if (connection.Metrics.Enabled)
                connection.Metrics.IncrementBatchEmptyScalarExecution();
            return Task.FromResult<object?>(null);
        }

        BatchExecutionGuards.RequireOpenConnectionState(connection);

        var metricsEnabled = connection.Metrics.Enabled;
        if (metricsEnabled)
            connection.Metrics.IncrementBatchScalarCommand();

        var command = commandFactory(commands[0]);
        var executionTask = metricsEnabled
            ? BatchPhaseExecutionTelemetry.ExecuteAsync(
                connection,
                BatchMetricKeys.Phases.Scalar,
                () => command.ExecuteScalarAsync(cancellationToken))
            : command.ExecuteScalarAsync(cancellationToken);

        if (executionTask.Status == TaskStatus.RanToCompletion)
        {
            using (command)
            {
                return Task.FromResult<object?>(executionTask.Result);
            }
        }

        return ExecuteFirstScalarAsyncCore(command, executionTask);
    }

    private static async Task<object?> ExecuteFirstScalarAsyncCore(
        DbCommand command,
        Task<object?> executionTask)
    {
        using (command)
        {
            return await executionTask.ConfigureAwait(false);
        }
    }
}
