namespace DbSqlLikeMem;

internal static class BatchNonQueryExecutionRunner
{
    public static int ExecuteCommand(DbConnectionMockBase connection, DbCommand command)
    {
        var metricsEnabled = connection.Metrics.Enabled;
        return ExecuteCommand(connection, command, metricsEnabled);
    }

    public static int ExecuteCommand(DbConnectionMockBase connection, DbCommand command, bool metricsEnabled)
    {
        if (metricsEnabled)
        {
            connection.Metrics.IncrementBatchNonQueryCommand();
            connection.Metrics.IncrementBatchCommandTypeHit($"{BatchMetricKeys.TypePrefixes.NonQuery}{command.CommandType}");
        }

        try
        {
            return metricsEnabled
                ? BatchPhaseExecutionTelemetry.Execute(
                    connection,
                    BatchMetricKeys.Phases.NonQuery,
                    command.ExecuteNonQuery)
                : command.ExecuteNonQuery();
        }
        catch (InvalidOperationException ex) when (ShouldTreatAsZeroAffectedRows(ex))
        {
            return 0;
        }
    }

    public static Task<int> ExecuteCommandAsync(
        DbConnectionMockBase connection,
        DbCommand command,
        CancellationToken cancellationToken)
    {
        var metricsEnabled = connection.Metrics.Enabled;
        return ExecuteCommandAsync(connection, command, cancellationToken, metricsEnabled);
    }

    public static Task<int> ExecuteCommandAsync(
        DbConnectionMockBase connection,
        DbCommand command,
        CancellationToken cancellationToken,
        bool metricsEnabled)
    {
        if (metricsEnabled)
        {
            connection.Metrics.IncrementBatchNonQueryCommand();
            connection.Metrics.IncrementBatchCommandTypeHit($"{BatchMetricKeys.TypePrefixes.NonQuery}{command.CommandType}");
        }

        var executionTask = metricsEnabled
            ? BatchPhaseExecutionTelemetry.ExecuteAsync(
                connection,
                BatchMetricKeys.Phases.NonQuery,
                () => command.ExecuteNonQueryAsync(cancellationToken))
            : command.ExecuteNonQueryAsync(cancellationToken);

        if (executionTask.Status == TaskStatus.RanToCompletion)
        {
            using (command)
            {
                return Task.FromResult(executionTask.Result);
            }
        }

        return ExecuteCommandAsyncCore(command, executionTask);
    }

    private static async Task<int> ExecuteCommandAsyncCore(
        DbCommand command,
        Task<int> executionTask)
    {
        using (command)
        {
            return await executionTask.ConfigureAwait(false);
        }
    }

    private static bool ShouldTreatAsZeroAffectedRows(InvalidOperationException ex)
        => string.Equals(ex.Message, SqlExceptionMessages.UseExecuteReaderForSelect(), StringComparison.Ordinal)
            || string.Equals(ex.Message, SqlExceptionMessages.UseExecuteReaderForSelectUnion(), StringComparison.Ordinal);
}
