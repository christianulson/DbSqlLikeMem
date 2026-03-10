namespace DbSqlLikeMem;

internal static class BatchNonQueryExecutionRunner
{
    public static int ExecuteCommand(DbConnectionMockBase connection, DbCommand command)
    {
        connection.Metrics.IncrementBatchNonQueryCommand();
        connection.Metrics.IncrementBatchCommandTypeHit($"{BatchMetricKeys.TypePrefixes.NonQuery}{command.CommandType}");
        try
        {
            return BatchPhaseExecutionTelemetry.Execute(
                connection,
                BatchMetricKeys.Phases.NonQuery,
                command.ExecuteNonQuery);
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
        connection.Metrics.IncrementBatchNonQueryCommand();
        connection.Metrics.IncrementBatchCommandTypeHit($"{BatchMetricKeys.TypePrefixes.NonQuery}{command.CommandType}");
        return ExecuteCommandAsyncCore(connection, command, cancellationToken);
    }

    private static async Task<int> ExecuteCommandAsyncCore(
        DbConnectionMockBase connection,
        DbCommand command,
        CancellationToken cancellationToken)
    {
        try
        {
            return await BatchPhaseExecutionTelemetry.ExecuteAsync(
                connection,
                BatchMetricKeys.Phases.NonQuery,
                () => command.ExecuteNonQueryAsync(cancellationToken))
                .ConfigureAwait(false);
        }
        catch (InvalidOperationException ex) when (ShouldTreatAsZeroAffectedRows(ex))
        {
            return 0;
        }
    }

    private static bool ShouldTreatAsZeroAffectedRows(InvalidOperationException ex)
        => string.Equals(ex.Message, SqlExceptionMessages.UseExecuteReaderForSelect(), StringComparison.Ordinal)
            || string.Equals(ex.Message, SqlExceptionMessages.UseExecuteReaderForSelectUnion(), StringComparison.Ordinal);
}
