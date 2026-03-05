namespace DbSqlLikeMem;

internal static class BatchNonQueryExecutionRunner
{
    public static int ExecuteCommand(DbConnectionMockBase connection, DbCommand command)
    {
        connection.Metrics.IncrementBatchNonQueryCommand();
        connection.Metrics.IncrementBatchCommandTypeHit($"{BatchMetricKeys.TypePrefixes.NonQuery}{command.CommandType}");
        return BatchPhaseExecutionTelemetry.Execute(
            connection,
            BatchMetricKeys.Phases.NonQuery,
            command.ExecuteNonQuery);
    }

    public static Task<int> ExecuteCommandAsync(
        DbConnectionMockBase connection,
        DbCommand command,
        CancellationToken cancellationToken)
    {
        connection.Metrics.IncrementBatchNonQueryCommand();
        connection.Metrics.IncrementBatchCommandTypeHit($"{BatchMetricKeys.TypePrefixes.NonQuery}{command.CommandType}");
        return BatchPhaseExecutionTelemetry.ExecuteAsync(
            connection,
            BatchMetricKeys.Phases.NonQuery,
            () => command.ExecuteNonQueryAsync(cancellationToken));
    }
}
