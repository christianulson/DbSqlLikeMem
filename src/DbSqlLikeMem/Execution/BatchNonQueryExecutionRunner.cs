using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;

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

    public static async Task<int> ExecuteCommandAsync(
        DbConnectionMockBase connection,
        DbCommand command,
        CancellationToken cancellationToken)
    {
        connection.Metrics.IncrementBatchNonQueryCommand();
        connection.Metrics.IncrementBatchCommandTypeHit($"{BatchMetricKeys.TypePrefixes.NonQuery}{command.CommandType}");
        return await BatchPhaseExecutionTelemetry.ExecuteAsync(
            connection,
            BatchMetricKeys.Phases.NonQuery,
            () => command.ExecuteNonQueryAsync(cancellationToken))
            .ConfigureAwait(false);
    }
}
