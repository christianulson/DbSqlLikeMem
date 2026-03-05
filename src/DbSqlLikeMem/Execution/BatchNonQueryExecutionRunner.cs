using System.Data.Common;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;

namespace DbSqlLikeMem;

internal static class BatchNonQueryExecutionRunner
{
    public static int ExecuteCommand(DbConnectionMockBase connection, DbCommand command)
    {
        var startedAt = Stopwatch.GetTimestamp();
        connection.Metrics.IncrementBatchNonQueryCommand();
        connection.Metrics.IncrementBatchCommandTypeHit($"{BatchMetricKeys.TypePrefixes.NonQuery}{command.CommandType}");
        try
        {
            var affectedRows = command.ExecuteNonQuery();
            connection.Metrics.IncrementBatchPhaseElapsedTicks(BatchMetricKeys.Phases.NonQuery, Stopwatch.GetElapsedTime(startedAt).Ticks);
            return affectedRows;
        }
        catch
        {
            connection.Metrics.IncrementBatchPhaseFailure(BatchMetricKeys.Phases.NonQuery);
            connection.Metrics.IncrementBatchException();
            throw;
        }
    }

    public static async Task<int> ExecuteCommandAsync(
        DbConnectionMockBase connection,
        DbCommand command,
        CancellationToken cancellationToken)
    {
        var startedAt = Stopwatch.GetTimestamp();
        connection.Metrics.IncrementBatchNonQueryCommand();
        connection.Metrics.IncrementBatchCommandTypeHit($"{BatchMetricKeys.TypePrefixes.NonQuery}{command.CommandType}");
        try
        {
            var affectedRows = await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            connection.Metrics.IncrementBatchPhaseElapsedTicks(BatchMetricKeys.Phases.NonQuery, Stopwatch.GetElapsedTime(startedAt).Ticks);
            return affectedRows;
        }
        catch
        {
            connection.Metrics.IncrementBatchPhaseFailure(BatchMetricKeys.Phases.NonQuery);
            connection.Metrics.IncrementBatchException();
            throw;
        }
    }
}
