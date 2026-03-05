using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;

namespace DbSqlLikeMem;

internal static class BatchScalarExecutionRunner
{
    public static object? ExecuteFirstScalar<TBatchCommand>(
        DbConnectionMockBase connection,
        IReadOnlyList<TBatchCommand> commands,
        Func<TBatchCommand, DbCommand> commandFactory)
        where TBatchCommand : DbBatchCommand
    {
        if (commands.Count == 0)
            return null;

        var startedAt = Stopwatch.GetTimestamp();
        connection.Metrics.IncrementBatchScalarCommand();
        using var command = commandFactory(commands[0]);
        connection.Metrics.IncrementBatchCommandTypeHit($"{BatchMetricKeys.TypePrefixes.Scalar}{command.CommandType}");
        try
        {
            var result = command.ExecuteScalar();
            connection.Metrics.IncrementBatchPhaseElapsedTicks(BatchMetricKeys.Phases.Scalar, Stopwatch.GetElapsedTime(startedAt).Ticks);
            return result;
        }
        catch
        {
            connection.Metrics.IncrementBatchPhaseFailure(BatchMetricKeys.Phases.Scalar);
            connection.Metrics.IncrementBatchException();
            throw;
        }
    }

    public static async Task<object?> ExecuteFirstScalarAsync<TBatchCommand>(
        DbConnectionMockBase connection,
        IReadOnlyList<TBatchCommand> commands,
        Func<TBatchCommand, DbCommand> commandFactory,
        CancellationToken cancellationToken)
        where TBatchCommand : DbBatchCommand
    {
        if (commands.Count == 0)
            return null;

        var startedAt = Stopwatch.GetTimestamp();
        connection.Metrics.IncrementBatchScalarCommand();
        using var command = commandFactory(commands[0]);
        connection.Metrics.IncrementBatchCommandTypeHit($"{BatchMetricKeys.TypePrefixes.Scalar}{command.CommandType}");
        try
        {
            var result = await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
            connection.Metrics.IncrementBatchPhaseElapsedTicks(BatchMetricKeys.Phases.Scalar, Stopwatch.GetElapsedTime(startedAt).Ticks);
            return result;
        }
        catch
        {
            connection.Metrics.IncrementBatchPhaseFailure(BatchMetricKeys.Phases.Scalar);
            connection.Metrics.IncrementBatchException();
            throw;
        }
    }
}
