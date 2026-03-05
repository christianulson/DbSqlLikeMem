using System;
using System.Collections.Generic;
using System.Data.Common;
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
        {
            connection.Metrics.IncrementBatchEmptyScalarExecution();
            return null;
        }

        BatchExecutionGuards.RequireOpenConnectionState(connection);

        connection.Metrics.IncrementBatchScalarCommand();
        using var command = commandFactory(commands[0]);
        connection.Metrics.IncrementBatchCommandTypeHit($"{BatchMetricKeys.TypePrefixes.Scalar}{command.CommandType}");
        return BatchPhaseExecutionTelemetry.Execute(
            connection,
            BatchMetricKeys.Phases.Scalar,
            command.ExecuteScalar);
    }

    public static async Task<object?> ExecuteFirstScalarAsync<TBatchCommand>(
        DbConnectionMockBase connection,
        IReadOnlyList<TBatchCommand> commands,
        Func<TBatchCommand, DbCommand> commandFactory,
        CancellationToken cancellationToken)
        where TBatchCommand : DbBatchCommand
    {
        if (commands.Count == 0)
        {
            connection.Metrics.IncrementBatchEmptyScalarExecution();
            return null;
        }

        BatchExecutionGuards.RequireOpenConnectionState(connection);

        connection.Metrics.IncrementBatchScalarCommand();
        using var command = commandFactory(commands[0]);
        connection.Metrics.IncrementBatchCommandTypeHit($"{BatchMetricKeys.TypePrefixes.Scalar}{command.CommandType}");
        return await BatchPhaseExecutionTelemetry.ExecuteAsync(
            connection,
            BatchMetricKeys.Phases.Scalar,
            () => command.ExecuteScalarAsync(cancellationToken))
            .ConfigureAwait(false);
    }
}
