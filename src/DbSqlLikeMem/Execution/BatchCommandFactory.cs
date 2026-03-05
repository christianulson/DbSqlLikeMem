using System.Data.Common;
using System.Diagnostics;

namespace DbSqlLikeMem;

internal static class BatchCommandFactory
{
    public static TCommand Create<TCommand, TBatchCommand>(
        DbConnectionMockBase connection,
        Func<TCommand> commandFactory,
        TBatchCommand batchCommand,
        int timeout)
        where TCommand : DbCommand
        where TBatchCommand : DbBatchCommand
    {
        return Create(
            connection,
            commandFactory,
            batchCommand,
            timeout,
            static (command, source, commandTimeout) => BatchCommandMaterializer.Apply(command, source, commandTimeout));
    }

    public static TCommand Create<TCommand, TBatchCommand>(
        DbConnectionMockBase connection,
        Func<TCommand> commandFactory,
        TBatchCommand batchCommand,
        int timeout,
        Action<TCommand, TBatchCommand, int> materialize)
        where TCommand : DbCommand
        where TBatchCommand : DbBatchCommand
    {
        var startedAt = Stopwatch.GetTimestamp();
        try
        {
            var command = commandFactory();
            materialize(command, batchCommand, timeout);
            connection.Metrics.IncrementBatchMaterialization();
            connection.Metrics.IncrementBatchCommandTypeHit($"{BatchMetricKeys.TypePrefixes.Materialize}{command.CommandType}");
            connection.Metrics.IncrementBatchPhaseElapsedTicks(BatchMetricKeys.Phases.Materialization, Stopwatch.GetElapsedTime(startedAt).Ticks);
            return command;
        }
        catch
        {
            connection.Metrics.IncrementBatchPhaseFailure(BatchMetricKeys.Phases.Materialization);
            connection.Metrics.IncrementBatchException();
            throw;
        }
    }
}
