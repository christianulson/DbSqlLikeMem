namespace DbSqlLikeMem;

internal static class BatchCommandFactory
{
    public static TCommand Create<TCommand, TBatchCommand>(
        DbConnectionMockBase connection,
        Func<TCommand> commandFactory,
        TBatchCommand batchCommand,
        int timeout)
        where TCommand : DbCommand
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
    {
        return BatchPhaseExecutionTelemetry.Execute(
            connection,
            BatchMetricKeys.Phases.Materialization,
            () =>
            {
                var command = commandFactory();
                materialize(command, batchCommand, timeout);
                connection.Metrics.IncrementBatchMaterialization();
                connection.Metrics.IncrementBatchCommandTypeHit($"{BatchMetricKeys.TypePrefixes.Materialize}{command.CommandType}");
                return command;
            });
    }
}
