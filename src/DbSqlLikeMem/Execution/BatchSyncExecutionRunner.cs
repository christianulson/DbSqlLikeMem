using System.Collections.Generic;
using System.Data;
using System.Data.Common;

namespace DbSqlLikeMem;

internal static class BatchSyncExecutionRunner
{
    public static int ExecuteNonQueryCommands<TBatchCommand>(
        DbConnectionMockBase connection,
        IReadOnlyList<TBatchCommand> commands,
        Func<TBatchCommand, DbCommand> commandFactory)
        where TBatchCommand : DbBatchCommand
    {
        if (commands.Count == 0)
        {
            connection.Metrics.IncrementBatchEmptyNonQueryExecution();
            return 0;
        }

        BatchExecutionGuards.RequireOpenConnectionState(connection);

        var affected = 0;
        foreach (var batchCommand in commands)
        {
            using var command = commandFactory(batchCommand);
            affected += BatchNonQueryExecutionRunner.ExecuteCommand(connection, command);
        }

        return affected;
    }

    public static List<TableResultMock> ExecuteReaderCommands<TBatchCommand>(
        DbConnectionMockBase connection,
        IReadOnlyList<TBatchCommand> commands,
        Func<TBatchCommand, DbCommand> commandFactory,
        CommandBehavior behavior)
        where TBatchCommand : DbBatchCommand
    {
        if (commands.Count == 0)
        {
            connection.Metrics.IncrementBatchEmptyReaderExecution();
            return new List<TableResultMock>(0);
        }

        BatchExecutionGuards.RequireOpenConnectionState(connection);

        var tables = new List<TableResultMock>(commands.Count);
        foreach (var batchCommand in commands)
        {
            using var command = commandFactory(batchCommand);
            BatchCommandExecutionRunner.ExecuteIntoTables(
                connection,
                command,
                tables,
                () => command.ExecuteReader(behavior));
        }

        return tables;
    }

    public static TReader ExecuteReaderCommands<TBatchCommand, TReader>(
        DbConnectionMockBase connection,
        IReadOnlyList<TBatchCommand> commands,
        Func<TBatchCommand, DbCommand> commandFactory,
        CommandBehavior behavior,
        Func<List<TableResultMock>, TReader> readerFactory)
        where TBatchCommand : DbBatchCommand
    {
        var tables = ExecuteReaderCommands(connection, commands, commandFactory, behavior);
        return readerFactory(tables);
    }
}
