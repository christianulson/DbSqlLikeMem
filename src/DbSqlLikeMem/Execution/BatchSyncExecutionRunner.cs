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
        var tables = new List<TableResultMock>();
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
