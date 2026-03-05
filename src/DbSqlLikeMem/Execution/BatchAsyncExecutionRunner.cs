using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;

namespace DbSqlLikeMem;

internal static class BatchAsyncExecutionRunner
{
    public static async Task<int> ExecuteNonQueryCommandsAsync<TBatchCommand>(
        DbConnectionMockBase connection,
        IReadOnlyList<TBatchCommand> commands,
        Func<TBatchCommand, DbCommand> commandFactory,
        CancellationToken cancellationToken)
        where TBatchCommand : DbBatchCommand
    {
        var affected = 0;
        foreach (var batchCommand in commands)
        {
            using var command = commandFactory(batchCommand);
            affected += await BatchNonQueryExecutionRunner
                .ExecuteCommandAsync(connection, command, cancellationToken)
                .ConfigureAwait(false);
        }

        return affected;
    }

    public static async Task<List<TableResultMock>> ExecuteReaderCommandsAsync<TBatchCommand>(
        DbConnectionMockBase connection,
        IReadOnlyList<TBatchCommand> commands,
        Func<TBatchCommand, DbCommand> commandFactory,
        CommandBehavior behavior,
        CancellationToken cancellationToken)
        where TBatchCommand : DbBatchCommand
    {
        var tables = new List<TableResultMock>();
        foreach (var batchCommand in commands)
        {
            using var command = commandFactory(batchCommand);
            await BatchCommandExecutionRunner
                .ExecuteIntoTablesAsync(
                    connection,
                    command,
                    tables,
                    ct => command.ExecuteReaderAsync(behavior, ct),
                    cancellationToken)
                .ConfigureAwait(false);
        }

        return tables;
    }

    public static async Task<TReader> ExecuteReaderCommandsAsync<TBatchCommand, TReader>(
        DbConnectionMockBase connection,
        IReadOnlyList<TBatchCommand> commands,
        Func<TBatchCommand, DbCommand> commandFactory,
        CommandBehavior behavior,
        Func<List<TableResultMock>, TReader> readerFactory,
        CancellationToken cancellationToken)
        where TBatchCommand : DbBatchCommand
    {
        var tables = await ExecuteReaderCommandsAsync(
            connection,
            commands,
            commandFactory,
            behavior,
            cancellationToken).ConfigureAwait(false);
        return readerFactory(tables);
    }
}
