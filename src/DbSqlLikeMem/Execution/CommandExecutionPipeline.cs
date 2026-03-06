using System.Diagnostics;

namespace DbSqlLikeMem;

internal delegate bool TryExecutePipelineCommand(string sqlRaw, out int affectedRows);

internal sealed class CommandExecutionPipelineOptions
{
    public bool AllowMerge { get; init; }
    public bool UnionUsesSelectMessage { get; init; }
    public TryExecutePipelineCommand? TryExecuteTransactionControl { get; init; }
    public TryExecutePipelineCommand? TryExecuteSpecialCommand { get; init; }
    public Action<string>? ValidateBeforeParse { get; init; }
    public IReadOnlyList<INonQueryCommandHandler>? Handlers { get; init; }
}

/// <summary>
/// EN: Shared non-query execution pipeline with provider hooks for special commands.
/// PT: Pipeline compartilhado de non-query com hooks por provider para comandos especiais.
/// </summary>
internal sealed class CommandExecutionPipeline : ICommandExecutionPipeline
{
    private static readonly IReadOnlyList<INonQueryCommandHandler> DefaultHandlers =
    [
        new TransactionControlNonQueryCommandHandler(),
        new CallNonQueryCommandHandler(),
        new CreateTableAsSelectNonQueryCommandHandler(),
        new SpecialNonQueryCommandHandler(),
        new AstDmlNonQueryCommandHandler(),
        new AstDdlNonQueryCommandHandler(),
        new AstReadGuardNonQueryCommandHandler(),
        new AstUnsupportedNonQueryCommandHandler()
    ];

    public int ExecuteNonQuery(
        DbConnectionMockBase connection,
        string sql,
        DbParameterCollection pars,
        CommandExecutionPipelineOptions options)
    {
        var context = new CommandExecutionPipelineContext(connection, pars, options);
        var handlers = options.Handlers ?? DefaultHandlers;
        var statements = SqlQueryParser
            .SplitStatements(sql, connection.Db.Dialect)
            .Where(s => !string.IsNullOrWhiteSpace(s))
            .ToList();

        var affectedTotal = 0;
        foreach (var statementSql in statements)
        {
            var sqlRaw = statementSql.Trim();
            if (string.IsNullOrWhiteSpace(sqlRaw))
                continue;
            connection.Metrics.IncrementNonQueryStatement();

            var handled = false;
            foreach (var handler in handlers)
            {
                var startedAt = Stopwatch.GetTimestamp();
                try
                {
                    if (!handler.TryHandle(context, sqlRaw, out var affectedRows))
                        continue;

                    connection.SetLastFoundRows(affectedRows);
                    var handlerName = handler.GetType().Name;
                    connection.Metrics.IncrementNonQueryHandlerHit(handlerName);
                    var elapsedTicks = StopwatchCompatible.GetElapsedTicks(startedAt);
                    connection.Metrics.IncrementNonQueryHandlerElapsedTicks(handlerName, elapsedTicks);
                    affectedTotal += affectedRows;
                    handled = true;
                    break;
                }
                catch
                {
                    var handlerName = handler.GetType().Name;
                    connection.Metrics.IncrementNonQueryHandlerFailure(handlerName);
                    connection.Metrics.IncrementNonQueryException();
                    throw;
                }
            }

            if (!handled)
            {
                connection.Metrics.IncrementNonQueryUnhandledStatement();
                throw new InvalidOperationException(SqlExceptionMessages.NonQueryHandlerCouldNotProcessStatement());
            }
        }

        return affectedTotal;
    }
}
