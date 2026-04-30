namespace DbSqlLikeMem;

internal delegate bool TryExecutePipelineCommand(string sqlRaw, out DmlExecutionResult affectedRows);

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
        var affectedTotal = 0;
        var metricsEnabled = connection.Metrics.Enabled;

        foreach (var statementSql in SqlQueryParser.SplitStatements(sql, connection.ExecutionDialect))
        {
            if (string.IsNullOrWhiteSpace(statementSql))
                continue;

            var sqlRaw = (statementSql.Length > 0 && (char.IsWhiteSpace(statementSql[0]) || char.IsWhiteSpace(statementSql[^1])))
                ? statementSql.Trim()
                : statementSql;

            if (metricsEnabled)
                connection.Metrics.IncrementNonQueryStatement();

            if (!NonQueryHandlerExecutionRunner.TryHandleStatement(context, sqlRaw, handlers, out var affectedRows))
            {
                if (metricsEnabled)
                    connection.Metrics.IncrementNonQueryUnhandledStatement();
                throw new InvalidOperationException(SqlExceptionMessages.NonQueryHandlerCouldNotProcessStatement());
            }

            affectedTotal += affectedRows.AffectedRows;
        }

        return affectedTotal;
    }
}
