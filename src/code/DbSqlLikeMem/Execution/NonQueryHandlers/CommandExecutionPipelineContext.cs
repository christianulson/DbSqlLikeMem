namespace DbSqlLikeMem;

internal sealed class CommandExecutionPipelineContext(
    DbConnectionMockBase connection,
    DbParameterCollection pars,
    CommandExecutionPipelineOptions options)
{
    private string? parsedSqlRaw;
    private SqlQueryBase? parsedQuery;
    private string? validatedSqlRaw;

    public DbConnectionMockBase Connection { get; } = connection;
    public DbParameterCollection Parameters { get; } = pars;
    public CommandExecutionPipelineOptions Options { get; } = options;

    private QueryExecutionContext? _executionContext;

    /// <summary>
    /// EN: Lazily builds a QueryExecutionContext for this pipeline run, reusing it across handlers.
    /// PT: Constrói lazily um QueryExecutionContext para este pipeline, reutilizando-o entre handlers.
    /// </summary>
    public QueryExecutionContext ExecutionContext
        => _executionContext ??= QueryExecutionContext.FromConnection(Connection, Parameters);

    public void EnsureValidatedBeforeParse(string sqlRaw)
    {
        if (string.Equals(validatedSqlRaw, sqlRaw, StringComparison.Ordinal))
            return;

        Options.ValidateBeforeParse?.Invoke(sqlRaw);
        validatedSqlRaw = sqlRaw;
    }

    public SqlQueryBase GetParsedQuery(string sqlRaw)
    {
        EnsureValidatedBeforeParse(sqlRaw);

        if (parsedQuery is not null &&
            string.Equals(parsedSqlRaw, sqlRaw, StringComparison.Ordinal))
        {
            if (Connection.Metrics.Enabled)
                Connection.Metrics.IncrementNonQueryParseCacheHit();
            return parsedQuery;
        }

        if (Connection.Metrics.Enabled)
            Connection.Metrics.IncrementNonQueryParseCacheMiss();
        parsedSqlRaw = sqlRaw;
        parsedQuery = Connection.Db.ExecuteWithLock(() => SqlQueryParser.Parse(
            sqlRaw,
            Connection.Db,
            Connection.ExecutionDialect,
            null,
            SqlCustomFunctionResolverFactory.Create(ExecutionContext)));
        return parsedQuery;
    }
}
