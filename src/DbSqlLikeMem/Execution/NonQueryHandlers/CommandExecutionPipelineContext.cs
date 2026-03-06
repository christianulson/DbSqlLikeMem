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
            Connection.Metrics.IncrementNonQueryParseCacheHit();
            return parsedQuery;
        }

        Connection.Metrics.IncrementNonQueryParseCacheMiss();
        parsedSqlRaw = sqlRaw;
        parsedQuery = SqlQueryParser.Parse(sqlRaw, Connection.Db.Dialect);
        return parsedQuery;
    }
}
