namespace DbSqlLikeMem;

internal static class CommandReaderExecutionPrelude
{
    public static bool TryHandleExecuteReaderPrelude(
        this DbConnectionMockBase connection,
        CommandType commandType,
        string commandText,
        DbParameterCollection pars,
        Func<DbDataReader> emptyReaderFactory,
        bool normalizeSqlInput,
        out DbDataReader? reader,
        out List<string> statements)
    {
        statements = [];

        if (commandType == CommandType.StoredProcedure)
        {
            connection.ExecuteStoredProcedure(commandText, pars);
            connection.SetLastFoundRows(0);
            connection.Metrics.IncrementReaderProcessedStatements();
            connection.Metrics.IncrementReaderStoredProcedureStatement();
            reader = emptyReaderFactory();
            return true;
        }

        var sql = normalizeSqlInput
            ? commandText.NormalizeString()
            : commandText;
        statements = [.. SqlQueryParser
            .SplitStatements(sql, connection.ExecutionDialect)
            .Where(s => !string.IsNullOrWhiteSpace(s))];

        if (statements.Count == 1 && statements[0].TrimStart().StartsWith("CALL", StringComparison.OrdinalIgnoreCase))
        {
            connection.ExecuteCall(statements[0], pars);
            connection.SetLastFoundRows(0);
            connection.Metrics.IncrementReaderProcessedStatements();
            connection.Metrics.IncrementReaderCallStatement();
            reader = emptyReaderFactory();
            return true;
        }

        reader = null;
        return false;
    }
}
