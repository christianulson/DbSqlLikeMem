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
        var metricsEnabled = connection.Metrics.Enabled;

        if (commandType == CommandType.StoredProcedure)
        {
            connection.ExecuteStoredProcedure(commandText, pars);
            connection.SetLastFoundRows(0);
            if (metricsEnabled)
            {
                connection.Metrics.IncrementReaderProcessedStatements();
                connection.Metrics.IncrementReaderStoredProcedureStatement();
            }
            reader = emptyReaderFactory();
            return true;
        }

        var sql = normalizeSqlInput
            ? commandText.NormalizeString()
            : commandText;
        statements = new List<string>();
        foreach (var s in SqlQueryParser.SplitStatements(sql, connection.ExecutionDialect))
        {
            if (string.IsNullOrWhiteSpace(s))
                continue;

            // Preserve previous behavior: store trimmed statements.
            var trimmed = (s.Length > 0 && (char.IsWhiteSpace(s[0]) || char.IsWhiteSpace(s[^1])))
                ? s.Trim()
                : s;
            statements.Add(trimmed);
        }

        if (statements.Count == 1 && statements[0].TrimStart().StartsWith("CALL", StringComparison.OrdinalIgnoreCase))
        {
            connection.ExecuteCall(statements[0], pars);
            connection.SetLastFoundRows(0);
            if (metricsEnabled)
            {
                connection.Metrics.IncrementReaderProcessedStatements();
                connection.Metrics.IncrementReaderCallStatement();
            }
            reader = emptyReaderFactory();
            return true;
        }

        reader = null;
        return false;
    }
}

