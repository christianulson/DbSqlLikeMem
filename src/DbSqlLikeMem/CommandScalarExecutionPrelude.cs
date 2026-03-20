namespace DbSqlLikeMem;

internal delegate bool TransactionControlCommandHandler(string sqlRaw, out DmlExecutionResult affectedRows);

internal static class CommandScalarExecutionPrelude
{
    public static bool TryHandleExecuteScalarPrelude(
        this DbConnectionMockBase connection,
        CommandType commandType,
        string commandText,
        DbParameterCollection pars,
        Func<DbDataReader> emptyReaderFactory,
        bool normalizeSqlInput,
        TransactionControlCommandHandler? tryExecuteTransactionControl,
        out object? scalar)
    {
        scalar = DBNull.Value;

        if (connection.TryHandleExecuteReaderPrelude(
            commandType,
            commandText,
            pars,
            emptyReaderFactory,
            normalizeSqlInput,
            out var earlyReader,
            out var statements))
        {
            if (earlyReader is null)
                return false;

            using (earlyReader)
            {
                if (earlyReader.Read())
                {
                    scalar = earlyReader.GetValue(0);
                    return true;
                }
            }

            return true;
        }

        if (statements.Count != 1)
            return false;

        var sqlRaw = statements[0].Trim();
        if (string.IsNullOrWhiteSpace(sqlRaw))
            return false;

        var parsedStatementCount = 0;
        if (tryExecuteTransactionControl is not null
            && connection.TryHandleReaderControlCommand(
                sqlRaw,
                pars,
                TryExecuteTransactionControlAdapter,
                ref parsedStatementCount))
        {
            scalar = connection.GetLastFoundRows();
            return true;
        }

        bool TryExecuteTransactionControlAdapter(string sqlRaw2, out DmlExecutionResult affectedRows)
            => tryExecuteTransactionControl(sqlRaw2, out affectedRows);

        if (sqlRaw.Equals("SELECT CHANGES()", StringComparison.OrdinalIgnoreCase)
            || sqlRaw.Equals("SELECT FOUND_ROWS()", StringComparison.OrdinalIgnoreCase)
            || sqlRaw.Equals("SELECT TOTAL_CHANGES()", StringComparison.OrdinalIgnoreCase)
            || sqlRaw.Equals("SELECT SQLITE3_CHANGES64()", StringComparison.OrdinalIgnoreCase)
            || sqlRaw.Equals("SELECT SQLITE3_TOTAL_CHANGES64()", StringComparison.OrdinalIgnoreCase))
        {
            scalar = connection.GetLastFoundRows();
            return true;
        }

        var q = SqlQueryParser.Parse(sqlRaw, connection.ExecutionDialect, pars);
        if (q is SqlSelectQuery rowCountQuery && IsRowCountHelperSelect(rowCountQuery))
        {
            scalar = connection.GetLastFoundRows();
            return true;
        }

        if (q is not SqlSelectQuery selectQuery || selectQuery.SelectItems.Count != 1)
            return false;

        var executor = AstQueryExecutorFactory.Create(connection.ExecutionDialect, connection, pars);
        var table = executor.ExecuteSelect(selectQuery);
        if (table.Count <= 0)
        {
            scalar = DBNull.Value;
            return true;
        }

        var firstRow = table[0];
        scalar = firstRow.TryGetValue(0, out var value) ? value ?? DBNull.Value : DBNull.Value;
        return true;
    }

    private static bool IsRowCountHelperSelect(SqlSelectQuery query)
    {
        if (query.SelectItems.Count != 1)
            return false;

        if (query.Table is not null
            && !string.Equals(query.Table.Name, "DUAL", StringComparison.OrdinalIgnoreCase))
        {
            return false;
        }

        var raw = query.SelectItems[0].Raw.Trim();
        return raw.Equals("CHANGES()", StringComparison.OrdinalIgnoreCase)
            || raw.Equals("ROW_COUNT()", StringComparison.OrdinalIgnoreCase)
            || raw.Equals("FOUND_ROWS()", StringComparison.OrdinalIgnoreCase)
            || raw.Equals("ROWCOUNT()", StringComparison.OrdinalIgnoreCase)
            || raw.Equals("@@ROWCOUNT", StringComparison.OrdinalIgnoreCase);
    }
}
