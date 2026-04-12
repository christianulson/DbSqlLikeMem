namespace DbSqlLikeMem;

internal static class StandardTransactionControlCommandHandler
{
    public static bool TryExecuteStandardTransactionControl(
        this DbConnectionMockBase connection,
        string sqlRaw,
        bool releaseSavepointAsNoOp,
        out DmlExecutionResult affectedRows)
    {
        affectedRows = new DmlExecutionResult();

        if (sqlRaw.Equals("begin", StringComparison.OrdinalIgnoreCase) ||
            sqlRaw.Equals("begin transaction", StringComparison.OrdinalIgnoreCase) ||
            sqlRaw.Equals("start transaction", StringComparison.OrdinalIgnoreCase))
        {
            if (connection.State != ConnectionState.Open)
                connection.Open();

            if (!connection.HasActiveTransaction)
                connection.BeginTransaction();

            return true;
        }

        var isSqlServer = string.Equals(
            connection.ProviderExecutionDialect.Name,
            "sqlserver",
            StringComparison.OrdinalIgnoreCase);

        if (isSqlServer && sqlRaw.StartsWith("save transaction ", StringComparison.OrdinalIgnoreCase))
        {
            connection.CreateSavepoint(ExtractSavepointName(sqlRaw, "save transaction ".Length));
            return true;
        }

        if (sqlRaw.StartsWith("savepoint ", StringComparison.OrdinalIgnoreCase))
        {
            connection.CreateSavepoint(ExtractSavepointName(sqlRaw, "savepoint ".Length));
            return true;
        }

        if (isSqlServer && sqlRaw.StartsWith("rollback transaction ", StringComparison.OrdinalIgnoreCase))
        {
            connection.RollbackTransaction(ExtractSavepointName(sqlRaw, "rollback transaction ".Length));
            return true;
        }

        if (sqlRaw.StartsWith("rollback to savepoint ", StringComparison.OrdinalIgnoreCase))
        {
            connection.RollbackTransaction(ExtractSavepointName(sqlRaw, "rollback to savepoint ".Length));
            return true;
        }

        if (sqlRaw.StartsWith("release savepoint ", StringComparison.OrdinalIgnoreCase))
        {
            if (!releaseSavepointAsNoOp)
                connection.ReleaseSavepoint(ExtractSavepointName(sqlRaw, "release savepoint ".Length));
            return true;
        }

        if (sqlRaw.Equals("commit", StringComparison.OrdinalIgnoreCase))
        {
            connection.CommitTransaction();
            return true;
        }

        if (sqlRaw.Equals("commit transaction", StringComparison.OrdinalIgnoreCase))
        {
            connection.CommitTransaction();
            return true;
        }

        if (sqlRaw.Equals("rollback", StringComparison.OrdinalIgnoreCase))
        {
            connection.RollbackTransaction();
            return true;
        }

        if (sqlRaw.Equals("rollback transaction", StringComparison.OrdinalIgnoreCase))
        {
            connection.RollbackTransaction();
            return true;
        }

        return false;
    }

    private static string ExtractSavepointName(string sqlRaw, int prefixLength)
    {
        var remainder = sqlRaw.AsSpan(prefixLength).Trim();
        var end = 0;
        while (end < remainder.Length)
        {
            var ch = remainder[end];
            if (ch is ' ' or '\t' or '\r' or '\n')
                break;

            end++;
        }

        return end == remainder.Length
            ? remainder.ToString()
            : remainder[..end].ToString();
    }
}

