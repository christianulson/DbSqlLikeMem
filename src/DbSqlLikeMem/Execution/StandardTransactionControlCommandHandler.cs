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

        if (sqlRaw.StartsWith("savepoint ", StringComparison.OrdinalIgnoreCase))
        {
            connection.CreateSavepoint(ExtractSavepointName(sqlRaw, "savepoint ".Length));
            return true;
        }

        if (sqlRaw.StartsWith("rollback to savepoint ", StringComparison.OrdinalIgnoreCase))
        {
            connection.RollbackTransaction(sqlRaw[22..].Trim());
            return true;
        }

        if (sqlRaw.StartsWith("release savepoint ", StringComparison.OrdinalIgnoreCase))
        {
            if (!releaseSavepointAsNoOp)
                connection.ReleaseSavepoint(sqlRaw[18..].Trim());
            return true;
        }

        if (sqlRaw.Equals("commit", StringComparison.OrdinalIgnoreCase))
        {
            connection.CommitTransaction();
            return true;
        }

        if (sqlRaw.Equals("rollback", StringComparison.OrdinalIgnoreCase))
        {
            connection.RollbackTransaction();
            return true;
        }

        return false;
    }
    private static string ExtractSavepointName(string sqlRaw, int prefixLength)
    {
        var remainder = sqlRaw[prefixLength..].Trim();
        if (string.IsNullOrWhiteSpace(remainder))
            return remainder;

        var end = remainder.IndexOfAny([' ', '\t', '\r', '\n']);
        return end < 0
            ? remainder
            : remainder[..end];
    }
}

