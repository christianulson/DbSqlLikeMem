namespace DbSqlLikeMem;

internal static class StandardTransactionControlCommandHandler
{
    public static bool TryExecuteStandardTransactionControl(
        this DbConnectionMockBase connection,
        string sqlRaw,
        bool releaseSavepointAsNoOp,
        out int affectedRows)
    {
        affectedRows = 0;

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
            connection.CreateSavepoint(sqlRaw[10..].Trim());
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
}
