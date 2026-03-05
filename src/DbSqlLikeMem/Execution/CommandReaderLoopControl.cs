namespace DbSqlLikeMem;

internal static class CommandReaderLoopControl
{
    public static bool TryHandleReaderControlCommand(
        this DbConnectionMockBase connection,
        string sqlRaw,
        DbParameterCollection pars,
        TryExecutePipelineCommand tryExecuteTransactionControl,
        ref int parsedStatementCount)
    {
        if (tryExecuteTransactionControl(sqlRaw, out var transactionControlResult))
        {
            connection.SetLastFoundRows(transactionControlResult);
            connection.Metrics.IncrementReaderControlStatement();
            parsedStatementCount++;
            return true;
        }

        if (sqlRaw.StartsWith("CALL", StringComparison.OrdinalIgnoreCase))
        {
            connection.ExecuteCall(sqlRaw, pars);
            connection.SetLastFoundRows(0);
            connection.Metrics.IncrementReaderCallStatement();
            parsedStatementCount++;
            return true;
        }

        return false;
    }
}
