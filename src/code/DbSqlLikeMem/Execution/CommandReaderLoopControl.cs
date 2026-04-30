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
        var metricsEnabled = connection.Metrics.Enabled;
        if (tryExecuteTransactionControl(sqlRaw, out var transactionControlResult))
        {
            connection.SetLastFoundRows(transactionControlResult.AffectedRows);
            if (metricsEnabled)
                connection.Metrics.IncrementReaderControlStatement();
            parsedStatementCount++;
            return true;
        }

        if (sqlRaw.StartsWith("CALL", StringComparison.OrdinalIgnoreCase))
        {
            connection.ExecuteCall(sqlRaw, pars);
            connection.SetLastFoundRows(0);
            if (metricsEnabled)
                connection.Metrics.IncrementReaderCallStatement();
            parsedStatementCount++;
            return true;
        }

        return false;
    }
}