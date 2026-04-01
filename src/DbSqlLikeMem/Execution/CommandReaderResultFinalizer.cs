namespace DbSqlLikeMem;

internal static class CommandReaderResultFinalizer
{
    public static void FinalizeReaderExecution(
        this DbConnectionMockBase connection,
        IReadOnlyCollection<TableResultMock> tables,
        int parsedStatementCount)
    {
        var metricsEnabled = connection.Metrics.Enabled;
        var returnedRows = tables.Sum(static t => t.Count);
        if (metricsEnabled)
        {
            connection.Metrics.IncrementReaderProcessedStatements(parsedStatementCount);
            connection.Metrics.IncrementReaderResultTables(tables.Count);
            connection.Metrics.IncrementReaderRowsReturned(returnedRows);
        }

        if (tables.Count == 0 && parsedStatementCount > 0)
        {
            if (metricsEnabled)
                connection.Metrics.IncrementReaderWithoutSelectError();
            throw new InvalidOperationException(SqlExceptionMessages.ExecuteReaderWithoutSelectQuery());
        }

        if (metricsEnabled)
            connection.Metrics.Selects += returnedRows;

        if (tables.Count > 0)
            connection.SetLastFoundRows(tables.Last().Count);
    }
}
