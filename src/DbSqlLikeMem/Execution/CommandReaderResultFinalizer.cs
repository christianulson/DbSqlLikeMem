namespace DbSqlLikeMem;

internal static class CommandReaderResultFinalizer
{
    public static void FinalizeReaderExecution(
        this DbConnectionMockBase connection,
        IReadOnlyCollection<TableResultMock> tables,
        int parsedStatementCount)
    {
        var returnedRows = tables.Sum(static t => t.Count);
        connection.Metrics.IncrementReaderProcessedStatements(parsedStatementCount);
        connection.Metrics.IncrementReaderResultTables(tables.Count);
        connection.Metrics.IncrementReaderRowsReturned(returnedRows);

        if (tables.Count == 0 && parsedStatementCount > 0)
        {
            connection.Metrics.IncrementReaderWithoutSelectError();
            throw new InvalidOperationException(SqlExceptionMessages.ExecuteReaderWithoutSelectQuery());
        }

        connection.Metrics.Selects += returnedRows;
    }
}
