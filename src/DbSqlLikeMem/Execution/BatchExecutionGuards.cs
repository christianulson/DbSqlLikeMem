namespace DbSqlLikeMem;

internal static class BatchExecutionGuards
{
    public static TConnection RequireConnection<TConnection>(TConnection? connection)
        where TConnection : DbConnectionMockBase
    {
        return connection ?? throw new InvalidOperationException(SqlExceptionMessages.BatchConnectionRequired());
    }
}
