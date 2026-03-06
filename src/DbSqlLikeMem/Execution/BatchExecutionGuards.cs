namespace DbSqlLikeMem;

internal static class BatchExecutionGuards
{
    public static TConnection RequireConnection<TConnection>(TConnection? connection)
        where TConnection : DbConnectionMockBase
    {
        return connection ?? throw new InvalidOperationException(SqlExceptionMessages.BatchConnectionRequired());
    }

    public static void RequireAtLeastOneCommand(int commandCount)
    {
        if (commandCount == 0)
            throw new InvalidOperationException(SqlExceptionMessages.BatchCommandsMustContainCommand());
    }

    public static InvalidOperationException? GetInvalidConnectionStateException(
        DbConnectionMockBase connection,
        bool allowConnectingState)
    {
        var state = connection.State;
        if (allowConnectingState)
        {
            if (state is ConnectionState.Open or ConnectionState.Connecting)
                return null;
        }
        else if (state == ConnectionState.Open)
        {
            return null;
        }

        return new InvalidOperationException(SqlExceptionMessages.BatchConnectionMustBeOpenCurrentState(state));
    }

    public static void RequireOpenConnectionState(DbConnectionMockBase connection)
    {
        var exception = GetInvalidConnectionStateException(connection, allowConnectingState: false);
        if (exception is not null)
            throw exception;
    }
}
