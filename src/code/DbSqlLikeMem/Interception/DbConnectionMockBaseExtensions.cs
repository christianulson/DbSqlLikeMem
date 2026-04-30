namespace DbSqlLikeMem;

internal static class DbConnectionMockBaseExtensions
{
    internal static DbConnection? UnwrapInnerConnection(this DbConnection connection)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(connection, nameof(connection));

        while (connection is InterceptingDbConnection intercepted)
            connection = intercepted.InnerConnection;

        return connection;
    }

    internal static DbConnectionMockBase? AsMockConnection(this DbConnection connection)
        => connection.UnwrapInnerConnection() as DbConnectionMockBase;
}
