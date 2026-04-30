namespace DbSqlLikeMem.Sqlite.Dapper.Test;

/// <summary>
/// EN: Covers SQLite stored procedure execution scenarios against the Dapper provider.
/// PT: Cobre cenarios de execucao de stored procedures SQLite contra o provedor Dapper.
/// </summary>
public sealed class StoredProcedureExecutionTests(
    ITestOutputHelper helper
) : SqliteStoredProcedureExecutionTestsBase(helper)
{
    /// <inheritdoc />
    protected override SqliteConnectionMock CreateOpenConnection()
    {
        var connection = new SqliteConnectionMock();
        connection.Open();
        return connection;
    }

    /// <inheritdoc />
    protected override SqliteCommandMock CreateStoredProcedureCommand(SqliteConnectionMock connection, string procedureName)
        => new(connection)
        {
            CommandType = CommandType.StoredProcedure,
            CommandText = procedureName
        };

    /// <inheritdoc />
    protected override SqliteCommandMock CreateTextCommand(SqliteConnectionMock connection, string commandText)
        => new(connection)
        {
            CommandText = commandText
        };
}
