namespace DbSqlLikeMem.Sqlite.Dapper.Test;

/// <summary>
/// EN: Defines the class StoredProcedureExecutionTests.
/// PT: Define a classe StoredProcedureExecutionTests.
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
