namespace DbSqlLikeMem.MySql.Dapper.Test;

/// <summary>
/// EN: Defines the class StoredProcedureExecutionTests.
/// PT: Define a classe StoredProcedureExecutionTests.
/// </summary>
public sealed class StoredProcedureExecutionTests(
    ITestOutputHelper helper
) : StoredProcedureExecutionTestsBase<MySqlConnectionMock, MySqlCommandMock, MySqlParameter, MySqlMockException>(helper)
{
    /// <inheritdoc />
    protected override MySqlConnectionMock CreateOpenConnection()
    {
        var connection = new MySqlConnectionMock();
        connection.Open();
        return connection;
    }

    /// <inheritdoc />
    protected override MySqlCommandMock CreateStoredProcedureCommand(MySqlConnectionMock connection, string procedureName)
        => new(connection)
        {
            CommandType = CommandType.StoredProcedure,
            CommandText = procedureName
        };

    /// <inheritdoc />
    protected override MySqlCommandMock CreateTextCommand(MySqlConnectionMock connection, string commandText)
        => new(connection)
        {
            CommandText = commandText
        };

    /// <inheritdoc />
    protected override MySqlParameter CreateParameter(string name, object? value, DbType dbType, ParameterDirection direction = ParameterDirection.Input)
        => new()
        {
            ParameterName = name.StartsWith("@") ? name : "@" + name,
            Value = value ?? DBNull.Value,
            DbType = dbType,
            Direction = direction
        };

    /// <inheritdoc />
    protected override int GetErrorCode(MySqlMockException exception) => exception.ErrorCode;
}
