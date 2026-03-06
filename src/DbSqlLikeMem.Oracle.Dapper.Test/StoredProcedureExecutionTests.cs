namespace DbSqlLikeMem.Oracle.Test;

/// <summary>
/// EN: Defines the class StoredProcedureExecutionTests.
/// PT: Define a classe StoredProcedureExecutionTests.
/// </summary>
public sealed class StoredProcedureExecutionTests(
    ITestOutputHelper helper
) : StoredProcedureExecutionTestsBase<OracleConnectionMock, OracleCommandMock, OracleParameter, OracleMockException>(helper)
{
    /// <inheritdoc />
    protected override OracleConnectionMock CreateOpenConnection()
    {
        var connection = new OracleConnectionMock();
        connection.Open();
        return connection;
    }

    /// <inheritdoc />
    protected override OracleCommandMock CreateStoredProcedureCommand(OracleConnectionMock connection, string procedureName)
        => new(connection)
        {
            CommandType = CommandType.StoredProcedure,
            CommandText = procedureName
        };

    /// <inheritdoc />
    protected override OracleCommandMock CreateTextCommand(OracleConnectionMock connection, string commandText)
        => new(connection)
        {
            CommandText = commandText
        };

    /// <inheritdoc />
    protected override OracleParameter CreateParameter(string name, object? value, DbType dbType, ParameterDirection direction = ParameterDirection.Input)
        => new()
        {
            ParameterName = name.StartsWith("@") ? name : "@" + name,
            Value = value ?? DBNull.Value,
            DbType = dbType,
            Direction = direction
        };

    /// <inheritdoc />
    protected override int GetErrorCode(OracleMockException exception) => exception.ErrorCode;
}
