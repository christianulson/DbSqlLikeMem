namespace DbSqlLikeMem.Firebird.Dapper.Test;

/// <summary>
/// EN: Covers Firebird stored procedure execution scenarios against the Dapper provider.
/// PT: Cobre cenarios de execucao de stored procedure Firebird contra o provedor Dapper.
/// </summary>
public sealed class StoredProcedureExecutionTests(
    ITestOutputHelper helper
) : StoredProcedureExecutionTestsBase<FirebirdConnectionMock, FirebirdCommandMock, FbParameter, FirebirdMockException>(helper)
{
    /// <inheritdoc />
    protected override FirebirdConnectionMock CreateOpenConnection()
    {
        var connection = new FirebirdConnectionMock();
        connection.Open();
        return connection;
    }

    /// <inheritdoc />
    protected override FirebirdCommandMock CreateStoredProcedureCommand(FirebirdConnectionMock connection, string procedureName)
        => new(connection)
        {
            CommandType = CommandType.StoredProcedure,
            CommandText = procedureName
        };

    /// <inheritdoc />
    protected override FirebirdCommandMock CreateTextCommand(FirebirdConnectionMock connection, string commandText)
        => new(connection)
        {
            CommandText = commandText
        };

    /// <inheritdoc />
    protected override FbParameter CreateParameter(string name, object? value, DbType dbType, ParameterDirection direction = ParameterDirection.Input)
        => new()
        {
            ParameterName = name.StartsWith("@") ? name : "@" + name,
            Value = value ?? DBNull.Value,
            DbType = dbType,
            Direction = direction
        };

    /// <inheritdoc />
    protected override int GetErrorCode(FirebirdMockException exception) => exception.ErrorCode;
}
