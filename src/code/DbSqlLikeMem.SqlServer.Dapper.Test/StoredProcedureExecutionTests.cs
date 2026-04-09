namespace DbSqlLikeMem.SqlServer.Dapper.Test;

/// <summary>
/// EN: Covers SQL Server stored procedure execution scenarios against the Dapper provider.
/// PT: Cobre cenarios de execucao de stored procedures SQL Server contra o provedor Dapper.
/// </summary>
public sealed class StoredProcedureExecutionTests(
    ITestOutputHelper helper
) : StoredProcedureExecutionTestsBase<SqlServerConnectionMock, SqlServerCommandMock, SqlParameter, SqlServerMockException>(helper)
{
    /// <inheritdoc />
    protected override SqlServerConnectionMock CreateOpenConnection()
    {
        var connection = new SqlServerConnectionMock();
        connection.Open();
        return connection;
    }

    /// <inheritdoc />
    protected override SqlServerCommandMock CreateStoredProcedureCommand(SqlServerConnectionMock connection, string procedureName)
        => new(connection)
        {
            CommandType = CommandType.StoredProcedure,
            CommandText = procedureName
        };

    /// <inheritdoc />
    protected override SqlServerCommandMock CreateTextCommand(SqlServerConnectionMock connection, string commandText)
        => new(connection)
        {
            CommandText = commandText
        };

    /// <inheritdoc />
    protected override SqlParameter CreateParameter(string name, object? value, DbType dbType, ParameterDirection direction = ParameterDirection.Input)
        => new()
        {
            ParameterName = name.StartsWith("@") ? name : "@" + name,
            Value = value ?? DBNull.Value,
            DbType = dbType,
            Direction = direction
        };

    /// <inheritdoc />
    protected override int GetErrorCode(SqlServerMockException exception) => exception.ErrorCode;
}
