namespace DbSqlLikeMem.Npgsql.Test;

/// <summary>
/// EN: Covers PostgreSQL stored procedure execution scenarios against the Dapper provider.
/// PT: Cobre cenarios de execucao de stored procedures PostgreSQL contra o provedor Dapper.
/// </summary>
public sealed class StoredProcedureExecutionTests(
    ITestOutputHelper helper
) : StoredProcedureExecutionTestsBase<NpgsqlConnectionMock, NpgsqlCommandMock, NpgsqlParameter, NpgsqlMockException>(helper)
{
    /// <inheritdoc />
    protected override NpgsqlConnectionMock CreateOpenConnection()
    {
        var connection = new NpgsqlConnectionMock();
        connection.Open();
        return connection;
    }

    /// <inheritdoc />
    protected override NpgsqlCommandMock CreateStoredProcedureCommand(NpgsqlConnectionMock connection, string procedureName)
        => new(connection)
        {
            CommandType = CommandType.StoredProcedure,
            CommandText = procedureName
        };

    /// <inheritdoc />
    protected override NpgsqlCommandMock CreateTextCommand(NpgsqlConnectionMock connection, string commandText)
        => new(connection)
        {
            CommandText = commandText
        };

    /// <inheritdoc />
    protected override NpgsqlParameter CreateParameter(string name, object? value, DbType dbType, ParameterDirection direction = ParameterDirection.Input)
        => new()
        {
            ParameterName = name.StartsWith("@") ? name : "@" + name,
            Value = value ?? DBNull.Value,
            DbType = dbType,
            Direction = direction
        };

    /// <inheritdoc />
    protected override int GetErrorCode(NpgsqlMockException exception) => exception.ErrorCode;
}
