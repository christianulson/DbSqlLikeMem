#if NET462
using DB2Parameter = IBM.Data.DB2.iSeries.iDB2Parameter;
#endif

namespace DbSqlLikeMem.Db2.Dapper.Test;

/// <summary>
/// EN: Defines the class StoredProcedureExecutionTests.
/// PT: Define a classe StoredProcedureExecutionTests.
/// </summary>
public sealed class StoredProcedureExecutionTests(
    ITestOutputHelper helper
) : StoredProcedureExecutionTestsBase<Db2ConnectionMock, Db2CommandMock, DB2Parameter, Db2MockException>(helper)
{
    /// <inheritdoc />
    protected override Db2ConnectionMock CreateOpenConnection()
    {
        var connection = new Db2ConnectionMock();
        connection.Open();
        return connection;
    }

    /// <inheritdoc />
    protected override Db2CommandMock CreateStoredProcedureCommand(Db2ConnectionMock connection, string procedureName)
        => new(connection)
        {
            CommandType = CommandType.StoredProcedure,
            CommandText = procedureName
        };

    /// <inheritdoc />
    protected override Db2CommandMock CreateTextCommand(Db2ConnectionMock connection, string commandText)
        => new(connection)
        {
            CommandText = commandText
        };

    /// <inheritdoc />
    protected override DB2Parameter CreateParameter(string name, object? value, DbType dbType, ParameterDirection direction = ParameterDirection.Input)
        => new()
        {
            ParameterName = name.StartsWith("@") ? name : "@" + name,
            Value = value ?? DBNull.Value,
            DbType = dbType,
            Direction = direction
        };

    /// <inheritdoc />
    protected override int GetErrorCode(Db2MockException exception) => exception.ErrorCode;
}
