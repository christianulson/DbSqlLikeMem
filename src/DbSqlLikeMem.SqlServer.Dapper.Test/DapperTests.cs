namespace DbSqlLikeMem.SqlServer.Dapper.Test;

/// <summary>
/// EN: Defines the class DapperTests.
/// PT: Define a classe DapperTests.
/// </summary>
public sealed class DapperTests(
    ITestOutputHelper helper
) : DapperCrudTestsBase(
    helper,
    dbFactory: static () => new SqlServerDbMock(),
    connectionFactory: static db => new SqlServerConnectionMock((SqlServerDbMock)db),
    commandFactory: static connection => new SqlServerCommandMock((SqlServerConnectionMock)connection));
