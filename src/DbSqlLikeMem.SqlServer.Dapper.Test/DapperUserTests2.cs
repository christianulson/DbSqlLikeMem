namespace DbSqlLikeMem.SqlServer.Dapper.Test;

/// <summary>
/// EN: Defines the class DapperUserTests2.
/// PT: Define a classe DapperUserTests2.
/// </summary>
public sealed class DapperUserTests2(
    ITestOutputHelper helper
) : DapperUserTestsBase(
    helper,
    dbFactory: static () => new SqlServerDbMock(),
    connectionFactory: static db => new SqlServerConnectionMock((SqlServerDbMock)db),
    commandFactory: static connection => new SqlServerCommandMock((SqlServerConnectionMock)connection));
