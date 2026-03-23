namespace DbSqlLikeMem.SqlServer.Dapper.Test;

/// <summary>
/// EN: Covers SQL Server Dapper user-query scenarios against the mock provider.
/// PT: Cobre cenarios de consulta de usuarios do Dapper para SQL Server contra o provedor mock.
/// </summary>
public sealed class DapperUserTests(
    ITestOutputHelper helper
) : DapperUserTestsBase(
    helper,
    dbFactory: static () => new SqlServerDbMock(),
    connectionFactory: static db => new SqlServerConnectionMock((SqlServerDbMock)db),
    commandFactory: static connection => new SqlServerCommandMock((SqlServerConnectionMock)connection));
