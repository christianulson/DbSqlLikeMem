namespace DbSqlLikeMem.SqlServer.Dapper.Test;

/// <summary>
/// EN: Covers the second SQL Server Dapper user-query scenario set against the mock provider.
/// PT: Cobre o segundo conjunto de cenarios de consulta de usuarios do Dapper para SQL Server contra o provedor mock.
/// </summary>
public sealed class DapperUserTests2(
    ITestOutputHelper helper
) : DapperUserTestsBase(
    helper,
    dbFactory: static () => new SqlServerDbMock(),
    connectionFactory: static db => new SqlServerConnectionMock((SqlServerDbMock)db),
    commandFactory: static connection => new SqlServerCommandMock((SqlServerConnectionMock)connection));
