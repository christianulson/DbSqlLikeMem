namespace DbSqlLikeMem.SqlServer.Dapper.Test;

/// <summary>
/// EN: Covers SQL Server Dapper CRUD scenarios against the mock provider.
/// PT: Cobre cenarios CRUD de Dapper para SQL Server contra o provedor mock.
/// </summary>
public sealed class DapperTests(
    ITestOutputHelper helper
) : DapperCrudTestsBase(
    helper,
    dbFactory: static () => new SqlServerDbMock(),
    connectionFactory: static db => new SqlServerConnectionMock((SqlServerDbMock)db),
    commandFactory: static connection => new SqlServerCommandMock((SqlServerConnectionMock)connection));
