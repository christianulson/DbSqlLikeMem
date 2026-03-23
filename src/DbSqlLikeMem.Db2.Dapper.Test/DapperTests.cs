namespace DbSqlLikeMem.Db2.Dapper.Test;

/// <summary>
/// EN: Covers DB2 Dapper CRUD scenarios against the mock provider.
/// PT: Cobre cenarios CRUD de Dapper para DB2 contra o provedor mock.
/// </summary>
public sealed class DapperTests(
    ITestOutputHelper helper
) : DapperCrudTestsBase(
    helper,
    dbFactory: static () => new Db2DbMock(),
    connectionFactory: static db => new Db2ConnectionMock((Db2DbMock)db),
    commandFactory: static connection => new Db2CommandMock((Db2ConnectionMock)connection));
