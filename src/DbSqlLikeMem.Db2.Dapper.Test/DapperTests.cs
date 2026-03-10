namespace DbSqlLikeMem.Db2.Dapper.Test;

/// <summary>
/// EN: Defines the class DapperTests.
/// PT: Define a classe DapperTests.
/// </summary>
public sealed class DapperTests(
    ITestOutputHelper helper
) : DapperCrudTestsBase(
    helper,
    dbFactory: static () => new Db2DbMock(),
    connectionFactory: static db => new Db2ConnectionMock((Db2DbMock)db),
    commandFactory: static connection => new Db2CommandMock((Db2ConnectionMock)connection));
