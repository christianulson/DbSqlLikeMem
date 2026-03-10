namespace DbSqlLikeMem.Sqlite.Dapper.Test;

/// <summary>
/// EN: Defines the class DapperUserTests.
/// PT: Define a classe DapperUserTests.
/// </summary>
public sealed class DapperUserTests(
    ITestOutputHelper helper
) : DapperUserTestsBase(
    helper,
    dbFactory: static () => new SqliteDbMock(),
    connectionFactory: static db => new SqliteConnectionMock((SqliteDbMock)db),
    commandFactory: static connection => new SqliteCommandMock((SqliteConnectionMock)connection));
