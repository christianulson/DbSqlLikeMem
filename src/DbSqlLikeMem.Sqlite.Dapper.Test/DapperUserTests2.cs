namespace DbSqlLikeMem.Sqlite.Dapper.Test;

/// <summary>
/// EN: Defines the class DapperUserTests2.
/// PT: Define a classe DapperUserTests2.
/// </summary>
public sealed class DapperUserTests2(
    ITestOutputHelper helper
) : DapperUserTestsBase(
    helper,
    dbFactory: static () => new SqliteDbMock(),
    connectionFactory: static db => new SqliteConnectionMock((SqliteDbMock)db),
    commandFactory: static connection => new SqliteCommandMock((SqliteConnectionMock)connection));
