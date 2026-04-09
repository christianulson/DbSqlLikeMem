namespace DbSqlLikeMem.Sqlite.Dapper.Test;

/// <summary>
/// EN: Covers SQLite Dapper user-query scenarios against the mock provider.
/// PT: Cobre cenarios de consulta de usuarios do Dapper para SQLite contra o provedor mock.
/// </summary>
public sealed class DapperUserTests(
    ITestOutputHelper helper
) : DapperUserTestsBase(
    helper,
    dbFactory: static () => new SqliteDbMock(),
    connectionFactory: static db => new SqliteConnectionMock((SqliteDbMock)db),
    commandFactory: static connection => new SqliteCommandMock((SqliteConnectionMock)connection));
