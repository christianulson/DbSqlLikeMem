namespace DbSqlLikeMem.Sqlite.Dapper.Test;

/// <summary>
/// EN: Covers SQLite Dapper CRUD scenarios against the mock provider.
/// PT-br: Cobre cenarios CRUD de Dapper para SQLite contra o provedor mock.
/// </summary>
public sealed class DapperTests(
    ITestOutputHelper helper
) : DapperCrudTestsBase(
    helper,
    dbFactory: static () => new SqliteDbMock(),
    connectionFactory: static db => new SqliteConnectionMock((SqliteDbMock)db),
    commandFactory: static connection => new SqliteCommandMock((SqliteConnectionMock)connection));
