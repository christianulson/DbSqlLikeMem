namespace DbSqlLikeMem.Sqlite.Dapper.Test;

/// <summary>
/// EN: Covers the second SQLite Dapper user-query scenario set against the mock provider.
/// PT: Cobre o segundo conjunto de cenarios de consulta de usuarios do Dapper para SQLite contra o provedor mock.
/// </summary>
public sealed class DapperUserTests2(
    ITestOutputHelper helper
) : DapperUserTestsBase(
    helper,
    dbFactory: static () => new SqliteDbMock(),
    connectionFactory: static db => new SqliteConnectionMock((SqliteDbMock)db),
    commandFactory: static connection => new SqliteCommandMock((SqliteConnectionMock)connection));
