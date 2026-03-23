namespace DbSqlLikeMem.MySql.Dapper.Test;

/// <summary>
/// EN: Covers MySQL Dapper CRUD scenarios against the mock provider.
/// PT: Cobre cenarios CRUD de Dapper para MySQL contra o provedor mock.
/// </summary>
public sealed class DapperTests(
    ITestOutputHelper helper
) : DapperCrudTestsBase(
    helper,
    dbFactory: static () => new MySqlDbMock(),
    connectionFactory: static db => new MySqlConnectionMock((MySqlDbMock)db),
    commandFactory: static connection => new MySqlCommandMock((MySqlConnectionMock)connection));
