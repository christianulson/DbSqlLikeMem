namespace DbSqlLikeMem.Npgsql.Test;

/// <summary>
/// EN: Covers PostgreSQL Dapper CRUD scenarios against the mock provider.
/// PT: Cobre cenarios CRUD de Dapper para PostgreSQL contra o provedor mock.
/// </summary>
public sealed class DapperTests(
    ITestOutputHelper helper
) : DapperCrudTestsBase(
    helper,
    dbFactory: static () => new NpgsqlDbMock(),
    connectionFactory: static db => new NpgsqlConnectionMock((NpgsqlDbMock)db),
    commandFactory: static connection => new NpgsqlCommandMock((NpgsqlConnectionMock)connection));
