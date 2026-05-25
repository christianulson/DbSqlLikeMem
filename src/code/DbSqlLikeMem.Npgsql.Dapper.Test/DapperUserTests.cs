namespace DbSqlLikeMem.Npgsql.Test;

/// <summary>
/// EN: Covers PostgreSQL Dapper user-query scenarios against the mock provider.
/// PT-br: Cobre cenarios de consulta de usuarios do Dapper para PostgreSQL contra o provedor mock.
/// </summary>
public sealed class DapperUserTests(
    ITestOutputHelper helper
) : DapperUserTestsBase(
    helper,
    dbFactory: static () => new NpgsqlDbMock(),
    connectionFactory: static db => new NpgsqlConnectionMock((NpgsqlDbMock)db),
    commandFactory: static connection => new NpgsqlCommandMock((NpgsqlConnectionMock)connection));
