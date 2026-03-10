namespace DbSqlLikeMem.Npgsql.Test;

/// <summary>
/// EN: Defines the class DapperUserTests.
/// PT: Define a classe DapperUserTests.
/// </summary>
public sealed class DapperUserTests(
    ITestOutputHelper helper
) : DapperUserTestsBase(
    helper,
    dbFactory: static () => new NpgsqlDbMock(),
    connectionFactory: static db => new NpgsqlConnectionMock((NpgsqlDbMock)db),
    commandFactory: static connection => new NpgsqlCommandMock((NpgsqlConnectionMock)connection));
