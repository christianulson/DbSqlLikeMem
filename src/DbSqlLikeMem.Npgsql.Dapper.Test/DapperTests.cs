namespace DbSqlLikeMem.Npgsql.Test;

/// <summary>
/// EN: Defines the class DapperTests.
/// PT: Define a classe DapperTests.
/// </summary>
public sealed class DapperTests(
    ITestOutputHelper helper
) : DapperCrudTestsBase(
    helper,
    dbFactory: static () => new NpgsqlDbMock(),
    connectionFactory: static db => new NpgsqlConnectionMock((NpgsqlDbMock)db),
    commandFactory: static connection => new NpgsqlCommandMock((NpgsqlConnectionMock)connection));
