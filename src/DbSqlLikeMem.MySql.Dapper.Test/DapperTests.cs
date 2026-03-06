namespace DbSqlLikeMem.MySql.Dapper.Test;

/// <summary>
/// EN: Defines the class DapperTests.
/// PT: Define a classe DapperTests.
/// </summary>
public sealed class DapperTests(
    ITestOutputHelper helper
) : DapperCrudTestsBase(
    helper,
    dbFactory: static () => new MySqlDbMock(),
    connectionFactory: static db => new MySqlConnectionMock((MySqlDbMock)db),
    commandFactory: static connection => new MySqlCommandMock((MySqlConnectionMock)connection));
