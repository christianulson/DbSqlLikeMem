namespace DbSqlLikeMem.Oracle.Test;

/// <summary>
/// EN: Defines the class DapperTests.
/// PT: Define a classe DapperTests.
/// </summary>
public sealed class DapperTests(
    ITestOutputHelper helper
) : DapperCrudTestsBase(
    helper,
    dbFactory: static () => new OracleDbMock(),
    connectionFactory: static db => new OracleConnectionMock((OracleDbMock)db),
    commandFactory: static connection => new OracleCommandMock((OracleConnectionMock)connection));
