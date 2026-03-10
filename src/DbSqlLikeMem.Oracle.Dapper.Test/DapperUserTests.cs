namespace DbSqlLikeMem.Oracle.Test;

/// <summary>
/// EN: Defines the class DapperUserTests.
/// PT: Define a classe DapperUserTests.
/// </summary>
public sealed class DapperUserTests(
    ITestOutputHelper helper
) : DapperUserTestsBase(
    helper,
    dbFactory: static () => new OracleDbMock(),
    connectionFactory: static db => new OracleConnectionMock((OracleDbMock)db),
    commandFactory: static connection => new OracleCommandMock((OracleConnectionMock)connection));
