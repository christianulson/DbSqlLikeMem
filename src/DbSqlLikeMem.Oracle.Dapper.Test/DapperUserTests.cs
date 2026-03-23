namespace DbSqlLikeMem.Oracle.Test;

/// <summary>
/// EN: Covers Oracle Dapper user-query scenarios against the mock provider.
/// PT: Cobre cenarios de consulta de usuarios do Dapper para Oracle contra o provedor mock.
/// </summary>
public sealed class DapperUserTests(
    ITestOutputHelper helper
) : DapperUserTestsBase(
    helper,
    dbFactory: static () => new OracleDbMock(),
    connectionFactory: static db => new OracleConnectionMock((OracleDbMock)db),
    commandFactory: static connection => new OracleCommandMock((OracleConnectionMock)connection));
