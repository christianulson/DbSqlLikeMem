namespace DbSqlLikeMem.Oracle.Test;

/// <summary>
/// EN: Covers Oracle Dapper CRUD scenarios against the mock provider.
/// PT: Cobre cenarios CRUD de Dapper para Oracle contra o provedor mock.
/// </summary>
public sealed class DapperTests(
    ITestOutputHelper helper
) : DapperCrudTestsBase(
    helper,
    dbFactory: static () => new OracleDbMock(),
    connectionFactory: static db => new OracleConnectionMock((OracleDbMock)db),
    commandFactory: static connection => new OracleCommandMock((OracleConnectionMock)connection));
