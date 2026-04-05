namespace DbSqlLikeMem.Firebird.Dapper.Test;

/// <summary>
/// EN: Covers Firebird Dapper user-query scenarios against the mock provider.
/// PT: Cobre cenarios de consulta de usuarios do Dapper para Firebird contra o provedor simulado.
/// </summary>
public sealed class DapperUserTests(
    ITestOutputHelper helper
) : DapperUserTestsBase(
    helper,
    dbFactory: static () => new FirebirdDbMock(),
    connectionFactory: static db => new FirebirdConnectionMock((FirebirdDbMock)db),
    commandFactory: static connection => new FirebirdCommandMock((FirebirdConnectionMock)connection));
