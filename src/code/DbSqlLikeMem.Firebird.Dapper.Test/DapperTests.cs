namespace DbSqlLikeMem.Firebird.Dapper.Test;

/// <summary>
/// EN: Covers Firebird Dapper CRUD scenarios against the mock provider.
/// PT-br: Cobre cenarios CRUD de Dapper para Firebird contra o provedor simulado.
/// </summary>
public sealed class DapperTests(
    ITestOutputHelper helper
) : DapperCrudTestsBase(
    helper,
    dbFactory: static () => new FirebirdDbMock(),
    connectionFactory: static db => new FirebirdConnectionMock((FirebirdDbMock)db),
    commandFactory: static connection => new FirebirdCommandMock((FirebirdConnectionMock)connection));
