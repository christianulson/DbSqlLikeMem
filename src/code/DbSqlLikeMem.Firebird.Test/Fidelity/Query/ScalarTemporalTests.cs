using DbSqlLikeMem.TestTools.Tests.Query;

namespace DbSqlLikeMem.Firebird.Test.Fidelity.Query;

/// <summary>
/// EN: Runs Firebird fidelity tests for the shared scalar temporal workflows.
/// PT-br: Executa testes de fidelidade Firebird para os fluxos compartilhados temporais escalares.
/// </summary>
[FidelityNativeClientSkip]
public class ScalarTemporalTests(
    ITestOutputHelper helper
    ) : ScalarTemporalTestsBase<FirebirdConnectionMock, FbConnection>(
    helper,
    new FirebirdProviderSqlDialect(),
    () => new FirebirdConnectionMock(Get(FirebirdDbVersions.Default, _ => new FirebirdDbMock(_) { ThreadSafe = true })),
    FirebirdConnectionFactory.Create
    )
{
}

