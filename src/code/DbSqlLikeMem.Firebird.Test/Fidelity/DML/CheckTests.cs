using DbSqlLikeMem.TestTools.Tests.DML;

namespace DbSqlLikeMem.Firebird.Test.Fidelity.DML;

/// <summary>
/// EN: Runs Firebird fidelity tests for the shared check-constraint workflows.
/// PT: Executa testes de fidelidade Firebird para os fluxos compartilhados de restricao check.
/// </summary>
[FidelityNativeClientSkip]
public class CheckTests(
    ITestOutputHelper helper
    ) : CheckTestsBase<FirebirdConnectionMock, FbConnection>(
    helper,
    new FirebirdProviderSqlDialect(),
    static () => new FirebirdConnectionMock(Get(FirebirdDbVersions.Default, _ => new FirebirdDbMock(_) { ThreadSafe = true })),
    FirebirdConnectionFactory.Create
    )
{
}
