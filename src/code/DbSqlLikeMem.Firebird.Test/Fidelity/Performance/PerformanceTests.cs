using DbSqlLikeMem.TestTools.Tests.Performance;

namespace DbSqlLikeMem.Firebird.Test.Fidelity.Performance;

/// <summary>
/// EN: Runs Firebird fidelity tests for the shared performance workflows.
/// PT: Executa testes de fidelidade Firebird para os fluxos compartilhados de performance.
/// </summary>
[FidelityNativeClientSkip]
public sealed class PerformanceTests(
    ITestOutputHelper helper
    ) : PerformanceTestsBase<FirebirdConnectionMock, FbConnection>(
    helper,
    new FirebirdProviderSqlDialect(),
    () => new FirebirdConnectionMock(Get(FirebirdDbVersions.Default, _ => new FirebirdDbMock(_) { ThreadSafe = true })),
    FirebirdConnectionFactory.Create
    )
{
}

