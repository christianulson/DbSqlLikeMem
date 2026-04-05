using DbSqlLikeMem.TestTools.Tests.Performance;

namespace DbSqlLikeMem.Firebird.Test.Fidelity.Performance;

/// <summary>
/// EN: Runs Firebird fidelity tests for the shared performance workflows.
/// PT: Executa testes de fidelidade Firebird para os fluxos compartilhados de performance.
/// </summary>
public sealed class PerformanceTests(
    ITestOutputHelper helper
    ) : PerformanceTestsBase<FirebirdConnectionMock, FbConnection>(
    helper,
    new FirebirdProviderSqlDialect(),
    () => new FirebirdConnectionMock(),
    s => new FbConnection(s)
    )
{
}
