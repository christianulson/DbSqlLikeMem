using DbSqlLikeMem.Oracle.TestTools;
using DbSqlLikeMem.TestTools.Tests.Performance;

namespace DbSqlLikeMem.Oracle.Test.Fidelity.Performance;

/// <summary>
/// EN: Runs Oracle fidelity tests for the shared performance workflows.
/// PT: Executa testes de fidelidade Oracle para os fluxos compartilhados de performance.
/// </summary>
public class PerformanceTests(
    ITestOutputHelper helper
    ) : PerformanceTestsBase<OracleConnectionMock, OracleConnection>(
    helper,
    new OracleProviderSqlDialect(),
    () => new OracleConnectionMock(),
    s => new OracleConnection(s)
    )
{
}
