using DbSqlLikeMem.MySql.TestTools;
using DbSqlLikeMem.TestTools.Tests.Performance;

namespace DbSqlLikeMem.MySql.Test.Fidelity.Performance;

/// <summary>
/// EN: Runs MySQL fidelity tests for the shared performance workflows.
/// PT: Executa testes de fidelidade MySQL para os fluxos compartilhados de performance.
/// </summary>
public class PerformanceTests(
    ITestOutputHelper helper
    ) : PerformanceTestsBase<MySqlConnectionMock, MySqlConnection>(
    helper,
    new MySqlProviderSqlDialect(),
    () => new MySqlConnectionMock(),
    s => new MySqlConnection(s)
    )
{
}
