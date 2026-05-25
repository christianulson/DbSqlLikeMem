using DbSqlLikeMem.SqlServer.TestTools;
using DbSqlLikeMem.TestTools.Tests.Performance;

namespace DbSqlLikeMem.SqlServer.Test.Fidelity.Performance;

/// <summary>
/// EN: Runs SQL Server fidelity tests for the shared performance workflows.
/// PT-br: Executa testes de fidelidade SQL Server para os fluxos compartilhados de performance.
/// </summary>
public class PerformanceTests(
    ITestOutputHelper helper
    ) : PerformanceTestsBase<SqlServerConnectionMock, SqlConnection>(
    helper,
    new SqlServerProviderSqlDialect(),
    () => new SqlServerConnectionMock(),
    s => new SqlConnection(s)
    )
{
}
