using DbSqlLikeMem.SqlAzure.TestTools;
using DbSqlLikeMem.TestTools.Tests.Performance;

namespace DbSqlLikeMem.SqlAzure.Test.Fidelity.Performance;

/// <summary>
/// EN: Runs SQL Azure fidelity tests for the shared performance workflows.
/// PT-br: Executa testes de fidelidade SQL Azure para os fluxos compartilhados de performance.
/// </summary>
public class PerformanceTests(
    ITestOutputHelper helper
    ) : PerformanceTestsBase<SqlAzureConnectionMock, SqlConnection>(
    helper,
    new SqlAzureProviderSqlDialect(),
    () => new SqlAzureConnectionMock(),
    s => new SqlConnection(s)
    )
{
}
