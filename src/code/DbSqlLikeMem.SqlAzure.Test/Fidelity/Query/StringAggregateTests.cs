using DbSqlLikeMem.SqlAzure.TestTools;
using DbSqlLikeMem.TestTools.Tests.Query;

namespace DbSqlLikeMem.SqlAzure.Test.Fidelity.Query;

/// <summary>
/// EN: Runs SQL Azure fidelity tests for the shared string-aggregation workflows.
/// PT: Executa testes de fidelidade SQL Azure para os fluxos compartilhados de agregacao de strings.
/// </summary>
public class StringAggregateTests(
    ITestOutputHelper helper
    ) : StringAggregateTestsBase<SqlAzureConnectionMock, SqlConnection>(
    helper,
    new SqlAzureProviderSqlDialect(),
    () => new SqlAzureConnectionMock(),
    s => new SqlConnection(s)
    )
{
}
