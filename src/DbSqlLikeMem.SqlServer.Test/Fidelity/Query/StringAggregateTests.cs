using DbSqlLikeMem.SqlServer.TestTools;
using DbSqlLikeMem.TestTools.Tests.Query;

namespace DbSqlLikeMem.SqlServer.Test.Fidelity.Query;

/// <summary>
/// EN: Runs SQL Server fidelity tests for the shared string-aggregation workflows.
/// PT: Executa testes de fidelidade SQL Server para os fluxos compartilhados de agregacao de strings.
/// </summary>
public class StringAggregateTests(
    ITestOutputHelper helper
    ) : StringAggregateTestsBase<SqlServerConnectionMock, SqlConnection>(
    helper,
    new SqlServerProviderSqlDialect(),
    () => new SqlServerConnectionMock(),
    s => new SqlConnection(s)
    )
{
}
