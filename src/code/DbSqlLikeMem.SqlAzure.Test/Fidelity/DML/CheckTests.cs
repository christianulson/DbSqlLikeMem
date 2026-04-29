using DbSqlLikeMem.SqlAzure.TestTools;
using DbSqlLikeMem.TestTools.Tests.DML;

namespace DbSqlLikeMem.SqlAzure.Test.Fidelity.DML;

/// <summary>
/// EN: Runs SQL Azure fidelity tests for the shared check-constraint workflows.
/// PT: Executa testes de fidelidade SQL Azure para os fluxos compartilhados de restricao check.
/// </summary>
public class CheckTests(
    ITestOutputHelper helper
    ) : CheckTestsBase<SqlAzureConnectionMock, SqlConnection>(
    helper,
    new SqlAzureProviderSqlDialect(),
    static () => new SqlAzureConnectionMock(Db),
    s => new SqlConnection(s)
    )
{
    private static readonly SqlAzureDbMock Db = new() { ThreadSafe = true };
}
