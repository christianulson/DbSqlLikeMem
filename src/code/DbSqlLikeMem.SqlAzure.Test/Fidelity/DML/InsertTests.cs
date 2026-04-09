using DbSqlLikeMem.SqlAzure.TestTools;
using DbSqlLikeMem.TestTools.Tests.DML;

namespace DbSqlLikeMem.SqlAzure.Test.Fidelity.DML;

/// <summary>
/// EN: Runs SQL Azure fidelity tests for the shared insert workflows.
/// PT: Executa testes de fidelidade SQL Azure para os fluxos compartilhados de insert.
/// </summary>
public class InsertTests(
    ITestOutputHelper helper
    ) : InsertTestsBase<SqlAzureConnectionMock, SqlConnection>(
    helper,
    new SqlAzureProviderSqlDialect(),
    () => new SqlAzureConnectionMock(),
    s => new SqlConnection(s)
    )
{
}
