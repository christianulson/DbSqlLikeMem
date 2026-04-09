using DbSqlLikeMem.SqlAzure.TestTools;
using DbSqlLikeMem.TestTools.Tests.Transactions;

namespace DbSqlLikeMem.SqlAzure.Test.Fidelity.Transactions;

/// <summary>
/// EN: Runs SQL Azure fidelity tests for the shared transaction workflows.
/// PT: Executa testes de fidelidade SQL Azure para os fluxos compartilhados de transacao.
/// </summary>
public class TransactionTests(
    ITestOutputHelper helper
    ) : TransactionTestsBase<SqlAzureConnectionMock, SqlConnection>(
    helper,
    new SqlAzureProviderSqlDialect(),
    () => new SqlAzureConnectionMock(),
    s => new SqlConnection(s)
    )
{
}
