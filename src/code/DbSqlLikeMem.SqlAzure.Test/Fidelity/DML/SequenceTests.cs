using DbSqlLikeMem.SqlAzure.TestTools;
using DbSqlLikeMem.TestTools.Tests.DML;

namespace DbSqlLikeMem.SqlAzure.Test.Fidelity.DML;

/// <summary>
/// EN: Runs SQL Azure fidelity tests for the shared sequence workflows.
/// PT: Executa testes de fidelidade SQL Azure para os fluxos compartilhados de sequence.
/// </summary>
public class SequenceTests(
    ITestOutputHelper helper
    ) : SequenceTestsBase<SqlAzureConnectionMock, SqlConnection>(
    helper,
    new SqlAzureProviderSqlDialect(),
    () => new SqlAzureConnectionMock(),
    s => new SqlConnection(s)
    )
{
}
