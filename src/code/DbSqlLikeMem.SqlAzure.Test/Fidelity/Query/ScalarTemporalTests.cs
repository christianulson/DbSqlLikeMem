using DbSqlLikeMem.SqlAzure.TestTools;
using DbSqlLikeMem.TestTools.Tests.Query;

namespace DbSqlLikeMem.SqlAzure.Test.Fidelity.Query;

/// <summary>
/// EN: Runs SQL Azure fidelity tests for the shared scalar temporal workflow.
/// PT: Executa testes de fidelidade SQL Azure para o fluxo escalar temporal compartilhado.
/// </summary>
public class ScalarTemporalTests(
    ITestOutputHelper helper
    ) : ScalarTemporalTestsBase<SqlAzureConnectionMock, SqlConnection>(
    helper,
    new SqlAzureProviderSqlDialect(),
    () => new SqlAzureConnectionMock(),
    s => new SqlConnection(s)
    )
{
}
