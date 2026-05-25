using DbSqlLikeMem.SqlServer.TestTools;
using DbSqlLikeMem.TestTools.Tests.Query;

namespace DbSqlLikeMem.SqlServer.Test.Fidelity.Query;

/// <summary>
/// EN: Runs SQL Server fidelity tests for the shared scalar temporal workflow.
/// PT-br: Executa testes de fidelidade SQL Server para o fluxo escalar temporal compartilhado.
/// </summary>
public class ScalarTemporalTests(
    ITestOutputHelper helper
    ) : ScalarTemporalTestsBase<SqlServerConnectionMock, SqlConnection>(
    helper,
    new SqlServerProviderSqlDialect(),
    () => new SqlServerConnectionMock(),
    s => new SqlConnection(s)
    )
{
}
