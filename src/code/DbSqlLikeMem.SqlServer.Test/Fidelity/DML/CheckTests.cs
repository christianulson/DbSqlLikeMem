using DbSqlLikeMem.SqlServer.TestTools;
using DbSqlLikeMem.TestTools.Tests.DML;

namespace DbSqlLikeMem.SqlServer.Test.Fidelity.DML;

/// <summary>
/// EN: Runs SQL Server fidelity tests for the shared check-constraint workflows.
/// PT-br: Executa testes de fidelidade SQL Server para os fluxos compartilhados de restricao check.
/// </summary>
public class CheckTests(
    ITestOutputHelper helper
    ) : CheckTestsBase<SqlServerConnectionMock, SqlConnection>(
    helper,
    new SqlServerProviderSqlDialect(),
    static () => new SqlServerConnectionMock(Db),
    s => new SqlConnection(s)
    )
{
    private static readonly SqlServerDbMock Db = new() { ThreadSafe = true };
}
