using DbSqlLikeMem.SqlServer.TestTools;
using DbSqlLikeMem.TestTools.Tests.DML;

namespace DbSqlLikeMem.SqlServer.Test.Fidelity.DML;

/// <summary>
/// EN: Runs SQL Server fidelity tests for the shared insert workflows.
/// PT: Executa testes de fidelidade SQL Server para os fluxos compartilhados de insert.
/// </summary>
public class InsertTests(
    ITestOutputHelper helper
    ) : InsertTestsBase<SqlServerConnectionMock, SqlConnection>(
    helper,
    new SqlServerProviderSqlDialect(),
    static () => new SqlServerConnectionMock(Db),
    s => new SqlConnection(s)
    )
{
    private static readonly SqlServerDbMock Db = new() { ThreadSafe = true };
}
