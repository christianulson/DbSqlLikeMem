using DbSqlLikeMem.SqlServer.TestTools;
using DbSqlLikeMem.TestTools.Tests.DML;

namespace DbSqlLikeMem.SqlServer.Test.Fidelity.DML;

/// <summary>
/// EN: Runs SQL Server fidelity tests for the shared batch workflows.
/// PT: Executa testes de fidelidade SQL Server para os fluxos compartilhados de batch.
/// </summary>
public class BatchTests(
    ITestOutputHelper helper
    ) : BatchTestsBase<SqlServerConnectionMock, SqlConnection>(
    helper,
    new SqlServerProviderSqlDialect(),
    () => new SqlServerConnectionMock(),
    s => new SqlConnection(s)
    )
{
}
