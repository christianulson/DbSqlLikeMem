using DbSqlLikeMem.SqlServer.TestTools;
using DbSqlLikeMem.TestTools.Tests.DML;

namespace DbSqlLikeMem.SqlServer.Test.Fidelity.DML;

/// <summary>
/// EN: Runs SQL Server fidelity tests for the shared CRUD workflows.
/// PT-br: Executa testes de fidelidade SQL Server para os fluxos compartilhados de CRUD.
/// </summary>
public class CrudTests(
    ITestOutputHelper helper
    ) : CrudTestsBase<SqlServerConnectionMock, SqlConnection>(
    helper,
    new SqlServerProviderSqlDialect(),
    () => new SqlServerConnectionMock(),
    s => new SqlConnection(s)
    )
{
}
