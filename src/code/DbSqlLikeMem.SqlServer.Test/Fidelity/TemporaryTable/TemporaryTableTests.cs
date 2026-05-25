using DbSqlLikeMem.SqlServer.TestTools;
using DbSqlLikeMem.TestTools.Tests.TemporaryTable;

namespace DbSqlLikeMem.SqlServer.Test.Fidelity.TemporaryTable;

/// <summary>
/// EN: Runs SQL Server fidelity tests for the shared temporary-table scenario.
/// PT-br: Executa testes de fidelidade SQL Server para o cenario compartilhado de tabela temporaria.
/// </summary>
public class TemporaryTableTests(
    ITestOutputHelper helper
    ) : TemporaryTableTestsBase<SqlServerConnectionMock, SqlConnection>(
    helper,
    new SqlServerProviderSqlDialect(),
    () => new SqlServerConnectionMock(),
    s => new SqlConnection(s)
    )
{
}
