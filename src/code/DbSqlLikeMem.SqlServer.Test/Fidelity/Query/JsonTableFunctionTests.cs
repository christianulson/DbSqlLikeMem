using DbSqlLikeMem.SqlServer.TestTools;
using DbSqlLikeMem.TestTools.Tests.Query;

namespace DbSqlLikeMem.SqlServer.Test.Fidelity.Query;

/// <summary>
/// EN: Runs SQL Server fidelity tests for the shared JSON table-valued function workflows.
/// PT: Executa testes de fidelidade do SQL Server para os fluxos compartilhados de funcoes tabulares JSON.
/// </summary>
public class JsonTableFunctionTests(
    ITestOutputHelper helper
    ) : JsonTableFunctionTestsBase<SqlServerConnectionMock, SqlConnection>(
    helper,
    new SqlServerProviderSqlDialect(),
    () => new SqlServerConnectionMock(),
    s => new SqlConnection(s)
    )
{
}
