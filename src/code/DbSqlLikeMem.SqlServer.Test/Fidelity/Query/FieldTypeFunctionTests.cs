using DbSqlLikeMem.SqlServer.TestTools;
using DbSqlLikeMem.TestTools.Tests.Query;

namespace DbSqlLikeMem.SqlServer.Test.Fidelity.Query;

/// <summary>
/// EN: Runs SQL Server fidelity tests for the shared typed-field and function workflows.
/// PT: Executa testes de fidelidade SQL Server para os fluxos compartilhados de campos tipados e funcoes.
/// </summary>
public class FieldTypeFunctionTests(
    ITestOutputHelper helper
    ) : FieldTypeFunctionTestsBase<SqlServerConnectionMock, SqlConnection>(
    helper,
    new SqlServerProviderSqlDialect(),
    () => new SqlServerConnectionMock(),
    s => new SqlConnection(s)
    )
{
}
