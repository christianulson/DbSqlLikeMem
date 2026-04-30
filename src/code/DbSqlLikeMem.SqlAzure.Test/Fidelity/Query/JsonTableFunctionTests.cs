using DbSqlLikeMem.SqlAzure.TestTools;
using DbSqlLikeMem.TestTools.Tests.Query;

namespace DbSqlLikeMem.SqlAzure.Test.Fidelity.Query;

/// <summary>
/// EN: Runs SQL Azure fidelity tests for the shared JSON table-valued function workflows.
/// PT: Executa testes de fidelidade do SQL Azure para os fluxos compartilhados de funcoes tabulares JSON.
/// </summary>
public class JsonTableFunctionTests(
    ITestOutputHelper helper
    ) : JsonTableFunctionTestsBase<SqlAzureConnectionMock, SqlConnection>(
    helper,
    new SqlAzureProviderSqlDialect(),
    () => new SqlAzureConnectionMock(),
    s => new SqlConnection(s)
    )
{
}
