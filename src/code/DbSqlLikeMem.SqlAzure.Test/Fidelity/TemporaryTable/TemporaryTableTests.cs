using DbSqlLikeMem.SqlAzure.TestTools;
using DbSqlLikeMem.TestTools.Tests.TemporaryTable;

namespace DbSqlLikeMem.SqlAzure.Test.Fidelity.TemporaryTable;

/// <summary>
/// EN: Runs Azure SQL fidelity tests for the shared temporary-table scenario.
/// PT-br: Executa testes de fidelidade Azure SQL para o cenario compartilhado de tabela temporaria.
/// </summary>
public class TemporaryTableTests(
    ITestOutputHelper helper
    ) : TemporaryTableTestsBase<SqlAzureConnectionMock, SqlConnection>(
    helper,
    new SqlAzureProviderSqlDialect(),
    () => new SqlAzureConnectionMock(),
    s => new SqlConnection(s)
    )
{
}
