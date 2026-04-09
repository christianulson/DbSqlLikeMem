using DbSqlLikeMem.SqlAzure.TestTools;
using DbSqlLikeMem.TestTools.Tests.Schema;

namespace DbSqlLikeMem.SqlAzure.Test.Fidelity.Schema;

/// <summary>
/// EN: Runs SQL Azure fidelity tests for the shared schema-snapshot workflows.
/// PT: Executa testes de fidelidade SQL Azure para os fluxos compartilhados de snapshot de schema.
/// </summary>
public class SchemaSnapshotTests(
    ITestOutputHelper helper
    ) : SchemaSnapshotTestsBase<SqlAzureConnectionMock, SqlConnection>(
    helper,
    new SqlAzureProviderSqlDialect(),
    () => new SqlAzureConnectionMock(),
    s => new SqlConnection(s)
    )
{
}
