using DbSqlLikeMem.SqlServer.TestTools;
using DbSqlLikeMem.TestTools.Tests.Schema;

namespace DbSqlLikeMem.SqlServer.Test.Fidelity.Schema;

/// <summary>
/// EN: Runs SQL Server fidelity tests for the shared schema-snapshot workflows.
/// PT-br: Executa testes de fidelidade SQL Server para os fluxos compartilhados de snapshot de schema.
/// </summary>
public class SchemaSnapshotTests(
    ITestOutputHelper helper
    ) : SchemaSnapshotTestsBase<SqlServerConnectionMock, SqlConnection>(
    helper,
    new SqlServerProviderSqlDialect(),
    () => new SqlServerConnectionMock(),
    s => new SqlConnection(s)
    )
{
}
