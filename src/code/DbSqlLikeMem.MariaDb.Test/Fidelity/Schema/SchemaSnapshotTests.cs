using DbSqlLikeMem.MariaDb.TestTools;
using DbSqlLikeMem.TestTools.Tests.Schema;

namespace DbSqlLikeMem.MariaDb.Test.Fidelity.Schema;

/// <summary>
/// EN: Runs MariaDB fidelity tests for the shared schema-snapshot workflows.
/// PT-br: Executa testes de fidelidade MariaDB para os fluxos compartilhados de snapshot de schema.
/// </summary>
public class SchemaSnapshotTests(
    ITestOutputHelper helper
    ) : SchemaSnapshotTestsBase<MariaDbConnectionMock, MySqlConnection>(
    helper,
    new MariaDbProviderSqlDialect(),
    () => new MariaDbConnectionMock(),
    s => new MySqlConnection(s)
    )
{
}
