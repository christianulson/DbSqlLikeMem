using DbSqlLikeMem.MySql.TestTools;
using DbSqlLikeMem.TestTools.Tests.Schema;

namespace DbSqlLikeMem.MySql.Test.Fidelity.Schema;

/// <summary>
/// EN: Runs MySQL fidelity tests for the shared schema-snapshot workflows.
/// PT: Executa testes de fidelidade MySQL para os fluxos compartilhados de snapshot de schema.
/// </summary>
public class SchemaSnapshotTests(
    ITestOutputHelper helper
    ) : SchemaSnapshotTestsBase<MySqlConnectionMock, MySqlConnection>(
    helper,
    new MySqlProviderSqlDialect(),
    () => new MySqlConnectionMock(),
    s => new MySqlConnection(s)
    )
{
}
