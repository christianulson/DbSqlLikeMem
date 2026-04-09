using DbSqlLikeMem.MariaDb.TestTools;
using DbSqlLikeMem.TestTools.Tests.DML;

namespace DbSqlLikeMem.MariaDb.Test.Fidelity.DML;

/// <summary>
/// EN: Runs MariaDB fidelity tests for the shared upsert workflows.
/// PT: Executa testes de fidelidade MariaDB para os fluxos compartilhados de upsert.
/// </summary>
public class UpsertTests(
    ITestOutputHelper helper
    ) : UpsertTestsBase<MariaDbConnectionMock, MySqlConnection>(
    helper,
    new MariaDbProviderSqlDialect(),
    () => new MariaDbConnectionMock(),
    s => new MySqlConnection(s)
    )
{
}
