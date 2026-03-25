using DbSqlLikeMem.MariaDb.TestTools;
using DbSqlLikeMem.TestTools.Tests.DML;

namespace DbSqlLikeMem.MariaDb.Test.Fidelity.DML;

/// <summary>
/// EN: Runs MariaDB fidelity tests for the shared batch RETURNING workflow.
/// PT: Executa testes de fidelidade MariaDB para o fluxo compartilhado de batch RETURNING.
/// </summary>
public class BatchReturningTests(
    ITestOutputHelper helper
    ) : BatchReturningTestsBase<MariaDbConnectionMock, MySqlConnection>(
    helper,
    new MariaDbProviderSqlDialect(),
    () => new MariaDbConnectionMock(),
    s => new MySqlConnection(s)
    )
{
}
