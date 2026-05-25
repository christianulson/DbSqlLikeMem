using DbSqlLikeMem.MariaDb.TestTools;
using DbSqlLikeMem.TestTools.Tests.DML;

namespace DbSqlLikeMem.MariaDb.Test.Fidelity.DML;

/// <summary>
/// EN: Runs MariaDB fidelity tests for the shared check-constraint workflows.
/// PT-br: Executa testes de fidelidade MariaDB para os fluxos compartilhados de restricao check.
/// </summary>
public class CheckTests(
    ITestOutputHelper helper
    ) : CheckTestsBase<MariaDbConnectionMock, MySqlConnection>(
    helper,
    new MariaDbProviderSqlDialect(),
    static () => new MariaDbConnectionMock(Db),
    s => new MySqlConnection(s)
    )
{
    private static readonly MariaDbDbMock Db = new() { ThreadSafe = true };
}
