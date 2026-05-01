using DbSqlLikeMem.MariaDb.TestTools;
using DbSqlLikeMem.TestTools.Tests.DML;

namespace DbSqlLikeMem.MariaDb.Test.Fidelity.DML;

/// <summary>
/// EN: Runs MariaDB fidelity tests for the shared insert workflows.
/// PT-br: Executa testes de fidelidade MariaDB para os fluxos compartilhados de insert.
/// </summary>
public class InsertTests(
    ITestOutputHelper helper
    ) : InsertTestsBase<MariaDbConnectionMock, MySqlConnection>(
    helper,
    new MariaDbProviderSqlDialect(),
    static () => new MariaDbConnectionMock(Db),
    s => new MySqlConnection(s)
    )
{
    private static readonly MariaDbDbMock Db = new() { ThreadSafe = true };
}
