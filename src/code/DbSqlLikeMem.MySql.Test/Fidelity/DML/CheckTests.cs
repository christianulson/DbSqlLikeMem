using DbSqlLikeMem.MySql.TestTools;
using DbSqlLikeMem.TestTools.Tests.DML;

namespace DbSqlLikeMem.MySql.Test.Fidelity.DML;

/// <summary>
/// EN: Runs MySQL fidelity tests for the shared check-constraint workflows.
/// PT: Executa testes de fidelidade MySQL para os fluxos compartilhados de restricao check.
/// </summary>
public class CheckTests(
    ITestOutputHelper helper
    ) : CheckTestsBase<MySqlConnectionMock, MySqlConnection>(
    helper,
    new MySqlProviderSqlDialect(),
    static () => new MySqlConnectionMock(Db),
    s => new MySqlConnection(s)
    )
{
    private static readonly MySqlDbMock Db = new() { ThreadSafe = true };
}
