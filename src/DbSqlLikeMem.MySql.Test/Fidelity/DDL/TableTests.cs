
using DbSqlLikeMem.TestTools.Tests.DDL;

namespace DbSqlLikeMem.MySql.Test.Fidelity.DDL;

/// <summary>
/// EN: Runs MySQL fidelity tests for the shared table scenarios.
/// PT: Executa testes de fidelidade MySQL para os cenarios compartilhados de tabela.
/// </summary>
public class TableTests(
    ITestOutputHelper helper
    ) : TableTestsBase<MySqlConnectionMock, MySqlConnection>(
    helper,
    new TestTools.MySqlProviderSqlDialect(),
    () => new MySqlConnectionMock(),
    s => new MySqlConnection(s)
    )
{
}
