using DbSqlLikeMem.MariaDb.TestTools;
using DbSqlLikeMem.TestTools.Tests.DDL;

namespace DbSqlLikeMem.MariaDb.Test.Fidelity.DDL;

/// <summary>
/// EN: Runs MariaDB fidelity tests for the shared table scenarios.
/// PT: Executa testes de fidelidade do MariaDB para os cenarios compartilhados de tabela.
/// </summary>
public class TableTests(
    ITestOutputHelper helper
    ) : TableTestsBase<MariaDbConnectionMock, MySqlConnection>(
    helper,
    new MariaDbProviderSqlDialect(),
    () => new MariaDbConnectionMock(),
    s => new MySqlConnection(s)
    )
{
}
