using DbSqlLikeMem.MariaDb.TestTools;
using DbSqlLikeMem.TestTools.Tests.TemporaryTable;

namespace DbSqlLikeMem.MariaDb.Test.Fidelity.TemporaryTable;

/// <summary>
/// EN: Runs MariaDB fidelity tests for the shared temporary-table scenario.
/// PT: Executa testes de fidelidade MariaDB para o cenario compartilhado de tabela temporaria.
/// </summary>
public class TemporaryTableTests(
    ITestOutputHelper helper
    ) : TemporaryTableTestsBase<MariaDbConnectionMock, MySqlConnection>(
    helper,
    new MariaDbProviderSqlDialect(),
    () => new MariaDbConnectionMock(),
    s => new MySqlConnection(s)
    )
{
}
