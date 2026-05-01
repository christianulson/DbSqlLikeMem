using DbSqlLikeMem.MySql.TestTools;
using DbSqlLikeMem.TestTools.Tests.TemporaryTable;

namespace DbSqlLikeMem.MySql.Test.Fidelity.TemporaryTable;

/// <summary>
/// EN: Runs MySQL fidelity tests for the shared temporary-table scenario.
/// PT-br: Executa testes de fidelidade MySQL para o cenario compartilhado de tabela temporaria.
/// </summary>
public class TemporaryTableTests(
    ITestOutputHelper helper
    ) : TemporaryTableTestsBase<MySqlConnectionMock, MySqlConnection>(
    helper,
    new MySqlProviderSqlDialect(),
    () => new MySqlConnectionMock(),
    s => new MySqlConnection(s)
    )
{
}
