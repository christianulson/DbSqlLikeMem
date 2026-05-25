using DbSqlLikeMem.MariaDb.TestTools;
using DbSqlLikeMem.TestTools.Tests.Query;

namespace DbSqlLikeMem.MariaDb.Test.Fidelity.Query;

/// <summary>
/// EN: Runs MariaDB fidelity tests for the shared typed-field and function workflows.
/// PT-br: Executa testes de fidelidade MariaDB para os fluxos compartilhados de campos tipados e funcoes.
/// </summary>
public class FieldTypeFunctionTests(
    ITestOutputHelper helper
    ) : FieldTypeFunctionTestsBase<MariaDbConnectionMock, MySqlConnection>(
    helper,
    new MariaDbProviderSqlDialect(),
    () => new MariaDbConnectionMock(),
    s => new MySqlConnection(s)
    )
{
}
