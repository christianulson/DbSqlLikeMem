using DbSqlLikeMem.MySql.TestTools;
using DbSqlLikeMem.TestTools.Tests.Query;

namespace DbSqlLikeMem.MySql.Test.Fidelity.Query;

/// <summary>
/// EN: Runs MySQL fidelity tests for the shared typed-field and function workflows.
/// PT: Executa testes de fidelidade MySQL para os fluxos compartilhados de campos tipados e funcoes.
/// </summary>
public class FieldTypeFunctionTests(
    ITestOutputHelper helper
    ) : FieldTypeFunctionTestsBase<MySqlConnectionMock, MySqlConnection>(
    helper,
    new MySqlProviderSqlDialect(),
    () => new MySqlConnectionMock(),
    s => new MySqlConnection(s)
    )
{
}
