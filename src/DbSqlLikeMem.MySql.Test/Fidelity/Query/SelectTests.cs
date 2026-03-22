using DbSqlLikeMem.TestTools.Tests.Query;

namespace DbSqlLikeMem.MySql.Test.Fidelity.Query;

/// <summary>
/// EN: Runs MySQL fidelity tests for the shared primary-key select scenario.
/// PT: Executa testes de fidelidade MySQL para o cenario compartilhado de selecao por chave primaria.
/// </summary>
public class SelectTests(
    ITestOutputHelper helper
    ) : SelectTestsBase<MySqlConnectionMock, MySqlConnection>(
    helper,
    new TestTools.MySqlProviderSqlDialect(),
    () => new MySqlConnectionMock(),
    s => new MySqlConnection(s)
    )
{
}
