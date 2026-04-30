using DbSqlLikeMem.MariaDb.TestTools;
using DbSqlLikeMem.TestTools.Tests.Query;

namespace DbSqlLikeMem.MariaDb.Test.Fidelity.Query;

/// <summary>
/// EN: Runs MariaDB fidelity tests for the shared JSON table-valued function workflows.
/// PT: Executa testes de fidelidade do MariaDB para os fluxos compartilhados de funcoes tabulares JSON.
/// </summary>
public class JsonTableFunctionTests(
    ITestOutputHelper helper
    ) : JsonTableFunctionTestsBase<MariaDbConnectionMock, MySqlConnection>(
    helper,
    new MariaDbProviderSqlDialect(),
    () => new MariaDbConnectionMock(),
    s => new MySqlConnection(s)
    )
{
}
