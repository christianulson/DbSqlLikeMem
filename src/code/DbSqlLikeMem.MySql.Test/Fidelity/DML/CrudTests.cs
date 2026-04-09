using DbSqlLikeMem.MySql.TestTools;
using DbSqlLikeMem.TestTools.Tests.DML;

namespace DbSqlLikeMem.MySql.Test.Fidelity.DML;

/// <summary>
/// EN: Runs MySQL fidelity tests for the shared CRUD workflows.
/// PT: Executa testes de fidelidade MySQL para os fluxos compartilhados de CRUD.
/// </summary>
public class CrudTests(
    ITestOutputHelper helper
    ) : CrudTestsBase<MySqlConnectionMock, MySqlConnection>(
    helper,
    new MySqlProviderSqlDialect(),
    () => new MySqlConnectionMock(),
    s => new MySqlConnection(s)
    )
{
}
