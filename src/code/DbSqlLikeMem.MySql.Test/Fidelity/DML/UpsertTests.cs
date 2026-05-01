using DbSqlLikeMem.MySql.TestTools;
using DbSqlLikeMem.TestTools.Tests.DML;

namespace DbSqlLikeMem.MySql.Test.Fidelity.DML;

/// <summary>
/// EN: Runs MySQL fidelity tests for the shared upsert workflows.
/// PT-br: Executa testes de fidelidade MySQL para os fluxos compartilhados de upsert.
/// </summary>
public class UpsertTests(
    ITestOutputHelper helper
    ) : UpsertTestsBase<MySqlConnectionMock, MySqlConnection>(
    helper,
    new MySqlProviderSqlDialect(),
    () => new MySqlConnectionMock(),
    s => new MySqlConnection(s)
    )
{
}
