using DbSqlLikeMem.MySql.TestTools;
using DbSqlLikeMem.TestTools.Tests.DML;

namespace DbSqlLikeMem.MySql.Test.Fidelity.DML;

/// <summary>
/// EN: Runs MySQL fidelity tests for the shared batch workflows.
/// PT: Executa testes de fidelidade MySQL para os fluxos compartilhados de batch.
/// </summary>
public class BatchTests(
    ITestOutputHelper helper
    ) : BatchTestsBase<MySqlConnectionMock, MySqlConnection>(
    helper,
    new MySqlProviderSqlDialect(),
    () => new MySqlConnectionMock(),
    s => new MySqlConnection(s)
    )
{
}
