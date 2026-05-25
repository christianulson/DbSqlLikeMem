using DbSqlLikeMem.MySql.TestTools;
using DbSqlLikeMem.TestTools.Tests.Query;

namespace DbSqlLikeMem.MySql.Test.Fidelity.Query;

/// <summary>
/// EN: Runs MySQL fidelity tests for the shared string-aggregation workflows.
/// PT-br: Executa testes de fidelidade MySQL para os fluxos compartilhados de agregacao de strings.
/// </summary>
public class StringAggregateTests(
    ITestOutputHelper helper
    ) : StringAggregateTestsBase<MySqlConnectionMock, MySqlConnection>(
    helper,
    new MySqlProviderSqlDialect(),
    () => new MySqlConnectionMock(),
    s => new MySqlConnection(s)
    )
{
}
