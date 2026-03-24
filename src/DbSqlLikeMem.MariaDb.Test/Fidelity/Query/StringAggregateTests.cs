using DbSqlLikeMem.MariaDb.TestTools;
using DbSqlLikeMem.TestTools.Tests.Query;

namespace DbSqlLikeMem.MariaDb.Test.Fidelity.Query;

/// <summary>
/// EN: Runs MariaDB fidelity tests for the shared string-aggregation workflows.
/// PT: Executa testes de fidelidade MariaDB para os fluxos compartilhados de agregacao de strings.
/// </summary>
public class StringAggregateTests(
    ITestOutputHelper helper
    ) : StringAggregateTestsBase<MariaDbConnectionMock, MySqlConnection>(
    helper,
    new MariaDbProviderSqlDialect(),
    () => new MariaDbConnectionMock(),
    s => new MySqlConnection(s)
    )
{
}
