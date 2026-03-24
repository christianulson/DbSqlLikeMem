using DbSqlLikeMem.MariaDb.TestTools;
using DbSqlLikeMem.TestTools.Tests.DML;

namespace DbSqlLikeMem.MariaDb.Test.Fidelity.DML;

/// <summary>
/// EN: Runs MariaDB fidelity tests for the shared sequence workflows.
/// PT: Executa testes de fidelidade MariaDB para os fluxos compartilhados de sequence.
/// </summary>
public class SequenceTests(
    ITestOutputHelper helper
    ) : SequenceTestsBase<MariaDbConnectionMock, MySqlConnection>(
    helper,
    new MariaDbProviderSqlDialect(),
    () => new MariaDbConnectionMock(),
    s => new MySqlConnection(s)
    )
{
}
