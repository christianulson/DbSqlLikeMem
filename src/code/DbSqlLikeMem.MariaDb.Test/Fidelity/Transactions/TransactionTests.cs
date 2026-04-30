using DbSqlLikeMem.MariaDb.TestTools;
using DbSqlLikeMem.TestTools.Tests.Transactions;

namespace DbSqlLikeMem.MariaDb.Test.Fidelity.Transactions;

/// <summary>
/// EN: Runs MariaDB fidelity tests for the shared transaction workflows.
/// PT: Executa testes de fidelidade MariaDB para os fluxos compartilhados de transacao.
/// </summary>
public class TransactionTests(
    ITestOutputHelper helper
    ) : TransactionTestsBase<MariaDbConnectionMock, MySqlConnection>(
    helper,
    new MariaDbProviderSqlDialect(),
    () => new MariaDbConnectionMock(),
    s => new MySqlConnection(s)
    )
{
}
