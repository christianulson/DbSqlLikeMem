using DbSqlLikeMem.Oracle.TestTools;
using DbSqlLikeMem.TestTools.Tests.Transactions;

namespace DbSqlLikeMem.Oracle.Test.Fidelity.Transactions;

/// <summary>
/// EN: Runs Oracle fidelity tests for the shared transaction workflows.
/// PT: Executa testes de fidelidade Oracle para os fluxos compartilhados de transacao.
/// </summary>
public class TransactionTests(
    ITestOutputHelper helper
    ) : TransactionTestsBase<OracleConnectionMock, OracleConnection>(
    helper,
    new OracleProviderSqlDialect(),
    () => new OracleConnectionMock(),
    s => new OracleConnection(s)
    )
{
}
