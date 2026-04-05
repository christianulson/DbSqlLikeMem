using DbSqlLikeMem.TestTools.Tests.Transactions;

namespace DbSqlLikeMem.Firebird.Test.Fidelity.Transactions;

/// <summary>
/// EN: Runs Firebird fidelity tests for the shared transaction workflows.
/// PT: Executa testes de fidelidade Firebird para os fluxos compartilhados de transacao.
/// </summary>
public class TransactionTests(
    ITestOutputHelper helper
    ) : TransactionTestsBase<FirebirdConnectionMock, FbConnection>(
    helper,
    new FirebirdProviderSqlDialect(),
    () => new FirebirdConnectionMock(),
    s => new FbConnection(s)
    )
{
}
