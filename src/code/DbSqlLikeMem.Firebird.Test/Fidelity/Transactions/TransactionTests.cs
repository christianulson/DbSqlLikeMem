using DbSqlLikeMem.TestTools.Tests.Transactions;

namespace DbSqlLikeMem.Firebird.Test.Fidelity.Transactions;

/// <summary>
/// EN: Runs Firebird fidelity tests for the shared transaction workflows.
/// PT-br: Executa testes de fidelidade Firebird para os fluxos compartilhados de transacao.
/// </summary>
[FidelityNativeClientSkip]
public class TransactionTests(
    ITestOutputHelper helper
    ) : TransactionTestsBase<FirebirdConnectionMock, FbConnection>(
    helper,
    new FirebirdProviderSqlDialect(),
    () => new FirebirdConnectionMock(Get(FirebirdDbVersions.Default, _ => new FirebirdDbMock(_) { ThreadSafe = true })),
    FirebirdConnectionFactory.Create
    )
{
}

