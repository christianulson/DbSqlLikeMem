using DbSqlLikeMem.TestTools.Tests.DML;

namespace DbSqlLikeMem.Firebird.Test.Fidelity.DML;

/// <summary>
/// EN: Runs Firebird fidelity tests for the shared batch RETURNING workflow.
/// PT-br: Executa testes de fidelidade Firebird para o fluxo compartilhado de batch RETURNING.
/// </summary>
[FidelityNativeClientSkip]
public sealed class BatchReturningTests(
    ITestOutputHelper helper
    ) : BatchReturningTestsBase<FirebirdConnectionMock, FbConnection>(
    helper,
    new FirebirdProviderSqlDialect(),
    () => new FirebirdConnectionMock(Get(FirebirdDbVersions.Default, _ => new FirebirdDbMock(_) { ThreadSafe = true })),
    FirebirdConnectionFactory.Create
    )
{
}

