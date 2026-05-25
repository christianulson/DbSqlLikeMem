using DbSqlLikeMem.TestTools.Tests.DML;

namespace DbSqlLikeMem.Firebird.Test.Fidelity.DML;

/// <summary>
/// EN: Runs Firebird fidelity tests for the shared upsert workflows.
/// PT-br: Executa testes de fidelidade Firebird para os fluxos compartilhados de upsert.
/// </summary>
[FidelityNativeClientSkip]
public class UpsertTests(
    ITestOutputHelper helper
    ) : UpsertTestsBase<FirebirdConnectionMock, FbConnection>(
    helper,
    new FirebirdProviderSqlDialect(),
    () => new FirebirdConnectionMock(Get(FirebirdDbVersions.Default, _ => new FirebirdDbMock(_) { ThreadSafe = true })),
    FirebirdConnectionFactory.Create
    )
{
}

