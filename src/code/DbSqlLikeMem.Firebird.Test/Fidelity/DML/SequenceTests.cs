using DbSqlLikeMem.TestTools.Tests.DML;

namespace DbSqlLikeMem.Firebird.Test.Fidelity.DML;

/// <summary>
/// EN: Runs Firebird fidelity tests for the shared sequence workflows.
/// PT: Executa testes de fidelidade Firebird para os fluxos compartilhados de sequence.
/// </summary>
[FidelityNativeClientSkip]
public class SequenceTests(
    ITestOutputHelper helper
    ) : SequenceTestsBase<FirebirdConnectionMock, FbConnection>(
    helper,
    new FirebirdProviderSqlDialect(),
    () => new FirebirdConnectionMock(Get(FirebirdDbVersions.Default, _ => new FirebirdDbMock(_) { ThreadSafe = true })),
    FirebirdConnectionFactory.Create
    )
{
}

