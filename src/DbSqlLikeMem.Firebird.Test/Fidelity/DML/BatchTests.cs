using DbSqlLikeMem.TestTools.Tests.DML;

namespace DbSqlLikeMem.Firebird.Test.Fidelity.DML;

/// <summary>
/// EN: Runs Firebird fidelity tests for the shared batch workflows.
/// PT: Executa testes de fidelidade Firebird para os fluxos compartilhados de batch.
/// </summary>
public sealed class BatchTests(
    ITestOutputHelper helper
    ) : BatchTestsBase<FirebirdConnectionMock, FbConnection>(
    helper,
    new FirebirdProviderSqlDialect(),
    () => new FirebirdConnectionMock(),
    s => new FbConnection(s)
    )
{
}
