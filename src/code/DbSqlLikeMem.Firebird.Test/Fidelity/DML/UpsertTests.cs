using DbSqlLikeMem.TestTools.Tests.DML;

namespace DbSqlLikeMem.Firebird.Test.Fidelity.DML;

/// <summary>
/// EN: Runs Firebird fidelity tests for the shared upsert workflows.
/// PT: Executa testes de fidelidade Firebird para os fluxos compartilhados de upsert.
/// </summary>
public class UpsertTests(
    ITestOutputHelper helper
    ) : UpsertTestsBase<FirebirdConnectionMock, FbConnection>(
    helper,
    new FirebirdProviderSqlDialect(),
    () => new FirebirdConnectionMock(),
    s => new FbConnection(s)
    )
{
}
