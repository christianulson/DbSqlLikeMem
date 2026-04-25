using DbSqlLikeMem.TestTools.Tests.DML;

namespace DbSqlLikeMem.Firebird.Test.Fidelity.DML;

/// <summary>
/// EN: Runs Firebird fidelity tests for the shared insert workflows.
/// PT: Executa testes de fidelidade Firebird para os fluxos compartilhados de insert.
/// </summary>
public class InsertTests(
    ITestOutputHelper helper
    ) : InsertTestsBase<FirebirdConnectionMock, FbConnection>(
    helper,
    new FirebirdProviderSqlDialect(),
    static () => new FirebirdConnectionMock(Get(FirebirdDbVersions.Default, _ => new FirebirdDbMock(_) { ThreadSafe = true })),
    FirebirdConnectionFactory.Create
    )
{

}
