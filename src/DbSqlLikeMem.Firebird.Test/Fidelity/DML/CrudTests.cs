using DbSqlLikeMem.TestTools.Tests.DML;

namespace DbSqlLikeMem.Firebird.Test.Fidelity.DML;

/// <summary>
/// EN: Runs Firebird fidelity tests for the shared CRUD workflows.
/// PT: Executa testes de fidelidade Firebird para os fluxos compartilhados de CRUD.
/// </summary>
public class CrudTests(
    ITestOutputHelper helper
    ) : CrudTestsBase<FirebirdConnectionMock, FbConnection>(
    helper,
    new FirebirdProviderSqlDialect(),
    () => new FirebirdConnectionMock(),
    s => new FbConnection(s)
    )
{
}
