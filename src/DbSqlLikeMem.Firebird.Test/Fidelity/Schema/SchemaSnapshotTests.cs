using DbSqlLikeMem.TestTools.Tests.Schema;

namespace DbSqlLikeMem.Firebird.Test.Fidelity.Schema;

/// <summary>
/// EN: Runs Firebird fidelity tests for the shared schema snapshot workflows.
/// PT: Executa testes de fidelidade Firebird para os fluxos compartilhados de snapshot de schema.
/// </summary>
public class SchemaSnapshotTests(
    ITestOutputHelper helper
    ) : SchemaSnapshotTestsBase<FirebirdConnectionMock, FbConnection>(
    helper,
    new FirebirdProviderSqlDialect(),
    () => new FirebirdConnectionMock(),
    s => new FbConnection(s)
    )
{
}
