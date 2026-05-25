using DbSqlLikeMem.TestTools.Tests.Query;

namespace DbSqlLikeMem.Firebird.Test.Fidelity.Query;

/// <summary>
/// EN: Runs Firebird fidelity tests for the shared typed-field and SQL function workflows.
/// PT-br: Executa testes de fidelidade Firebird para os fluxos compartilhados de campos tipados e funcoes SQL.
/// </summary>
[FidelityNativeClientSkip]
public class FieldTypeFunctionTests(
    ITestOutputHelper helper
    ) : FieldTypeFunctionTestsBase<FirebirdConnectionMock, FbConnection>(
    helper,
    new FirebirdProviderSqlDialect(),
    () => new FirebirdConnectionMock(Get(FirebirdDbVersions.Default, _ => new FirebirdDbMock(_) { ThreadSafe = true })),
    FirebirdConnectionFactory.Create
    )
{
}

