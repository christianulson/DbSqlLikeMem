using DbSqlLikeMem.TestTools.Tests.DDL;

namespace DbSqlLikeMem.Firebird.Test.Fidelity.DDL;

/// <summary>
/// EN: Runs Firebird fidelity tests for the shared table DDL workflows.
/// PT-br: Executa testes de fidelidade Firebird para os fluxos compartilhados de DDL de tabela.
/// </summary>
[FidelityNativeClientSkip]
public class TableTests(
    ITestOutputHelper helper
    ) : TableTestsBase<FirebirdConnectionMock, FbConnection>(
    helper,
    new FirebirdProviderSqlDialect(),
    () => new FirebirdConnectionMock(Get(FirebirdDbVersions.Default, _ => new FirebirdDbMock(_) { ThreadSafe = true })),
    FirebirdConnectionFactory.Create
    )
{
}

