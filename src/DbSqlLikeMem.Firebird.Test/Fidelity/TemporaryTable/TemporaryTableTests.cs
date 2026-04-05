using DbSqlLikeMem.TestTools.Tests.TemporaryTable;

namespace DbSqlLikeMem.Firebird.Test.Fidelity.TemporaryTable;

/// <summary>
/// EN: Runs Firebird fidelity tests for the shared temporary-table workflows.
/// PT: Executa testes de fidelidade Firebird para os fluxos compartilhados de tabela temporaria.
/// </summary>
public class TemporaryTableTests(
    ITestOutputHelper helper
    ) : TemporaryTableTestsBase<FirebirdConnectionMock, FbConnection>(
    helper,
    new FirebirdProviderSqlDialect(),
    () => new FirebirdConnectionMock(),
    s => new FbConnection(s)
    )
{
}
