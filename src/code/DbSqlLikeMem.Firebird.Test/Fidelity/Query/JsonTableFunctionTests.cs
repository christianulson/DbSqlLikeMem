using DbSqlLikeMem.TestTools.Tests.Query;

namespace DbSqlLikeMem.Firebird.Test.Fidelity.Query;

/// <summary>
/// EN: Runs Firebird fidelity tests for the shared JSON table-valued function workflows.
/// PT: Executa testes de fidelidade do Firebird para os fluxos compartilhados de funcoes tabulares JSON.
/// </summary>
public class JsonTableFunctionTests(
    ITestOutputHelper helper
    ) : JsonTableFunctionTestsBase<FirebirdConnectionMock, FbConnection>(
    helper,
    new FirebirdProviderSqlDialect(),
    () => new FirebirdConnectionMock(),
    s => new FbConnection(s)
    )
{
}
