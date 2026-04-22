using DbSqlLikeMem.TestTools.Tests.Query;

namespace DbSqlLikeMem.Firebird.Test.Fidelity.Query;

/// <summary>
/// EN: Runs Firebird fidelity tests for the shared select workflows.
/// PT: Executa testes de fidelidade Firebird para os fluxos compartilhados de select.
/// </summary>
public class SelectTests(
    ITestOutputHelper helper
    ) : SelectTestsBase<FirebirdConnectionMock, FbConnection>(
    helper,
    new FirebirdProviderSqlDialect(),
    () => new FirebirdConnectionMock(),
    s => new FbConnection(s)
    )
{
    /// <inheritdoc />
    protected override decimal TextMatchAlreadyValue => 0m;
}
