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
    () => new FirebirdConnectionMock(Get(FirebirdDbVersions.Default, _ => new FirebirdDbMock(_) { ThreadSafe = true })),
    FirebirdConnectionFactory.Create
    )
{
    /// <inheritdoc />
    protected override string[] NormalizeSnapshotColumnNames(string[] columnNames)
        => [.. columnNames.Select(static name => name.ToUpperInvariant())];

    /// <inheritdoc />
    protected override string[] ApplyProjectionColumnNames()
        => ["USERID", "USERNAME", "NOTE"];

    /// <inheritdoc />
    protected override decimal TextMatchAlreadyValue => 0m;
}
