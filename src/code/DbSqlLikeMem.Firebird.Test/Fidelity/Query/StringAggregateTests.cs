using DbSqlLikeMem.TestTools.Tests.Query;

namespace DbSqlLikeMem.Firebird.Test.Fidelity.Query;

/// <summary>
/// EN: Runs Firebird fidelity tests for the shared string aggregation workflows.
/// PT-br: Executa testes de fidelidade Firebird para os fluxos compartilhados de agregacao de strings.
/// </summary>
[FidelityNativeClientSkip]
public class StringAggregateTests(
    ITestOutputHelper helper
    ) : StringAggregateTestsBase<FirebirdConnectionMock, FbConnection>(
    helper,
    new FirebirdProviderSqlDialect(),
    () => new FirebirdConnectionMock(Get(FirebirdDbVersions.Default, _ => new FirebirdDbMock(_) { ThreadSafe = true })),
    FirebirdConnectionFactory.Create
    )
{
    /// <inheritdoc />
    protected override string[] NormalizeSnapshotColumnNames(string[] columnNames)
        => [.. columnNames.Select(static name => name.ToUpperInvariant())];
}

