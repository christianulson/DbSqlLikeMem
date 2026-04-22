using DbSqlLikeMem.Oracle.TestTools;
using DbSqlLikeMem.TestTools.Tests.Query;

namespace DbSqlLikeMem.Oracle.Test.Fidelity.Query;

/// <summary>
/// EN: Runs Oracle fidelity tests for the shared string-aggregation workflows.
/// PT: Executa testes de fidelidade Oracle para os fluxos compartilhados de agregacao de strings.
/// </summary>
public class StringAggregateTests(
    ITestOutputHelper helper
    ) : StringAggregateTestsBase<OracleConnectionMock, OracleConnection>(
    helper,
    new OracleProviderSqlDialect(),
    () => new OracleConnectionMock(),
    s => new OracleConnection(s)
    )
{
    /// <inheritdoc />
    protected override string[] NormalizeSnapshotColumnNames(string[] columnNames)
        => Array.ConvertAll(columnNames, static name => name.ToUpperInvariant());
}
