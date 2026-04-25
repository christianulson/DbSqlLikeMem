using DbSqlLikeMem.Npgsql.TestTools;
using DbSqlLikeMem.TestTools.Tests.Query;

namespace DbSqlLikeMem.Npgsql.Test.Fidelity.Query;

/// <summary>
/// EN: Runs PostgreSQL fidelity tests for the shared string-aggregation workflows.
/// PT: Executa testes de fidelidade PostgreSQL para os fluxos compartilhados de agregacao de strings.
/// </summary>
public class StringAggregateTests(
    ITestOutputHelper helper
    ) : StringAggregateTestsBase<NpgsqlConnectionMock, NpgsqlConnection>(
    helper,
    new NpgsqlProviderSqlDialect(),
    () => new NpgsqlConnectionMock(),
    s => new NpgsqlConnection(s)
)
{
    /// <inheritdoc />
    protected override string[] NormalizeSnapshotColumnNames(string[] columnNames)
        => Array.ConvertAll(columnNames, static name => name.ToLowerInvariant());
}
