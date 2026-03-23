using DbSqlLikeMem.Npgsql.TestTools;
using DbSqlLikeMem.TestTools.Tests.Schema;

namespace DbSqlLikeMem.Npgsql.Test.Fidelity.Schema;

/// <summary>
/// EN: Runs PostgreSQL fidelity tests for the shared schema-snapshot workflows.
/// PT: Executa testes de fidelidade PostgreSQL para os fluxos compartilhados de snapshot de schema.
/// </summary>
public class SchemaSnapshotTests(
    ITestOutputHelper helper
    ) : SchemaSnapshotTestsBase<NpgsqlConnectionMock, NpgsqlConnection>(
    helper,
    new NpgsqlProviderSqlDialect(),
    () => new NpgsqlConnectionMock(),
    s => new NpgsqlConnection(s)
    )
{
}
