using DbSqlLikeMem.Sqlite.TestTools;
using DbSqlLikeMem.TestTools.Tests.Schema;

namespace DbSqlLikeMem.Sqlite.Test.Fidelity.Schema;

/// <summary>
/// EN: Runs SQLite fidelity tests for the shared schema-snapshot workflows.
/// PT: Executa testes de fidelidade SQLite para os fluxos compartilhados de snapshot de schema.
/// </summary>
public class SchemaSnapshotTests(
    ITestOutputHelper helper
    ) : SchemaSnapshotTestsBase<SqliteConnectionMock, SqliteConnection>(
    helper,
    new SqliteProviderSqlDialect(),
    () => new SqliteConnectionMock(),
    s => new SqliteConnection(s)
    )
{
    private static readonly int _bootstrap = InitializeBootstrap();

    private static int InitializeBootstrap()
    {
        global::DbSqlLikeMem.Sqlite.Test.SqliteBootstrap.Initialize();
        return 0;
    }
}
