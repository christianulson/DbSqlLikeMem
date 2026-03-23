using DbSqlLikeMem.Sqlite.TestTools;
using DbSqlLikeMem.TestTools.Tests.Performance;

namespace DbSqlLikeMem.Sqlite.Test.Fidelity.Performance;

/// <summary>
/// EN: Runs SQLite fidelity tests for the shared performance workflows.
/// PT: Executa testes de fidelidade SQLite para os fluxos compartilhados de performance.
/// </summary>
public class PerformanceTests(
    ITestOutputHelper helper
    ) : PerformanceTestsBase<SqliteConnectionMock, SqliteConnection>(
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
