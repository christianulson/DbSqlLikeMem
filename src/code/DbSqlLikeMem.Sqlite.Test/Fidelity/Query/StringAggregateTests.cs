using DbSqlLikeMem.Sqlite.TestTools;
using DbSqlLikeMem.TestTools.Tests.Query;

namespace DbSqlLikeMem.Sqlite.Test.Fidelity.Query;

/// <summary>
/// EN: Runs SQLite fidelity tests for the shared string-aggregation workflows.
/// PT: Executa testes de fidelidade SQLite para os fluxos compartilhados de agregacao de strings.
/// </summary>
public class StringAggregateTests(
    ITestOutputHelper helper
    ) : StringAggregateTestsBase<SqliteConnectionMock, SqliteConnection>(
    helper,
    new SqliteProviderSqlDialect(),
    () => new SqliteConnectionMock(),
    s => new SqliteConnection(s)
    )
{
    private static readonly int _bootstrap = InitializeBootstrap();

    private static int InitializeBootstrap()
    {
        Sqlite.Test.SqliteBootstrap.Initialize();
        return 0;
    }
}
