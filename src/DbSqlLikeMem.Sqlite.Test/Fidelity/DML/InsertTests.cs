using DbSqlLikeMem.Sqlite.TestTools;
using DbSqlLikeMem.TestTools.Tests.DML;

namespace DbSqlLikeMem.Sqlite.Test.Fidelity.DML;

/// <summary>
/// EN: Runs SQLite fidelity tests for the shared insert workflows.
/// PT: Executa testes de fidelidade SQLite para os fluxos compartilhados de insert.
/// </summary>
public class InsertTests(
    ITestOutputHelper helper
    ) : InsertTestsBase<SqliteConnectionMock, SqliteConnection>(
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
