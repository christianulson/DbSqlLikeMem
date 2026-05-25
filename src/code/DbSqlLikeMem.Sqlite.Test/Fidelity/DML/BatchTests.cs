using DbSqlLikeMem.Sqlite.TestTools;
using DbSqlLikeMem.TestTools.Tests.DML;

namespace DbSqlLikeMem.Sqlite.Test.Fidelity.DML;

/// <summary>
/// EN: Runs SQLite fidelity tests for the shared batch workflows.
/// PT-br: Executa testes de fidelidade SQLite para os fluxos compartilhados de batch.
/// </summary>
public class BatchTests(
    ITestOutputHelper helper
    ) : BatchTestsBase<SqliteConnectionMock, SqliteConnection>(
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
