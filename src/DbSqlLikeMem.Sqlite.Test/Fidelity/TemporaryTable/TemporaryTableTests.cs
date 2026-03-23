using DbSqlLikeMem.Sqlite.TestTools;
using DbSqlLikeMem.TestTools.Tests.TemporaryTable;

namespace DbSqlLikeMem.Sqlite.Test.Fidelity.TemporaryTable;

/// <summary>
/// EN: Runs SQLite fidelity tests for the shared temporary-table scenario.
/// PT: Executa testes de fidelidade SQLite para o cenario compartilhado de tabela temporaria.
/// </summary>
public class TemporaryTableTests(
    ITestOutputHelper helper
    ) : TemporaryTableTestsBase<SqliteConnectionMock, SqliteConnection>(
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
