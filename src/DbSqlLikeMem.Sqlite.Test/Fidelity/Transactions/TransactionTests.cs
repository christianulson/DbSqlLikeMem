using DbSqlLikeMem.Sqlite.TestTools;
using DbSqlLikeMem.TestTools.Tests.Transactions;

namespace DbSqlLikeMem.Sqlite.Test.Fidelity.Transactions;

/// <summary>
/// EN: Runs SQLite fidelity tests for the shared transaction workflows.
/// PT: Executa testes de fidelidade SQLite para os fluxos compartilhados de transacao.
/// </summary>
public class TransactionTests(
    ITestOutputHelper helper
    ) : TransactionTestsBase<SqliteConnectionMock, SqliteConnection>(
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
