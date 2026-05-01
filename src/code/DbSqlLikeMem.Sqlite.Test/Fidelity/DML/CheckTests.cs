using DbSqlLikeMem.Sqlite.TestTools;
using DbSqlLikeMem.TestTools.Tests.DML;

namespace DbSqlLikeMem.Sqlite.Test.Fidelity.DML;

/// <summary>
/// EN: Runs SQLite fidelity tests for the shared check-constraint workflows.
/// PT-br: Executa testes de fidelidade SQLite para os fluxos compartilhados de restricao check.
/// </summary>
public class CheckTests(
    ITestOutputHelper helper
    ) : CheckTestsBase<SqliteConnectionMock, SqliteConnection>(
    helper,
    new SqliteProviderSqlDialect(),
    static () => new SqliteConnectionMock(Db),
    s => new SqliteConnection(s)
    )
{
    private static readonly SqliteDbMock Db = new() { ThreadSafe = true };
}
