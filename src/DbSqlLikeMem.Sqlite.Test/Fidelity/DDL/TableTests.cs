using DbSqlLikeMem.Sqlite.TestTools;
using DbSqlLikeMem.TestTools.Tests.DDL;

namespace DbSqlLikeMem.Sqlite.Test.Fidelity.DDL;

/// <summary>
/// EN: Runs SQLite fidelity tests for the shared table scenarios.
/// PT: Executa testes de fidelidade do SQLite para os cenarios compartilhados de tabela.
/// </summary>
public class TableTests(
    ITestOutputHelper helper
    ) : TableTestsBase<SqliteConnectionMock, SqliteConnection>(
    helper,
    new SqliteProviderSqlDialect(),
    () => new SqliteConnectionMock(),
    s => new SqliteConnection(s)
    )
{
}
