using DbSqlLikeMem.Sqlite.TestTools;
using DbSqlLikeMem.TestTools.Tests.Query;

namespace DbSqlLikeMem.Sqlite.Test.Fidelity.Query;

/// <summary>
/// EN: Runs SQLite fidelity tests for the shared typed-field and function workflows.
/// PT-br: Executa testes de fidelidade SQLite para os fluxos compartilhados de campos tipados e funcoes.
/// </summary>
public class FieldTypeFunctionTests(
    ITestOutputHelper helper
    ) : FieldTypeFunctionTestsBase<SqliteConnectionMock, SqliteConnection>(
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
