using DbSqlLikeMem.Sqlite.TestTools;
using DbSqlLikeMem.TestTools.Tests.Query;

namespace DbSqlLikeMem.Sqlite.Test.Fidelity.Query;

/// <summary>
/// EN: Runs SQLite fidelity tests for the shared primary-key select scenario.
/// PT-br: Executa testes de fidelidade do SQLite para o cenario compartilhado de selecao por chave primaria.
/// </summary>
public class SelectTests(
    ITestOutputHelper helper
    ) : SelectTestsBase<SqliteConnectionMock, SqliteConnection>(
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

    /// <inheritdoc />
    protected override decimal TextMatchAlreadyValue => 0m;
}
