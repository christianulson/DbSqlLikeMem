namespace DbSqlLikeMem.Sqlite.Test;

/// <summary>
/// EN: Validates EXISTS and NOT EXISTS semantics for SQLite.
/// PT: Valida a semântica de EXISTS e NOT EXISTS para SQLite.
/// </summary>
/// <param name="helper">
/// EN: Output helper used by the test base.
/// PT: Helper de saída usado pela base de testes.
/// </param>
public sealed class ExistsTests(
        ITestOutputHelper helper
    ) : ExistsTestsBase(helper)
{
    /// <summary>
    /// EN: Creates the SQLite connection mock used in tests.
    /// PT: Cria o simulado de conexão SQLite usado nos testes.
    /// </summary>
    protected override DbConnectionMockBase CreateConnection() => new SqliteConnectionMock();
}
