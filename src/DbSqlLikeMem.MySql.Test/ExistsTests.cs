namespace DbSqlLikeMem.MySql.Test;

/// <summary>
/// EN: Validates EXISTS and NOT EXISTS semantics for MySQL.
/// PT: Valida a semântica de EXISTS e NOT EXISTS para MySQL.
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
    /// EN: Creates the MySQL connection mock used in tests.
    /// PT: Cria o mock de conexão MySQL usado nos testes.
    /// </summary>
    protected override DbConnectionMockBase CreateConnection() => new MySqlConnectionMock();
}
