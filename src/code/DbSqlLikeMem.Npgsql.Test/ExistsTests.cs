namespace DbSqlLikeMem.Npgsql.Test;

/// <summary>
/// EN: Validates EXISTS and NOT EXISTS semantics for PostgreSQL.
/// PT-br: Valida a semântica de EXISTS e NOT EXISTS para PostgreSQL.
/// </summary>
/// <param name="helper">
/// EN: Output helper used by the test base.
/// PT-br: Helper de saída usado pela base de testes.
/// </param>
public sealed class ExistsTests(
        ITestOutputHelper helper
    ) : ExistsTestsBase(helper)
{
    /// <summary>
    /// EN: Creates the PostgreSQL connection mock used in tests.
    /// PT-br: Cria o simulado de conexão PostgreSQL usado nos testes.
    /// </summary>
    protected override DbConnectionMockBase CreateConnection() => new NpgsqlConnectionMock();
}
