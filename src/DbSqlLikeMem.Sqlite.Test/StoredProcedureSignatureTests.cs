namespace DbSqlLikeMem.Sqlite.Test;

/// <summary>
/// EN: Verifies stored procedure signature behavior for SQLite.
/// PT: Verifica o comportamento de assinatura de procedures para SQLite.
/// </summary>
/// <param name="helper">
/// EN: Output helper used by the test base.
/// PT: Helper de saída usado pela base de testes.
/// </param>
public sealed class StoredProcedureSignatureTests(
        ITestOutputHelper helper
    ) : StoredProcedureSignatureTestsBase<SqliteMockException>(helper)
{
    /// <summary>
    /// EN: Creates the SQLite connection mock used in tests.
    /// PT: Cria o simulado de conexão SQLite usado nos testes.
    /// </summary>
    protected override DbConnectionMockBase CreateConnection() => new SqliteConnectionMock();
}
