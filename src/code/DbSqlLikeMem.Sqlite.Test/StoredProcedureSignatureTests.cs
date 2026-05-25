namespace DbSqlLikeMem.Sqlite.Test;

/// <summary>
/// EN: Verifies stored procedure signature behavior for SQLite.
/// PT-br: Verifica o comportamento de assinatura de procedures para SQLite.
/// </summary>
/// <param name="helper">
/// EN: Output helper used by the test base.
/// PT-br: Helper de saída usado pela base de testes.
/// </param>
public sealed class StoredProcedureSignatureTests(
        ITestOutputHelper helper
    ) : StoredProcedureSignatureTestsBase<SqliteMockException>(helper)
{
    /// <inheritdoc />
    protected override ProviderSqlDialect Dialect { get; } = new TestTools.SqliteProviderSqlDialect();

    /// <summary>
    /// EN: Creates the SQLite connection mock used in tests.
    /// PT-br: Cria o simulado de conexão SQLite usado nos testes.
    /// </summary>
    protected override DbConnectionMockBase CreateConnection() => new SqliteConnectionMock();
}
