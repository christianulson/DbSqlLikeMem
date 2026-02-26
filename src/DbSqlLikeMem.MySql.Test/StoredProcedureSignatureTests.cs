namespace DbSqlLikeMem.MySql.Test;

/// <summary>
/// EN: Verifies stored procedure signature behavior for MySQL.
/// PT: Verifica o comportamento de assinatura de procedures para MySQL.
/// </summary>
/// <param name="helper">
/// EN: Output helper used by the test base.
/// PT: Helper de saída usado pela base de testes.
/// </param>
public sealed class StoredProcedureSignatureTests(
        ITestOutputHelper helper
    ) : StoredProcedureSignatureTestsBase<MySqlMockException>(helper)
{
    /// <summary>
    /// EN: Creates the MySQL connection mock used in tests.
    /// PT: Cria o simulado de conexão MySQL usado nos testes.
    /// </summary>
    protected override DbConnectionMockBase CreateConnection() => new MySqlConnectionMock();
}
