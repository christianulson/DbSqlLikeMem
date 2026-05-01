namespace DbSqlLikeMem.MySql.Test;

/// <summary>
/// EN: Verifies stored procedure signature behavior for MySQL.
/// PT-br: Verifica o comportamento de assinatura de procedures para MySQL.
/// </summary>
/// <param name="helper">
/// EN: Output helper used by the test base.
/// PT-br: Helper de saída usado pela base de testes.
/// </param>
public sealed class StoredProcedureSignatureTests(
        ITestOutputHelper helper
    ) : StoredProcedureSignatureTestsBase<MySqlMockException>(helper)
{
    /// <inheritdoc />
    protected override DbSqlLikeMem.TestTools.ProviderSqlDialect Dialect { get; } = new DbSqlLikeMem.MySql.TestTools.MySqlProviderSqlDialect();

    /// <summary>
    /// EN: Creates the MySQL connection mock used in tests.
    /// PT-br: Cria o simulado de conexão MySQL usado nos testes.
    /// </summary>
    protected override DbConnectionMockBase CreateConnection() => new MySqlConnectionMock();
}
