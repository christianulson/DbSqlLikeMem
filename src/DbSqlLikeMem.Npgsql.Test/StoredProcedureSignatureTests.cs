namespace DbSqlLikeMem.Npgsql.Test;

/// <summary>
/// EN: Verifies stored procedure signature behavior for PostgreSQL.
/// PT: Verifica o comportamento de assinatura de procedures para PostgreSQL.
/// </summary>
/// <param name="helper">
/// EN: Output helper used by the test base.
/// PT: Helper de saída usado pela base de testes.
/// </param>
public sealed class StoredProcedureSignatureTests(
        ITestOutputHelper helper
    ) : StoredProcedureSignatureTestsBase<NpgsqlMockException>(helper)
{
    /// <summary>
    /// EN: Creates the PostgreSQL connection mock used in tests.
    /// PT: Cria o mock de conexão PostgreSQL usado nos testes.
    /// </summary>
    protected override DbConnectionMockBase CreateConnection() => new NpgsqlConnectionMock();
}
