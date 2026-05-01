namespace DbSqlLikeMem.Npgsql.Test;

/// <summary>
/// EN: Verifies stored procedure signature behavior for PostgreSQL.
/// PT-br: Verifica o comportamento de assinatura de procedures para PostgreSQL.
/// </summary>
/// <param name="helper">
/// EN: Output helper used by the test base.
/// PT-br: Helper de saída usado pela base de testes.
/// </param>
public sealed class StoredProcedureSignatureTests(
        ITestOutputHelper helper
    ) : StoredProcedureSignatureTestsBase<NpgsqlMockException>(helper)
{
    /// <inheritdoc />
    protected override DbSqlLikeMem.TestTools.ProviderSqlDialect Dialect { get; } = new DbSqlLikeMem.Npgsql.TestTools.NpgsqlProviderSqlDialect();

    /// <summary>
    /// EN: Creates the PostgreSQL connection mock used in tests.
    /// PT-br: Cria o simulado de conexão PostgreSQL usado nos testes.
    /// </summary>
    protected override DbConnectionMockBase CreateConnection() => new NpgsqlConnectionMock();
}
