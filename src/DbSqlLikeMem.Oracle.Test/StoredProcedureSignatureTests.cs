namespace DbSqlLikeMem.Oracle.Test;

/// <summary>
/// EN: Runs shared stored procedure signature tests using the Oracle mock connection.
/// PT: Executa os testes compartilhados de assinatura de procedure usando a conexão mock de Oracle.
/// </summary>
/// <param name="helper">
/// EN: xUnit output helper used by the shared base test class.
/// PT: Helper de saída do xUnit usado pela classe base de testes compartilhada.
/// </param>
public sealed class StoredProcedureSignatureTests(
        ITestOutputHelper helper
    ) : StoredProcedureSignatureTestsBase<OracleMockException>(helper)
{
    /// <summary>
    /// EN: Creates an Oracle mock connection used by stored procedure signature tests.
    /// PT: Cria uma conexão mock de Oracle usada pelos testes de assinatura de procedure.
    /// </summary>
    protected override DbConnectionMockBase CreateConnection() => new OracleConnectionMock();
}
