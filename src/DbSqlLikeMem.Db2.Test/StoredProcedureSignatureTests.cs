namespace DbSqlLikeMem.Db2.Test;

/// <summary>
/// EN: Runs shared stored procedure signature tests using the Db2 mock connection.
/// PT: Executa os testes compartilhados de assinatura de procedure usando a conexão simulada de Db2.
/// </summary>
/// <param name="helper">
/// EN: xUnit output helper used by the shared base test class.
/// PT: Helper de saída do xUnit usado pela classe base de testes compartilhada.
/// </param>
public sealed class StoredProcedureSignatureTests(
        ITestOutputHelper helper
    ) : StoredProcedureSignatureTestsBase<Db2MockException>(helper)
{
    /// <summary>
    /// EN: Creates a Db2 mock connection used by stored procedure signature tests.
    /// PT: Cria uma conexão simulada de Db2 usada pelos testes de assinatura de procedure.
    /// </summary>
    protected override DbConnectionMockBase CreateConnection() => new Db2ConnectionMock();
}
