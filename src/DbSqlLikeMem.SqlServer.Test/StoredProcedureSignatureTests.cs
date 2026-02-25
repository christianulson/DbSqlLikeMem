namespace DbSqlLikeMem.SqlServer.Test;

/// <summary>
/// EN: Runs shared stored procedure signature tests using the SQL Server mock connection.
/// PT: Executa os testes compartilhados de assinatura de procedure usando a conexão simulada de SQL Server.
/// </summary>
/// <param name="helper">
/// EN: xUnit output helper used by the shared base test class.
/// PT: Helper de saída do xUnit usado pela classe base de testes compartilhada.
/// </param>
public sealed class StoredProcedureSignatureTests(
        ITestOutputHelper helper
    ) : StoredProcedureSignatureTestsBase<SqlServerMockException>(helper)
{
    /// <summary>
    /// EN: Creates a SQL Server mock connection used by stored procedure signature tests.
    /// PT: Cria uma conexão simulada de SQL Server usada pelos testes de assinatura de procedure.
    /// </summary>
    protected override DbConnectionMockBase CreateConnection() => new SqlServerConnectionMock();
}
