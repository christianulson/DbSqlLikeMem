namespace DbSqlLikeMem.SqlServer.Test;

/// <summary>
/// EN: Runs shared EXISTS/NOT EXISTS tests using the SQL Server mock connection.
/// PT: Executa os testes compartilhados de EXISTS/NOT EXISTS usando a conexão simulada de SQL Server.
/// </summary>
/// <param name="helper">
/// EN: xUnit output helper used by the shared base test class.
/// PT: Helper de saída do xUnit usado pela classe base de testes compartilhada.
/// </param>
public sealed class ExistsTests(
        ITestOutputHelper helper
    ) : ExistsTestsBase(helper)
{
    /// <summary>
    /// EN: Creates a SQL Server mock connection used by shared EXISTS tests.
    /// PT: Cria uma conexão simulada de SQL Server usada pelos testes compartilhados de EXISTS.
    /// </summary>
    protected override DbConnectionMockBase CreateConnection() => new SqlServerConnectionMock();
}
