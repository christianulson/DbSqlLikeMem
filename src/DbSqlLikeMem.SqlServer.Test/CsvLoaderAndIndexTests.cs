namespace DbSqlLikeMem.SqlServer.Test;

/// <summary>
/// EN: Runs CsvLoader and index shared tests using the SQL Server mock implementation.
/// PT: Executa os testes compartilhados de CsvLoader e índices usando a implementação simulado de SQL Server.
/// </summary>
/// <param name="helper">
/// EN: xUnit output helper used by the shared base test class.
/// PT: Helper de saída do xUnit usado pela classe base de testes compartilhada.
/// </param>
public sealed class CsvLoaderAndIndexTests(
    ITestOutputHelper helper
    ) : CsvLoaderAndIndexTestBase<SqlServerDbMock, SqlServerMockException>(helper)
{
    /// <summary>
    /// EN: Creates a new SQL Server mock database for each test execution.
    /// PT: Cria um novo banco simulado de SQL Server para cada execução de teste.
    /// </summary>
    protected override SqlServerDbMock CreateDb() => [];
}
