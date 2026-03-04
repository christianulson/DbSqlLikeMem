namespace DbSqlLikeMem.SqlAzure.Test;

/// <summary>
/// EN: Runs CsvLoader and index shared tests using the SQL Azure mock implementation.
/// PT: Executa os testes compartilhados de CsvLoader e índices usando a implementação simulada de SQL Azure.
/// </summary>
/// <param name="helper">
/// EN: xUnit output helper used by the shared base test class.
/// PT: Helper de saída do xUnit usado pela classe base de testes compartilhada.
/// </param>
public sealed class CsvLoaderAndIndexTests(
    ITestOutputHelper helper
    ) : CsvLoaderAndIndexTestBase<SqlAzureDbMock, SqlAzureMockException>(helper)
{
    /// <summary>
    /// EN: Creates a new SQL Azure mock database for each test execution.
    /// PT: Cria um novo banco simulado de SQL Azure para cada execução de teste.
    /// </summary>
    protected override SqlAzureDbMock CreateDb() => [];
}
