namespace DbSqlLikeMem.Oracle.Test;

/// <summary>
/// EN: Runs CsvLoader and index shared tests using the Oracle mock implementation.
/// PT: Executa os testes compartilhados de CsvLoader e índices usando a implementação simulado de Oracle.
/// </summary>
/// <param name="helper">
/// EN: xUnit output helper used by the shared base test class.
/// PT: Helper de saída do xUnit usado pela classe base de testes compartilhada.
/// </param>
public sealed class CsvLoaderAndIndexTests(
    ITestOutputHelper helper
    ) : CsvLoaderAndIndexTestBase<OracleDbMock, OracleMockException>(helper)
{
    /// <summary>
    /// EN: Creates a new Oracle mock database for each test execution.
    /// PT: Cria um novo banco simulado de Oracle para cada execução de teste.
    /// </summary>
    protected override OracleDbMock CreateDb() => [];
}
