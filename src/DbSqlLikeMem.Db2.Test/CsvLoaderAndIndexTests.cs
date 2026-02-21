namespace DbSqlLikeMem.Db2.Test;

/// <summary>
/// EN: Runs CsvLoader and index shared tests using the Db2 mock implementation.
/// PT: Executa os testes compartilhados de CsvLoader e índices usando a implementação mock de Db2.
/// </summary>
/// <param name="helper">
/// EN: xUnit output helper used by the shared base test class.
/// PT: Helper de saída do xUnit usado pela classe base de testes compartilhada.
/// </param>
public sealed class CsvLoaderAndIndexTests(
    ITestOutputHelper helper
    ) : CsvLoaderAndIndexTestBase<Db2DbMock, Db2MockException>(helper)
{
    /// <summary>
    /// EN: Creates a new Db2 mock database for each test execution.
    /// PT: Cria um novo banco mock de Db2 para cada execução de teste.
    /// </summary>
    protected override Db2DbMock CreateDb() => [];
}
