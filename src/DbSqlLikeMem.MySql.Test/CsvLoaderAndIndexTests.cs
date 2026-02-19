namespace DbSqlLikeMem.MySql.Test;

/// <summary>
/// EN: Provides MySQL-specific coverage for CSV loading and index behaviors.
/// PT: Fornece cobertura específica de MySQL para comportamentos de carregamento de CSV e índices.
/// </summary>
/// <param name="helper">
/// EN: Output helper used by the test base.
/// PT: Helper de saída usado pela base de testes.
/// </param>
public sealed class CsvLoaderAndIndexTests(
    ITestOutputHelper helper
    ) : CsvLoaderAndIndexTestBase<MySqlDbMock, MySqlMockException>(helper)
{
    /// <summary>
    /// EN: Creates a new MySQL mock database for each scenario.
    /// PT: Cria um novo banco mock de MySQL para cada cenário.
    /// </summary>
    protected override MySqlDbMock CreateDb() => new();
}
