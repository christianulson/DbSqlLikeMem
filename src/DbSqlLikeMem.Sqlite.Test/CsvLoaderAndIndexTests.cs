namespace DbSqlLikeMem.Sqlite.Test;

/// <summary>
/// EN: Provides SQLite-specific coverage for CSV loading and index behaviors.
/// PT: Fornece cobertura específica de SQLite para comportamentos de carregamento de CSV e índices.
/// </summary>
/// <param name="helper">
/// EN: Output helper used by the test base.
/// PT: Helper de saída usado pela base de testes.
/// </param>
public sealed class CsvLoaderAndIndexTests(
    ITestOutputHelper helper
    ) : CsvLoaderAndIndexTestBase<SqliteDbMock, SqliteMockException>(helper)
{
    /// <summary>
    /// EN: Creates a new SQLite mock database for each scenario.
    /// PT: Cria um novo banco mock de SQLite para cada cenário.
    /// </summary>
    protected override SqliteDbMock CreateDb() => new();
}
