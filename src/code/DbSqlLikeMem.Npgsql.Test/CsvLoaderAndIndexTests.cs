namespace DbSqlLikeMem.Npgsql.Test;

/// <summary>
/// EN: Provides PostgreSQL-specific coverage for CSV loading and index behaviors.
/// PT-br: Fornece cobertura específica de PostgreSQL para comportamentos de carregamento de CSV e índices.
/// </summary>
/// <param name="helper">
/// EN: Output helper used by the test base.
/// PT-br: Helper de saída usado pela base de testes.
/// </param>
public sealed class CsvLoaderAndIndexTests(
    ITestOutputHelper helper
    ) : CsvLoaderAndIndexTestBase<NpgsqlDbMock, NpgsqlMockException>(helper)
{
    /// <summary>
    /// EN: Creates a new PostgreSQL mock database for each scenario.
    /// PT-br: Cria um novo banco simulado de PostgreSQL para cada cenário.
    /// </summary>
    protected override NpgsqlDbMock CreateDb() => [];
}
