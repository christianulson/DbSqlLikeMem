namespace DbSqlLikeMem.Firebird.Test;

/// <summary>
/// EN: Provides Firebird-specific coverage for CSV loading and index behaviors.
/// PT: Fornece cobertura específica de Firebird para comportamentos de carregamento de CSV e índices.
/// </summary>
/// <param name="helper">
/// EN: xUnit output helper used by the shared base test class.
/// PT: Helper de saída do xUnit usado pela classe base de testes compartilhada.
/// </param>
public sealed class CsvLoaderAndIndexTests(
    ITestOutputHelper helper
    ) : CsvLoaderAndIndexTestBase<FirebirdDbMock, FirebirdMockException>(helper)
{
    /// <summary>
    /// EN: Creates a new Firebird mock database for each scenario.
    /// PT: Cria um novo banco simulado Firebird para cada cenário.
    /// </summary>
    protected override FirebirdDbMock CreateDb() => [];
}
