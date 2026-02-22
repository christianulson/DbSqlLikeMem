namespace DbSqlLikeMem.Oracle.Test;

/// <summary>
/// EN: Runs shared EXISTS/NOT EXISTS tests using the Oracle mock connection.
/// PT: Executa os testes compartilhados de EXISTS/NOT EXISTS usando a conexão simulada de Oracle.
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
    /// EN: Creates an Oracle mock connection used by shared EXISTS tests.
    /// PT: Cria uma conexão simulada de Oracle usada pelos testes compartilhados de EXISTS.
    /// </summary>
    protected override DbConnectionMockBase CreateConnection() => new OracleConnectionMock();
}
