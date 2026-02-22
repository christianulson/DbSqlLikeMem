namespace DbSqlLikeMem.Db2.Test;

/// <summary>
/// EN: Runs shared EXISTS/NOT EXISTS tests using the Db2 mock connection.
/// PT: Executa os testes compartilhados de EXISTS/NOT EXISTS usando a conexão simulada de Db2.
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
    /// EN: Creates a Db2 mock connection used by shared EXISTS tests.
    /// PT: Cria uma conexão simulada de Db2 usada pelos testes compartilhados de EXISTS.
    /// </summary>
    protected override DbConnectionMockBase CreateConnection() => new Db2ConnectionMock();
}
