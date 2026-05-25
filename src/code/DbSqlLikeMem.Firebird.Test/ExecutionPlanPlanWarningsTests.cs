namespace DbSqlLikeMem.Firebird.Test;

/// <summary>
/// EN: Runs shared execution-plan warning tests for Firebird.
/// PT-br: Executa testes compartilhados de alertas de plano de execução para Firebird.
/// </summary>
/// <param name="helper">
/// EN: xUnit output helper used by the shared base test class.
/// PT-br: Helper de saída do xUnit usado pela classe base de testes compartilhada.
/// </param>
public sealed class ExecutionPlanPlanWarningsTests(
    ITestOutputHelper helper
    ) : ExecutionPlanPlanWarningsTestsBase(helper)
{
    /// <summary>
    /// EN: Creates a Firebird mock connection for warning test scenarios.
    /// PT-br: Cria uma conexão mock Firebird para cenários de teste de alertas.
    /// </summary>
    protected override DbConnectionMockBase CreateConnection() => new FirebirdConnectionMock();

    /// <summary>
    /// EN: Creates a provider command bound to the provided connection and SQL text.
    /// PT-br: Cria um comando do provedor associado à conexão e ao texto SQL informados.
    /// </summary>
    protected override DbCommand CreateCommand(DbConnectionMockBase connection, string commandText)
        => new FirebirdCommandMock((FirebirdConnectionMock)connection) { CommandText = commandText };

    /// <summary>
    /// EN: Gets provider-specific ORDER BY query with FIRST used by shared tests.
    /// PT-br: Obtém consulta ORDER BY com FIRST específica do provedor usada pelos testes compartilhados.
    /// </summary>
    protected override string SelectOrderByWithLimitSql => "SELECT FIRST 10 Id FROM users ORDER BY Id";
}
