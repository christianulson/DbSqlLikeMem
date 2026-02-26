using System.Data.Common;

namespace DbSqlLikeMem.SqlServer.Test;

/// <summary>
/// EN: Runs shared execution-plan warning tests for SQL Server.
/// PT: Executa testes compartilhados de alertas de plano de execução para SQL Server.
/// </summary>
/// <param name="helper">EN: xUnit output helper. PT: Helper de saída do xUnit.</param>
public sealed class ExecutionPlanPlanWarningsTests(ITestOutputHelper helper)
    : ExecutionPlanPlanWarningsTestsBase(helper)
{
    /// <summary>
    /// EN: Creates a SQL Server mock connection for warning test scenarios.
    /// PT: Cria uma conexão mock SQL Server para cenários de teste de alertas.
    /// </summary>
    protected override DbConnectionMockBase CreateConnection() => new SqlServerConnectionMock();

    /// <summary>
    /// EN: Creates a provider command bound to the provided connection and SQL text.
    /// PT: Cria um comando do provedor associado à conexão e ao texto SQL informados.
    /// </summary>
    protected override DbCommand CreateCommand(DbConnectionMockBase connection, string commandText)
        => new SqlServerCommandMock((SqlServerConnectionMock)connection) { CommandText = commandText };

    /// <summary>
    /// EN: Gets provider-specific ORDER BY query with TOP used by shared tests.
    /// PT: Obtém consulta ORDER BY com TOP específica do provedor usada pelos testes compartilhados.
    /// </summary>
    protected override string SelectOrderByWithLimitSql => "SELECT TOP 10 Id FROM users ORDER BY Id";
}
