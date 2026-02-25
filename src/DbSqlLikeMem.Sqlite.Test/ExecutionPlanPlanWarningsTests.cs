using System.Data.Common;

namespace DbSqlLikeMem.Sqlite.Test;

/// <summary>
/// EN: Runs shared execution-plan warning tests for SQLite.
/// PT: Executa testes compartilhados de alertas de plano de execução para SQLite.
/// </summary>
/// <param name="helper">EN: xUnit output helper. PT: Helper de saída do xUnit.</param>
public sealed class ExecutionPlanPlanWarningsTests(ITestOutputHelper helper)
    : ExecutionPlanPlanWarningsTestsBase(helper)
{
    /// <summary>
    /// EN: Creates a SQLite mock connection for warning test scenarios.
    /// PT: Cria uma conexão mock SQLite para cenários de teste de alertas.
    /// </summary>
    protected override DbConnectionMockBase CreateConnection() => new SqliteConnectionMock();

    /// <summary>
    /// EN: Creates a SQLite command bound to the provided connection and SQL text.
    /// PT: Cria um comando SQLite associado à conexão e ao texto SQL informados.
    /// </summary>
    protected override DbCommand CreateCommand(DbConnectionMockBase connection, string commandText)
        => new SqliteCommandMock((SqliteConnectionMock)connection) { CommandText = commandText };

    /// <summary>
    /// EN: Gets provider-specific ORDER BY query with LIMIT used by shared tests.
    /// PT: Obtém consulta ORDER BY com LIMIT específica do provedor usada pelos testes compartilhados.
    /// </summary>
    protected override string SelectOrderByWithLimitSql => "SELECT Id FROM users ORDER BY Id LIMIT 10";
}
