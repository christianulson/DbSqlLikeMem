namespace DbSqlLikeMem.TestTools.TemporaryTable;

/// <summary>
/// EN: Creates and drops the source users table used by temporary-table workflows.
/// PT: Cria e remove a tabela fonte de usuarios usada pelos fluxos de tabela temporaria.
/// </summary>
public sealed class TemporaryTableScenario<T>(ProviderSqlDialect dialect) : ITestScenario<T>
    where T : DbConnection
{
    /// <summary>
    /// EN: Creates the source users table and seeds the rows used by the temporary-table tests.
    /// PT: Cria a tabela fonte de usuarios e preenche as linhas usadas pelos testes de tabela temporaria.
    /// </summary>
    /// <param name="service">EN: The shared test service used to execute SQL. PT: O servico de teste compartilhado usado para executar SQL.</param>
    /// <param name="pars">EN: The scenario parameters. PT: Os parametros do cenario.</param>
    public void CreateScenario(
        BaseServiceTest<T> service,
        params object[] pars)
    {
        var users = (string)pars[0];
        var uId = (string)pars[1];
        var tableName = ResolveSourceTableName(users, uId);

        service.ExecuteNonQuery($@"
CREATE TABLE {tableName} (
    Id INT NOT NULL PRIMARY KEY,
    Name VARCHAR(100) NOT NULL,
    TenantId INT NOT NULL
)");

        service.ExecuteNonQuery($"INSERT INTO {tableName} (Id, Name, TenantId) VALUES (1, 'John', 10)");
        service.ExecuteNonQuery($"INSERT INTO {tableName} (Id, Name, TenantId) VALUES (2, 'Bob', 10)");
        service.ExecuteNonQuery($"INSERT INTO {tableName} (Id, Name, TenantId) VALUES (3, 'Jane', 20)");
    }

    /// <summary>
    /// EN: Drops the source users table created for the temporary-table workflow.
    /// PT: Remove a tabela fonte de usuarios criada para o fluxo de tabela temporaria.
    /// </summary>
    /// <param name="service">EN: The shared test service used to execute SQL. PT: O servico de teste compartilhado usado para executar SQL.</param>
    /// <param name="pars">EN: The scenario parameters. PT: Os parametros do cenario.</param>
    public void DropScenario(
        BaseServiceTest<T> service,
        params object[] pars)
    {
        var users = (string)pars[0];
        var uId = (string)pars[1];
        if (dialect.Provider == ProviderId.Oracle)
        {
            TryDropOracleSourceTable(service, users, uId);
            return;
        }

        service.ExecuteNonQuery(dialect.DropTable(users, uId));
    }

    private static void TryDropOracleSourceTable(
        BaseServiceTest<T> service,
        string users,
        string uId)
    {
        TryExecuteDrop(service, $"DROP TABLE {ResolveSourceTableName(users, uId)}");
        TryExecuteDrop(service, $"DROP TABLE {users}");
    }

    private static void TryExecuteDrop(
        BaseServiceTest<T> service,
        string sql)
    {
        try
        {
            service.ExecuteNonQuery(sql);
        }
        catch (Exception ex) when (IsMissingTableException(ex))
        {
        }
    }

    private static string ResolveSourceTableName(string users, string uId)
        => $"{users}_{uId}".ToLowerInvariant();

    private static bool IsMissingTableException(Exception ex)
    {
        var message = ex.GetBaseException().Message;
        return message.Contains("does not exist", StringComparison.OrdinalIgnoreCase)
            || message.Contains("doesn't exist", StringComparison.OrdinalIgnoreCase)
            || message.Contains("doesnt exist", StringComparison.OrdinalIgnoreCase)
            || message.Contains("not exist", StringComparison.OrdinalIgnoreCase)
            || message.Contains("undefined name", StringComparison.OrdinalIgnoreCase)
            || message.Contains("not found", StringComparison.OrdinalIgnoreCase)
            || message.Contains("ora-00942", StringComparison.OrdinalIgnoreCase)
            || message.Contains("tabela ou view", StringComparison.OrdinalIgnoreCase)
            || message.Contains("given key was not present", StringComparison.OrdinalIgnoreCase);
    }
}
