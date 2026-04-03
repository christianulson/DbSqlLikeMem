namespace DbSqlLikeMem.TestTools.Query;

/// <summary>
/// EN: Creates the users table and seed row required by the primary-key select scenario.
/// PT: Cria a tabela de usuarios e a linha inicial exigidas pelo cenario de selecao por chave primaria.
/// </summary>
public class SelectTableScenario<T>(
    ProviderSqlDialect dialect
    ) : ITestScenario<T>
     where T : DbConnection
{
    /// <summary>
    /// EN: Creates the users table and inserts the seed row used by the select scenario.
    /// PT: Cria a tabela de usuarios e insere a linha base usada pelo cenario de select.
    /// </summary>
    /// <param name="service">EN: Scenario service used to execute the setup SQL. PT: Servico de cenario usado para executar o SQL de configuracao.</param>
    /// <param name="pars">EN: Scenario arguments containing the users table name and token. PT: Argumentos do cenario contendo o nome da tabela de usuarios e o token.</param>
    public void CreateScenario(
        BaseServiceTest<T> service,
        params object[] pars)
    {
        var users = (string)pars[0];
        var uId = (string)pars[1];
        var tableName = ResolveScenarioTableName(users, uId);

        service.ExecuteNonQuery(dialect.CreateUsersTable(users, uId));
        service.ExecuteNonQuery(dialect.InsertUser(tableName, 1, "Alice"));
    }

    /// <summary>
    /// EN: Drops the users table created for the select scenario.
    /// PT: Remove a tabela de usuarios criada para o cenario de select.
    /// </summary>
    /// <param name="service">EN: Scenario service used to execute the cleanup SQL. PT: Servico de cenario usado para executar o SQL de limpeza.</param>
    /// <param name="pars">EN: Scenario arguments containing the users table name and token. PT: Argumentos do cenario contendo o nome da tabela de usuarios e o token.</param>
    public void DropScenario(
        BaseServiceTest<T> service,
        params object[] pars)
    {
        var users = (string)pars[0];
        var uId = (string)pars[1];
        if (dialect.Provider == ProviderId.Oracle)
        {
            service.ExecuteNonQuery($"DROP TABLE {ResolveScenarioTableName(users, uId)}");
            return;
        }

        service.ExecuteNonQuery(dialect.DropTable(users, uId));
    }

    private string ResolveScenarioTableName(string tableName, string uId)
        => dialect.Provider == ProviderId.Oracle
            ? $"{tableName}_{uId}".ToLowerInvariant()
            : $"{tableName}_{uId}";
}
