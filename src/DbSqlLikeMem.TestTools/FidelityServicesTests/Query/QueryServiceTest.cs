namespace DbSqlLikeMem.TestTools.Query;

/// <summary>
/// EN: Executes shared query benchmark workflows and validates the observed provider results.
/// PT: Executa fluxos compartilhados de benchmark de consulta e valida os resultados observados do provedor.
/// </summary>
public partial class QueryServiceTest<T>(
    T connection,
    ITestScenario<T> testScenario,
    ProviderSqlDialect dialect
    ) : BaseServiceTest<T>(connection, testScenario, dialect)
    where T : DbConnection
{
    /// <summary>
    /// EN: Resolves a scenario table name to the provider-specific physical table name used in the current test run.
    /// PT: Resolve o nome logico de uma tabela de cenario para o nome fisico especifico do provedor usado na execucao atual.
    /// </summary>
    protected new string ResolveScenarioTableName(string tableName)
    {
        var scenarioArgs = CurrentScenarioArgs;
        if (scenarioArgs is null || scenarioArgs.Count < 2)
            return tableName;

        var uId = scenarioArgs[scenarioArgs.Count - 1]?.ToString();
        if (string.IsNullOrWhiteSpace(uId))
            return tableName;

        if (tableName.EndsWith($"_{uId}", StringComparison.OrdinalIgnoreCase))
            return Dialect.Provider == ProviderId.Oracle
                ? tableName.ToLowerInvariant()
                : tableName;

        return Dialect.Provider == ProviderId.Oracle
            ? $"{tableName}_{uId}".ToLowerInvariant()
            : $"{tableName}_{uId}";
    }
}
