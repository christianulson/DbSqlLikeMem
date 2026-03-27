namespace DbSqlLikeMem.TestTools;

/// <summary>
/// EN: Resolves provider-specific connection strings for container-backed fidelity tests and benchmark sessions.
/// PT: ResolveRowsFrameRange strings de conexao especificas do provedor para testes de fidelidade com container e sessoes de benchmark.
/// </summary>
public static class ProviderConnectionStringResolver
{
    /// <summary>
    /// EN: Tries to resolve a container connection string for the specified provider from environment variables.
    /// PT: Tenta resolver uma string de conexao de container para o provedor informado a partir de variaveis de ambiente.
    /// </summary>
    /// <param name="provider">EN: The provider identifier used to select the environment variable set. PT: O identificador do provedor usado para selecionar o conjunto de variaveis de ambiente.</param>
    /// <param name="connectionString">EN: The resolved connection string when one is available. PT: A string de conexao resolvida quando houver uma disponivel.</param>
    /// <returns>EN: True when a connection string was resolved or when SQLite uses an empty local string. PT: True quando uma string de conexao foi resolvida ou quando o SQLite usa uma string local vazia.</returns>
    public static bool TryResolve(ProviderId provider, out string connectionString)
        => TryResolve(provider, out connectionString, out _);

    /// <summary>
    /// EN: Tries to resolve a container connection string and the source variable name for the specified provider.
    /// PT: Tenta resolver uma string de conexao de container e o nome da variavel de origem para o provedor informado.
    /// </summary>
    /// <param name="provider">EN: The provider identifier used to select the environment variable set. PT: O identificador do provedor usado para selecionar o conjunto de variaveis de ambiente.</param>
    /// <param name="connectionString">EN: The resolved connection string when one is available. PT: A string de conexao resolvida quando houver uma disponivel.</param>
    /// <param name="sourceName">EN: The environment variable name that produced the resolved connection string. PT: O nome da variavel de ambiente que produziu a string de conexao resolvida.</param>
    /// <returns>EN: True when a connection string was resolved or when SQLite uses an empty local string. PT: True quando uma string de conexao foi resolvida ou quando o SQLite usa uma string local vazia.</returns>
    public static bool TryResolve(
        ProviderId provider,
        out string connectionString,
        out string sourceName)
    {
        if (provider == ProviderId.Sqlite)
        {
            connectionString = string.Empty;
            sourceName = string.Empty;
            return true;
        }

        foreach (var variableName in GetEnvironmentVariableNames(provider))
        {
            var value = Environment.GetEnvironmentVariable(variableName);
            if (!string.IsNullOrWhiteSpace(value))
            {
                connectionString = value;
                sourceName = variableName;
                return true;
            }
        }

        connectionString = string.Empty;
        sourceName = string.Empty;
        return false;
    }

    private static IReadOnlyList<string> GetEnvironmentVariableNames(ProviderId provider)
        => provider switch
        {
            ProviderId.MySql =>
            [
                "DBSQLLIKEMEM_BENCH_MYSQL_CONNECTION_STRING",
                "MYSQL_CONNECTION_STRING"
            ],
            ProviderId.MariaDb =>
            [
                "DBSQLLIKEMEM_BENCH_MARIADB_CONNECTION_STRING",
                "MARIADB_CONNECTION_STRING"
            ],
            ProviderId.SqlServer or ProviderId.SqlAzure =>
            [
                "DBSQLLIKEMEM_BENCH_SQLSERVER_CONNECTION_STRING",
                "SQLSERVER_CONNECTION_STRING"
            ],
            ProviderId.Oracle =>
            [
                "DBSQLLIKEMEM_BENCH_ORACLE_CONNECTION_STRING",
                "ORACLE_CONNECTION_STRING"
            ],
            ProviderId.Npgsql =>
            [
                "DBSQLLIKEMEM_BENCH_NPGSQL_CONNECTION_STRING",
                "DBSQLLIKEMEM_BENCH_POSTGRES_CONNECTION_STRING",
                "NPGSQL_CONNECTION_STRING",
                "POSTGRES_CONNECTION_STRING"
            ],
            ProviderId.Db2 =>
            [
                "DBSQLLIKEMEM_BENCH_DB2_CONNECTION_STRING",
                "DB2_CONNECTION_STRING"
            ],
            _ => Array.Empty<string>()
        };
}
