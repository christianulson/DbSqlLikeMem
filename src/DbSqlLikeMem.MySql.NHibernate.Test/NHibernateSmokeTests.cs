namespace DbSqlLikeMem.MySql.NHibernate.Test;

/// <summary>
/// Runs smoke tests for NHibernate integration using the MySQL in-memory mock provider.
/// Executa testes de fumaça para integração do NHibernate usando o provedor simulado em memória do MySQL.
/// </summary>
public sealed class NHibernateSmokeTests(
    ITestOutputHelper helper
) : NHibernateSmokeTestsBase(
    helper,
    nhDialectClass: "NHibernate.Dialect.MySQLDialect, NHibernate",
    connectionFactory: static () =>
    {
        var connection = new MySqlConnectionMock([]);
        connection.Open();
        return connection;
    },
    nhDriverClass: typeof(MySqlNhMockDriver).AssemblyQualifiedName!,
    useInMemoryPaginationFallback: true);
