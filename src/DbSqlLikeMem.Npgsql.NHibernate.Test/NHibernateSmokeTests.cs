namespace DbSqlLikeMem.Npgsql.NHibernate.Test;

/// <summary>
/// Runs smoke tests for NHibernate integration using the PostgreSQL in-memory mock provider.
/// Executa testes de fumaça para integração do NHibernate usando o provedor simulado em memória do Npgsql.
/// </summary>
public sealed class NHibernateSmokeTests(
    ITestOutputHelper helper
) : NHibernateSmokeTestsBase(
    helper,
    nhDialectClass: "NHibernate.Dialect.PostgreSQL83Dialect, NHibernate",
    connectionFactory: static () =>
    {
        var connection = new NpgsqlConnectionMock([]);
        connection.Open();
        return connection;
    },
    nhDriverClass: typeof(NpgsqlNhMockDriver).AssemblyQualifiedName!,
    useInMemoryPaginationFallback: true);
