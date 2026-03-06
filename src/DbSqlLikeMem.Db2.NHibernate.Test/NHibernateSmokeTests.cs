namespace DbSqlLikeMem.Db2.NHibernate.Test;

/// <summary>
/// Runs smoke tests for NHibernate integration using the DB2 in-memory mock provider.
/// Executa testes de fumaça para integração do NHibernate usando o provedor simulado em memória do DB2.
/// </summary>
public sealed class NHibernateSmokeTests(
    ITestOutputHelper helper
) : NHibernateSmokeTestsBase(
    helper,
    nhDialectClass: "NHibernate.Dialect.DB2Dialect, NHibernate",
    connectionFactory: static () =>
    {
        var connection = new Db2ConnectionMock();
        connection.Open();
        return connection;
    },
    nhDriverClass: typeof(Db2NhMockDriver).AssemblyQualifiedName!,
    useInMemoryPaginationFallback: true);
