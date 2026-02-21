namespace DbSqlLikeMem.Npgsql.NHibernate.Test;

/// <summary>
/// Runs smoke tests for NHibernate integration using the PostgreSQL in-memory mock provider.
/// Executa testes de fumaça para integração do NHibernate usando o provedor simulado em memória do Npgsql.
/// </summary>
public sealed class NHibernateSmokeTests : NHibernateSupportTestsBase
{
    /// <summary>
    /// Gets the NHibernate dialect class used to emulate PostgreSQL SQL behavior.
    /// Obtém a classe de dialeto do NHibernate usada para emular o comportamento SQL do Npgsql.
    /// </summary>
    protected override string NhDialectClass => "NHibernate.Dialect.PostgreSQL83Dialect, NHibernate";

    /// <summary>
    /// EN: Enables pagination fallback due to mocked parser limitations for parameterized LIMIT/OFFSET.
    /// PT: Habilita fallback de paginação devido a limitações do parser mock com LIMIT/OFFSET parametrizado.
    /// </summary>
    protected override bool UseInMemoryPaginationFallback => true;

    /// <summary>
    /// Gets the NHibernate driver class that connects NHibernate to the PostgreSQL mock connection.
    /// Obtém a classe de driver do NHibernate que conecta o NHibernate à conexão simulada de Npgsql.
    /// </summary>
    protected override string NhDriverClass => typeof(NpgsqlNhMockDriver).AssemblyQualifiedName!;

    /// <summary>
    /// Creates and opens a PostgreSQL mock connection for NHibernate smoke test execution.
    /// Cria e abre uma conexão simulada de Npgsql para execução dos testes de fumaça do NHibernate.
    /// </summary>
    protected override DbConnection CreateOpenConnection()
    {
        var connection = new NpgsqlConnectionMock([]);
        connection.Open();
        return connection;
    }
}
