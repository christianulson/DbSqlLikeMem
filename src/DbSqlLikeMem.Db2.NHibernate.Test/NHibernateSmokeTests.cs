namespace DbSqlLikeMem.Db2.NHibernate.Test;

/// <summary>
/// Runs smoke tests for NHibernate integration using the DB2 in-memory mock provider.
/// Executa testes de fumaça para integração do NHibernate usando o provedor simulado em memória do DB2.
/// </summary>
public sealed class NHibernateSmokeTests : NHibernateSupportTestsBase
{
    /// <summary>
    /// Gets the NHibernate dialect class used to emulate DB2 SQL behavior.
    /// Obtém a classe de dialeto do NHibernate usada para emular o comportamento SQL do DB2.
    /// </summary>
    protected override string NhDialectClass => "NHibernate.Dialect.DB2Dialect, NHibernate";

    /// <summary>
    /// EN: Enables pagination fallback due to mocked parser limitations for parameterized LIMIT/OFFSET.
    /// PT: Habilita fallback de paginação devido a limitações do parser simulado com LIMIT/OFFSET parametrizado.
    /// </summary>
    protected override bool UseInMemoryPaginationFallback => true;

    /// <summary>
    /// Gets the NHibernate driver class that connects NHibernate to the DB2 mock connection.
    /// Obtém a classe de driver do NHibernate que conecta o NHibernate à conexão simulada de DB2.
    /// </summary>
    protected override string NhDriverClass => typeof(Db2NhMockDriver).AssemblyQualifiedName!;

    /// <summary>
    /// Creates and opens a DB2 mock connection for NHibernate smoke test execution.
    /// Cria e abre uma conexão simulada de DB2 para execução dos testes de fumaça do NHibernate.
    /// </summary>
    protected override DbConnection CreateOpenConnection()
    {
        var connection = new Db2ConnectionMock();
        connection.Open();
        return connection;
    }
}
