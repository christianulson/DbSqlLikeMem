namespace DbSqlLikeMem.SqlServer.NHibernate.Test;

/// <summary>
/// Runs smoke tests for NHibernate integration using the SQL Server in-memory mock provider.
/// Executa testes de fumaça para integração do NHibernate usando o provedor simulado em memória do SQL Server.
/// </summary>
public sealed class NHibernateSmokeTests : NHibernateSupportTestsBase
{
    /// <summary>
    /// Gets the NHibernate dialect class used to emulate SQL Server SQL behavior.
    /// Obtém a classe de dialeto do NHibernate usada para emular o comportamento SQL do SQL Server.
    /// </summary>
    protected override string NhDialectClass => "NHibernate.Dialect.MsSql2012Dialect, NHibernate";

    /// <summary>
    /// Gets the NHibernate driver class that connects NHibernate to the SQL Server mock connection.
    /// Obtém a classe de driver do NHibernate que conecta o NHibernate à conexão simulada de SQL Server.
    /// </summary>
    protected override string NhDriverClass => typeof(SqlServerNhMockDriver).AssemblyQualifiedName!;

    /// <summary>
    /// Creates and opens a SQL Server mock connection for NHibernate smoke test execution.
    /// Cria e abre uma conexão simulada de SQL Server para execução dos testes de fumaça do NHibernate.
    /// </summary>
    protected override DbConnection CreateOpenConnection()
    {
        var connection = new SqlServerConnectionMock([]);
        connection.Open();
        return connection;
    }
}
