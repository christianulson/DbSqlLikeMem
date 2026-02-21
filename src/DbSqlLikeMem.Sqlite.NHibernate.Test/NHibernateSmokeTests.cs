namespace DbSqlLikeMem.Sqlite.NHibernate.Test;

/// <summary>
/// Runs smoke tests for NHibernate integration using the SQLite in-memory mock provider.
/// Executa testes de fumaça para integração do NHibernate usando o provedor simulado em memória do SQLite.
/// </summary>
public sealed class NHibernateSmokeTests : NHibernateSupportTestsBase
{
    /// <summary>
    /// Gets the NHibernate dialect class used to emulate SQLite SQL behavior.
    /// Obtém a classe de dialeto do NHibernate usada para emular o comportamento SQL do SQLite.
    /// </summary>
    protected override string NhDialectClass => "NHibernate.Dialect.SQLiteDialect, NHibernate";

    /// <summary>
    /// Gets the NHibernate driver class that connects NHibernate to the SQLite mock connection.
    /// Obtém a classe de driver do NHibernate que conecta o NHibernate à conexão simulada de SQLite.
    /// </summary>
    protected override string NhDriverClass => typeof(SqliteNhMockDriver).AssemblyQualifiedName!;

    /// <summary>
    /// Creates and opens a SQLite mock connection for NHibernate smoke test execution.
    /// Cria e abre uma conexão simulada de SQLite para execução dos testes de fumaça do NHibernate.
    /// </summary>
    protected override DbConnection CreateOpenConnection()
    {
        var connection = new SqliteConnectionMock([]);
        connection.Open();
        return connection;
    }
}
