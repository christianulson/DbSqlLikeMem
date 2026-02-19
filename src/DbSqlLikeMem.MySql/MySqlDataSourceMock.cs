namespace DbSqlLikeMem.MySql;

/// <summary>
/// MySQL mock data source implementation for tests.
/// Implementação de fonte de dados mock de MySQL para testes.
/// </summary>
/// <param name="db">Optional in-memory database backing instance.
/// Instância opcional de banco em memória usada como base.</param>
public sealed class MySqlDataSourceMock(MySqlDbMock? db = null)
#if NET8_0_OR_GREATER
    : DbDataSource
#endif
{
    /// <summary>
    /// Gets the connection string exposed by this mock data source.
    /// Obtém a string de conexão exposta por esta fonte de dados mock.
    /// </summary>
    public
#if NET8_0_OR_GREATER
    override
#endif
        string ConnectionString => string.Empty;

#if NET8_0_OR_GREATER
    /// <summary>
    /// Creates a database connection bound to the configured mock database.
    /// Cria uma conexão de banco vinculada ao banco de dados mock configurado.
    /// </summary>
    protected override DbConnection CreateDbConnection() => new MySqlConnectionMock(db);
#else
    /// <summary>
    /// Creates a database connection bound to the configured mock database.
    /// Cria uma conexão de banco vinculada ao banco de dados mock configurado.
    /// </summary>
    public DbConnection CreateDbConnection() => new MySqlConnectionMock(db);
#endif
}
