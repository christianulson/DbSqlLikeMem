namespace DbSqlLikeMem.SqlAzure;

/// <summary>
/// EN: Represents the Sql Azure Data Source Mock type used by provider mocks.
/// PT: Representa a fonte de dados simulada do SQL Azure usada pelos mocks do provedor.
/// </summary>
public sealed class SqlAzureDataSourceMock(SqlAzureDbMock? db = null)
#if NET7_0_OR_GREATER
    : DbDataSource
#endif
{
    /// <summary>
    /// EN: Gets the connection string exposed by this SQL Azure data source mock.
    /// PT: Obtem a string de conexao exposta por esta fonte de dados simulada do SQL Azure.
    /// </summary>
    public
#if NET7_0_OR_GREATER
    override
#endif
    string ConnectionString => string.Empty;

#if NET7_0_OR_GREATER
    /// <summary>
    /// EN: Creates a database connection instance for this SQL Azure data source mock.
    /// PT: Cria uma instancia de conexao de banco para esta fonte de dados simulada do SQL Azure.
    /// </summary>
    protected override DbConnection CreateDbConnection() => new SqlAzureConnectionMock(db);
#else
    /// <summary>
    /// EN: Creates a typed SQL Azure connection for this SQL Azure data source mock.
    /// PT: Cria uma conexao tipada do SQL Azure para esta fonte de dados simulada do SQL Azure.
    /// </summary>
    public SqlAzureConnectionMock CreateDbConnection() => new SqlAzureConnectionMock(db);
#endif

    /// <summary>
    /// EN: Creates a typed SQL Azure connection from this data source mock.
    /// PT: Cria uma conexao tipada do SQL Azure a partir desta fonte de dados simulada.
    /// </summary>
    public
#if NET7_0_OR_GREATER
    new
#endif
    SqlAzureConnectionMock CreateConnection() => new SqlAzureConnectionMock(db);
}