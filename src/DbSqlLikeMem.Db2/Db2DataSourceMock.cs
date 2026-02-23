namespace DbSqlLikeMem.Db2;

/// <summary>
/// EN: Represents the Db2 Data Source Mock type used by provider mocks.
/// PT: Representa a fonte de dados simulada do Db2 usada pelos mocks do provedor.
/// </summary>
public sealed class Db2DataSourceMock(Db2DbMock? db = null)
#if NET7_0_OR_GREATER
    : DbDataSource
#endif
{
    /// <summary>
    /// EN: Gets the connection string exposed by this mock data source.
    /// PT: Obtém a string de conexão exposta por esta fonte de dados simulada.
    /// </summary>
    public
#if NET7_0_OR_GREATER
    override
#endif
    string ConnectionString => string.Empty;

#if NET7_0_OR_GREATER
    /// <summary>
    /// EN: Creates a new db connection instance.
    /// PT: Cria uma nova instância de conexão de banco de dados.
    /// </summary>
    protected override DbConnection CreateDbConnection() => new Db2ConnectionMock(db);
#else
    /// <summary>
    /// EN: Creates a new db connection instance.
    /// PT: Cria uma nova instância de conexão de banco de dados.
    /// </summary>
    public DbConnection CreateDbConnection() => new Db2ConnectionMock(db);
#endif
}
