using DbConnection = System.Data.Common.DbConnection;
namespace DbSqlLikeMem.SqlServer;

/// <summary>
/// EN: Represents the Sql Server Data Source Mock type used by provider mocks.
/// PT-br: Representa a fonte de dados simulada do SQL Server usada pelos mocks do provedor.
/// </summary>
public sealed class SqlServerDataSourceMock(SqlServerDbMock? db = null)
#if NET7_0_OR_GREATER
    : DbDataSource
#endif
{
    /// <summary>
    /// EN: Gets the connection string exposed by this mock data source.
    /// PT-br: Obtém a string de conexão exposta por esta fonte de dados simulada.
    /// </summary>
    public
#if NET7_0_OR_GREATER
    override
#endif
    string ConnectionString => string.Empty;

#if NET7_0_OR_GREATER
    /// <summary>
    /// EN: Creates a new db connection instance.
    /// PT-br: Cria uma nova instância de conexão de banco de dados.
    /// </summary>
    protected override DbConnection CreateDbConnection() => new SqlServerConnectionMock(db);
#else
    /// <summary>
    /// EN: Creates a new db connection instance.
    /// PT-br: Cria uma nova instância de conexão de banco de dados.
    /// </summary>
    public SqlServerConnectionMock CreateDbConnection() => new SqlServerConnectionMock(db);
#endif

    /// <summary>
    /// EN: Creates a new connection instance.
    /// PT-br: Cria uma nova instância de conexão.
    /// </summary>
    public
#if NET7_0_OR_GREATER
    new
#endif
    SqlServerConnectionMock CreateConnection() => new SqlServerConnectionMock(db);

}
