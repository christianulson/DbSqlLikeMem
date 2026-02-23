using DbConnection = System.Data.Common.DbConnection;
namespace DbSqlLikeMem.Sqlite;

/// <summary>
/// EN: Represents the Sqlite Data Source Mock type used by provider mocks.
/// PT: Representa a fonte de dados simulada do SQLite usada pelos mocks do provedor.
/// </summary>
public sealed class SqliteDataSourceMock(SqliteDbMock? db = null)
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
    protected override DbConnection CreateDbConnection() => new SqliteConnectionMock(db);
#else
    /// <summary>
    /// EN: Creates a new db connection instance.
    /// PT: Cria uma nova instância de conexão de banco de dados.
    /// </summary>
    public SqliteConnectionMock CreateDbConnection() => new SqliteConnectionMock(db);
#endif

    /// <summary>
    /// EN: Creates a new connection instance.
    /// PT: Cria uma nova instância de conexão.
    /// </summary>
    public
#if NET7_0_OR_GREATER
    new
#endif
    SqliteConnectionMock CreateConnection() => new SqliteConnectionMock(db);

}
