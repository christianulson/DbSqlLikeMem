using DbConnection = System.Data.Common.DbConnection;

namespace DbSqlLikeMem.Firebird;

/// <summary>
/// EN: Represents the Firebird data source mock type used by provider mocks.
/// PT-br: Representa a fonte de dados simulada do Firebird usada pelos mocks do provedor.
/// </summary>
public sealed class FirebirdDataSourceMock(FirebirdDbMock? db = null)
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
    protected override DbConnection CreateDbConnection() => new FirebirdConnectionMock(db);
#else
    /// <summary>
    /// EN: Creates a new db connection instance.
    /// PT-br: Cria uma nova instância de conexão de banco de dados.
    /// </summary>
    public FirebirdConnectionMock CreateDbConnection() => new FirebirdConnectionMock(db);
#endif

    /// <summary>
    /// EN: Creates a new connection instance.
    /// PT-br: Cria uma nova instância de conexão.
    /// </summary>
    public
#if NET7_0_OR_GREATER
    new
#endif
    FirebirdConnectionMock CreateConnection() => new FirebirdConnectionMock(db);
}

