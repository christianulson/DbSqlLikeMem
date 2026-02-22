using DbConnection = System.Data.Common.DbConnection;
namespace DbSqlLikeMem.Npgsql;

/// <summary>
/// EN: Represents the Npgsql Data Source Mock type used by provider mocks.
/// PT: Representa o tipo Npgsql fonte de dados simulado usado pelos mocks do provedor.
/// </summary>
public sealed class NpgsqlDataSourceMock(NpgsqlDbMock? db = null)
#if NET7_0_OR_GREATER
    : DbDataSource
#endif
{
    /// <summary>
    /// EN: Executes connection string.
    /// PT: Executa string de conexão.
    /// </summary>
    public
#if NET7_0_OR_GREATER
    override
#endif
    string ConnectionString => string.Empty;

#if NET7_0_OR_GREATER
    /// <summary>
    /// EN: Creates a new db connection instance.
    /// PT: Cria uma nova instância de db conexão.
    /// </summary>
    protected override DbConnection CreateDbConnection() => new NpgsqlConnectionMock(db);
#else
    /// <summary>
    /// EN: Creates a new db connection instance.
    /// PT: Cria uma nova instância de db conexão.
    /// </summary>
    public NpgsqlConnectionMock CreateDbConnection() => new NpgsqlConnectionMock(db);
#endif

    /// <summary>
    /// EN: Creates a new connection instance.
    /// PT: Cria uma nova instância de conexão.
    /// </summary>
    public
#if NET7_0_OR_GREATER
    new
#endif
    NpgsqlConnectionMock CreateConnection() => new NpgsqlConnectionMock(db);

}
