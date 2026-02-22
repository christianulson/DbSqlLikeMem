namespace DbSqlLikeMem.Db2;

/// <summary>
/// EN: Represents the Db2 Data Source Mock type used by provider mocks.
/// PT: Representa o tipo Db2 fonte de dados simulado usado pelos mocks do provedor.
/// </summary>
public sealed class Db2DataSourceMock(Db2DbMock? db = null)
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
    protected override DbConnection CreateDbConnection() => new Db2ConnectionMock(db);
#else
    /// <summary>
    /// EN: Creates a new db connection instance.
    /// PT: Cria uma nova instância de db conexão.
    /// </summary>
    public DbConnection CreateDbConnection() => new Db2ConnectionMock(db);
#endif
}
