using DbProviderFactory = System.Data.Common.DbProviderFactory;
using DbCommand = System.Data.Common.DbCommand;
using DbConnection = System.Data.Common.DbConnection;
using DbConnectionStringBuilder = System.Data.Common.DbConnectionStringBuilder;
using DbParameter = System.Data.Common.DbParameter;
using DbDataAdapter = System.Data.Common.DbDataAdapter;
#if NET6_0_OR_GREATER
using DbBatch = System.Data.Common.DbBatch;
using DbBatchCommand = System.Data.Common.DbBatchCommand;
#endif
using Microsoft.Data.SqlClient;
namespace DbSqlLikeMem.SqlServer;

/// <summary>
/// EN: Represents the Sql Server Connector Factory Mock type used by provider mocks.
/// PT: Representa o tipo Sql Server Connector Factory simulado usado pelos mocks do provedor.
/// </summary>
public sealed class SqlServerConnectorFactoryMock : DbProviderFactory
{
    private static SqlServerConnectorFactoryMock? instance;
    private readonly SqlServerDbMock? db;

    /// <summary>
    /// EN: Returns the singleton factory instance for this provider mock.
    /// PT: Retorna a instância única da fábrica deste simulado de provedor.
    /// </summary>
    public static SqlServerConnectorFactoryMock GetInstance(SqlServerDbMock? db = null)
        => instance ??= new SqlServerConnectorFactoryMock(db);

    internal SqlServerConnectorFactoryMock(SqlServerDbMock? db = null)
    {
        this.db = db;
    }

    /// <summary>
    /// EN: Creates a new command instance.
    /// PT: Cria uma nova instância de comando.
    /// </summary>
    public override DbCommand CreateCommand() => new SqlServerCommandMock();

    /// <summary>
    /// EN: Creates a new connection instance.
    /// PT: Cria uma nova instância de conexão.
    /// </summary>
    public override DbConnection CreateConnection() => new SqlServerConnectionMock(db);

    /// <summary>
    /// EN: Creates a new connection string builder instance.
    /// PT: Cria uma nova instância de construtor de string de conexão.
    /// </summary>
    public override DbConnectionStringBuilder CreateConnectionStringBuilder() => new DbConnectionStringBuilder();

    /// <summary>
    /// EN: Creates a new parameter instance.
    /// PT: Cria uma nova instância de parâmetro.
    /// </summary>
    public override DbParameter CreateParameter() => new SqlParameter();

#if NETCOREAPP3_0_OR_GREATER || NETSTANDARD2_1_OR_GREATER
    /// <summary>
    /// EN: Gets whether data adapter creation is supported.
    /// PT: Obtém se a criação de adaptador de dados é suportada.
    /// </summary>
    public override bool CanCreateDataAdapter => true;
#endif

    /// <summary>
    /// EN: Creates a new data adapter instance.
    /// PT: Cria uma nova instância de adaptador de dados.
    /// </summary>
    public override DbDataAdapter CreateDataAdapter() => new SqlServerDataAdapterMock();

    /// <summary>
    /// EN: Gets whether data source enumerator creation is supported.
    /// PT: Obtém se a criação de enumerador de fonte de dados é suportada.
    /// </summary>
    public override bool CanCreateDataSourceEnumerator => false;

#if NET6_0_OR_GREATER
    /// <summary>
    /// EN: Gets whether batch creation is supported.
    /// PT: Obtém se a criação de lote é suportada.
    /// </summary>
    public override bool CanCreateBatch => true;

    /// <summary>
    /// EN: Creates a new batch instance.
    /// PT: Cria uma nova instância de lote.
    /// </summary>
    public override DbBatch CreateBatch() => new SqlServerBatchMock();

    /// <summary>
    /// EN: Creates a new batch command instance.
    /// PT: Cria uma nova instância de comando em lote.
    /// </summary>
    public override DbBatchCommand CreateBatchCommand() => new SqlServerBatchCommandMock();
#endif

    /// <summary>
    /// EN: Creates a new data source instance.
    /// PT: Cria uma nova instância de fonte de dados.
    /// </summary>
#if NET7_0_OR_GREATER
    public override DbDataSource CreateDataSource(string connectionString) => new SqlServerDataSourceMock(db);
#else
    public SqlServerDataSourceMock CreateDataSource(string connectionString) => new(db);
#endif
}
