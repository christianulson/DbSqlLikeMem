using Microsoft.Data.SqlClient;
namespace DbSqlLikeMem.SqlAzure;

/// <summary>
/// EN: Represents the Sql Azure Connector Factory Mock type used by provider mocks.
/// PT: Representa o tipo Sql Azure Connector Factory simulado usado pelos mocks do provedor.
/// </summary>
public sealed class SqlAzureConnectorFactoryMock
    : DbProviderFactory
{
    private static SqlAzureConnectorFactoryMock? instance;
    private readonly SqlAzureDbMock? db;

    /// <summary>
    /// EN: Gets the singleton SQL Azure provider factory mock instance.
    /// PT: Obtem a instancia singleton da fabrica simulada do provedor SQL Azure.
    /// </summary>
    public static SqlAzureConnectorFactoryMock GetInstance(SqlAzureDbMock? db = null)
        => instance ??= new SqlAzureConnectorFactoryMock(db);

    internal SqlAzureConnectorFactoryMock(SqlAzureDbMock? db = null)
    {
        this.db = db;
    }

    /// <summary>
    /// EN: Creates a SQL Azure command mock instance.
    /// PT: Cria uma instancia de comando simulado do SQL Azure.
    /// </summary>
    public override DbCommand CreateCommand() => new SqlAzureCommandMock();

    /// <summary>
    /// EN: Creates a SQL Azure connection mock instance.
    /// PT: Cria uma instancia de conexao simulada do SQL Azure.
    /// </summary>
    public override DbConnection CreateConnection() => new SqlAzureConnectionMock(db);

    /// <summary>
    /// EN: Creates a generic connection string builder for provider scenarios.
    /// PT: Cria um construtor generico de string de conexao para cenarios de provedor.
    /// </summary>
    public override DbConnectionStringBuilder CreateConnectionStringBuilder() => [];

    /// <summary>
    /// EN: Creates a provider parameter instance compatible with SQL Azure mocks.
    /// PT: Cria uma instancia de parametro de provedor compativel com mocks de SQL Azure.
    /// </summary>
    public override DbParameter CreateParameter() => new SqlParameter();

#if NETCOREAPP3_0_OR_GREATER
    /// <summary>
    /// EN: Indicates whether this provider factory can create data adapter instances.
    /// PT: Indica se esta fabrica de provedor pode criar instancias de adaptador de dados.
    /// </summary>
    public override bool CanCreateDataAdapter => true;
#endif

    /// <summary>
    /// EN: Creates a SQL Azure data adapter mock instance.
    /// PT: Cria uma instancia de adaptador de dados simulado do SQL Azure.
    /// </summary>
    public override DbDataAdapter CreateDataAdapter() => new SqlAzureDataAdapterMock();

    /// <summary>
    /// EN: Indicates whether this provider factory can create a data source enumerator.
    /// PT: Indica se esta fabrica de provedor pode criar um enumerador de fontes de dados.
    /// </summary>
    public override bool CanCreateDataSourceEnumerator => false;

#if NET6_0_OR_GREATER
    /// <summary>
    /// EN: Indicates whether this provider factory can create batch objects.
    /// PT: Indica se esta fabrica de provedor pode criar objetos de lote.
    /// </summary>
    public override bool CanCreateBatch => true;

    /// <summary>
    /// EN: Creates a SQL Azure batch mock instance.
    /// PT: Cria uma instancia de lote simulado do SQL Azure.
    /// </summary>
    public override DbBatch CreateBatch() => new SqlAzureBatchMock();

    /// <summary>
    /// EN: Creates a SQL Azure batch command mock instance.
    /// PT: Cria uma instancia de comando de lote simulado do SQL Azure.
    /// </summary>
    public override DbBatchCommand CreateBatchCommand() => new SqlAzureBatchCommandMock();
#endif

#if NET7_0_OR_GREATER
    /// <summary>
    /// EN: Creates a SQL Azure data source mock for the provided connection string.
    /// PT: Cria uma fonte de dados simulada do SQL Azure para a string de conexao informada.
    /// </summary>
    public override DbDataSource CreateDataSource(string connectionString) => new SqlAzureDataSourceMock(db);
#else
    /// <summary>
    /// EN: Creates a SQL Azure data source mock for the provided connection string.
    /// PT: Cria uma fonte de dados simulada do SQL Azure para a string de conexao informada.
    /// </summary>
    public SqlAzureDataSourceMock CreateDataSource(string connectionString) => new(db);
#endif
}