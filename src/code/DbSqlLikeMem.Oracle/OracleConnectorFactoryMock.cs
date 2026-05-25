using Oracle.ManagedDataAccess.Client;

namespace DbSqlLikeMem.Oracle;

/// <summary>
/// EN: Represents the Oracle Connector Factory Mock type used by provider mocks.
/// PT-br: Representa o tipo Oracle Connector Factory simulado usado pelos mocks do provedor.
/// </summary>
public sealed class OracleConnectorFactoryMock : DbProviderFactory
{
    private readonly OracleDbMock? db;

    /// <summary>
    /// EN: Creates an Oracle provider factory mock instance.
    /// PT-br: Cria uma instancia de fabrica simulada do provedor Oracle.
    /// </summary>
    public static OracleConnectorFactoryMock GetInstance(OracleDbMock? db = null)
        => new(db);

    internal OracleConnectorFactoryMock(OracleDbMock? db = null)
    {
        this.db = db;
    }

    /// <summary>
    /// EN: Creates a new command instance.
    /// PT-br: Cria uma nova instância de comando.
    /// </summary>
    public override DbCommand CreateCommand() => new OracleCommandMock();

    /// <summary>
    /// EN: Creates a new connection instance.
    /// PT-br: Cria uma nova instância de conexão.
    /// </summary>
    public override DbConnection CreateConnection() => new OracleConnectionMock(db);

    /// <summary>
    /// EN: Creates a new connection string builder instance.
    /// PT-br: Cria uma nova instância de construtor de string de conexão.
    /// </summary>
    public override DbConnectionStringBuilder CreateConnectionStringBuilder() => [];

    /// <summary>
    /// EN: Creates a new parameter instance.
    /// PT-br: Cria uma nova instância de parâmetro.
    /// </summary>
    public override DbParameter CreateParameter() => new OracleParameter();

#if NETCOREAPP3_0_OR_GREATER
    /// <summary>
    /// EN: Gets whether data adapter creation is supported.
    /// PT-br: Obtém se a criação de adaptador de dados é suportada.
    /// </summary>
    public override bool CanCreateDataAdapter => true;
#endif

    /// <summary>
    /// EN: Creates a new data adapter instance.
    /// PT-br: Cria uma nova instância de adaptador de dados.
    /// </summary>
    public override DbDataAdapter CreateDataAdapter() => new OracleDataAdapterMock();

    /// <summary>
    /// EN: Gets whether data source enumerator creation is supported.
    /// PT-br: Obtém se a criação de enumerador de fonte de dados é suportada.
    /// </summary>
    public override bool CanCreateDataSourceEnumerator => false;

#if NET6_0_OR_GREATER
    /// <summary>
    /// EN: Gets whether batch creation is supported.
    /// PT-br: Obtém se a criação de lote é suportada.
    /// </summary>
    public override bool CanCreateBatch => true;

    /// <summary>
    /// EN: Creates a new batch instance.
    /// PT-br: Cria uma nova instância de lote.
    /// </summary>
    public override DbBatch CreateBatch() => new OracleBatchMock();

    /// <summary>
    /// EN: Creates a new batch command instance.
    /// PT-br: Cria uma nova instância de comando em lote.
    /// </summary>
    public override DbBatchCommand CreateBatchCommand() => new OracleBatchCommandMock();
#endif

    /// <summary>
    /// EN: Creates a new data source instance.
    /// PT-br: Cria uma nova instância de fonte de dados.
    /// </summary>
#if NET7_0_OR_GREATER
    public override DbDataSource CreateDataSource(string connectionString) => new OracleDataSourceMock(db);
#else
    public OracleDataSourceMock CreateDataSource(string connectionString) => new(db);
#endif
}

