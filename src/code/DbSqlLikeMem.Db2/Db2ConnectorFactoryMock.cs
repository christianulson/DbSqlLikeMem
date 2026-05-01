#if NET462
using DB2Parameter = IBM.Data.DB2.iSeries.iDB2Parameter;
#endif

namespace DbSqlLikeMem.Db2;

/// <summary>
/// EN: Represents the Db2 Connector Factory Mock type used by provider mocks.
/// PT-br: Representa o tipo Db2 Connector Factory simulado usado pelos mocks do provedor.
/// </summary>
public sealed class Db2ConnectorFactoryMock : DbProviderFactory
{
    private readonly Db2DbMock? db;

    /// <summary>
    /// EN: Creates a Db2 provider factory mock instance.
    /// PT-br: Cria uma instancia de fabrica simulada do provedor Db2.
    /// </summary>
    public static Db2ConnectorFactoryMock GetInstance(Db2DbMock? db = null)
        => new(db);

    internal Db2ConnectorFactoryMock(Db2DbMock? db = null)
    {
        this.db = db;
    }

    /// <summary>
    /// EN: Creates a new command instance.
    /// PT-br: Cria uma nova instância de comando.
    /// </summary>
    public override DbCommand CreateCommand() => new Db2CommandMock();

    /// <summary>
    /// EN: Creates a new connection instance.
    /// PT-br: Cria uma nova instância de conexão.
    /// </summary>
    public override DbConnection CreateConnection() => new Db2ConnectionMock(db);

    /// <summary>
    /// EN: Creates a new connection string builder instance.
    /// PT-br: Cria uma nova instância de construtor de string de conexão.
    /// </summary>
    public override DbConnectionStringBuilder CreateConnectionStringBuilder() => [];

    /// <summary>
    /// EN: Creates a new parameter instance.
    /// PT-br: Cria uma nova instância de parâmetro.
    /// </summary>
    public override DbParameter CreateParameter() => new DB2Parameter();

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
    public override DbDataAdapter CreateDataAdapter() => new Db2DataAdapterMock();

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
    public override DbBatch CreateBatch() => new Db2BatchMock();

    /// <summary>
    /// EN: Creates a new batch command instance.
    /// PT-br: Cria uma nova instância de comando em lote.
    /// </summary>
    public override DbBatchCommand CreateBatchCommand() => new Db2BatchCommandMock();
#endif

    /// <summary>
    /// EN: Creates a provider-specific data source mock for the supplied connection string.
    /// PT-br: Cria um simulado de fonte de dados específico do provedor para a string de conexão informada.
    /// </summary>
    public
#if NET7_0_OR_GREATER
    override
    DbDataSource
#else
    Db2DataSourceMock
#endif
    CreateDataSource(string connectionString) => new Db2DataSourceMock(db);
}

