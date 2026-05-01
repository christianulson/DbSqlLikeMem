using DbProviderFactory = System.Data.Common.DbProviderFactory;
using DbCommand = System.Data.Common.DbCommand;
using DbConnection = System.Data.Common.DbConnection;
using DbConnectionStringBuilder = System.Data.Common.DbConnectionStringBuilder;
using DbParameter = System.Data.Common.DbParameter;
using DbDataAdapter = System.Data.Common.DbDataAdapter;
#if NET8_0_OR_GREATER
using DbBatch = System.Data.Common.DbBatch;
using DbBatchCommand = System.Data.Common.DbBatchCommand;
#endif

namespace DbSqlLikeMem.Firebird;

/// <summary>
/// EN: Represents the Firebird connector factory mock type used by provider mocks.
/// PT-br: Representa o tipo de fabrica de conectores Firebird simulado usado pelos mocks do provedor.
/// </summary>
public sealed class FirebirdConnectorFactoryMock : DbProviderFactory
{
    private readonly FirebirdDbMock? db;

    /// <summary>
    /// EN: Creates a Firebird provider factory mock instance.
    /// PT-br: Cria uma instancia de fabrica simulada do provedor Firebird.
    /// </summary>
    public static FirebirdConnectorFactoryMock GetInstance(FirebirdDbMock? db = null)
        => new(db);

    internal FirebirdConnectorFactoryMock(FirebirdDbMock? db = null)
    {
        this.db = db;
    }

    /// <summary>
    /// EN: Creates a new command instance.
    /// PT-br: Cria uma nova instancia de comando.
    /// </summary>
    public override DbCommand CreateCommand() => new FirebirdCommandMock();

    /// <summary>
    /// EN: Creates a new connection instance.
    /// PT-br: Cria uma nova instancia de conexao.
    /// </summary>
    public override DbConnection CreateConnection() => new FirebirdConnectionMock(db);

    /// <summary>
    /// EN: Creates a new connection string builder instance.
    /// PT-br: Cria uma nova instancia de construtor de string de conexao.
    /// </summary>
    public override DbConnectionStringBuilder CreateConnectionStringBuilder() => new FbConnectionStringBuilder();

    /// <summary>
    /// EN: Creates a new parameter instance.
    /// PT-br: Cria uma nova instancia de parametro.
    /// </summary>
    public override DbParameter CreateParameter() => new FbParameter();

#if NETCOREAPP3_0_OR_GREATER
    /// <summary>
    /// EN: Gets whether data adapter creation is supported.
    /// PT-br: Obtém se a criação de adaptador de dados é suportada.
    /// </summary>
    public override bool CanCreateDataAdapter => true;
#endif

    /// <summary>
    /// EN: Creates a new data adapter instance.
    /// PT-br: Cria uma nova instancia de adaptador de dados.
    /// </summary>
    public override DbDataAdapter CreateDataAdapter() => new FirebirdDataAdapterMock();

    /// <summary>
    /// EN: Gets whether data source enumerator creation is supported.
    /// PT-br: Obtém se a criação de enumerador de fonte de dados é suportada.
    /// </summary>
    public override bool CanCreateDataSourceEnumerator => false;

#if NET8_0_OR_GREATER
    /// <summary>
    /// EN: Gets whether batch creation is supported.
    /// PT-br: Obtém se a criação de lote é suportada.
    /// </summary>
    public override bool CanCreateBatch => true;

    /// <summary>
    /// EN: Creates a new batch instance.
    /// PT-br: Cria uma nova instância de lote.
    /// </summary>
    public override DbBatch CreateBatch() => new FirebirdBatchMock();

    /// <summary>
    /// EN: Creates a new batch command instance.
    /// PT-br: Cria uma nova instância de comando em lote.
    /// </summary>
    public override DbBatchCommand CreateBatchCommand() => new FirebirdBatchCommandMock();
#endif

    /// <summary>
    /// EN: Creates a new data source instance.
    /// PT-br: Cria uma nova instancia de fonte de dados.
    /// </summary>
#if NET7_0_OR_GREATER
    public override DbDataSource CreateDataSource(string connectionString) => new FirebirdDataSourceMock(db);
#else
    public FirebirdDataSourceMock CreateDataSource(string connectionString) => new(db);
#endif
}



