using DbProviderFactory = System.Data.Common.DbProviderFactory;
using DbCommand = System.Data.Common.DbCommand;
using DbConnection = System.Data.Common.DbConnection;
using DbConnectionStringBuilder = System.Data.Common.DbConnectionStringBuilder;
using DbParameter = System.Data.Common.DbParameter;
using DbDataAdapter = System.Data.Common.DbDataAdapter;
using DbBatch = System.Data.Common.DbBatch;
using DbBatchCommand = System.Data.Common.DbBatchCommand;
using Npgsql;
namespace DbSqlLikeMem.Npgsql;

/// <summary>
/// EN: Represents the Npgsql Connector Factory Mock type used by provider mocks.
/// PT-br: Representa o tipo Npgsql Connector Factory simulado usado pelos mocks do provedor.
/// </summary>
public sealed class NpgsqlConnectorFactoryMock : DbProviderFactory
{
    private readonly NpgsqlDbMock? db;

    /// <summary>
    /// EN: Creates an Npgsql provider factory mock instance.
    /// PT-br: Cria uma instancia de fabrica simulada do provedor Npgsql.
    /// </summary>
    public static NpgsqlConnectorFactoryMock GetInstance(NpgsqlDbMock? db = null)
        => new(db);

    internal NpgsqlConnectorFactoryMock(NpgsqlDbMock? db = null)
    {
        this.db = db;
    }

    /// <summary>
    /// EN: Creates a new command instance.
    /// PT-br: Cria uma nova instância de comando.
    /// </summary>
    public override DbCommand CreateCommand() => new NpgsqlCommandMock();

    /// <summary>
    /// EN: Creates a new connection instance.
    /// PT-br: Cria uma nova instância de conexão.
    /// </summary>
    public override DbConnection CreateConnection() => new NpgsqlConnectionMock(db);

    /// <summary>
    /// EN: Creates a new connection string builder instance.
    /// PT-br: Cria uma nova instância de construtor de string de conexão.
    /// </summary>
    public override DbConnectionStringBuilder CreateConnectionStringBuilder() => [];

    /// <summary>
    /// EN: Creates a new parameter instance.
    /// PT-br: Cria uma nova instância de parâmetro.
    /// </summary>
    public override DbParameter CreateParameter() => new NpgsqlParameter();

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
    public override DbDataAdapter CreateDataAdapter() => new NpgsqlDataAdapterMock();

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
    public override DbBatch CreateBatch() => new NpgsqlBatchMock();

    /// <summary>
    /// EN: Creates a new batch command instance.
    /// PT-br: Cria uma nova instância de comando em lote.
    /// </summary>
    public override DbBatchCommand CreateBatchCommand() => new NpgsqlBatchCommandMock();
#endif

    /// <summary>
    /// EN: Creates a new data source instance.
    /// PT-br: Cria uma nova instância de fonte de dados.
    /// </summary>
#if NET7_0_OR_GREATER
    public override DbDataSource CreateDataSource(string connectionString) => new NpgsqlDataSourceMock(db);
#else
    public NpgsqlDataSourceMock CreateDataSource(string connectionString) => new(db);
#endif
}

