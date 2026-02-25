namespace DbSqlLikeMem.MySql;

/// <summary>
/// EN: Represents the My Sql Connector Factory Mock type used by provider mocks.
/// PT: Representa o tipo My Sql Connector Factory simulado usado pelos mocks do provedor.
/// </summary>
public sealed class MySqlConnectorFactoryMock : DbProviderFactory
{
    private static MySqlConnectorFactoryMock? Instance;

    /// <summary>
    /// EN: Returns the singleton factory instance for this provider mock.
    /// PT: Retorna a instância única da fábrica deste simulado de provedor.
    /// </summary>
    public static MySqlConnectorFactoryMock GetInstance(MySqlDbMock? db = null)
        => Instance ??= new MySqlConnectorFactoryMock(db);

    private readonly MySqlDbMock? Db;

    /// <summary>
    /// EN: Creates a new command instance.
    /// PT: Cria uma nova instância de comando.
    /// </summary>
    public override DbCommand CreateCommand() => new MySqlCommandMock();

    /// <summary>
    /// EN: Creates a new connection instance.
    /// PT: Cria uma nova instância de conexão.
    /// </summary>
    public override DbConnection CreateConnection() => new MySqlConnectionMock(Db);

    /// <summary>
    /// EN: Creates a new connection string builder instance.
    /// PT: Cria uma nova instância de construtor de string de conexão.
    /// </summary>
    public override DbConnectionStringBuilder CreateConnectionStringBuilder() => new MySqlConnectionStringBuilder();

    /// <summary>
    /// EN: Creates a new parameter instance.
    /// PT: Cria uma nova instância de parâmetro.
    /// </summary>
    public override DbParameter CreateParameter() => new MySqlParameter();

    /// <summary>
    /// EN: Creates a new command builder instance.
    /// PT: Cria uma nova instância de construtor de comandos.
    /// </summary>
    public override DbCommandBuilder CreateCommandBuilder() => new MySqlCommandBuilder();

    /// <summary>
    /// EN: Creates a new data adapter instance.
    /// PT: Cria uma nova instância de adaptador de dados.
    /// </summary>
    public override DbDataAdapter CreateDataAdapter() => new MySqlDataAdapterMock();

    /// <summary>
    /// EN: Gets whether data source enumerator creation is supported.
    /// PT: Obtém se a criação de enumerador de fonte de dados é suportada.
    /// </summary>
    public override bool CanCreateDataSourceEnumerator => false;

#if NETCOREAPP3_0_OR_GREATER || NETSTANDARD2_1_OR_GREATER
    /// <summary>
    /// EN: Gets whether command builder creation is supported.
    /// PT: Obtém se a criação de construtor de comandos é suportada.
    /// </summary>
    public override bool CanCreateCommandBuilder => true;

    /// <summary>
    /// EN: Gets whether data adapter creation is supported.
    /// PT: Obtém se a criação de adaptador de dados é suportada.
    /// </summary>
    public override bool CanCreateDataAdapter => true;
#endif

#pragma warning disable CA1822 // Mark members as static
#if NET6_0_OR_GREATER
    /// <summary>
    /// EN: Creates a new batch instance.
    /// PT: Cria uma nova instância de lote.
    /// </summary>
    public override DbBatch CreateBatch() => new MySqlBatchMock();
#else
    /// <summary>
    /// EN: Creates a new batch instance.
    /// PT: Cria uma nova instância de lote.
    /// </summary>
    public MySqlBatchMock CreateBatch() => new();
#endif

#if NET6_0_OR_GREATER
    /// <summary>
    /// EN: Creates a new batch command instance.
    /// PT: Cria uma nova instância de comando em lote.
    /// </summary>
    public override DbBatchCommand CreateBatchCommand() => new MySqlBatchCommandMock();
#else
    /// <summary>
    /// EN: Creates a new batch command instance.
    /// PT: Cria uma nova instância de comando em lote.
    /// </summary>
    public MySqlBatchCommandMock CreateBatchCommand() => new();
#endif

#if NET6_0_OR_GREATER
    /// <summary>
    /// EN: Gets whether batch creation is supported.
    /// PT: Obtém se a criação de lote é suportada.
    /// </summary>
    public override bool CanCreateBatch => true;
#else
    /// <summary>
    /// EN: Gets whether batch creation is supported.
    /// PT: Obtém se a criação de lote é suportada.
    /// </summary>
    public bool CanCreateBatch => true;
#endif

    /// <summary>
    /// EN: Creates a provider-specific data source mock for the supplied connection string.
    /// PT: Cria um simulado de fonte de dados específico do provedor para a string de conexão informada.
    /// </summary>
    public
#if NET7_0_OR_GREATER
    override
    DbDataSource
#else
    MySqlDataSourceMock
#endif
    CreateDataSource(string connectionString) => new MySqlDataSourceMock(Db);
#pragma warning restore CA1822 // Mark members as static

    internal MySqlConnectorFactoryMock(
        MySqlDbMock? db = null
    )
    {
        Db = db;
    }
}
