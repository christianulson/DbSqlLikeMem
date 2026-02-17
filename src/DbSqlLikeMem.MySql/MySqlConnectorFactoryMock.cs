namespace DbSqlLikeMem.MySql;

/// <summary>
/// Factory that creates MySQL mock ADO.NET provider objects.
/// Fábrica que cria objetos do provedor ADO.NET mock de MySQL.
/// </summary>
public sealed class MySqlConnectorFactoryMock : DbProviderFactory
{
    /// <summary>
    /// Provides an instance of <see cref="DbProviderFactory"/> that can create MySqlConnector objects.
    /// </summary>
    private static MySqlConnectorFactoryMock? Instance;

    /// <summary>
    /// Gets a singleton factory instance bound to the optional mock database.
    /// Obtém uma instância singleton da fábrica vinculada ao banco mock opcional.
    /// </summary>
    /// <param name="db">Optional mock database used by created objects.
    /// Banco mock opcional usado pelos objetos criados.</param>
    public static MySqlConnectorFactoryMock GetInstance(MySqlDbMock? db = null)
        => Instance ??= new MySqlConnectorFactoryMock(db);

    private readonly MySqlDbMock? Db;

    /// <summary>
    /// Creates a new <see cref="MySqlCommand"/> object.
    /// </summary>
    public override DbCommand CreateCommand() => new MySqlCommandMock();

    /// <summary>
    /// Creates a new <see cref="MySqlConnection"/> object.
    /// </summary>
    public override DbConnection CreateConnection() => new MySqlConnectionMock();

    /// <summary>
    /// Creates a new <see cref="MySqlConnectionStringBuilder"/> object.
    /// </summary>
    public override DbConnectionStringBuilder CreateConnectionStringBuilder() => new MySqlConnectionStringBuilder();

    /// <summary>
    /// Creates a new <see cref="MySqlParameter"/> object.
    /// </summary>
    public override DbParameter CreateParameter() => new MySqlParameter();

    /// <summary>
    /// Creates a new <see cref="MySqlCommandBuilder"/> object.
    /// </summary>
    public override DbCommandBuilder CreateCommandBuilder() => new MySqlCommandBuilder();

    /// <summary>
    /// Creates a new <see cref="MySqlDataAdapter"/> object.
    /// </summary>
    public override DbDataAdapter CreateDataAdapter() => new MySqlDataAdapter();

    /// <summary>
    /// Returns <c>false</c>.
    /// </summary>
    /// <remarks><see cref="DbDataSourceEnumerator"/> is not supported by MySqlConnector.</remarks>
    public override bool CanCreateDataSourceEnumerator => false;

#if NETCOREAPP3_0_OR_GREATER || NETSTANDARD2_1_OR_GREATER
    /// <summary>
    /// Returns <c>true</c>.
    /// </summary>
    public override bool CanCreateCommandBuilder => true;

    /// <summary>
    /// Returns <c>true</c>.
    /// </summary>
    public override bool CanCreateDataAdapter => true;
#endif

#pragma warning disable CA1822 // Mark members as static
#if NET6_0_OR_GREATER
    /// <summary>
    /// Creates a new <see cref="MySqlBatchMock"/> object.
    /// Cria um novo objeto <see cref="MySqlBatchMock"/>.
    /// </summary>
    public override DbBatch CreateBatch() => new MySqlBatchMock();
#else
    /// <summary>
    /// Creates a new <see cref="MySqlBatchMock"/> object.
    /// Cria um novo objeto <see cref="MySqlBatchMock"/>.
    /// </summary>
    public MySqlBatchMock CreateBatch() => new();
#endif

#if NET6_0_OR_GREATER
    /// <summary>
    /// Creates a new <see cref="MySqlBatchCommandMock"/> object.
    /// Cria um novo objeto <see cref="MySqlBatchCommandMock"/>.
    /// </summary>
    public override DbBatchCommand CreateBatchCommand() => new MySqlBatchCommandMock();
#else
    /// <summary>
    /// Creates a new <see cref="MySqlBatchCommandMock"/> object.
    /// Cria um novo objeto <see cref="MySqlBatchCommandMock"/>.
    /// </summary>
    public MySqlBatchCommandMock CreateBatchCommand() => new();
#endif

#if NET6_0_OR_GREATER
    /// <summary>
    /// Returns <c>true</c>.
    /// Retorna <c>true</c>.
    /// </summary>
    public override bool CanCreateBatch => true;
#else
    /// <summary>
    /// Returns <c>true</c>.
    /// Retorna <c>true</c>.
    /// </summary>
    public bool CanCreateBatch => true;
#endif

    /// <summary>
    /// Creates a new <see cref="MySqlDataSourceMock"/> object.
    /// </summary>
    /// <param name="connectionString">The connection string.</param>
    public
#if NET7_0_OR_GREATER
    override
#endif
    MySqlDataSourceMock CreateDataSource(string connectionString) => new(Db);
#pragma warning restore CA1822 // Mark members as static

    internal MySqlConnectorFactoryMock(
        MySqlDbMock? db = null
    )
    {
        Db = db;
    }
}
