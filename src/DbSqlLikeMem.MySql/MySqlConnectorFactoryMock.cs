namespace DbSqlLikeMem.MySql;

/// <summary>
/// EN: Summary for MySqlConnectorFactoryMock.
/// PT: Resumo para MySqlConnectorFactoryMock.
/// </summary>
public sealed class MySqlConnectorFactoryMock : DbProviderFactory
{
    private static MySqlConnectorFactoryMock? Instance;

    /// <summary>
    /// EN: Summary for GetInstance.
    /// PT: Resumo para GetInstance.
    /// </summary>
    public static MySqlConnectorFactoryMock GetInstance(MySqlDbMock? db = null)
        => Instance ??= new MySqlConnectorFactoryMock(db);

    private readonly MySqlDbMock? Db;

    /// <summary>
    /// EN: Summary for CreateCommand.
    /// PT: Resumo para CreateCommand.
    /// </summary>
    public override DbCommand CreateCommand() => new MySqlCommandMock();

    /// <summary>
    /// EN: Summary for CreateConnection.
    /// PT: Resumo para CreateConnection.
    /// </summary>
    public override DbConnection CreateConnection() => new MySqlConnectionMock(Db);

    /// <summary>
    /// EN: Summary for CreateConnectionStringBuilder.
    /// PT: Resumo para CreateConnectionStringBuilder.
    /// </summary>
    public override DbConnectionStringBuilder CreateConnectionStringBuilder() => new MySqlConnectionStringBuilder();

    /// <summary>
    /// EN: Summary for CreateParameter.
    /// PT: Resumo para CreateParameter.
    /// </summary>
    public override DbParameter CreateParameter() => new MySqlParameter();

    /// <summary>
    /// EN: Summary for CreateCommandBuilder.
    /// PT: Resumo para CreateCommandBuilder.
    /// </summary>
    public override DbCommandBuilder CreateCommandBuilder() => new MySqlCommandBuilder();

    /// <summary>
    /// EN: Summary for CreateDataAdapter.
    /// PT: Resumo para CreateDataAdapter.
    /// </summary>
    public override DbDataAdapter CreateDataAdapter() => new MySqlDataAdapterMock();

    /// <summary>
    /// EN: Summary for member.
    /// PT: Resumo para member.
    /// </summary>
    public override bool CanCreateDataSourceEnumerator => false;

#if NETCOREAPP3_0_OR_GREATER || NETSTANDARD2_1_OR_GREATER
    /// <summary>
    /// EN: Summary for member.
    /// PT: Resumo para member.
    /// </summary>
    public override bool CanCreateCommandBuilder => true;

    /// <summary>
    /// EN: Summary for member.
    /// PT: Resumo para member.
    /// </summary>
    public override bool CanCreateDataAdapter => true;
#endif

#pragma warning disable CA1822 // Mark members as static
#if NET6_0_OR_GREATER
    /// <summary>
    /// EN: Summary for CreateBatch.
    /// PT: Resumo para CreateBatch.
    /// </summary>
    public override DbBatch CreateBatch() => new MySqlBatchMock();
#else
    /// <summary>
    /// EN: Summary for CreateBatch.
    /// PT: Resumo para CreateBatch.
    /// </summary>
    public MySqlBatchMock CreateBatch() => new();
#endif

#if NET6_0_OR_GREATER
    /// <summary>
    /// EN: Summary for CreateBatchCommand.
    /// PT: Resumo para CreateBatchCommand.
    /// </summary>
    public override DbBatchCommand CreateBatchCommand() => new MySqlBatchCommandMock();
#else
    /// <summary>
    /// EN: Summary for CreateBatchCommand.
    /// PT: Resumo para CreateBatchCommand.
    /// </summary>
    public MySqlBatchCommandMock CreateBatchCommand() => new();
#endif

#if NET6_0_OR_GREATER
    /// <summary>
    /// EN: Summary for member.
    /// PT: Resumo para member.
    /// </summary>
    public override bool CanCreateBatch => true;
#else
    /// <summary>
    /// EN: Summary for member.
    /// PT: Resumo para member.
    /// </summary>
    public bool CanCreateBatch => true;
#endif

    /// <summary>
    /// EN: Summary for member.
    /// PT: Resumo para member.
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
