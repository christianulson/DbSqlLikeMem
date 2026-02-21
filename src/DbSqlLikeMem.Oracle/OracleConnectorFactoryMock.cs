using Oracle.ManagedDataAccess.Client;

namespace DbSqlLikeMem.Oracle;

/// <summary>
/// EN: Summary for OracleConnectorFactoryMock.
/// PT: Resumo para OracleConnectorFactoryMock.
/// </summary>
public sealed class OracleConnectorFactoryMock : DbProviderFactory
{
    private static OracleConnectorFactoryMock? instance;
    private readonly OracleDbMock? db;

    /// <summary>
    /// EN: Summary for GetInstance.
    /// PT: Resumo para GetInstance.
    /// </summary>
    public static OracleConnectorFactoryMock GetInstance(OracleDbMock? db = null)
        => instance ??= new OracleConnectorFactoryMock(db);

    internal OracleConnectorFactoryMock(OracleDbMock? db = null)
    {
        this.db = db;
    }

    /// <summary>
    /// EN: Summary for CreateCommand.
    /// PT: Resumo para CreateCommand.
    /// </summary>
    public override DbCommand CreateCommand() => new OracleCommandMock();

    /// <summary>
    /// EN: Summary for CreateConnection.
    /// PT: Resumo para CreateConnection.
    /// </summary>
    public override DbConnection CreateConnection() => new OracleConnectionMock(db);

    /// <summary>
    /// EN: Summary for CreateConnectionStringBuilder.
    /// PT: Resumo para CreateConnectionStringBuilder.
    /// </summary>
    public override DbConnectionStringBuilder CreateConnectionStringBuilder() => new DbConnectionStringBuilder();

    /// <summary>
    /// EN: Summary for CreateParameter.
    /// PT: Resumo para CreateParameter.
    /// </summary>
    public override DbParameter CreateParameter() => new OracleParameter();

#if NETCOREAPP3_0_OR_GREATER || NETSTANDARD2_1_OR_GREATER
    /// <summary>
    /// EN: Summary for member.
    /// PT: Resumo para member.
    /// </summary>
    public override bool CanCreateDataAdapter => true;
#endif

    /// <summary>
    /// EN: Summary for CreateDataAdapter.
    /// PT: Resumo para CreateDataAdapter.
    /// </summary>
    public override DbDataAdapter CreateDataAdapter() => new OracleDataAdapterMock();

    /// <summary>
    /// EN: Summary for member.
    /// PT: Resumo para member.
    /// </summary>
    public override bool CanCreateDataSourceEnumerator => false;

#if NET6_0_OR_GREATER
    /// <summary>
    /// EN: Summary for member.
    /// PT: Resumo para member.
    /// </summary>
    public override bool CanCreateBatch => true;

    /// <summary>
    /// EN: Summary for CreateBatch.
    /// PT: Resumo para CreateBatch.
    /// </summary>
    public override DbBatch CreateBatch() => new OracleBatchMock();

    /// <summary>
    /// EN: Summary for CreateBatchCommand.
    /// PT: Resumo para CreateBatchCommand.
    /// </summary>
    public override DbBatchCommand CreateBatchCommand() => new OracleBatchCommandMock();
#endif

    /// <summary>
    /// EN: Summary for member.
    /// PT: Resumo para member.
    /// </summary>
#if NET7_0_OR_GREATER
    public override DbDataSource CreateDataSource(string connectionString) => new OracleDataSourceMock(db);
#else
    public OracleDataSourceMock CreateDataSource(string connectionString) => new(db);
#endif
}
