using System.Data.Common;

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
/// EN: Summary for SqlServerConnectorFactoryMock.
/// PT: Resumo para SqlServerConnectorFactoryMock.
/// </summary>
public sealed class SqlServerConnectorFactoryMock : DbProviderFactory
{
    private static SqlServerConnectorFactoryMock? instance;
    private readonly SqlServerDbMock? db;

    /// <summary>
    /// EN: Summary for GetInstance.
    /// PT: Resumo para GetInstance.
    /// </summary>
    public static SqlServerConnectorFactoryMock GetInstance(SqlServerDbMock? db = null)
        => instance ??= new SqlServerConnectorFactoryMock(db);

    internal SqlServerConnectorFactoryMock(SqlServerDbMock? db = null)
    {
        this.db = db;
    }

    /// <summary>
    /// EN: Summary for CreateCommand.
    /// PT: Resumo para CreateCommand.
    /// </summary>
    public override DbCommand CreateCommand() => new SqlServerCommandMock();

    /// <summary>
    /// EN: Summary for CreateConnection.
    /// PT: Resumo para CreateConnection.
    /// </summary>
    public override DbConnection CreateConnection() => new SqlServerConnectionMock(db);

    /// <summary>
    /// EN: Summary for CreateConnectionStringBuilder.
    /// PT: Resumo para CreateConnectionStringBuilder.
    /// </summary>
    public override DbConnectionStringBuilder CreateConnectionStringBuilder() => new DbConnectionStringBuilder();

    /// <summary>
    /// EN: Summary for CreateParameter.
    /// PT: Resumo para CreateParameter.
    /// </summary>
    public override DbParameter CreateParameter() => new SqlParameter();

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
    public override DbDataAdapter CreateDataAdapter() => new SqlServerDataAdapterMock();

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
    public override DbBatch CreateBatch() => new SqlServerBatchMock();

    /// <summary>
    /// EN: Summary for CreateBatchCommand.
    /// PT: Resumo para CreateBatchCommand.
    /// </summary>
    public override DbBatchCommand CreateBatchCommand() => new SqlServerBatchCommandMock();
#endif

    /// <summary>
    /// EN: Summary for member.
    /// PT: Resumo para member.
    /// </summary>
    public SqlServerDataSourceMock CreateDataSource(string connectionString) => new(db);
}
