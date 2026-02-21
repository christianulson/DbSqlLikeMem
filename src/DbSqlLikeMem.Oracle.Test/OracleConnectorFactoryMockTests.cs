namespace DbSqlLikeMem.Oracle.Test;

/// <summary>
/// EN: Summary for OracleConnectorFactoryMockTests.
/// PT: Resumo para OracleConnectorFactoryMockTests.
/// </summary>
public sealed class OracleConnectorFactoryMockTests
{
    /// <summary>
    /// EN: Summary for CreateCoreMembers_ShouldReturnProviderMocks.
    /// PT: Resumo para CreateCoreMembers_ShouldReturnProviderMocks.
    /// </summary>
    [Fact]
    public void CreateCoreMembers_ShouldReturnProviderMocks()
    {
        var factory = OracleConnectorFactoryMock.GetInstance(new OracleDbMock());

        Assert.IsType<OracleCommandMock>(factory.CreateCommand());
        Assert.IsType<OracleConnectionMock>(factory.CreateConnection());
        Assert.IsType<OracleDataAdapterMock>(factory.CreateDataAdapter());
        Assert.IsType<System.Data.Common.DbConnectionStringBuilder>(factory.CreateConnectionStringBuilder());
        Assert.NotNull(factory.CreateParameter());
    }

#if NET6_0_OR_GREATER
    /// <summary>
    /// EN: Summary for CreateBatchMembers_ShouldReturnProviderMocks.
    /// PT: Resumo para CreateBatchMembers_ShouldReturnProviderMocks.
    /// </summary>
    [Fact]
    public void CreateBatchMembers_ShouldReturnProviderMocks()
    {
        var factory = OracleConnectorFactoryMock.GetInstance(new OracleDbMock());

        Assert.True(factory.CanCreateBatch);
        Assert.IsType<OracleBatchMock>(factory.CreateBatch());
        Assert.IsType<OracleBatchCommandMock>(factory.CreateBatchCommand());
    }
#endif

#if NET7_0_OR_GREATER
    /// <summary>
    /// EN: Summary for CreateDataSource_ShouldReturnProviderDataSourceMock.
    /// PT: Resumo para CreateDataSource_ShouldReturnProviderDataSourceMock.
    /// </summary>
    [Fact]
    public void CreateDataSource_ShouldReturnProviderDataSourceMock()
    {
        var factory = OracleConnectorFactoryMock.GetInstance(new OracleDbMock());

        var dataSource = factory.CreateDataSource("Host=mock");
        Assert.IsType<OracleDataSourceMock>(dataSource);
    }
#endif
}
