namespace DbSqlLikeMem.SqlServer.Test;

/// <summary>
/// EN: Summary for SqlServerConnectorFactoryMockTests.
/// PT: Resumo para SqlServerConnectorFactoryMockTests.
/// </summary>
public sealed class SqlServerConnectorFactoryMockTests
{
    [Fact]
    /// <summary>
    /// EN: Summary for CreateCoreMembers_ShouldReturnProviderMocks.
    /// PT: Resumo para CreateCoreMembers_ShouldReturnProviderMocks.
    /// </summary>
    public void CreateCoreMembers_ShouldReturnProviderMocks()
    {
        var factory = SqlServerConnectorFactoryMock.GetInstance(new SqlServerDbMock());

        Assert.IsType<SqlServerCommandMock>(factory.CreateCommand());
        Assert.IsType<SqlServerConnectionMock>(factory.CreateConnection());
        Assert.IsType<SqlServerDataAdapterMock>(factory.CreateDataAdapter());
        Assert.IsType<System.Data.Common.DbConnectionStringBuilder>(factory.CreateConnectionStringBuilder());
        Assert.NotNull(factory.CreateParameter());
    }

#if NET6_0_OR_GREATER
    [Fact]
    /// <summary>
    /// EN: Summary for CreateBatchMembers_ShouldReturnProviderMocks.
    /// PT: Resumo para CreateBatchMembers_ShouldReturnProviderMocks.
    /// </summary>
    public void CreateBatchMembers_ShouldReturnProviderMocks()
    {
        var factory = SqlServerConnectorFactoryMock.GetInstance(new SqlServerDbMock());

        Assert.True(factory.CanCreateBatch);
        Assert.IsType<SqlServerBatchMock>(factory.CreateBatch());
        Assert.IsType<SqlServerBatchCommandMock>(factory.CreateBatchCommand());
    }
#endif

#if NET7_0_OR_GREATER
    [Fact]
    /// <summary>
    /// EN: Summary for CreateDataSource_ShouldReturnProviderDataSourceMock.
    /// PT: Resumo para CreateDataSource_ShouldReturnProviderDataSourceMock.
    /// </summary>
    public void CreateDataSource_ShouldReturnProviderDataSourceMock()
    {
        var factory = SqlServerConnectorFactoryMock.GetInstance(new SqlServerDbMock());

        var dataSource = factory.CreateDataSource("Host=mock");
        Assert.IsType<SqlServerDataSourceMock>(dataSource);
    }
#endif
}
