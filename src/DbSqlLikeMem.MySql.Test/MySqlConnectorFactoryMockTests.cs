namespace DbSqlLikeMem.MySql.Test;

/// <summary>
/// EN: Summary for MySqlConnectorFactoryMockTests.
/// PT: Resumo para MySqlConnectorFactoryMockTests.
/// </summary>
public sealed class MySqlConnectorFactoryMockTests
{
    [Fact]
    /// <summary>
    /// EN: Summary for CreateCoreMembers_ShouldReturnProviderMocks.
    /// PT: Resumo para CreateCoreMembers_ShouldReturnProviderMocks.
    /// </summary>
    public void CreateCoreMembers_ShouldReturnProviderMocks()
    {
        var factory = MySqlConnectorFactoryMock.GetInstance(new MySqlDbMock());

        Assert.IsType<MySqlCommandMock>(factory.CreateCommand());
        Assert.IsType<MySqlConnectionMock>(factory.CreateConnection());
        Assert.IsType<MySqlDataAdapterMock>(factory.CreateDataAdapter());
        Assert.IsType<MySqlParameter>(factory.CreateParameter());
    }

#if NET6_0_OR_GREATER
    [Fact]
    /// <summary>
    /// EN: Summary for CreateBatchMembers_ShouldReturnProviderMocks.
    /// PT: Resumo para CreateBatchMembers_ShouldReturnProviderMocks.
    /// </summary>
    public void CreateBatchMembers_ShouldReturnProviderMocks()
    {
        var factory = MySqlConnectorFactoryMock.GetInstance(new MySqlDbMock());

        Assert.True(factory.CanCreateBatch);
        Assert.IsType<MySqlBatchMock>(factory.CreateBatch());
        Assert.IsType<MySqlBatchCommandMock>(factory.CreateBatchCommand());
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
        var factory = MySqlConnectorFactoryMock.GetInstance(new MySqlDbMock());

        var dataSource = factory.CreateDataSource("Host=mock");
        Assert.IsType<MySqlDataSourceMock>(dataSource);
    }
#endif
}
