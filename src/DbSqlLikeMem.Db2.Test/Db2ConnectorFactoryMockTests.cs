namespace DbSqlLikeMem.Db2.Test;

/// <summary>
/// EN: Summary for Db2ConnectorFactoryMockTests.
/// PT: Resumo para Db2ConnectorFactoryMockTests.
/// </summary>
public sealed class Db2ConnectorFactoryMockTests
{
    /// <summary>
    /// EN: Summary for CreateCoreMembers_ShouldReturnProviderMocks.
    /// PT: Resumo para CreateCoreMembers_ShouldReturnProviderMocks.
    /// </summary>
    [Fact]
    public void CreateCoreMembers_ShouldReturnProviderMocks()
    {
        var factory = Db2ConnectorFactoryMock.GetInstance(new Db2DbMock());

        Assert.IsType<Db2CommandMock>(factory.CreateCommand());
        Assert.IsType<Db2ConnectionMock>(factory.CreateConnection());
        Assert.IsType<Db2DataAdapterMock>(factory.CreateDataAdapter());
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
        var factory = Db2ConnectorFactoryMock.GetInstance(new Db2DbMock());

        Assert.True(factory.CanCreateBatch);
        Assert.IsType<Db2BatchMock>(factory.CreateBatch());
        Assert.IsType<Db2BatchCommandMock>(factory.CreateBatchCommand());
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
        var factory = Db2ConnectorFactoryMock.GetInstance(new Db2DbMock());

        var dataSource = factory.CreateDataSource("Host=mock");
        Assert.IsType<Db2DataSourceMock>(dataSource);
    }
#endif
}
