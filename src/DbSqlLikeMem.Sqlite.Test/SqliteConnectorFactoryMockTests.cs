namespace DbSqlLikeMem.Sqlite.Test;

/// <summary>
/// EN: Summary for SqliteConnectorFactoryMockTests.
/// PT: Resumo para SqliteConnectorFactoryMockTests.
/// </summary>
public sealed class SqliteConnectorFactoryMockTests
{
    [Fact]
    /// <summary>
    /// EN: Summary for CreateCoreMembers_ShouldReturnProviderMocks.
    /// PT: Resumo para CreateCoreMembers_ShouldReturnProviderMocks.
    /// </summary>
    public void CreateCoreMembers_ShouldReturnProviderMocks()
    {
        var factory = SqliteConnectorFactoryMock.GetInstance(new SqliteDbMock());

        Assert.IsType<SqliteCommandMock>(factory.CreateCommand());
        Assert.IsType<SqliteConnectionMock>(factory.CreateConnection());
        Assert.IsType<SqliteDataAdapterMock>(factory.CreateDataAdapter());
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
        var factory = SqliteConnectorFactoryMock.GetInstance(new SqliteDbMock());

        Assert.True(factory.CanCreateBatch);
        Assert.IsType<SqliteBatchMock>(factory.CreateBatch());
        Assert.IsType<SqliteBatchCommandMock>(factory.CreateBatchCommand());
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
        var factory = SqliteConnectorFactoryMock.GetInstance(new SqliteDbMock());

        var dataSource = factory.CreateDataSource("Host=mock");
        Assert.IsType<SqliteDataSourceMock>(dataSource);
    }
#endif
}
