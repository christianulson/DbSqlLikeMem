namespace DbSqlLikeMem.Npgsql.Test;

/// <summary>
/// EN: Summary for NpgsqlConnectorFactoryMockTests.
/// PT: Resumo para NpgsqlConnectorFactoryMockTests.
/// </summary>
public sealed class NpgsqlConnectorFactoryMockTests
{
    /// <summary>
    /// EN: Summary for CreateCoreMembers_ShouldReturnProviderMocks.
    /// PT: Resumo para CreateCoreMembers_ShouldReturnProviderMocks.
    /// </summary>
    [Fact]
    public void CreateCoreMembers_ShouldReturnProviderMocks()
    {
        var factory = NpgsqlConnectorFactoryMock.GetInstance(new NpgsqlDbMock());

        Assert.IsType<NpgsqlCommandMock>(factory.CreateCommand());
        Assert.IsType<NpgsqlConnectionMock>(factory.CreateConnection());
        Assert.IsType<NpgsqlDataAdapterMock>(factory.CreateDataAdapter());
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
        var factory = NpgsqlConnectorFactoryMock.GetInstance(new NpgsqlDbMock());

        Assert.True(factory.CanCreateBatch);
        Assert.IsType<NpgsqlBatchMock>(factory.CreateBatch());
        Assert.IsType<NpgsqlBatchCommandMock>(factory.CreateBatchCommand());
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
        var factory = NpgsqlConnectorFactoryMock.GetInstance(new NpgsqlDbMock());

        var dataSource = factory.CreateDataSource("Host=mock");
        Assert.IsType<NpgsqlDataSourceMock>(dataSource);
    }
#endif
}
