namespace DbSqlLikeMem.SqlAzure.Test;

/// <summary>
/// EN: Contains tests for SQL Azure connector factory mock.
/// PT: Contém testes para a fábrica de conectores simulada do SQL Azure.
/// </summary>
public sealed class SqlAzureConnectorFactoryMockTests(
        ITestOutputHelper helper
    ) : XUnitTestBase(helper)
{
    /// <summary>
    /// EN: Ensures core factory members return SQL Azure provider mocks.
    /// PT: Garante que os membros principais da fábrica retornem mocks do provedor SQL Azure.
    /// </summary>
    [Fact]
    public void CreateCoreMembers_ShouldReturnProviderMocks()
    {
        var factory = SqlAzureConnectorFactoryMock.GetInstance([]);

        Assert.IsType<SqlAzureCommandMock>(factory.CreateCommand());
        Assert.IsType<SqlAzureConnectionMock>(factory.CreateConnection());
        Assert.IsType<SqlAzureDataAdapterMock>(factory.CreateDataAdapter());
        Assert.IsType<System.Data.Common.DbConnectionStringBuilder>(factory.CreateConnectionStringBuilder());
        Assert.NotNull(factory.CreateParameter());
    }

#if NET8_0_OR_GREATER
    /// <summary>
    /// EN: Ensures batch members return SQL Azure batch mocks.
    /// PT: Garante que membros de lote retornem mocks de lote SQL Azure.
    /// </summary>
    [Fact]
    public void CreateBatchMembers_ShouldReturnProviderMocks()
    {
        var factory = SqlAzureConnectorFactoryMock.GetInstance([]);

        Assert.True(factory.CanCreateBatch);
        Assert.IsType<SqlAzureBatchMock>(factory.CreateBatch());
        Assert.IsType<SqlAzureBatchCommandMock>(factory.CreateBatchCommand());
    }
#endif

#if NET7_0_OR_GREATER
    /// <summary>
    /// EN: Ensures CreateDataSource returns SQL Azure data source mock.
    /// PT: Garante que CreateDataSource retorne o mock de fonte de dados SQL Azure.
    /// </summary>
    [Fact]
    public void CreateDataSource_ShouldReturnProviderDataSourceMock()
    {
        var factory = SqlAzureConnectorFactoryMock.GetInstance([]);

        var dataSource = factory.CreateDataSource("Host=mock");
        Assert.IsType<SqlAzureDataSourceMock>(dataSource);
    }
#endif
}
