namespace DbSqlLikeMem.MySql.Test;

/// <summary>
/// EN: Contains tests for my sql connector factory mock.
/// PT: Contém testes para my sql fábrica de conectores simulada.
/// </summary>
public sealed class MySqlConnectorFactoryMockTests
{
    /// <summary>
    /// EN: Creates a new core members_should return provider mocks instance.
    /// PT: Verifica se os membros principais retornam mocks do provedor.
    /// </summary>
    [Fact]
    public void CreateCoreMembers_ShouldReturnProviderMocks()
    {
        var factory = MySqlConnectorFactoryMock.GetInstance(new MySqlDbMock());

        Assert.IsType<MySqlCommandMock>(factory.CreateCommand());
        Assert.IsType<MySqlConnectionMock>(factory.CreateConnection());
        Assert.IsType<MySqlDataAdapterMock>(factory.CreateDataAdapter());
        Assert.IsType<MySqlParameter>(factory.CreateParameter());
    }

#if NET6_0_OR_GREATER
    /// <summary>
    /// EN: Creates a new batch members_should return provider mocks instance.
    /// PT: Verifica se os membros de lote retornam mocks do provedor.
    /// </summary>
    [Fact]
    public void CreateBatchMembers_ShouldReturnProviderMocks()
    {
        var factory = MySqlConnectorFactoryMock.GetInstance(new MySqlDbMock());

        Assert.True(factory.CanCreateBatch);
        Assert.IsType<MySqlBatchMock>(factory.CreateBatch());
        Assert.IsType<MySqlBatchCommandMock>(factory.CreateBatchCommand());
    }
#endif

#if NET7_0_OR_GREATER
    /// <summary>
    /// EN: Creates a new data source_should return provider data source mock instance.
    /// PT: Verifica se a fonte de dados do provedor retorna um objeto de fonte de dados simulada.
    /// </summary>
    [Fact]
    public void CreateDataSource_ShouldReturnProviderDataSourceMock()
    {
        var factory = MySqlConnectorFactoryMock.GetInstance(new MySqlDbMock());

        var dataSource = factory.CreateDataSource("Host=mock");
        Assert.IsType<MySqlDataSourceMock>(dataSource);
    }
#endif
}
