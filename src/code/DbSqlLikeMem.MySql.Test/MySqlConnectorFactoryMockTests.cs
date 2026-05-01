namespace DbSqlLikeMem.MySql.Test;

/// <summary>
/// EN: Contains tests for my sql connector factory mock.
/// PT-br: Contém testes para my sql fábrica de conectores simulada.
/// </summary>
public sealed class MySqlConnectorFactoryMockTests(
        ITestOutputHelper helper
    ) : XUnitTestBase(helper)
{
    /// <summary>
    /// EN: Creates a new core members_should return provider mocks instance.
    /// PT-br: Verifica se os membros principais retornam mocks do provedor.
    /// </summary>
    [Fact]
    public void CreateCoreMembers_ShouldReturnProviderMocks()
    {
        var factory = MySqlConnectorFactoryMock.GetInstance([]);

        factory.CreateCommand().Should().BeOfType<MySqlCommandMock>();
        factory.CreateConnection().Should().BeOfType<MySqlConnectionMock>();
        factory.CreateDataAdapter().Should().BeOfType<MySqlDataAdapterMock>();
        factory.CreateParameter().Should().BeOfType<MySqlParameter>();
        factory.CreateConnectionStringBuilder().Should().BeOfType<MySqlConnectionStringBuilder>();
        factory.CreateCommandBuilder().Should().BeOfType<MySqlCommandBuilder>();
        factory.CanCreateDataSourceEnumerator.Should().BeFalse();
#if NETCOREAPP3_0_OR_GREATER
        factory.CanCreateCommandBuilder.Should().BeTrue();
        factory.CanCreateDataAdapter.Should().BeTrue();
#endif
    }

#if NET6_0_OR_GREATER
    /// <summary>
    /// EN: Creates a new batch members_should return provider mocks instance.
    /// PT-br: Verifica se os membros de lote retornam mocks do provedor.
    /// </summary>
    [Fact]
    public void CreateBatchMembers_ShouldReturnProviderMocks()
    {
        var factory = MySqlConnectorFactoryMock.GetInstance([]);

        factory.CanCreateBatch.Should().BeTrue();
        factory.CreateBatch().Should().BeOfType<MySqlBatchMock>();
        factory.CreateBatchCommand().Should().BeOfType<MySqlBatchCommandMock>();
    }
#endif

#if NET7_0_OR_GREATER
    /// <summary>
    /// EN: Creates a new data source_should return provider data source mock instance.
    /// PT-br: Verifica se a fonte de dados do provedor retorna um objeto de fonte de dados simulada.
    /// </summary>
    [Fact]
    public void CreateDataSource_ShouldReturnProviderDataSourceMock()
    {
        var factory = MySqlConnectorFactoryMock.GetInstance([]);

        var dataSource = factory.CreateDataSource("Host=mock");
        dataSource.Should().BeOfType<MySqlDataSourceMock>();
    }
#endif
}
