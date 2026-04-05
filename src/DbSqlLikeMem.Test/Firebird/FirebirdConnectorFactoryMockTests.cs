namespace DbSqlLikeMem.Test;

/// <summary>
/// EN: Contains tests for the Firebird connector factory mock surface.
/// PT: Contem testes para a surface da fabrica de conectores simulada do Firebird.
/// </summary>
public sealed class FirebirdConnectorFactoryMockTests(
        ITestOutputHelper helper
    ) : XUnitTestBase(helper)
{
    /// <summary>
    /// EN: Verifies that core factory methods create Firebird-specific mock instances.
    /// PT: Verifica se os metodos principais da fabrica criam instancias simuladas especificas do Firebird.
    /// </summary>
    [Fact]
    public void CreateCoreMembers_ShouldReturnProviderMocks()
    {
        var factory = FirebirdConnectorFactoryMock.GetInstance([]);

        Assert.IsType<FirebirdCommandMock>(factory.CreateCommand());
        Assert.IsType<FirebirdConnectionMock>(factory.CreateConnection());
        Assert.IsType<FirebirdDataAdapterMock>(factory.CreateDataAdapter());
        Assert.IsType<DbConnectionStringBuilder>(factory.CreateConnectionStringBuilder());
        Assert.NotNull(factory.CreateParameter());
    }

#if NET8_0_OR_GREATER
    /// <summary>
    /// EN: Verifies that batch factory methods create Firebird-specific batch mocks.
    /// PT: Verifica se os métodos de lote da fábrica criam mocks de lote específicos do Firebird.
    /// </summary>
    [Fact]
    public void CreateBatchMembers_ShouldReturnProviderMocks()
    {
        var factory = FirebirdConnectorFactoryMock.GetInstance([]);

        Assert.True(factory.CanCreateBatch);
        Assert.IsType<FirebirdBatchMock>(factory.CreateBatch());
        Assert.IsType<FirebirdBatchCommandMock>(factory.CreateBatchCommand());
    }
#endif

#if NET7_0_OR_GREATER
    /// <summary>
    /// EN: Verifies that CreateDataSource returns a Firebird data source mock.
    /// PT: Verifica se CreateDataSource retorna um mock de fonte de dados Firebird.
    /// </summary>
    [Fact]
    public void CreateDataSource_ShouldReturnProviderDataSourceMock()
    {
        var factory = FirebirdConnectorFactoryMock.GetInstance([]);

        var dataSource = factory.CreateDataSource("Database=mock");
        Assert.IsType<FirebirdDataSourceMock>(dataSource);
    }
#endif
}




