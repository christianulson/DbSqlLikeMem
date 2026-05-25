namespace DbSqlLikeMem.Firebird.Test;

/// <summary>
/// EN: Contains tests for the Firebird connector factory mock.
/// PT-br: Contém testes para a fábrica de conectores simulada do Firebird.
/// </summary>
public sealed class FirebirdConnectorFactoryMockTests(
        ITestOutputHelper helper
    ) : XUnitTestBase(helper)
{
    /// <summary>
    /// EN: Verifies the Firebird factory creates the provider-specific mock surfaces.
    /// PT-br: Verifica se a fábrica Firebird cria as superfícies simuladas específicas do provedor.
    /// </summary>
    [Fact]
    public void CreateCoreMembers_ShouldReturnProviderMocks()
    {
        var factory = FirebirdConnectorFactoryMock.GetInstance([]);

        factory.CreateCommand().Should().BeOfType<FirebirdCommandMock>();
        factory.CreateConnection().Should().BeOfType<FirebirdConnectionMock>();
        factory.CreateDataAdapter().Should().BeOfType<FirebirdDataAdapterMock>();
        factory.CreateParameter().Should().BeOfType<FbParameter>();
        factory.CreateConnectionStringBuilder().Should().BeOfType<FbConnectionStringBuilder>();
        factory.CanCreateDataSourceEnumerator.Should().BeFalse();
#if NETCOREAPP3_0_OR_GREATER
        factory.CanCreateDataAdapter.Should().BeTrue();
#endif
    }

#if NET8_0_OR_GREATER
    /// <summary>
    /// EN: Verifies batch creation returns Firebird batch mocks.
    /// PT-br: Verifica se a criação de lote retorna mocks de lote Firebird.
    /// </summary>
    [Fact]
    public void CreateBatchMembers_ShouldReturnProviderMocks()
    {
        var factory = FirebirdConnectorFactoryMock.GetInstance([]);

        factory.CanCreateBatch.Should().BeTrue();
        factory.CreateBatch().Should().BeOfType<FirebirdBatchMock>();
        factory.CreateBatchCommand().Should().BeOfType<FirebirdBatchCommandMock>();
    }
#endif

#if NET7_0_OR_GREATER
    /// <summary>
    /// EN: Verifies the Firebird factory creates a provider-specific data source mock.
    /// PT-br: Verifica se a fábrica Firebird cria um mock de fonte de dados específico do provedor.
    /// </summary>
    [Fact]
    public void CreateDataSource_ShouldReturnProviderDataSourceMock()
    {
        var factory = FirebirdConnectorFactoryMock.GetInstance([]);

        var dataSource = factory.CreateDataSource("Host=mock");
        dataSource.Should().BeOfType<FirebirdDataSourceMock>();
    }
#endif
}
