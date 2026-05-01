namespace DbSqlLikeMem.Firebird.Test;

/// <summary>
/// EN: Contains tests for Firebird provider surface mocks.
/// PT-br: Contém testes para mocks de superfície do provedor Firebird.
/// </summary>
public sealed class FirebirdProviderSurfaceMocksTests(
        ITestOutputHelper helper
    ) : XUnitTestBase(helper)
{
    /// <summary>
    /// EN: Ensures the typed SelectCommand property stays synchronized with the base SelectCommand.
    /// PT-br: Garante que a propriedade tipada SelectCommand permaneça sincronizada com a SelectCommand da classe base.
    /// </summary>
    [Fact]
    public void DataAdapter_ShouldKeepTypedSelectCommand()
    {
        using var connection = new FirebirdConnectionMock(new FirebirdDbMock());
        var adapter = new FirebirdDataAdapterMock("SELECT 1", connection);

        adapter.SelectCommand.Should().NotBeNull();
        adapter.SelectCommand!.CommandText.Should().Be("SELECT 1");
    }

    /// <summary>
    /// EN: Ensures the data source mock creates a provider-specific connection bound to the same in-memory database.
    /// PT-br: Garante que o mock de fonte de dados crie uma conexão específica do provedor vinculada ao mesmo banco em memória.
    /// </summary>
    [Fact]
    public void DataSource_ShouldCreateFirebirdConnection()
    {
        var source = new FirebirdDataSourceMock([]);
#if NET8_0_OR_GREATER
        using var connection = source.CreateConnection();
#else
        using var connection = source.CreateDbConnection();
#endif
        connection.Should().BeOfType<FirebirdConnectionMock>();
    }

    /// <summary>
    /// EN: Ensures default adapter state matches the provider contract surface.
    /// PT-br: Garante que o estado padrão do adaptador corresponda à superfície contratual do provedor.
    /// </summary>
    [Fact]
    public void DataAdapter_DefaultCtor_ShouldExposeExpectedDefaults()
    {
        var adapter = new FirebirdDataAdapterMock();

        adapter.LoadDefaults.Should().BeTrue();
        adapter.UpdateBatchSize.Should().Be(1);
    }

#if NET8_0_OR_GREATER
    /// <summary>
    /// EN: Ensures batch support is exposed by the provider surface.
    /// PT-br: Garante que o suporte a lote seja exposto pela superfície do provedor.
    /// </summary>
    [Fact]
    public void Batch_ShouldCreateProviderBatchAndCommand()
    {
        var factory = FirebirdConnectorFactoryMock.GetInstance([]);

        factory.CanCreateBatch.Should().BeTrue();
        factory.CreateBatch().Should().BeOfType<FirebirdBatchMock>();
        factory.CreateBatchCommand().Should().BeOfType<FirebirdBatchCommandMock>();
    }
#endif
}
