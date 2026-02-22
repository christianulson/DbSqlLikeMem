namespace DbSqlLikeMem.Npgsql.Test;

/// <summary>
/// EN: Contains tests for the Npgsql connector factory mock surface.
/// PT: Contém testes para a surface do simulado de fábrica de conectores do Npgsql.
/// </summary>
public sealed class NpgsqlConnectorFactoryMockTests
{
    /// <summary>
    /// EN: Verifies that core factory methods create Npgsql-specific mock instances.
    /// PT: Verifica se os métodos principais da fábrica criam instâncias de simulado específicas do Npgsql.
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
    /// EN: Verifies that batch factory methods create Npgsql-specific batch mocks.
    /// PT: Verifica se os métodos de lote da fábrica criam mocks de lote específicos do Npgsql.
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
    /// EN: Verifies that CreateDataSource returns an Npgsql data source mock.
    /// PT: Verifica se CreateDataSource retorna um simulado de fonte de dados do Npgsql.
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
