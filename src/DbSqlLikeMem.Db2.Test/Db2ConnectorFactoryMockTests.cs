namespace DbSqlLikeMem.Db2.Test;

/// <summary>
/// EN: Contains tests for db2 connector factory mock.
/// PT: Contém testes para db2 fábrica de conectores simulada.
/// </summary>
public sealed class Db2ConnectorFactoryMockTests
{
    /// <summary>
    /// EN: Creates a new core members_should return provider mocks instance.
    /// PT: Verifica se os membros principais retornam mocks do provedor.
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
    /// EN: Creates a new batch members_should return provider mocks instance.
    /// PT: Verifica se os membros de lote retornam mocks do provedor.
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
    /// EN: Creates a new data source_should return provider data source mock instance.
    /// PT: Verifica se a fonte de dados do provedor retorna um objeto de fonte de dados simulada.
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
