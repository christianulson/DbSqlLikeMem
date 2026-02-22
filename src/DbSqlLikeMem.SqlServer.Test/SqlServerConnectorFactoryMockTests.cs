namespace DbSqlLikeMem.SqlServer.Test;

/// <summary>
/// EN: Contains tests for sql server connector factory mock.
/// PT: Contém testes para sql server fábrica de conectores simulada.
/// </summary>
public sealed class SqlServerConnectorFactoryMockTests
{
    /// <summary>
    /// EN: Creates a new core members_should return provider mocks instance.
    /// PT: Verifica se os membros principais retornam mocks do provedor.
    /// </summary>
    [Fact]
    public void CreateCoreMembers_ShouldReturnProviderMocks()
    {
        var factory = SqlServerConnectorFactoryMock.GetInstance(new SqlServerDbMock());

        Assert.IsType<SqlServerCommandMock>(factory.CreateCommand());
        Assert.IsType<SqlServerConnectionMock>(factory.CreateConnection());
        Assert.IsType<SqlServerDataAdapterMock>(factory.CreateDataAdapter());
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
        var factory = SqlServerConnectorFactoryMock.GetInstance(new SqlServerDbMock());

        Assert.True(factory.CanCreateBatch);
        Assert.IsType<SqlServerBatchMock>(factory.CreateBatch());
        Assert.IsType<SqlServerBatchCommandMock>(factory.CreateBatchCommand());
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
        var factory = SqlServerConnectorFactoryMock.GetInstance(new SqlServerDbMock());

        var dataSource = factory.CreateDataSource("Host=mock");
        Assert.IsType<SqlServerDataSourceMock>(dataSource);
    }
#endif
}
