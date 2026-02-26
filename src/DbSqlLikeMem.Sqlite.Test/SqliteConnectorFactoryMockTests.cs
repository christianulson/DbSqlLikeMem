namespace DbSqlLikeMem.Sqlite.Test;

/// <summary>
/// EN: Contains tests for sqlite connector factory mock.
/// PT: Contém testes para sqlite fábrica de conectores simulada.
/// </summary>
public sealed class SqliteConnectorFactoryMockTests
{
    /// <summary>
    /// EN: Creates a new core members_should return provider mocks instance.
    /// PT: Verifica se os membros principais retornam mocks do provedor.
    /// </summary>
    [Fact]
    public void CreateCoreMembers_ShouldReturnProviderMocks()
    {
        var factory = SqliteConnectorFactoryMock.GetInstance(new SqliteDbMock());

        Assert.IsType<SqliteCommandMock>(factory.CreateCommand());
        Assert.IsType<SqliteConnectionMock>(factory.CreateConnection());
        Assert.IsType<SqliteDataAdapterMock>(factory.CreateDataAdapter());
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
        var factory = SqliteConnectorFactoryMock.GetInstance(new SqliteDbMock());

        Assert.True(factory.CanCreateBatch);
        Assert.IsType<SqliteBatchMock>(factory.CreateBatch());
        Assert.IsType<SqliteBatchCommandMock>(factory.CreateBatchCommand());
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
        var factory = SqliteConnectorFactoryMock.GetInstance(new SqliteDbMock());

        var dataSource = factory.CreateDataSource("Host=mock");
        Assert.IsType<SqliteDataSourceMock>(dataSource);
    }
#endif
}
