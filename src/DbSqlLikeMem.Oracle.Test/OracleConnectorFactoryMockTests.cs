namespace DbSqlLikeMem.Oracle.Test;

/// <summary>
/// EN: Contains tests for the Oracle connector factory mock surface.
/// PT: Contém testes para a surface do simulado de fábrica de conectores do Oracle.
/// </summary>
public sealed class OracleConnectorFactoryMockTests
{
    /// <summary>
    /// EN: Verifies that core factory methods create Oracle-specific mock instances.
    /// PT: Verifica se os métodos principais da fábrica criam instâncias de simulado específicas do Oracle.
    /// </summary>
    [Fact]
    public void CreateCoreMembers_ShouldReturnProviderMocks()
    {
        var factory = OracleConnectorFactoryMock.GetInstance(new OracleDbMock());

        Assert.IsType<OracleCommandMock>(factory.CreateCommand());
        Assert.IsType<OracleConnectionMock>(factory.CreateConnection());
        Assert.IsType<OracleDataAdapterMock>(factory.CreateDataAdapter());
        Assert.IsType<System.Data.Common.DbConnectionStringBuilder>(factory.CreateConnectionStringBuilder());
        Assert.NotNull(factory.CreateParameter());
    }

#if NET6_0_OR_GREATER
    /// <summary>
    /// EN: Verifies that batch factory methods create Oracle-specific batch mocks.
    /// PT: Verifica se os métodos de lote da fábrica criam mocks de lote específicos do Oracle.
    /// </summary>
    [Fact]
    public void CreateBatchMembers_ShouldReturnProviderMocks()
    {
        var factory = OracleConnectorFactoryMock.GetInstance(new OracleDbMock());

        Assert.True(factory.CanCreateBatch);
        Assert.IsType<OracleBatchMock>(factory.CreateBatch());
        Assert.IsType<OracleBatchCommandMock>(factory.CreateBatchCommand());
    }
#endif

#if NET7_0_OR_GREATER
    /// <summary>
    /// EN: Verifies that CreateDataSource returns an Oracle data source mock.
    /// PT: Verifica se CreateDataSource retorna um simulado de fonte de dados do Oracle.
    /// </summary>
    [Fact]
    public void CreateDataSource_ShouldReturnProviderDataSourceMock()
    {
        var factory = OracleConnectorFactoryMock.GetInstance(new OracleDbMock());

        var dataSource = factory.CreateDataSource("Host=mock");
        Assert.IsType<OracleDataSourceMock>(dataSource);
    }
#endif
}
