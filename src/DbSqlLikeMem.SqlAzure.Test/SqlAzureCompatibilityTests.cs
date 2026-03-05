namespace DbSqlLikeMem.SqlAzure.Test;

/// <summary>
/// EN: Covers SqlAzure compatibility-level behavior.
/// PT: Cobre o comportamento por nível de compatibilidade do SqlAzure.
/// </summary>
public sealed class SqlAzureCompatibilityTests
{
    /// <summary>
    /// EN: Provides sample SQL rows used by compatibility-level theory data composition.
    /// PT: Fornece linhas SQL de exemplo usadas pela composicao de dados teoricos por nivel de compatibilidade.
    /// </summary>
    public static IEnumerable<object[]> SqlSamples()
    {
        yield return ["SELECT 1"];
        yield return ["SELECT 2"];
    }

    /// <summary>
    /// EN: Ensures default SqlAzure compatibility level is used when not explicitly provided.
    /// PT: Garante que o nível de compatibilidade padrão do SqlAzure seja usado quando não for informado.
    /// </summary>
    [Fact]
    public void SqlAzureDbMock_ShouldUseDefaultCompatibilityLevel()
    {
        var db = new SqlAzureDbMock();
        Assert.Equal(SqlAzureDbCompatibilityLevels.Default, db.Version);
    }

    /// <summary>
    /// EN: Ensures explicit SQL Azure compatibility level is respected.
    /// PT: Garante que o nível explícito de compatibilidade SQL Azure seja respeitado.
    /// </summary>
    [Fact]
    public void SqlAzureDbMock_ShouldRespectExplicitCompatibilityLevel()
    {
        var db = new SqlAzureDbMock(SqlAzureDbCompatibilityLevels.SqlServer2016);
        db.Version.Should().Be(SqlAzureDbCompatibilityLevels.SqlServer2016);
    }

    /// <summary>
    /// EN: Ensures SQL Azure connection can be created with SQL Azure db mock.
    /// PT: Garante que a conexão SQL Azure possa ser criada com o db mock SQL Azure.
    /// </summary>
    [Fact]
    public void SqlAzureConnectionMock_ShouldAcceptSqlAzureDbMock()
    {
        using var connection = new SqlAzureConnectionMock(new SqlAzureDbMock());
        Assert.NotNull(connection);
    }

    /// <summary>
    /// EN: Ensures SQL Azure connection exposes SQL Azure server version text.
    /// PT: Garante que a conexão SQL Azure exponha texto de versão de servidor SQL Azure.
    /// </summary>
    [Fact]
    public void SqlAzureConnectionMock_ShouldExposeSqlAzureServerVersion()
    {
        using var connection = new SqlAzureConnectionMock(new SqlAzureDbMock(SqlAzureDbCompatibilityLevels.SqlServer2019));
        connection.ServerVersion.Should().Be("SQL Azure 150");
    }

    /// <summary>
    /// EN: Ensures SQL Azure data source creates SQL Azure connection instances.
    /// PT: Garante que a fonte de dados SQL Azure crie instâncias de conexão SQL Azure.
    /// </summary>
    [Fact]
    public void SqlAzureDataSourceMock_ShouldCreateSqlAzureConnection()
    {
        var source = new SqlAzureDataSourceMock(new SqlAzureDbMock());
#if NET8_0_OR_GREATER
        using var connection = source.CreateConnection();
#else
        using var connection = source.CreateDbConnection();
#endif
        Assert.IsType<SqlAzureConnectionMock>(connection);
    }

    /// <summary>
    /// EN: Ensures SQL Azure data adapter keeps typed select command synchronized.
    /// PT: Garante que o data adapter SQL Azure mantenha sincronizado o comando select tipado.
    /// </summary>
    [Fact]
    public void SqlAzureDataAdapterMock_ShouldKeepTypedSelectCommand()
    {
        using var connection = new SqlAzureConnectionMock(new SqlAzureDbMock());
        var adapter = new SqlAzureDataAdapterMock("SELECT 1", connection);
        Assert.NotNull(adapter.SelectCommand);
        Assert.Equal("SELECT 1", adapter.SelectCommand!.CommandText);
    }

    /// <summary>
    /// EN: Ensures SQL Azure connector factory creates SQL Azure specific objects.
    /// PT: Garante que a factory SQL Azure crie objetos específicos de SQL Azure.
    /// </summary>
    [Fact]
    public void SqlAzureConnectorFactoryMock_ShouldCreateSqlAzureObjects()
    {
        var factory = SqlAzureConnectorFactoryMock.GetInstance();
        Assert.IsType<SqlAzureConnectionMock>(factory.CreateConnection());
        Assert.IsType<SqlAzureCommandMock>(factory.CreateCommand());
        Assert.IsType<SqlAzureDataAdapterMock>(factory.CreateDataAdapter());
    }

    /// <summary>
    /// EN: Ensures provider-specific internal exception factory returns SQL Azure exception type.
    /// PT: Garante que a fábrica interna de exceções do provedor retorne tipo de exceção SQL Azure.
    /// </summary>
    [Fact]
    public void SqlAzureConnectionMock_NewException_ShouldReturnSqlAzureMockException()
    {
        using var connection = new SqlAzureConnectionMock(new SqlAzureDbMock());
        var exception = connection.NewException("boom", 42);

        exception.Should().BeOfType<SqlAzureMockException>();
        exception.Message.Should().Be("boom");
    }

    /// <summary>
    /// EN: Ensures SQL Azure compatibility levels are exposed in ascending order.
    /// PT: Garante que os níveis de compatibilidade do SQL Azure sejam expostos em ordem crescente.
    /// </summary>
    [Fact]
    public void SqlAzureDbCompatibilityLevels_ShouldExposeExpectedAscendingLevels()
    {
        var levels = SqlAzureDbCompatibilityLevels.Versions().ToArray();

        levels.Should().Equal(
            SqlAzureDbCompatibilityLevels.SqlServer2008,
            SqlAzureDbCompatibilityLevels.SqlServer2012,
            SqlAzureDbCompatibilityLevels.SqlServer2014,
            SqlAzureDbCompatibilityLevels.SqlServer2016,
            SqlAzureDbCompatibilityLevels.SqlServer2017,
            SqlAzureDbCompatibilityLevels.SqlServer2019,
            SqlAzureDbCompatibilityLevels.SqlServer2022,
            SqlAzureDbCompatibilityLevels.SqlServer2025);
    }

    /// <summary>
    /// EN: Ensures SqlAzureDbVersions aliases the same compatibility sequence.
    /// PT: Garante que SqlAzureDbVersions faça alias da mesma sequência de compatibilidade.
    /// </summary>
    [Fact]
    public void SqlAzureDbVersions_ShouldMatchCompatibilityLevels()
    {
        SqlAzureDbVersions.Versions().Should().Equal(SqlAzureDbCompatibilityLevels.Versions());
    }

    /// <summary>
    /// EN: Ensures MemberDataSqlAzureCompatibilityLevel emits valid compatibility levels.
    /// PT: Garante que MemberDataSqlAzureCompatibilityLevel emita níveis de compatibilidade válidos.
    /// </summary>
    [Theory]
    [MemberDataSqlAzureCompatibilityLevel]
    public void MemberDataSqlAzureCompatibilityLevel_ShouldEmitKnownLevels(int compatibilityLevel)
    {
        SqlAzureDbCompatibilityLevels
            .Versions()
            .Should()
            .Contain(compatibilityLevel);
    }

    /// <summary>
    /// EN: Ensures member data by compatibility level appends SQL Azure levels to each source row.
    /// PT: Garante que os dados por nível de compatibilidade anexem níveis SQL Azure em cada linha de origem.
    /// </summary>
    [Theory]
    [MemberDataBySqlAzureCompatibilityLevel(nameof(SqlSamples), VersionGraterOrEqual = SqlAzureDbCompatibilityLevels.SqlServer2019)]
    public void MemberDataBySqlAzureCompatibilityLevel_ShouldAppendCompatibilityLevel(string sql, int compatibilityLevel)
    {
        sql.Should().NotBeNullOrWhiteSpace();
        compatibilityLevel.Should().BeGreaterThanOrEqualTo(SqlAzureDbCompatibilityLevels.SqlServer2019);
    }
}
