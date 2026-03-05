namespace DbSqlLikeMem.SqlAzure.Test;

/// <summary>
/// EN: Validates SQL Azure test-connection factory helpers.
/// PT: Valida os helpers da fábrica de conexões de teste SQL Azure.
/// </summary>
public sealed class DbMockConnectionFactorySqlAzureTests
{
    /// <summary>
    /// EN: Verifies CreateSqlAzureWithTables returns SQL Azure db and connection mocks.
    /// PT: Verifica que CreateSqlAzureWithTables retorna mocks SQL Azure de banco e conexão.
    /// </summary>
    [Fact]
    public void CreateSqlAzureWithTables_ShouldCreateSqlAzureDbAndConnection()
    {
        var (db, connection) = DbMockConnectionFactory.CreateSqlAzureWithTables();

        db.Should().BeOfType<SqlAzureDbMock>();
        connection.Should().BeOfType<SqlAzureConnectionMock>();
    }

    /// <summary>
    /// EN: Verifies mapper callbacks are applied when using SQL Azure provider hint.
    /// PT: Verifica que callbacks de mapeamento são aplicados ao usar o hint de provedor SQL Azure.
    /// </summary>
    [Fact]
    public void CreateWithTables_ForSqlAzure_ShouldApplyTableMappers()
    {
        var (db, connection) = DbMockConnectionFactory.CreateWithTables(
            "SqlAzure",
            it =>
            {
                var tb = it.AddTable("Users");
                tb.AddColumn("Id", DbType.Int32, false);
                tb.AddColumn("Name", DbType.String, false);
                tb.Add(new Dictionary<int, object?> { [0] = 1, [1] = "Ana" });
            });

        db.Should().BeOfType<SqlAzureDbMock>();
        connection.Should().BeOfType<SqlAzureConnectionMock>();
        db.GetTable("Users").Should().HaveCount(1);
    }

    /// <summary>
    /// EN: Verifies SQL Azure provider aliases resolve to SQL Azure db/connection mocks.
    /// PT: Verifica que aliases do provedor SQL Azure resolvem para mocks de banco/conexão SQL Azure.
    /// </summary>
    [Theory]
    [InlineData("SqlAzure")]
    [InlineData("sqlazure")]
    [InlineData("AzureSql")]
    [InlineData("azure-sql")]
    [InlineData("azure_sql")]
    [InlineData("  azure-sql  ")]
    public void CreateWithTables_ForSqlAzureAliases_ShouldResolveSqlAzureTypes(string providerHint)
    {
        var (db, connection) = DbMockConnectionFactory.CreateWithTables(providerHint);

        db.Should().BeOfType<SqlAzureDbMock>();
        connection.Should().BeOfType<SqlAzureConnectionMock>();
    }

    /// <summary>
    /// EN: Verifies each factory call creates isolated SQL Azure instances.
    /// PT: Verifica que cada chamada da fábrica cria instâncias SQL Azure isoladas.
    /// </summary>
    [Fact]
    public void CreateWithTables_ForSqlAzure_ShouldCreateIsolatedInstancesBetweenCalls()
    {
        var (firstDb, _) = DbMockConnectionFactory.CreateWithTables(
            "SqlAzure",
            it =>
            {
                var tb = it.AddTable("Users");
                tb.AddColumn("Id", DbType.Int32, false);
                tb.Add(new Dictionary<int, object?> { [0] = 1 });
            });

        var (secondDb, _) = DbMockConnectionFactory.CreateWithTables("SqlAzure");

        firstDb.ContainsTable("Users").Should().BeTrue();
        secondDb.ContainsTable("Users").Should().BeFalse();
    }
}
