namespace DbSqlLikeMem.SqlServer.Test;

/// <summary>
/// EN: Validates SQL Server test-connection factory helpers.
/// PT: Valida os helpers da fábrica de conexões de teste SQL Server.
/// </summary>
public sealed class DbMockConnectionFactorySqlServerTests
{
    [Fact]
    public void CreateSqlServerWithTables_ShouldCreateSqlServerDbAndConnection()
    {
        var (db, connection) = DbMockConnectionFactory.CreateSqlServerWithTables();

        db.Should().BeOfType<SqlServerDbMock>();
        connection.Should().BeOfType<SqlServerConnectionMock>();
    }

    [Fact]
    public void CreateWithTables_ForSqlServer_ShouldApplyTableMappers()
    {
        var (db, connection) = DbMockConnectionFactory.CreateWithTables(
            "SqlServer",
            it =>
            {
                var tb = it.AddTable("Users");
                tb.AddColumn("Id", DbType.Int32, false);
                tb.AddColumn("Name", DbType.String, false);
                tb.Add(new Dictionary<int, object?> { [0] = 1, [1] = "Ana" });
            });

        db.Should().BeOfType<SqlServerDbMock>();
        connection.Should().BeOfType<SqlServerConnectionMock>();
        db.GetTable("Users").Should().HaveCount(1);
    }

    [Fact]
    public void CreateWithTables_ForSqlServer_ShouldCreateIsolatedInstancesBetweenCalls()
    {
        var (firstDb, _) = DbMockConnectionFactory.CreateWithTables(
            "SqlServer",
            it =>
            {
                var tb = it.AddTable("Users");
                tb.AddColumn("Id", DbType.Int32, false);
                tb.Add(new Dictionary<int, object?> { [0] = 1 });
            });

        var (secondDb, _) = DbMockConnectionFactory.CreateWithTables("SqlServer");

        firstDb.ContainsTable("Users").Should().BeTrue();
        secondDb.ContainsTable("Users").Should().BeFalse();
    }

    [Theory]
    [InlineData("SqlServer")]
    [InlineData("sqlserver")]
    [InlineData("  SQLSERVER  ")]
    public void CreateWithTables_ForSqlServerAliases_ShouldResolveSqlServerTypes(string providerHint)
    {
        var (db, connection) = DbMockConnectionFactory.CreateWithTables(providerHint);

        db.Should().BeOfType<SqlServerDbMock>();
        connection.Should().BeOfType<SqlServerConnectionMock>();
    }
}
