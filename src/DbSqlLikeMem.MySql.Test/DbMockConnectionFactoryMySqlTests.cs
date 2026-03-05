namespace DbSqlLikeMem.MySql.Test;

/// <summary>
/// EN: Validates MySQL test-connection factory helpers.
/// PT: Valida os helpers da fábrica de conexões de teste MySQL.
/// </summary>
public sealed class DbMockConnectionFactoryMySqlTests
{
    [Fact]
    public void CreateMySqlWithTables_ShouldCreateMySqlDbAndConnection()
    {
        var (db, connection) = DbMockConnectionFactory.CreateMySqlWithTables();

        db.Should().BeOfType<MySqlDbMock>();
        connection.Should().BeOfType<MySqlConnectionMock>();
    }

    [Fact]
    public void CreateWithTables_ForMySql_ShouldApplyTableMappers()
    {
        var (db, connection) = DbMockConnectionFactory.CreateWithTables(
            "MySql",
            it =>
            {
                var tb = it.AddTable("Users");
                tb.AddColumn("Id", DbType.Int32, false);
                tb.AddColumn("Name", DbType.String, false);
                tb.Add(new Dictionary<int, object?> { [0] = 1, [1] = "Ana" });
            });

        db.Should().BeOfType<MySqlDbMock>();
        connection.Should().BeOfType<MySqlConnectionMock>();
        db.GetTable("Users").Should().HaveCount(1);
    }

    [Fact]
    public void CreateWithTables_ForMySql_ShouldCreateIsolatedInstancesBetweenCalls()
    {
        var (firstDb, _) = DbMockConnectionFactory.CreateWithTables(
            "MySql",
            it =>
            {
                var tb = it.AddTable("Users");
                tb.AddColumn("Id", DbType.Int32, false);
                tb.Add(new Dictionary<int, object?> { [0] = 1 });
            });

        var (secondDb, _) = DbMockConnectionFactory.CreateWithTables("MySql");

        firstDb.ContainsTable("Users").Should().BeTrue();
        secondDb.ContainsTable("Users").Should().BeFalse();
    }

    [Theory]
    [InlineData("MySql")]
    [InlineData("mysql")]
    [InlineData("  MYSQL  ")]
    public void CreateWithTables_ForMySqlAliases_ShouldResolveMySqlTypes(string providerHint)
    {
        var (db, connection) = DbMockConnectionFactory.CreateWithTables(providerHint);

        db.Should().BeOfType<MySqlDbMock>();
        connection.Should().BeOfType<MySqlConnectionMock>();
    }
}
