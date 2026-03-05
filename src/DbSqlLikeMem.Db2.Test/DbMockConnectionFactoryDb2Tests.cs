namespace DbSqlLikeMem.Db2.Test;

/// <summary>
/// EN: Validates Db2 test-connection factory helpers.
/// PT: Valida os helpers da fábrica de conexões de teste Db2.
/// </summary>
public sealed class DbMockConnectionFactoryDb2Tests
{
    [Fact]
    public void CreateDb2WithTables_ShouldCreateDb2DbAndConnection()
    {
        var (db, connection) = DbMockConnectionFactory.CreateDb2WithTables();

        db.Should().BeOfType<Db2DbMock>();
        connection.Should().BeOfType<Db2ConnectionMock>();
    }

    [Fact]
    public void CreateWithTables_ForDb2_ShouldApplyTableMappers()
    {
        var (db, connection) = DbMockConnectionFactory.CreateWithTables(
            "Db2",
            it =>
            {
                var tb = it.AddTable("Users");
                tb.AddColumn("Id", DbType.Int32, false);
                tb.AddColumn("Name", DbType.String, false);
                tb.Add(new Dictionary<int, object?> { [0] = 1, [1] = "Ana" });
            });

        db.Should().BeOfType<Db2DbMock>();
        connection.Should().BeOfType<Db2ConnectionMock>();
        db.GetTable("Users").Should().HaveCount(1);
    }

    [Fact]
    public void CreateWithTables_ForDb2_ShouldCreateIsolatedInstancesBetweenCalls()
    {
        var (firstDb, _) = DbMockConnectionFactory.CreateWithTables(
            "Db2",
            it =>
            {
                var tb = it.AddTable("Users");
                tb.AddColumn("Id", DbType.Int32, false);
                tb.Add(new Dictionary<int, object?> { [0] = 1 });
            });

        var (secondDb, _) = DbMockConnectionFactory.CreateWithTables("Db2");

        firstDb.ContainsTable("Users").Should().BeTrue();
        secondDb.ContainsTable("Users").Should().BeFalse();
    }

    [Theory]
    [InlineData("Db2")]
    [InlineData("db2")]
    [InlineData("  DB2  ")]
    public void CreateWithTables_ForDb2Aliases_ShouldResolveDb2Types(string providerHint)
    {
        var (db, connection) = DbMockConnectionFactory.CreateWithTables(providerHint);

        db.Should().BeOfType<Db2DbMock>();
        connection.Should().BeOfType<Db2ConnectionMock>();
    }
}
