namespace DbSqlLikeMem.Oracle.Test;

/// <summary>
/// EN: Validates Oracle test-connection factory helpers.
/// PT: Valida os helpers da fábrica de conexões de teste Oracle.
/// </summary>
public sealed class DbMockConnectionFactoryOracleTests
{
    [Fact]
    public void CreateOracleWithTables_ShouldCreateOracleDbAndConnection()
    {
        var (db, connection) = DbMockConnectionFactory.CreateOracleWithTables();

        db.Should().BeOfType<OracleDbMock>();
        connection.Should().BeOfType<OracleConnectionMock>();
    }

    [Fact]
    public void CreateWithTables_ForOracle_ShouldApplyTableMappers()
    {
        var (db, connection) = DbMockConnectionFactory.CreateWithTables(
            "Oracle",
            it =>
            {
                var tb = it.AddTable("Users");
                tb.AddColumn("Id", DbType.Int32, false);
                tb.AddColumn("Name", DbType.String, false);
                tb.Add(new Dictionary<int, object?> { [0] = 1, [1] = "Ana" });
            });

        db.Should().BeOfType<OracleDbMock>();
        connection.Should().BeOfType<OracleConnectionMock>();
        db.GetTable("Users").Should().HaveCount(1);
    }

    [Fact]
    public void CreateWithTables_ForOracle_ShouldCreateIsolatedInstancesBetweenCalls()
    {
        var (firstDb, _) = DbMockConnectionFactory.CreateWithTables(
            "Oracle",
            it =>
            {
                var tb = it.AddTable("Users");
                tb.AddColumn("Id", DbType.Int32, false);
                tb.Add(new Dictionary<int, object?> { [0] = 1 });
            });

        var (secondDb, _) = DbMockConnectionFactory.CreateWithTables("Oracle");

        firstDb.ContainsTable("Users").Should().BeTrue();
        secondDb.ContainsTable("Users").Should().BeFalse();
    }

    [Theory]
    [InlineData("Oracle")]
    [InlineData("oracle")]
    [InlineData("  ORACLE  ")]
    public void CreateWithTables_ForOracleAliases_ShouldResolveOracleTypes(string providerHint)
    {
        var (db, connection) = DbMockConnectionFactory.CreateWithTables(providerHint);

        db.Should().BeOfType<OracleDbMock>();
        connection.Should().BeOfType<OracleConnectionMock>();
    }
}
