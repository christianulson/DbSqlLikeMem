namespace DbSqlLikeMem.Npgsql.Test;

/// <summary>
/// EN: Validates Npgsql test-connection factory helpers.
/// PT: Valida os helpers da fábrica de conexões de teste Npgsql.
/// </summary>
public sealed class DbMockConnectionFactoryNpgsqlTests
{
    [Fact]
    public void CreateNpgsqlWithTables_ShouldCreateNpgsqlDbAndConnection()
    {
        var (db, connection) = DbMockConnectionFactory.CreateNpgsqlWithTables();

        db.Should().BeOfType<NpgsqlDbMock>();
        connection.Should().BeOfType<NpgsqlConnectionMock>();
    }

    [Fact]
    public void CreateWithTables_ForNpgsql_ShouldApplyTableMappers()
    {
        var (db, connection) = DbMockConnectionFactory.CreateWithTables(
            "Npgsql",
            it =>
            {
                var tb = it.AddTable("Users");
                tb.AddColumn("Id", DbType.Int32, false);
                tb.AddColumn("Name", DbType.String, false);
                tb.Add(new Dictionary<int, object?> { [0] = 1, [1] = "Ana" });
            });

        db.Should().BeOfType<NpgsqlDbMock>();
        connection.Should().BeOfType<NpgsqlConnectionMock>();
        db.GetTable("Users").Should().HaveCount(1);
    }

    [Fact]
    public void CreateWithTables_ForNpgsql_ShouldCreateIsolatedInstancesBetweenCalls()
    {
        var (firstDb, _) = DbMockConnectionFactory.CreateWithTables(
            "Npgsql",
            it =>
            {
                var tb = it.AddTable("Users");
                tb.AddColumn("Id", DbType.Int32, false);
                tb.Add(new Dictionary<int, object?> { [0] = 1 });
            });

        var (secondDb, _) = DbMockConnectionFactory.CreateWithTables("Npgsql");

        firstDb.ContainsTable("Users").Should().BeTrue();
        secondDb.ContainsTable("Users").Should().BeFalse();
    }

    [Theory]
    [InlineData("Npgsql")]
    [InlineData("npgsql")]
    [InlineData("postgres")]
    [InlineData("postgresql")]
    [InlineData("  POSTGRES  ")]
    public void CreateWithTables_ForNpgsqlAliases_ShouldResolveNpgsqlTypes(string providerHint)
    {
        var (db, connection) = DbMockConnectionFactory.CreateWithTables(providerHint);

        db.Should().BeOfType<NpgsqlDbMock>();
        connection.Should().BeOfType<NpgsqlConnectionMock>();
    }
}
