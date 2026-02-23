namespace DbSqlLikeMem.Sqlite.Test;

public class DbMockConnectionFactoryTests
{
    [Fact]
    public void CreateSqliteWithTables_ShouldCreateSqliteDbAndConnection()
    {
        var (db, connection) = DbMockConnectionFactory.CreateSqliteWithTables();

        db.Should().BeOfType<SqliteDbMock>();
        connection.Should().BeOfType<SqliteConnectionMock>();
    }

    [Fact]
    public void CreateWithTables_ShouldApplyTableMappers()
    {
        var (db, _) = DbMockConnectionFactory.CreateWithTables(
            "Sqlite",
            it =>
            {
                var tb = it.AddTable("Users");
                tb.AddColumn("Id", DbType.Int32, false);
                tb.AddColumn("Name", DbType.String, false);
                tb.Add(new Dictionary<int, object?> { [0] = 1, [1] = "Ana" });
            });

        db.GetTable("Users").Should().HaveCount(1);
    }
}
