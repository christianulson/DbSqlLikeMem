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
            it => it.AddTable(
                "Users",
                [new Col("Id", DataTypeDef.Int32()), new Col("Name", DataTypeDef.String())],
                [new Dictionary<int, object?> { [0] = 1, [1] = "Ana" }]));

        db.GetTable("Users").Rows.Should().HaveCount(1);
    }
}
