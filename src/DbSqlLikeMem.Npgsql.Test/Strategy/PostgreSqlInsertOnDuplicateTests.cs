namespace DbSqlLikeMem.Npgsql.Test.Strategy;

public sealed class PostgreSqlOnConflictUpsertTests(ITestOutputHelper helper) : XUnitTestBase(helper)
{
    [Fact]
    public void Insert_OnConflict_ShouldInsert_WhenNoConflict()
    {
        var db = new NpgsqlDbMock();
        var t = db.AddTable("users");
        t.Columns["Id"] = new ColumnDef(0, DbType.Int32, false);
        t.Columns["Name"] = new ColumnDef(1, DbType.String, false);
        t.PrimaryKeyIndexes.Add(0);

        using var cnn = new NpgsqlConnectionMock(db);
        cnn.Open();

        const string sql = "INSERT INTO users (Id, Name) VALUES (1, 'A') ON CONFLICT (Id) DO UPDATE SET Name = EXCLUDED.Name";
        var affected = cnn.Execute(sql);

        Assert.Equal(1, affected);
        Assert.Single(t);
        Assert.Equal("A", (string)t[0][1]!);
    }

    [Fact]
    public void Insert_OnConflict_ShouldUpdate_WhenConflict()
    {
        var db = new NpgsqlDbMock();
        var t = db.AddTable("users");
        t.Columns["Id"] = new ColumnDef(0, DbType.Int32, false);
        t.Columns["Name"] = new ColumnDef(1, DbType.String, false);
        t.PrimaryKeyIndexes.Add(0);

        t.Add(new Dictionary<int, object?> { [0] = 1, [1] = "OLD" });

        using var cnn = new NpgsqlConnectionMock(db);
        cnn.Open();

        const string sql = "INSERT INTO users (Id, Name) VALUES (1, 'NEW') ON CONFLICT (Id) DO UPDATE SET Name = EXCLUDED.Name";
        var affected = cnn.Execute(sql);

        // PostgreSQL reports 1 row affected for DO UPDATE
        Assert.Equal(1, affected);
        Assert.Single(t);
        Assert.Equal("NEW", (string)t[0][1]!);
    }
}
