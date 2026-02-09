namespace DbSqlLikeMem.SqlServer.Test.Strategy;

public sealed class SqlServerMergeUpsertTests(ITestOutputHelper helper) : XUnitTestBase(helper)
{
    [Fact]
    public void Merge_ShouldInsert_WhenNotMatched()
    {
        var db = new SqlServerDbMock();
        var t = db.AddTable("users");
        t.Columns["Id"] = new ColumnDef(0, DbType.Int32, false);
        t.Columns["Name"] = new ColumnDef(1, DbType.String, false);
        t.PrimaryKeyIndexes.Add(0);

        using var cnn = new SqlServerConnectionMock(db);
        cnn.Open();

        const string sql = @"
MERGE INTO users AS target
USING (SELECT 1 AS Id, 'A' AS Name) AS src
ON target.Id = src.Id
WHEN NOT MATCHED THEN
    INSERT (Id, Name) VALUES (src.Id, src.Name);";

        var affected = cnn.Execute(sql);

        Assert.Equal(1, affected);
        Assert.Single(t);
        Assert.Equal(1, (int)t[0][0]!);
        Assert.Equal("A", (string)t[0][1]!);
    }

    [Fact]
    public void Merge_ShouldUpdate_WhenMatched()
    {
        var db = new SqlServerDbMock();
        var t = db.AddTable("users");
        t.Columns["Id"] = new ColumnDef(0, DbType.Int32, false);
        t.Columns["Name"] = new ColumnDef(1, DbType.String, false);
        t.PrimaryKeyIndexes.Add(0);

        t.Add(new Dictionary<int, object?> { [0] = 1, [1] = "OLD" });

        using var cnn = new SqlServerConnectionMock(db);
        cnn.Open();

        const string sql = @"
MERGE INTO users AS target
USING (SELECT 1 AS Id, 'NEW' AS Name) AS src
ON target.Id = src.Id
WHEN MATCHED THEN
    UPDATE SET Name = src.Name
WHEN NOT MATCHED THEN
    INSERT (Id, Name) VALUES (src.Id, src.Name);";

        var affected = cnn.Execute(sql);

        // SQL Server returns 1 for update in MERGE
        Assert.Equal(1, affected);
        Assert.Single(t);
        Assert.Equal("NEW", (string)t[0][1]!);
    }
}
