namespace DbSqlLikeMem.SqlServer.Dapper.Test.Strategy;

/// <summary>
/// EN: Defines the class SqlServerMergeUpsertTests.
/// PT: Define a classe SqlServerMergeUpsertTests.
/// </summary>
public sealed class SqlServerMergeUpsertTests(ITestOutputHelper helper) : XUnitTestBase(helper)
{
    /// <summary>
    /// EN: Tests Merge_ShouldInsert_WhenNotMatched behavior.
    /// PT: Testa o comportamento de Merge_ShouldInsert_WhenNotMatched.
    /// </summary>
    [Fact]
    [Trait("Category", "Strategy")]
    public void Merge_ShouldInsert_WhenNotMatched()
    {
        var db = new SqlServerDbMock();
        var t = db.AddTable("users");
        t.AddColumn("Id", DbType.Int32, false);
        t.AddColumn("Name", DbType.String, false);
        t.AddPrimaryKeyIndexes("id");

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

    /// <summary>
    /// EN: Tests Merge_ShouldUpdate_WhenMatched behavior.
    /// PT: Testa o comportamento de Merge_ShouldUpdate_WhenMatched.
    /// </summary>
    [Fact]
    [Trait("Category", "Strategy")]
    public void Merge_ShouldUpdate_WhenMatched()
    {
        var db = new SqlServerDbMock();
        var t = db.AddTable("users");
        t.AddColumn("Id", DbType.Int32, false);
        t.AddColumn("Name", DbType.String, false);
        t.AddPrimaryKeyIndexes("id");

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
