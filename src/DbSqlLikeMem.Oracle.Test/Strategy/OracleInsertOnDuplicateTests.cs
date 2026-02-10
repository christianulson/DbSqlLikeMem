namespace DbSqlLikeMem.Oracle.Test.Strategy;

public sealed class OracleMergeUpsertTests(ITestOutputHelper helper) : XUnitTestBase(helper)
{
    /// <summary>
    /// EN: Tests Merge_ShouldInsert_WhenNotMatched behavior.
    /// PT: Testa o comportamento de Merge_ShouldInsert_WhenNotMatched.
    /// </summary>
    [Fact]
    public void Merge_ShouldInsert_WhenNotMatched()
    {
        var db = new OracleDbMock();
        var t = db.AddTable("users");
        t.Columns["Id"] = new ColumnDef(0, DbType.Int32, false);
        t.Columns["Name"] = new ColumnDef(1, DbType.String, false);
        t.PrimaryKeyIndexes.Add(0);

        using var cnn = new OracleConnectionMock(db);
        cnn.Open();

        const string sql = @"
MERGE INTO users target
USING (SELECT 1 AS Id, 'A' AS Name FROM DUAL) src
ON (target.Id = src.Id)
WHEN NOT MATCHED THEN
    INSERT (Id, Name) VALUES (src.Id, src.Name)";

        var affected = cnn.Execute(sql);

        Assert.Equal(1, affected);
        Assert.Single(t);
        Assert.Equal("A", (string)t[0][1]!);
    }

    /// <summary>
    /// EN: Tests Merge_ShouldUpdate_WhenMatched behavior.
    /// PT: Testa o comportamento de Merge_ShouldUpdate_WhenMatched.
    /// </summary>
    [Fact]
    public void Merge_ShouldUpdate_WhenMatched()
    {
        var db = new OracleDbMock();
        var t = db.AddTable("users");
        t.Columns["Id"] = new ColumnDef(0, DbType.Int32, false);
        t.Columns["Name"] = new ColumnDef(1, DbType.String, false);
        t.PrimaryKeyIndexes.Add(0);

        t.Add(new Dictionary<int, object?> { [0] = 1, [1] = "OLD" });

        using var cnn = new OracleConnectionMock(db);
        cnn.Open();

        const string sql = @"
MERGE INTO users target
USING (SELECT 1 AS Id, 'NEW' AS Name FROM DUAL) src
ON (target.Id = src.Id)
WHEN MATCHED THEN
    UPDATE SET target.Name = src.Name
WHEN NOT MATCHED THEN
    INSERT (Id, Name) VALUES (src.Id, src.Name)";

        var affected = cnn.Execute(sql);

        Assert.Equal(1, affected);
        Assert.Single(t);
        Assert.Equal("NEW", (string)t[0][1]!);
    }
}
