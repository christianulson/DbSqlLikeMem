namespace DbSqlLikeMem.Db2.Test.Strategy;

/// <summary>
/// Auto-generated summary.
/// </summary>
public sealed class Db2InsertStrategyCoverageTests(
        ITestOutputHelper helper
    ) : XUnitTestBase(helper)
{
    /// <summary>
    /// EN: Tests Insert_MultiRowValues_ShouldInsertAllRows behavior.
    /// PT: Testa o comportamento de Insert_MultiRowValues_ShouldInsertAllRows.
    /// </summary>
    [Fact]
    public void Insert_MultiRowValues_ShouldInsertAllRows()
    {
        var db = new Db2DbMock();
        var t = db.AddTable("t");
        t.Columns["id"] = new ColumnDef(0, DbType.Int32, false) { Identity = false };
        t.Columns["name"] = new ColumnDef(1, DbType.String, false);

        using var cnn = new Db2ConnectionMock(db);
        using var cmd = new Db2CommandMock(cnn)
        {
            CommandText = "INSERT INTO t (id, name) VALUES (1, 'A'), (2, 'B')"
        };

        var inserted = cmd.ExecuteNonQuery();

        Assert.Equal(2, inserted);
        Assert.Equal(2, t.Count);
        Assert.Equal(1, (int)t[0][0]!);
        Assert.Equal("A", (string)t[0][1]!);
        Assert.Equal(2, (int)t[1][0]!);
        Assert.Equal("B", (string)t[1][1]!);
    }

    /// <summary>
    /// EN: Tests Insert_WithIdentityColumnOmitted_ShouldAutoIncrement behavior.
    /// PT: Testa o comportamento de Insert_WithIdentityColumnOmitted_ShouldAutoIncrement.
    /// </summary>
    [Fact]
    public void Insert_WithIdentityColumnOmitted_ShouldAutoIncrement()
    {
        var db = new Db2DbMock();
        var t = db.AddTable("t");
        t.Columns["id"] = new ColumnDef(0, DbType.Int32, false) { Identity = true };
        t.Columns["name"] = new ColumnDef(1, DbType.String, false);

        t.PrimaryKeyIndexes.Add(0);

        using var cnn = new Db2ConnectionMock(db);
        using var cmd = new Db2CommandMock(cnn)
        {
            CommandText = "INSERT INTO t (name) VALUES ('A'), ('B')"
        };

        var inserted = cmd.ExecuteNonQuery();

        Assert.Equal(2, inserted);
        Assert.Equal(2, t.Count);
        Assert.Equal(1, (int)t[0][0]!);
        Assert.Equal("A", (string)t[0][1]!);
        Assert.Equal(2, (int)t[1][0]!);
        Assert.Equal("B", (string)t[1][1]!);
    }

    /// <summary>
    /// EN: Tests InsertSelect_ShouldInsertRowsFromSelect behavior.
    /// PT: Testa o comportamento de InsertSelect_ShouldInsertRowsFromSelect.
    /// </summary>
    [Fact]
    public void InsertSelect_ShouldInsertRowsFromSelect()
    {
        var db = new Db2DbMock();
        var users = db.AddTable("users");
        users.Columns["id"] = new ColumnDef(0, DbType.Int32, false);
        users.Columns["tenantid"] = new ColumnDef(1, DbType.Int32, false);
        users.Add(new Dictionary<int, object?> { [0] = 1, [1] = 10 });
        users.Add(new Dictionary<int, object?> { [0] = 2, [1] = 10 });
        users.Add(new Dictionary<int, object?> { [0] = 3, [1] = 20 });

        var t = db.AddTable("t");
        t.Columns["id"] = new ColumnDef(0, DbType.Int32, false);

        using var cnn = new Db2ConnectionMock(db);

        using var cmd = new Db2CommandMock(cnn)
        {
            CommandText = "INSERT INTO t (id) SELECT id FROM users WHERE tenantid = 10"
        };

        var inserted = cmd.ExecuteNonQuery();

        Assert.Equal(2, inserted);
        Assert.Equal(2, t.Count);
        Assert.Equal([1, 2], [.. t.Select(r => (int)r[0]!)]);
    }
}
