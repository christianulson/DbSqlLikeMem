namespace DbSqlLikeMem.Sqlite.Test.Strategy;

/// <summary>
/// Auto-generated summary.
/// </summary>
public sealed class SqliteInsertStrategyCoverageTests(
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
        var db = new SqliteDbMock();
        var t = db.AddTable("t");
        t.AddColumn("id", DbType.Int32, false, identity: false);
        t.AddColumn("name", DbType.String, false);

        using var cnn = new SqliteConnectionMock(db);
        using var cmd = new SqliteCommandMock(cnn)
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
        var db = new SqliteDbMock();
        var t = db.AddTable("t");
        t.AddColumn("id", DbType.Int32, false, identity: true);
        t.AddColumn("name", DbType.String, false);

        t.AddPrimaryKeyIndexes("id");

        using var cnn = new SqliteConnectionMock(db);
        using var cmd = new SqliteCommandMock(cnn)
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
        var db = new SqliteDbMock();
        var users = db.AddTable("users");
        users.AddColumn("id", DbType.Int32, false);
        users.AddColumn("tenantid", DbType.Int32, false);
        users.Add(new Dictionary<int, object?> { [0] = 1, [1] = 10 });
        users.Add(new Dictionary<int, object?> { [0] = 2, [1] = 10 });
        users.Add(new Dictionary<int, object?> { [0] = 3, [1] = 20 });

        var t = db.AddTable("t");
        t.AddColumn("id", DbType.Int32, false);

        using var cnn = new SqliteConnectionMock(db);

        using var cmd = new SqliteCommandMock(cnn)
        {
            CommandText = "INSERT INTO t (id) SELECT id FROM users WHERE tenantid = 10"
        };

        var inserted = cmd.ExecuteNonQuery();

        Assert.Equal(2, inserted);
        Assert.Equal(2, t.Count);
        Assert.Equal([1, 2], [.. t.Select(r => (int)r[0]!)]);
    }
}
