namespace DbSqlLikeMem.SqlServer.Test;

public sealed class ExtendedMySqlMockTests(
        ITestOutputHelper helper
    ) : XUnitTestBase(helper)
{
    /// <summary>
    /// EN: Tests InsertAutoIncrementShouldAssignIdentityWhenNotSpecified behavior.
    /// PT: Testa o comportamento de InsertAutoIncrementShouldAssignIdentityWhenNotSpecified.
    /// </summary>
    [Fact]
    public void InsertAutoIncrementShouldAssignIdentityWhenNotSpecified()
    {
        var db = new SqlServerDbMock();
        var table = db.AddTable("users");
        table.Columns["id"] = new(0, DbType.Int32, false) { Identity = true };
        table.Columns["name"] = new(1, DbType.String, false);
        using var cnn = new SqlServerConnectionMock(db);
        cnn.Open();
        var rows1 = cnn.Execute("INSERT INTO users (name) VALUES (@name)", new { name = "Alice" });
        Assert.Equal(1, rows1);
        Assert.Single(table);
        Assert.Equal(1, table[0][0]);
        Assert.Equal("Alice", table[0][1]);

        var rows2 = cnn.Execute("INSERT INTO users (name) VALUES (@name)", new { name = "Bob" });
        Assert.Equal(1, rows2);
        Assert.Equal(2, table.Count);
        Assert.Equal(2, table[1][0]);
        Assert.Equal("Bob", table[1][1]);
    }

    /// <summary>
    /// EN: Tests InsertNullIntoNullableColumnShouldSucceed behavior.
    /// PT: Testa o comportamento de InsertNullIntoNullableColumnShouldSucceed.
    /// </summary>
    [Fact]
    public void InsertNullIntoNullableColumnShouldSucceed()
    {
        var db = new SqlServerDbMock();
        var table = db.AddTable("data");
        table.Columns["id"] = new(0, DbType.Int32, false);
        table.Columns["info"] = new(1, DbType.String, true);
        using var cnn = new SqlServerConnectionMock(db);
        cnn.Open();

        var rows = cnn.Execute("INSERT INTO data (id, info) VALUES (@id, @info)", new { id = 1, info = (string?)null });
        Assert.Equal(1, rows);
        Assert.Null(table[0][1]);
    }

    /// <summary>
    /// EN: Tests InsertNullIntoNonNullableColumnShouldThrow behavior.
    /// PT: Testa o comportamento de InsertNullIntoNonNullableColumnShouldThrow.
    /// </summary>
    [Fact]
    public void InsertNullIntoNonNullableColumnShouldThrow()
    {
        var db = new SqlServerDbMock();
        var table = db.AddTable("data");
        table.Columns["id"] = new(0, DbType.Int32, false);
        table.Columns["info"] = new(1, DbType.String, false);
        using var cnn = new SqlServerConnectionMock(db);
        cnn.Open();

        Assert.Throws<SqlServerMockException>(() =>
            cnn.Execute("INSERT INTO data (id, info) VALUES (@id, @info)", new { id = 1, info = (string?)null }));
    }

    private static readonly string[] item = ["first", "second"];

    /// <summary>
    /// EN: Tests CompositeIndexFilterShouldReturnCorrectRows behavior.
    /// PT: Testa o comportamento de CompositeIndexFilterShouldReturnCorrectRows.
    /// </summary>
    [Fact]
    public void CompositeIndexFilterShouldReturnCorrectRows()
    {
        var db = new SqlServerDbMock();
        var table = db.AddTable("t");
        table.Columns["first"] = new(0, DbType.String, false);
        table.Columns["second"] = new(1, DbType.String, false);
        table.Columns["value"] = new(2, DbType.Int32, false);
        table.Add(new Dictionary<int, object?> { { 0, "A" }, { 1, "X" }, { 2, 1 } });
        table.Add(new Dictionary<int, object?> { { 0, "A" }, { 1, "Y" }, { 2, 2 } });
        table.Add(new Dictionary<int, object?> { { 0, "B" }, { 1, "X" }, { 2, 3 } });
        table.CreateIndex(new IndexDef("ix_fs2", item, unique: false));

        using var cnn = new SqlServerConnectionMock(db);
        cnn.Open();

        var result = cnn.Query<dynamic>("SELECT * FROM t WHERE first = @f AND second = @s", new { f = "A", s = "X" }).ToList();
        Assert.Single(result);
        Assert.Equal(1, (int)result[0].value);
    }

    /// <summary>
    /// EN: Tests LikeFilterShouldReturnMatchingRows behavior.
    /// PT: Testa o comportamento de LikeFilterShouldReturnMatchingRows.
    /// </summary>
    [Fact]
    public void LikeFilterShouldReturnMatchingRows()
    {
        var db = new SqlServerDbMock();
        var table = db.AddTable("t");
        table.Columns["name"] = new(0, DbType.String, false);
        table.Add(new Dictionary<int, object?> { { 0, "alice" } });
        table.Add(new Dictionary<int, object?> { { 0, "bob" } });
        using var cnn = new SqlServerConnectionMock(db);
        cnn.Open();

        var res = cnn.Query<dynamic>("SELECT * FROM t WHERE name LIKE 'a%'").ToList();
        Assert.Single(res);
        Assert.Equal("alice", res[0].name);
    }

    /// <summary>
    /// EN: Tests InFilterShouldReturnMatchingRows behavior.
    /// PT: Testa o comportamento de InFilterShouldReturnMatchingRows.
    /// </summary>
    [Fact]
    public void InFilterShouldReturnMatchingRows()
    {
        var db = new SqlServerDbMock();
        var table = db.AddTable("t");
        table.Columns["id"] = new(0, DbType.Int32, false);
        table.Add(new Dictionary<int, object?> { { 0, 1 } });
        table.Add(new Dictionary<int, object?> { { 0, 2 } });
        table.Add(new Dictionary<int, object?> { { 0, 3 } });
        using var cnn = new SqlServerConnectionMock(db);
        cnn.Open();

        var res = cnn.Query<dynamic>("SELECT * FROM t WHERE id IN (1,3)").ToList();
        var ids = res.Select(r => (int)r.id).Order().ToArray();
        Assert.Equal([1, 3], ids);
    }

    /// <summary>
    /// EN: Tests OrderByLimitOffsetDistinctShouldReturnExpectedRows behavior.
    /// PT: Testa o comportamento de OrderByLimitOffsetDistinctShouldReturnExpectedRows.
    /// </summary>
    [Fact]
    public void OrderByLimitOffsetDistinctShouldReturnExpectedRows()
    {
        var db = new SqlServerDbMock();
        var table = db.AddTable("t");
        table.Columns["id"] = new(0, DbType.Int32, false);
        table.Add(new Dictionary<int, object?> { { 0, 2 } });
        table.Add(new Dictionary<int, object?> { { 0, 1 } });
        table.Add(new Dictionary<int, object?> { { 0, 2 } });
        using var cnn = new SqlServerConnectionMock(db);
        cnn.Open();

        var res = cnn.Query<dynamic>("SELECT DISTINCT id FROM t ORDER BY id DESC OFFSET 1 ROWS FETCH NEXT 2 ROWS ONLY").ToList();
        Assert.Single(res);
        Assert.Equal(1, (int)res[0].id);
    }

    /// <summary>
    /// EN: Tests HavingFilterShouldApplyAfterAggregation behavior.
    /// PT: Testa o comportamento de HavingFilterShouldApplyAfterAggregation.
    /// </summary>
    [Fact]
    public void HavingFilterShouldApplyAfterAggregation()
    {
        var db = new SqlServerDbMock();
        var table = db.AddTable("t");
        table.Columns["grp"] = new(0, DbType.String, false);
        table.Columns["val"] = new(1, DbType.Int32, false);
        table.Add(new Dictionary<int, object?> { { 0, "a" }, { 1, 1 } });
        table.Add(new Dictionary<int, object?> { { 0, "a" }, { 1, 2 } });
        table.Add(new Dictionary<int, object?> { { 0, "b" }, { 1, 3 } });
        using var cnn = new SqlServerConnectionMock(db);
        cnn.Open();
        const string sql = "SELECT grp, COUNT(val) AS C FROM t GROUP BY grp HAVING C > 1";

        var result = cnn.Query<dynamic>(sql).ToList();
        Assert.Single(result);
        Assert.Equal("a", result[0].grp);
        Assert.Equal(2L, result[0].C);
    }

    /// <summary>
    /// EN: Tests ForeignKeyDeleteShouldThrowOnReferencedParentDeletion behavior.
    /// PT: Testa o comportamento de ForeignKeyDeleteShouldThrowOnReferencedParentDeletion.
    /// </summary>
    [Fact]
    public void ForeignKeyDeleteShouldThrowOnReferencedParentDeletion()
    {
        // Parent
        var db = new SqlServerDbMock();
        var parent = db.AddTable("parent");
        parent.Columns["id"] = new(0, DbType.Int32, false);
        parent.Add(new Dictionary<int, object?> { { 0, 1 } });
        parent.PrimaryKeyIndexes.Add(parent.Columns["id"].Index);
        // Child with FK to parent
        var child = db.AddTable("child");
        child.Columns["pid"] = new(0, DbType.Int32, false);
        child.Columns["data"] = new(1, DbType.String, false);
        child.CreateForeignKey("pid", "parent", "id");
        child.Add(new Dictionary<int, object?> { { 0, 1 }, { 1, "x" } });

        using var cnn = new SqlServerConnectionMock(db);
        cnn.Open();

        Assert.Throws<SqlServerMockException>(() =>
            cnn.Execute("DELETE FROM parent WHERE id = 1"));
    }

    /// <summary>
    /// EN: Tests ForeignKeyDeleteShouldThrowOnReferencedParentDeletionWithouPK behavior.
    /// PT: Testa o comportamento de ForeignKeyDeleteShouldThrowOnReferencedParentDeletionWithouPK.
    /// </summary>
    [Fact]
    public void ForeignKeyDeleteShouldThrowOnReferencedParentDeletionWithouPK()
    {
        // Parent
        var db = new SqlServerDbMock();
        var parent = db.AddTable("parent");
        parent.Columns["id"] = new(0, DbType.Int32, false);
        parent.Add(new Dictionary<int, object?> { { 0, 1 } });
        // Child with FK to parent
        var child = db.AddTable("child");
        child.Columns["pid"] = new(0, DbType.Int32, false);
        child.Columns["data"] = new(1, DbType.String, false);
        child.CreateForeignKey("pid", "parent", "id");
        child.Add(new Dictionary<int, object?> { { 0, 1 }, { 1, "x" } });

        using var cnn = new SqlServerConnectionMock(db);
        cnn.Open();

        Assert.Throws<SqlServerMockException>(() =>
            cnn.Execute("DELETE FROM parent WHERE id = 1"));
    }

    /// <summary>
    /// EN: Tests MultipleParameterSetsInsertShouldInsertAllRows behavior.
    /// PT: Testa o comportamento de MultipleParameterSetsInsertShouldInsertAllRows.
    /// </summary>
    [Fact]
    public void MultipleParameterSetsInsertShouldInsertAllRows()
    {
        var db = new SqlServerDbMock();
        var table = db.AddTable("users");
        table.Columns["id"] = new(0, DbType.Int32, false);
        table.Columns["name"] = new(1, DbType.String, false);
        using var cnn = new SqlServerConnectionMock(db);
        cnn.Open();

        var data = new[]
        {
        new { id = 1, name = "A" },
        new { id = 2, name = "B" }
    };
        var rows = cnn.Execute("INSERT INTO users (id,name) VALUES (@id,@name)", data);
        Assert.Equal(2, rows);
        Assert.Equal(2, table.Count);
        Assert.Equal("A", table[0][1]);
        Assert.Equal("B", table[1][1]);
    }
}
