namespace DbSqlLikeMem.Npgsql.Test;

/// <summary>
/// Auto-generated summary.
/// </summary>
public sealed class SelectIntoInsertSelectUpdateDeleteFromSelectTests(
        ITestOutputHelper helper
    ) : XUnitTestBase(helper)
{
    /// <summary>
    /// EN: Tests CreateTableAsSelect_ShouldCreateNewTableWithRows behavior.
    /// PT: Testa o comportamento de CreateTableAsSelect_ShouldCreateNewTableWithRows.
    /// </summary>
    [Fact]
    public void CreateTableAsSelect_ShouldCreateNewTableWithRows()
    {
        var db = new NpgsqlDbMock();
        var users = db.AddTable("users");
        users.Columns["id"] = new ColumnDef(0, DbType.Int32, false);
        users.Columns["name"] = new ColumnDef(1, DbType.String, false);
        users.Columns["tenantid"] = new ColumnDef(2, DbType.Int32, false);
        users.Add(new Dictionary<int, object?> { { 0, 1 }, { 1, "A" }, { 2, 10 } });
        users.Add(new Dictionary<int, object?> { { 0, 2 }, { 1, "B" }, { 2, 20 } });
        users.Add(new Dictionary<int, object?> { { 0, 3 }, { 1, "C" }, { 2, 10 } });

        using var c = new NpgsqlConnectionMock(db);
        const string sql = "CREATE TABLE active_users AS SELECT id, name FROM users WHERE tenantid = 10";

        using var cmd = new NpgsqlCommandMock(c) { CommandText = sql };
        var affected = cmd.ExecuteNonQuery();

        Assert.Equal(0, affected); // DDL
        Assert.True(c.TryGetTable("active_users", out var active));
        Assert.NotNull(active);
        Assert.Equal(2, active!.Count);
        Assert.True(active.Columns.ContainsKey("id"));
        Assert.True(active.Columns.ContainsKey("name"));
        Assert.Equal(1, (int)active[0][0]!);
        Assert.Equal("A", active[0][1]);
    }

    /// <summary>
    /// EN: Tests InsertIntoSelect_ShouldInsertRowsFromQuery behavior.
    /// PT: Testa o comportamento de InsertIntoSelect_ShouldInsertRowsFromQuery.
    /// </summary>
    [Fact]
    public void InsertIntoSelect_ShouldInsertRowsFromQuery()
    {
        var db = new NpgsqlDbMock();
        var users = db.AddTable("users");
        users.Columns["id"] = new ColumnDef(0, DbType.Int32, false);
        users.Columns["name"] = new ColumnDef(1, DbType.String, false);
        users.Columns["tenantid"] = new ColumnDef(2, DbType.Int32, false);
        users.Add(new Dictionary<int, object?> { { 0, 1 }, { 1, "A" }, { 2, 10 } });
        users.Add(new Dictionary<int, object?> { { 0, 2 }, { 1, "B" }, { 2, 20 } });

        var audit = db.AddTable("audit_users");
        audit.Columns["userid"] = new ColumnDef(0, DbType.Int32, false);
        audit.Columns["username"] = new ColumnDef(1, DbType.String, false);

        using var c = new NpgsqlConnectionMock(db);
        const string sql = "INSERT INTO audit_users (userid, username) SELECT id, name FROM users WHERE tenantid = 10";

        using var cmd = new NpgsqlCommandMock(c) { CommandText = sql };
        var inserted = cmd.ExecuteNonQuery();

        Assert.Equal(1, inserted);
        Assert.Single(audit);
        Assert.Equal(1, (int)audit[0][0]!);
        Assert.Equal("A", audit[0][1]);
    }

    /// <summary>
    /// EN: Tests UpdateJoinDerivedSelect_ShouldUpdateRows behavior.
    /// PT: Testa o comportamento de UpdateJoinDerivedSelect_ShouldUpdateRows.
    /// </summary>
    [Fact]
    public void UpdateJoinDerivedSelect_ShouldUpdateRows()
    {
        var db = new NpgsqlDbMock();
        var users = db.AddTable("users");
        users.Columns["id"] = new ColumnDef(0, DbType.Int32, false);
        users.Columns["tenantid"] = new ColumnDef(1, DbType.Int32, false);
        users.Columns["total"] = new ColumnDef(2, DbType.Decimal, true);
        users.Add(new Dictionary<int, object?> { { 0, 1 }, { 1, 10 }, { 2, null } });
        users.Add(new Dictionary<int, object?> { { 0, 2 }, { 1, 10 }, { 2, null } });
        users.Add(new Dictionary<int, object?> { { 0, 3 }, { 1, 20 }, { 2, null } });

        var orders = db.AddTable("orders");
        orders.Columns["userid"] = new ColumnDef(0, DbType.Int32, false);
        orders.Columns["amount"] = new ColumnDef(1, DbType.Decimal, false);
        orders.Add(new Dictionary<int, object?> { { 0, 1 }, { 1, 10m } });
        orders.Add(new Dictionary<int, object?> { { 0, 1 }, { 1, 5m } });
        orders.Add(new Dictionary<int, object?> { { 0, 2 }, { 1, 7m } });

        using var c = new NpgsqlConnectionMock(db);
        const string sql = @"
UPDATE users u
JOIN (SELECT userid, SUM(amount) AS total FROM orders GROUP BY userid) s ON s.userid = u.id
SET u.total = s.total
WHERE u.tenantid = 10";

        using var cmd = new NpgsqlCommandMock(c) { CommandText = sql };
        var updated = cmd.ExecuteNonQuery();

        Assert.Equal(2, updated);
        Assert.Equal(15m, users[0][2]);
        Assert.Equal(7m, users[1][2]);
        Assert.Null(users[2][2]);
    }

    /// <summary>
    /// EN: Tests DeleteJoinDerivedSelect_ShouldDeleteRows behavior.
    /// PT: Testa o comportamento de DeleteJoinDerivedSelect_ShouldDeleteRows.
    /// </summary>
    [Fact]
    public void DeleteJoinDerivedSelect_ShouldDeleteRows()
    {
        var db = new NpgsqlDbMock();
        var users = db.AddTable("users");
        users.Columns["id"] = new ColumnDef(0, DbType.Int32, false);
        users.Columns["tenantid"] = new ColumnDef(1, DbType.Int32, false);
        users.Add(new Dictionary<int, object?> { { 0, 1 }, { 1, 10 } });
        users.Add(new Dictionary<int, object?> { { 0, 2 }, { 1, 10 } });
        users.Add(new Dictionary<int, object?> { { 0, 3 }, { 1, 20 } });

        using var c = new NpgsqlConnectionMock(db);
        const string sql = "DELETE FROM users WHERE id IN (SELECT id FROM users WHERE tenantid = 10)";

        using var cmd = new NpgsqlCommandMock(c) { CommandText = sql };
        var deleted = cmd.ExecuteNonQuery();

        Assert.Equal(2, deleted);
        Assert.Single(users);
        Assert.Equal(3, (int)users[0][0]!);
    }
}
