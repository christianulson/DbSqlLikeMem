namespace DbSqlLikeMem.MySql.Test;

/// <summary>
/// TDD guard-rail tests for SQL features where this in-memory MySql mock commonly diverges from real MySQL.
/// These tests are EXPECTED TO FAIL until the corresponding functionality is implemented in the parser/executor.
/// </summary>
public sealed class MySqlSqlCompatibilityGapTests : XUnitTestBase
{
    private readonly MySqlConnectionMock _cnn;

    public MySqlSqlCompatibilityGapTests(ITestOutputHelper helper) : base(helper)
    {
        // users
        var db = new MySqlDbMock();
        var users = db.AddTable("users");
        users.Columns["id"] = new(0, DbType.Int32, false);
        users.Columns["name"] = new(1, DbType.String, false);
        users.Columns["email"] = new(2, DbType.String, true);

        users.Add(new Dictionary<int, object?> { [0] = 1, [1] = "John", [2] = "john@x.com" });
        users.Add(new Dictionary<int, object?> { [0] = 2, [1] = "Bob",  [2] = null });
        users.Add(new Dictionary<int, object?> { [0] = 3, [1] = "Jane", [2] = "jane@x.com" });

        // orders
        var orders = db.AddTable("orders");
        orders.Columns["id"] = new(0, DbType.Int32, false);
        orders.Columns["userId"] = new(1, DbType.Int32, false);
        orders.Columns["amount"] = new(2, DbType.Decimal, false);

        orders.Add(new Dictionary<int, object?> { [0] = 10, [1] = 1, [2] = 50m });
        orders.Add(new Dictionary<int, object?> { [0] = 11, [1] = 2, [2] = 200m });
        orders.Add(new Dictionary<int, object?> { [0] = 12, [1] = 2, [2] = 10m });

        _cnn = new MySqlConnectionMock(db);

        _cnn.Open();
    }

    [Fact]
    public void Where_Precedence_AND_ShouldBindStrongerThan_OR()
    {
        // MySQL precedence: AND binds stronger than OR.
        // Equivalent to: id = 1 OR (id = 2 AND name = 'Bob')
        var rows = _cnn.Query<dynamic>("SELECT id FROM users WHERE id = 1 OR id = 2 AND name = 'Bob'").ToList();
        Assert.Equal([1, 2], [.. rows.Select(r => (int)r.id).Order()]);
    }

    [Fact]
    public void Where_OR_ShouldWork()
    {
        var rows = _cnn.Query<dynamic>("SELECT id FROM users WHERE id = 1 OR id = 3").ToList();
        Assert.Equal([1, 3], [.. rows.Select(r => (int)r.id).Order()]);
    }

    [Fact]
    public void Where_ParenthesesGrouping_ShouldWork()
    {
        // (id=1 OR id=2) AND email IS NULL => only user 2
        var rows = _cnn.Query<dynamic>("SELECT id FROM users WHERE (id = 1 OR id = 2) AND email IS NULL").ToList();
        Assert.Single(rows);
        Assert.Equal(2, (int)rows[0].id);
    }

    [Fact]
    public void Select_Expressions_Arithmetic_ShouldWork()
    {
        var rows = _cnn.Query<dynamic>("SELECT id, id + 1 AS nextId FROM users ORDER BY id").ToList();
        Assert.Equal([2, 3, 4], [.. rows.Select(r => (int)r.nextId)]);
    }

    [Fact]
    public void Select_Expressions_CASE_WHEN_ShouldWork()
    {
        var rows = _cnn.Query<dynamic>("SELECT id, CASE WHEN email IS NULL THEN 0 ELSE 1 END AS hasEmail FROM users ORDER BY id").ToList();
        Assert.Equal([1, 0, 1], [.. rows.Select(r => (int)r.hasEmail)]);
    }

    [Fact]
    public void Select_Expressions_IF_ShouldWork()
    {
        // MySQL: IF(cond, then, else)
        var rows = _cnn.Query<dynamic>("SELECT id, IF(email IS NULL, 'no', 'yes') AS flag FROM users ORDER BY id").ToList();
        Assert.Equal(["yes", "no", "yes"], [.. rows.Select(r => (string)r.flag)]);
    }

    [Fact]
    public void Select_Expressions_IIF_ShouldWork_AsAliasForIF()
    {
        // Not native MySQL, but requested as convenience.
        var rows = _cnn.Query<dynamic>("SELECT id, IIF(email IS NULL, 0, 1) AS hasEmail FROM users ORDER BY id").ToList();
        Assert.Equal([1, 0, 1], [.. rows.Select(r => (int)r.hasEmail)]);
    }

    [Fact]
    public void Functions_COALESCE_ShouldWork()
    {
        var rows = _cnn.Query<dynamic>("SELECT id, COALESCE(email, 'none') AS em FROM users ORDER BY id").ToList();
        Assert.Equal(["john@x.com", "none", "jane@x.com"], [.. rows.Select(r => (string)r.em)]);
    }

    [Fact]
    public void Functions_IFNULL_ShouldWork()
    {
        var rows = _cnn.Query<dynamic>("SELECT id, IFNULL(email, 'none') AS em FROM users ORDER BY id").ToList();
        Assert.Equal(["john@x.com", "none", "jane@x.com"], [.. rows.Select(r => (string)r.em)]);
    }

    [Fact]
    public void Functions_CONCAT_ShouldWork()
    {
        var rows = _cnn.Query<dynamic>("SELECT id, CONCAT(name, '#', id) AS tag FROM users ORDER BY id").ToList();
        Assert.Equal(["John#1", "Bob#2", "Jane#3"], [.. rows.Select(r => (string)r.tag)]);
    }

    [Fact]
    public void Distinct_ShouldBeConsistent()
    {
        // duplicate names
        _cnn.Execute("INSERT INTO users (id,name,email) VALUES (4,'John','j2@x.com')");
        var rows = _cnn.Query<dynamic>("SELECT DISTINCT name FROM users ORDER BY name").ToList();
        Assert.Equal(["Bob", "Jane", "John"], [.. rows.Select(r => (string)r.name)]);
    }

    [Fact]
    public void Join_ComplexOn_WithOr_ShouldWork()
    {
        // include orders joined when (o.userId = u.id OR o.userId = 0)
        // We'll add a global order with userId=0 and expect it to join to ALL users.
        _cnn.Execute("INSERT INTO orders (id,userId,amount) VALUES (13,0,1)");
        var rows = _cnn.Query<dynamic>(
            "SELECT u.id AS uid, o.id AS oid FROM users u " +
            "JOIN orders o ON (o.userId = u.id OR o.userId = 0) " +
            "WHERE u.id IN (1,2) ORDER BY u.id, o.id").ToList();

        // For uid=1: orders 10 and 13; for uid=2: orders 11,12,13
        Assert.Equal([(1,10),(1,13),(2,11),(2,12),(2,13)],
            [.. rows.Select(r => ((int)r.uid,(int)r.oid))]);
    }

    [Fact]
    public void GroupBy_Having_ShouldSupportAggregates()
    {
        var rows = _cnn.Query<dynamic>(
            "SELECT userId, SUM(amount) AS total " +
            "FROM orders GROUP BY userId HAVING SUM(amount) > 100 " +
            "ORDER BY userId").ToList();

        Assert.Single(rows);
        Assert.Equal(2, (int)rows[0].userId);
        Assert.Equal(210m, (decimal)rows[0].total);
    }

    [Fact]
    public void OrderBy_ShouldSupportAlias_And_Ordinal()
    {
        var rows1 = _cnn.Query<dynamic>("SELECT id, id + 1 AS x FROM users ORDER BY x DESC").ToList();
        Assert.Equal([3,2,1], [.. rows1.Select(r => (int)r.id)]);

        var rows2 = _cnn.Query<dynamic>("SELECT id, name FROM users ORDER BY 2 ASC, 1 DESC").ToList();
        // order by name asc, then id desc
        Assert.Equal([(2,"Bob"),(3,"Jane"),(1,"John")], [.. rows2.Select(r => ((int)r.id,(string)r.name))]);
    }

    [Fact]
    public void Union_ShouldWork()
    {
        var rows = _cnn.Query<dynamic>(
            "SELECT id FROM users WHERE id = 1 " +
            "UNION " +
            "SELECT id FROM users WHERE id = 2 " +
            "ORDER BY id").ToList();
        Assert.Equal([1,2], [.. rows.Select(r => (int)r.id)]);
    }

    [Fact]
    public void Union_Inside_SubSelect_ShouldWork()
    {
        var rows = _cnn.Query<dynamic>(@"
SELECT * FROM (
SELECT id FROM users WHERE id = 1
UNION
SELECT id FROM users WHERE id = 2
) X
ORDER BY id
").ToList();
        Assert.Equal([1, 2], [.. rows.Select(r => (int)r.id)]);
    }

    [Fact]
    public void Cte_With_ShouldWork()
    {
        var rows = _cnn.Query<dynamic>(
            "WITH u AS (SELECT id, name FROM users WHERE id <= 2) " +
            "SELECT id FROM u ORDER BY id DESC").ToList();
        Assert.Equal([2,1], [.. rows.Select(r => (int)r.id)]);
    }

    [Fact]
    public void Typing_ImplicitCasts_And_Collation_ShouldMatchMySqlDefault()
    {
        // Many MySQL installations use case-insensitive collations by default.
        var rows1 = _cnn.Query<dynamic>("SELECT id FROM users WHERE name = 'john'").ToList();
        Assert.Single(rows1);
        Assert.Equal(1, (int)rows1[0].id);

        // Implicit cast string->int for comparison
        var rows2 = _cnn.Query<dynamic>("SELECT id FROM users WHERE id = '2'").ToList();
        Assert.Single(rows2);
        Assert.Equal(2, (int)rows2[0].id);
    }

    protected override void Dispose(bool disposing)
    {
        _cnn?.Dispose();
        base.Dispose(disposing);
    }
}
