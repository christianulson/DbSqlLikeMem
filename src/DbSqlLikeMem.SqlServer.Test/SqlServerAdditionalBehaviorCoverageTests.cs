namespace DbSqlLikeMem.SqlServer.Test;

public sealed class SqlServerAdditionalBehaviorCoverageTests : XUnitTestBase
{
    private readonly SqlServerConnectionMock _cnn;
    private static readonly int[] param = [1, 3];
    private static readonly int[] paramArray = [1, 2];

    public SqlServerAdditionalBehaviorCoverageTests(
        ITestOutputHelper helper
        ) : base(helper)
    {
        // users
        var db = new SqlServerDbMock();
        var users = db.AddTable("users");
        users.Columns["id"] = new(0, DbType.Int32, false) { Identity = false };
        users.Columns["name"] = new(1, DbType.String, false);
        users.Columns["email"] = new(2, DbType.String, true);

        users.Add(new Dictionary<int, object?> { [0] = 1, [1] = "John", [2] = "john@x.com" });
        users.Add(new Dictionary<int, object?> { [0] = 2, [1] = "Bob", [2] = null });
        users.Add(new Dictionary<int, object?> { [0] = 3, [1] = "Jane", [2] = "jane@x.com" });

        // orders
        var orders = db.AddTable("orders");
        orders.Columns["id"] = new(0, DbType.Int32, false) { Identity = false };
        orders.Columns["userid"] = new(1, DbType.Int32, false);
        orders.Columns["amount"] = new(2, DbType.Decimal, false);

        orders.Add(new Dictionary<int, object?> { [0] = 10, [1] = 1, [2] = 50m });
        orders.Add(new Dictionary<int, object?> { [0] = 11, [1] = 2, [2] = 200m });
        orders.Add(new Dictionary<int, object?> { [0] = 12, [1] = 2, [2] = 10m });

        _cnn = new SqlServerConnectionMock(db);

        _cnn.Open();
    }

    [Fact]
    public void Where_IsNull_And_IsNotNull_ShouldWork()
    {
        var nullIds = _cnn.Query<int>("SELECT id FROM users WHERE email IS NULL ORDER BY id").ToList();
        Assert.Equal([2], nullIds);

        var notNullIds = _cnn.Query<int>("SELECT id FROM users WHERE email IS NOT NULL ORDER BY id").ToList();
        Assert.Equal([1, 3], notNullIds);
    }

    [Fact]
    public void Where_EqualNull_ShouldReturnNoRows()
    {
        // MySQL: any comparison with NULL yields UNKNOWN (i.e., filtered out in WHERE)
        var ids = _cnn.Query<int>("SELECT id FROM users WHERE email = NULL").ToList();
        Assert.Empty(ids);

        ids = _cnn.Query<int>("SELECT id FROM users WHERE email <> NULL").ToList();
        Assert.Empty(ids);
    }

    [Fact]
    public void LeftJoin_ShouldPreserveLeftRows_WhenNoMatch()
    {
        var rows = _cnn.Query<dynamic>(@"
SELECT u.id, o.amount
FROM users u
LEFT JOIN orders o ON o.userid = u.id AND o.amount > 100
ORDER BY u.id
").ToList();

        Assert.Equal(3, rows.Count);

        Assert.Equal(1, (int)rows[0].id);
        Assert.Null((object?)rows[0].amount);

        Assert.Equal(2, (int)rows[1].id);
        Assert.Equal(200m, (decimal)rows[1].amount);

        Assert.Equal(3, (int)rows[2].id);
        Assert.Null((object?)rows[2].amount);
    }

    [Fact]
    public void OrderBy_Desc_ThenAsc_ShouldBeDeterministic()
    {
        var rows = _cnn.Query<dynamic>(@"
SELECT id, amount
FROM orders
ORDER BY amount DESC, id ASC
").ToList();

        Assert.Equal([11, 10, 12], [.. rows.Select(r => (int)r.id)]);
        Assert.Equal([200m, 50m, 10m], [.. rows.Select(r => (decimal)r.amount)]);
    }

    [Fact]
    public void Aggregation_CountStar_Vs_CountColumn_ShouldRespectNulls()
    {
        var r = _cnn.QuerySingle<dynamic>("SELECT COUNT(*) c1, COUNT(email) c2 FROM users");

        Assert.Equal(3L, (long)r.c1);
        Assert.Equal(2L, (long)r.c2);
    }

    [Fact]
    public void Having_ShouldFilterGroups()
    {
        var userIds = _cnn.Query<int>(@"
SELECT userid
FROM orders
GROUP BY userid
HAVING SUM(amount) > 100
ORDER BY userid
").ToList();

        Assert.Equal([2], userIds);
    }

    [Fact]
    public void Where_In_WithParameterList_ShouldWork()
    {
        var ids = _cnn.Query<int>("SELECT id FROM users WHERE id IN @ids ORDER BY id", new { ids = param }).ToList();
        Assert.Equal([1, 3], ids);
    }

    [Fact]
    public void Insert_WithColumnsOutOfOrder_ShouldMapCorrectly()
    {
        _cnn.Execute("INSERT INTO users (name, id, email) VALUES (@name, @id, @email)", new { id = 4, name = "Zed", email = "zed@x.com" });

        var row = _cnn.QuerySingle<dynamic>("SELECT id, name, email FROM users WHERE id = 4");
        Assert.Equal(4, (int)row.id);
        Assert.Equal("Zed", (string)row.name);
        Assert.Equal("zed@x.com", (string)row.email);
    }

    [Fact]
    public void Delete_WithInParameterList_ShouldDeleteMatchingRows()
    {
        var deleted = _cnn.Execute("DELETE FROM users WHERE id IN @ids", new { ids = param });
        Assert.Equal(2, deleted);

        var remaining = _cnn.Query<int>("SELECT id FROM users ORDER BY id").ToList();
        Assert.Equal([2], remaining);
    }

    [Fact]
    public void Update_SetExpression_ShouldUpdateRows()
    {
        var db = new SqlServerDbMock();
        var users = db.AddTable("users");
        users.Columns["id"] = new(0, DbType.Int32, false) { Identity = false };
        users.Columns["counter"] = new(1, DbType.Int32, false);

        users.Add(new Dictionary<int, object?> { [0] = 1, [1] = 0 });
        users.Add(new Dictionary<int, object?> { [0] = 2, [1] = 0 });
        users.Add(new Dictionary<int, object?> { [0] = 3, [1] = 0 });

        using var cnn = new SqlServerConnectionMock(db);
        cnn.Open();

        var updated = cnn.Execute("UPDATE users SET counter = counter + 1 WHERE id IN @ids", new { ids = paramArray });

        Assert.Equal(2, updated);

        var counters = cnn.Query<dynamic>("SELECT id, counter FROM users ORDER BY id").ToList();
        Assert.Equal(1, (int)counters[0].counter);
        Assert.Equal(1, (int)counters[1].counter);
        Assert.Equal(0, (int)counters[2].counter);
    }

    protected override void Dispose(bool disposing)
    {
        _cnn?.Dispose();
        base.Dispose(disposing);
    }
}
