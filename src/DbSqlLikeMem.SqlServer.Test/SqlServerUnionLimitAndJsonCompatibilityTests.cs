namespace DbSqlLikeMem.SqlServer.Test;

/// <summary>
/// Tests that lock-in expected behavior for MySQL features that the in-memory mock already supports.
/// Keep these green: they protect you from regressions while you implement more advanced gaps elsewhere.
/// </summary>
public sealed class SqlServerUnionLimitAndJsonCompatibilityTests : XUnitTestBase
{
    private readonly SqlServerConnectionMock _cnn;

    public SqlServerUnionLimitAndJsonCompatibilityTests(ITestOutputHelper helper) : base(helper)
    {
        var db = new SqlServerDbMock();
        var t = db.AddTable("t");
        t.Columns["id"] = new(0, DbType.Int32, false);
        t.Columns["payload"] = new(1, DbType.String, true);
        t.Add(new Dictionary<int, object?> { [0] = 1, [1] = "{\"a\":{\"b\":123}}" });
        t.Add(new Dictionary<int, object?> { [0] = 2, [1] = "{\"a\":{\"b\":456}}" });
        t.Add(new Dictionary<int, object?> { [0] = 3, [1] = null });

        _cnn = new SqlServerConnectionMock(db);
        _cnn.Open();
    }

    [Fact]
    public void UnionAll_ShouldKeepDuplicates_UnionShouldRemoveDuplicates()
    {
        // UNION ALL keeps duplicates
        var all = _cnn.Query<dynamic>(@"
SELECT id FROM t WHERE id = 1
UNION ALL
SELECT id FROM t WHERE id = 1
").ToList();
        Assert.Equal([1, 1], [.. all.Select(r => (int)r.id)]);

        // UNION removes duplicates
        var distinct = _cnn.Query<dynamic>(@"
SELECT id FROM t WHERE id = 1
UNION
SELECT id FROM t WHERE id = 1
").ToList();
        Assert.Equal([1], [.. distinct.Select(r => (int)r.id)]);
    }

    [Fact]
    public void OffsetFetch_ShouldWork()
    {
        // SQL Server: OFFSET/FETCH
        var rows = _cnn.Query<dynamic>("SELECT id FROM t ORDER BY id OFFSET 1 ROWS FETCH NEXT 2 ROWS ONLY").ToList();
        Assert.Equal([2, 3], [.. rows.Select(r => (int)r.id)]);
    }

    [Fact]
    public void JsonValue_SimpleObjectPath_ShouldWork()
    {
        var rows = _cnn.Query<dynamic>("SELECT id, TRY_CAST(JSON_VALUE(payload, '$.a.b') AS DECIMAL(18,0)) AS v FROM t ORDER BY id").ToList();

        // implemented as best-effort; null JSON -> null
        Assert.Equal([123m, 456m, null], [.. rows.Select(r => (object?)r.v)]);
    }

    protected override void Dispose(bool disposing)
    {
        _cnn?.Dispose();
        base.Dispose(disposing);
    }
}
