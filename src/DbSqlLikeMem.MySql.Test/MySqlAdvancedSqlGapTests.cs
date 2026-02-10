namespace DbSqlLikeMem.MySql.Test;

/// <summary>
/// These are TDD "gap" tests for MySQL features that are NOT implemented yet in the in-memory mock.
/// They are intentionally skipped so they don't break your build until you decide to implement them.
/// When you implement a feature, remove the Skip and make it green.
/// </summary>
public sealed class MySqlAdvancedSqlGapTests : XUnitTestBase
{
    private readonly MySqlConnectionMock _cnn;

    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public MySqlAdvancedSqlGapTests(ITestOutputHelper helper) : base(helper)
    {
        var db = new MySqlDbMock();
        var users = db.AddTable("users");
        users.Columns["id"] = new(0, DbType.Int32, false);
        users.Columns["name"] = new(1, DbType.String, false);
        users.Columns["tenantid"] = new(2, DbType.Int32, false);
        users.Columns["created"] = new(3, DbType.DateTime, false);

        users.Add(new Dictionary<int, object?> { [0] = 1, [1] = "John", [2] = 10, [3] = new DateTime(2020, 1, 1, 0, 0, 0, DateTimeKind.Local) });
        users.Add(new Dictionary<int, object?> { [0] = 2, [1] = "Bob", [2] = 10, [3] = new DateTime(2020, 1, 2, 0, 0, 0, DateTimeKind.Local) });
        users.Add(new Dictionary<int, object?> { [0] = 3, [1] = "Jane", [2] = 20, [3] = new DateTime(2020, 1, 3, 0, 0, 0, DateTimeKind.Local) });

        var orders = db.AddTable("orders");
        orders.Columns["id"] = new(0, DbType.Int32, false);
        orders.Columns["userid"] = new(1, DbType.Int32, false);
        orders.Columns["amount"] = new(2, DbType.Decimal, false);

        orders.Add(new Dictionary<int, object?> { [0] = 10, [1] = 1, [2] = 10m });
        orders.Add(new Dictionary<int, object?> { [0] = 11, [1] = 1, [2] = 5m });
        orders.Add(new Dictionary<int, object?> { [0] = 12, [1] = 2, [2] = 7m });

        _cnn = new MySqlConnectionMock(db);
        _cnn.Open();
    }

    /// <summary>
    /// EN: Tests Window_RowNumber_PartitionBy_ShouldWork behavior.
    /// PT: Testa o comportamento de Window_RowNumber_PartitionBy_ShouldWork.
    /// </summary>
    [Fact]
    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public void Window_RowNumber_PartitionBy_ShouldWork()
    {
        var rows = _cnn.Query<dynamic>(@"
SELECT id, tenantid,
       ROW_NUMBER() OVER (PARTITION BY tenantid ORDER BY id) AS rn
FROM users
ORDER BY tenantid, id").ToList();

        Assert.Equal([1, 2, 1], [.. rows.Select(r => (int)r.rn)]);
    }

    /// <summary>
    /// EN: Tests CorrelatedSubquery_InSelectList_ShouldWork behavior.
    /// PT: Testa o comportamento de CorrelatedSubquery_InSelectList_ShouldWork.
    /// </summary>
    [Fact]
    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public void CorrelatedSubquery_InSelectList_ShouldWork()
    {
        var rows = _cnn.Query<dynamic>(@"
SELECT u.id,
       (SELECT SUM(o.amount) FROM orders o WHERE o.userid = u.id) AS total
FROM users u
ORDER BY u.id").ToList();

        Assert.Equal([15m, 7m, 0m], [.. rows.Select(r => (decimal)(r.total ?? 0m))]);
    }

    /// <summary>
    /// EN: Tests DateAdd_IntervalDay_ShouldWork behavior.
    /// PT: Testa o comportamento de DateAdd_IntervalDay_ShouldWork.
    /// </summary>
    [Fact]
    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public void DateAdd_IntervalDay_ShouldWork()
    {
        var rows = _cnn.Query<dynamic>(@"
SELECT id, DATE_ADD(created, INTERVAL 1 DAY) AS d
FROM users
ORDER BY id").ToList();

        Assert.Equal([
            new DateTime(2020, 1, 2, 0, 0, 0, DateTimeKind.Local),
            new DateTime(2020, 1, 3, 0, 0, 0, DateTimeKind.Local),
            new DateTime(2020, 1, 4, 0, 0, 0, DateTimeKind.Local)],
            [.. rows.Select(r => (DateTime)r.d)]);
    }

    /// <summary>
    /// EN: Tests Cast_StringToInt_ShouldWork behavior.
    /// PT: Testa o comportamento de Cast_StringToInt_ShouldWork.
    /// </summary>
    [Fact]
    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public void Cast_StringToInt_ShouldWork()
    {
        var rows = _cnn.Query<dynamic>("SELECT CAST('42' AS SIGNED) AS v").ToList();
        Assert.Single(rows);
        Assert.Equal(42, (int)rows[0].v);
    }

    /// <summary>
    /// EN: Tests Regexp_Operator_ShouldWork behavior.
    /// PT: Testa o comportamento de Regexp_Operator_ShouldWork.
    /// </summary>
    [Fact]
    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public void Regexp_Operator_ShouldWork()
    {
        var rows = _cnn.Query<dynamic>("SELECT id FROM users WHERE name REGEXP '^J' ORDER BY id").ToList();
        Assert.Equal([1, 3], [.. rows.Select(r => (int)r.id)]);
    }

    /// <summary>
    /// EN: Tests OrderBy_Field_Function_ShouldWork behavior.
    /// PT: Testa o comportamento de OrderBy_Field_Function_ShouldWork.
    /// </summary>
    [Fact]
    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public void OrderBy_Field_Function_ShouldWork()
    {
        var rows = _cnn.Query<dynamic>("SELECT id FROM users ORDER BY FIELD(id, 3, 1, 2)").ToList();
        Assert.Equal([3, 1, 2], [.. rows.Select(r => (int)r.id)]);
    }

    /// <summary>
    /// EN: Tests Collation_CaseSensitivity_ShouldFollowColumnCollation behavior.
    /// PT: Testa o comportamento de Collation_CaseSensitivity_ShouldFollowColumnCollation.
    /// </summary>
    [Fact]
    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public void Collation_CaseSensitivity_ShouldFollowColumnCollation()
    {
        // Example expectation in MySQL: behavior depends on column collation.
        // This is intentionally a gap test — decide the mock rule, then implement it consistently.
        var rows = _cnn.Query<dynamic>("SELECT id FROM users WHERE name = 'john' ORDER BY id").ToList();
        Assert.Equal([1], [.. rows.Select(r => (int)r.id)]);
    }

    /// <summary>
    /// EN: Disposes test resources.
    /// PT: Descarta os recursos do teste.
    /// </summary>
    /// <param name="disposing">EN: True to dispose managed resources. PT: True para descartar recursos gerenciados.</param>
    protected override void Dispose(bool disposing)
    {
        _cnn?.Dispose();
        base.Dispose(disposing);
    }
}
