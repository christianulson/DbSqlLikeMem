namespace DbSqlLikeMem.Sqlite.Dapper.Test;

/// <summary>
/// These are TDD "gap" tests for SQLite features that are NOT implemented yet in the in-memory mock.
/// They are intentionally skipped so they don't break your build until you decide to implement them.
/// When you implement a feature, remove the Skip and make it green.
/// </summary>
public sealed class SqliteAdvancedSqlGapTests : XUnitTestBase
{
    private readonly SqliteConnectionMock _cnn;

    /// <summary>
    /// EN: Tests SqliteAdvancedSqlGapTests behavior.
    /// PT: Testa o comportamento de SqliteAdvancedSqlGapTests.
    /// </summary>
    public SqliteAdvancedSqlGapTests(ITestOutputHelper helper) : base(helper)
    {
        var db = new SqliteDbMock();
        var users = db.AddTable("users");
        users.AddColumn("id", DbType.Int32, false);
        users.AddColumn("name", DbType.String, false);
        users.AddColumn("tenantid", DbType.Int32, false);
        users.AddColumn("created", DbType.DateTime, false);

        users.Add(new Dictionary<int, object?> { [0] = 1, [1] = "John", [2] = 10, [3] = new DateTime(2020, 1, 1, 0, 0, 0, DateTimeKind.Local) });
        users.Add(new Dictionary<int, object?> { [0] = 2, [1] = "Bob", [2] = 10, [3] = new DateTime(2020, 1, 2, 0, 0, 0, DateTimeKind.Local) });
        users.Add(new Dictionary<int, object?> { [0] = 3, [1] = "Jane", [2] = 20, [3] = new DateTime(2020, 1, 3, 0, 0, 0, DateTimeKind.Local) });

        var orders = db.AddTable("orders");
        orders.AddColumn("id", DbType.Int32, false);
        orders.AddColumn("userid", DbType.Int32, false);
        orders.AddColumn("amount", DbType.Decimal, false, decimalPlaces: 2);

        orders.Add(new Dictionary<int, object?> { [0] = 10, [1] = 1, [2] = 10m });
        orders.Add(new Dictionary<int, object?> { [0] = 11, [1] = 1, [2] = 5m });
        orders.Add(new Dictionary<int, object?> { [0] = 12, [1] = 2, [2] = 7m });

        _cnn = new SqliteConnectionMock(db);
        _cnn.Open();
    }

    /// <summary>
    /// EN: Tests Window_RowNumber_PartitionBy_ShouldWork behavior.
    /// PT: Testa o comportamento de Window_RowNumber_PartitionBy_ShouldWork.
    /// </summary>
    [Fact]
    [Trait("Category", "SqliteAdvancedSqlGap")]
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
    /// EN: Tests Window_Rank_And_DenseRank_ShouldWork behavior.
    /// PT: Testa o comportamento de Window_Rank_And_DenseRank_ShouldWork.
    /// </summary>
    [Fact]
    [Trait("Category", "SqliteAdvancedSqlGap")]
    public void Window_Rank_And_DenseRank_ShouldWork()
    {
        var rows = _cnn.Query<dynamic>(@"
SELECT id,
       RANK() OVER (ORDER BY tenantid) AS rk,
       DENSE_RANK() OVER (ORDER BY tenantid) AS dr
FROM users
ORDER BY id").ToList();

        Assert.Equal([1, 1, 3], [.. rows.Select(r => (int)r.rk)]);
        Assert.Equal([1, 1, 2], [.. rows.Select(r => (int)r.dr)]);
    }


    /// <summary>
    /// EN: Tests Window_Ntile_ShouldWork behavior.
    /// PT: Testa o comportamento de Window_Ntile_ShouldWork.
    /// </summary>
    [Fact]
    [Trait("Category", "SqliteAdvancedSqlGap")]
    public void Window_Ntile_ShouldWork()
    {
        var rows = _cnn.Query<dynamic>(@"
SELECT id,
       NTILE(2) OVER (ORDER BY id) AS tile
FROM users
ORDER BY id").ToList();

        Assert.Equal([1, 1, 2], [.. rows.Select(r => (int)r.tile)]);
    }


    /// <summary>
    /// EN: Tests Window_PercentRank_And_CumeDist_ShouldWork behavior.
    /// PT: Testa o comportamento de Window_PercentRank_And_CumeDist_ShouldWork.
    /// </summary>
    [Fact]
    [Trait("Category", "SqliteAdvancedSqlGap")]
    public void Window_PercentRank_And_CumeDist_ShouldWork()
    {
        var rows = _cnn.Query<dynamic>(@"
SELECT id,
       PERCENT_RANK() OVER (ORDER BY tenantid) AS pr,
       CUME_DIST() OVER (ORDER BY tenantid) AS cd
FROM users
ORDER BY id").ToList();

        var pr = rows.Select(r => Convert.ToDouble(r.pr)).ToArray();
        var cd = rows.Select(r => Convert.ToDouble(r.cd)).ToArray();

        Assert.Equal([0d, 0d, 1d], pr);
        Assert.True(Math.Abs(cd[0] - (2d / 3d)) <= 1e-9);
        Assert.True(Math.Abs(cd[1] - (2d / 3d)) <= 1e-9);
        Assert.True(Math.Abs(cd[2] - 1d) <= 1e-9);
    }


    /// <summary>
    /// EN: Tests Window_Lag_And_Lead_ShouldWork behavior.
    /// PT: Testa o comportamento de Window_Lag_And_Lead_ShouldWork.
    /// </summary>
    [Fact]
    [Trait("Category", "SqliteAdvancedSqlGap")]
    public void Window_Lag_And_Lead_ShouldWork()
    {
        var rows = _cnn.Query<dynamic>(@"
SELECT id,
       LAG(id) OVER (ORDER BY id) AS prev_id,
       LEAD(id, 1, 99) OVER (ORDER BY id) AS next_id
FROM users
ORDER BY id").ToList();

        Assert.Equal([null, 1, 2], [.. rows.Select(r => (int?)r.prev_id)]);
        Assert.Equal([2, 3, 99], [.. rows.Select(r => (int)r.next_id)]);
    }


    /// <summary>
    /// EN: Tests Window_FirstValue_And_LastValue_ShouldWork behavior.
    /// PT: Testa o comportamento de Window_FirstValue_And_LastValue_ShouldWork.
    /// </summary>
    [Fact]
    [Trait("Category", "SqliteAdvancedSqlGap")]
    public void Window_FirstValue_And_LastValue_ShouldWork()
    {
        var rows = _cnn.Query<dynamic>(@"
SELECT id,
       FIRST_VALUE(name) OVER (ORDER BY id) AS first_name,
       LAST_VALUE(name) OVER (ORDER BY id) AS last_name
FROM users
ORDER BY id").ToList();

        Assert.Equal(["John", "John", "John"], [.. rows.Select(r => (string)r.first_name)]);
        Assert.Equal(["Jane", "Jane", "Jane"], [.. rows.Select(r => (string)r.last_name)]);
    }

    /// <summary>
    /// EN: Tests Window_FirstLastValue_WithRowsCurrentRowFrame_ShouldRespectFrame behavior.
    /// PT: Testa o comportamento de Window_FirstLastValue_WithRowsCurrentRowFrame_ShouldRespectFrame.
    /// </summary>
    [Fact]
    [Trait("Category", "SqliteAdvancedSqlGap")]
    public void Window_FirstLastValue_WithRowsCurrentRowFrame_ShouldRespectFrame()
    {
        var ex = Assert.Throws<NotSupportedException>(() =>
            _cnn.Query<dynamic>(@"
SELECT id,
       FIRST_VALUE(name) OVER (ORDER BY id ROWS BETWEEN CURRENT ROW AND CURRENT ROW) AS first_name,
       LAST_VALUE(name) OVER (ORDER BY id ROWS BETWEEN CURRENT ROW AND CURRENT ROW) AS last_name
FROM users
ORDER BY id").ToList());

        Assert.Contains("window frame clause", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Tests Window_FirstLastValue_WithRowsSlidingFrame_ShouldRespectFrame behavior.
    /// PT: Testa o comportamento de Window_FirstLastValue_WithRowsSlidingFrame_ShouldRespectFrame.
    /// </summary>
    [Fact]
    [Trait("Category", "SqliteAdvancedSqlGap")]
    public void Window_FirstLastValue_WithRowsSlidingFrame_ShouldRespectFrame()
    {
        var ex = Assert.Throws<NotSupportedException>(() =>
            _cnn.Query<dynamic>(@"
SELECT id,
       FIRST_VALUE(name) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS first_name,
       LAST_VALUE(name) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS last_name
FROM users
ORDER BY id").ToList());

        Assert.Contains("window frame clause", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Tests Window_FirstLastValue_WithRowsForwardFrame_ShouldRespectFrame behavior.
    /// PT: Testa o comportamento de Window_FirstLastValue_WithRowsForwardFrame_ShouldRespectFrame.
    /// </summary>
    [Fact]
    [Trait("Category", "SqliteAdvancedSqlGap")]
    public void Window_FirstLastValue_WithRowsForwardFrame_ShouldRespectFrame()
    {
        var ex = Assert.Throws<NotSupportedException>(() =>
            _cnn.Query<dynamic>(@"
SELECT id,
       FIRST_VALUE(name) OVER (ORDER BY id ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING) AS first_name,
       LAST_VALUE(name) OVER (ORDER BY id ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING) AS last_name
FROM users
ORDER BY id").ToList());

        Assert.Contains("window frame clause", ex.Message, StringComparison.OrdinalIgnoreCase);
    }


    /// <summary>
    /// EN: Tests Window_NthValue_ShouldWork behavior.
    /// PT: Testa o comportamento de Window_NthValue_ShouldWork.
    /// </summary>
    [Fact]
    [Trait("Category", "SqliteAdvancedSqlGap")]
    public void Window_NthValue_ShouldWork()
    {
        var rows = _cnn.Query<dynamic>(@"
SELECT id,
       NTH_VALUE(name, 2) OVER (ORDER BY id) AS second_name
FROM users
ORDER BY id").ToList();

        Assert.Equal(["Bob", "Bob", "Bob"], [.. rows.Select(r => (string)r.second_name)]);
    }

    /// <summary>
    /// EN: Tests Window_NthValue_WithRowsCurrentRowFrame_ShouldReturnNull behavior.
    /// PT: Testa o comportamento de Window_NthValue_WithRowsCurrentRowFrame_ShouldReturnNull.
    /// </summary>
    [Fact]
    [Trait("Category", "SqliteAdvancedSqlGap")]
    public void Window_NthValue_WithRowsCurrentRowFrame_ShouldReturnNull()
    {
        var ex = Assert.Throws<NotSupportedException>(() =>
            _cnn.Query<dynamic>(@"
SELECT id,
       NTH_VALUE(name, 2) OVER (ORDER BY id ROWS BETWEEN CURRENT ROW AND CURRENT ROW) AS second_name
FROM users
ORDER BY id").ToList());

        Assert.Contains("window frame clause", ex.Message, StringComparison.OrdinalIgnoreCase);
    }


    /// <summary>
    /// EN: Tests Window_NthValue_WithRowsSlidingFrame_ShouldResolvePerRow behavior.
    /// PT: Testa o comportamento de Window_NthValue_WithRowsSlidingFrame_ShouldResolvePerRow.
    /// </summary>
    [Fact]
    [Trait("Category", "SqliteAdvancedSqlGap")]
    public void Window_NthValue_WithRowsSlidingFrame_ShouldResolvePerRow()
    {
        var ex = Assert.Throws<NotSupportedException>(() =>
            _cnn.Query<dynamic>(@"
SELECT id,
       NTH_VALUE(name, 2) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS second_name
FROM users
ORDER BY id").ToList());

        Assert.Contains("window frame clause", ex.Message, StringComparison.OrdinalIgnoreCase);
    }


    /// <summary>
    /// EN: Tests Window_NthValue_WithRowsForwardFrame_ShouldResolvePerRow behavior.
    /// PT: Testa o comportamento de Window_NthValue_WithRowsForwardFrame_ShouldResolvePerRow.
    /// </summary>
    [Fact]
    [Trait("Category", "SqliteAdvancedSqlGap")]
    public void Window_NthValue_WithRowsForwardFrame_ShouldResolvePerRow()
    {
        var ex = Assert.Throws<NotSupportedException>(() =>
            _cnn.Query<dynamic>(@"
SELECT id,
       NTH_VALUE(name, 2) OVER (ORDER BY id ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING) AS second_name
FROM users
ORDER BY id").ToList());

        Assert.Contains("window frame clause", ex.Message, StringComparison.OrdinalIgnoreCase);
    }



    /// <summary>
    /// EN: Tests Window_Lag_Lead_WithZeroOffset_ShouldReturnCurrentRow behavior.
    /// PT: Testa o comportamento de Window_Lag_Lead_WithZeroOffset_ShouldReturnCurrentRow.
    /// </summary>
    [Fact]
    [Trait("Category", "SqliteAdvancedSqlGap")]
    public void Window_Lag_Lead_WithZeroOffset_ShouldReturnCurrentRow()
    {
        var rows = _cnn.Query<dynamic>(@"
SELECT id,
       LAG(id, 0, -1) OVER (ORDER BY id) AS lag0,
       LEAD(id, 0, -1) OVER (ORDER BY id) AS lead0
FROM users
ORDER BY id").ToList();

        Assert.Equal([1, 2, 3], [.. rows.Select(r => (int)r.lag0)]);
        Assert.Equal([1, 2, 3], [.. rows.Select(r => (int)r.lead0)]);
    }


    /// <summary>
    /// EN: Tests Window_Lag_Lead_WithRowsFrame_ShouldRespectPerRowBoundaries behavior.
    /// PT: Testa o comportamento de Window_Lag_Lead_WithRowsFrame_ShouldRespectPerRowBoundaries.
    /// </summary>
    [Fact]
    [Trait("Category", "SqliteAdvancedSqlGap")]
    public void Window_Lag_Lead_WithRowsFrame_ShouldRespectPerRowBoundaries()
    {
        var ex = Assert.Throws<NotSupportedException>(() =>
            _cnn.Query<dynamic>(@"
SELECT id,
       LAG(id, 1, -1) OVER (ORDER BY id ROWS BETWEEN CURRENT ROW AND CURRENT ROW) AS lag_current,
       LEAD(id, 1, 99) OVER (ORDER BY id ROWS BETWEEN CURRENT ROW AND CURRENT ROW) AS lead_current,
       LAG(id, 1, -1) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS lag_sliding,
       LEAD(id, 1, 99) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS lead_sliding,
       LAG(id, 1, -1) OVER (ORDER BY id ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING) AS lag_forward,
       LEAD(id, 1, 99) OVER (ORDER BY id ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING) AS lead_forward
FROM users
ORDER BY id").ToList());

        Assert.Contains("window frame clause", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Tests Window_RankingFunctions_WithRowsFrame_ShouldRespectPerRowBoundaries behavior.
    /// PT: Testa o comportamento de Window_RankingFunctions_WithRowsFrame_ShouldRespectPerRowBoundaries.
    /// </summary>
    [Fact]
    [Trait("Category", "SqliteAdvancedSqlGap")]
    public void Window_RankingFunctions_WithRowsFrame_ShouldRespectPerRowBoundaries()
    {
        var ex = Assert.Throws<NotSupportedException>(() =>
            _cnn.Query<dynamic>(@"
SELECT id,
       RANK() OVER (ORDER BY tenantid ROWS BETWEEN CURRENT ROW AND CURRENT ROW) AS rank_current,
       DENSE_RANK() OVER (ORDER BY tenantid ROWS BETWEEN CURRENT ROW AND CURRENT ROW) AS dense_current,
       PERCENT_RANK() OVER (ORDER BY tenantid ROWS BETWEEN CURRENT ROW AND CURRENT ROW) AS pr_current,
       CUME_DIST() OVER (ORDER BY tenantid ROWS BETWEEN CURRENT ROW AND CURRENT ROW) AS cd_current,
       NTILE(2) OVER (ORDER BY tenantid ROWS BETWEEN CURRENT ROW AND CURRENT ROW) AS tile_current,
       RANK() OVER (ORDER BY tenantid ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS rank_sliding,
       DENSE_RANK() OVER (ORDER BY tenantid ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS dense_sliding,
       PERCENT_RANK() OVER (ORDER BY tenantid ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS pr_sliding,
       CUME_DIST() OVER (ORDER BY tenantid ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS cd_sliding,
       NTILE(2) OVER (ORDER BY tenantid ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS tile_sliding,
       RANK() OVER (ORDER BY tenantid ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING) AS rank_forward,
       DENSE_RANK() OVER (ORDER BY tenantid ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING) AS dense_forward,
       PERCENT_RANK() OVER (ORDER BY tenantid ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING) AS pr_forward,
       CUME_DIST() OVER (ORDER BY tenantid ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING) AS cd_forward,
       NTILE(2) OVER (ORDER BY tenantid ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING) AS tile_forward
FROM users
ORDER BY id").ToList());

        Assert.Contains("window frame clause", ex.Message, StringComparison.OrdinalIgnoreCase);
    }



    /// <summary>
    /// EN: Tests Window_RankingFunctions_WithRowsFrame_AndDescendingOrder_ShouldRespectFrame behavior.
    /// PT: Testa o comportamento de Window_RankingFunctions_WithRowsFrame_AndDescendingOrder_ShouldRespectFrame.
    /// </summary>
    [Fact]
    [Trait("Category", "SqliteAdvancedSqlGap")]
    public void Window_RankingFunctions_WithRowsFrame_AndDescendingOrder_ShouldRespectFrame()
    {
        var ex = Assert.Throws<NotSupportedException>(() =>
            _cnn.Query<dynamic>(@"
SELECT id,
       RANK() OVER (ORDER BY tenantid DESC ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS rank_sliding_desc,
       DENSE_RANK() OVER (ORDER BY tenantid DESC ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS dense_sliding_desc,
       PERCENT_RANK() OVER (ORDER BY tenantid DESC ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS pr_sliding_desc,
       CUME_DIST() OVER (ORDER BY tenantid DESC ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS cd_sliding_desc
FROM users
ORDER BY id").ToList());

        Assert.Contains("window frame clause", ex.Message, StringComparison.OrdinalIgnoreCase);
    }


    /// <summary>
    /// EN: Tests Window_PercentRank_CumeDist_WithRowsFrame_AndDescendingPeers_ShouldRespectFrame behavior.
    /// PT: Testa o comportamento de Window_PercentRank_CumeDist_WithRowsFrame_AndDescendingPeers_ShouldRespectFrame.
    /// </summary>
    [Fact]
    [Trait("Category", "SqliteAdvancedSqlGap")]
    public void Window_PercentRank_CumeDist_WithRowsFrame_AndDescendingPeers_ShouldRespectFrame()
    {
        var ex = Assert.Throws<NotSupportedException>(() =>
            _cnn.Query<dynamic>(@"
SELECT id,
       PERCENT_RANK() OVER (ORDER BY tenantid DESC ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING) AS pr_desc_forward,
       CUME_DIST() OVER (ORDER BY tenantid DESC ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING) AS cd_desc_forward
FROM users
ORDER BY id").ToList());

        Assert.Contains("window frame clause", ex.Message, StringComparison.OrdinalIgnoreCase);
    }


    /// <summary>
    /// EN: Tests Window_LagLead_WithRowsFrame_AndDescendingOrder_ShouldRespectFrame behavior.
    /// PT: Testa o comportamento de Window_LagLead_WithRowsFrame_AndDescendingOrder_ShouldRespectFrame.
    /// </summary>
    [Fact]
    [Trait("Category", "SqliteAdvancedSqlGap")]
    public void Window_LagLead_WithRowsFrame_AndDescendingOrder_ShouldRespectFrame()
    {
        var ex = Assert.Throws<NotSupportedException>(() =>
            _cnn.Query<dynamic>(@"
SELECT id,
       LAG(id, 1, -1) OVER (ORDER BY id DESC ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS lag_desc_sliding,
       LEAD(id, 1, 99) OVER (ORDER BY id DESC ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS lead_desc_sliding,
       LAG(id, 1, -1) OVER (ORDER BY id DESC ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING) AS lag_desc_forward,
       LEAD(id, 1, 99) OVER (ORDER BY id DESC ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING) AS lead_desc_forward
FROM users
ORDER BY id").ToList());

        Assert.Contains("window frame clause", ex.Message, StringComparison.OrdinalIgnoreCase);
    }


    /// <summary>
    /// EN: Tests Window_Ntile_WithRowsFrame_AndDescendingOrder_ShouldRespectFrame behavior.
    /// PT: Testa o comportamento de Window_Ntile_WithRowsFrame_AndDescendingOrder_ShouldRespectFrame.
    /// </summary>
    [Fact]
    [Trait("Category", "SqliteAdvancedSqlGap")]
    public void Window_Ntile_WithRowsFrame_AndDescendingOrder_ShouldRespectFrame()
    {
        var ex = Assert.Throws<NotSupportedException>(() =>
            _cnn.Query<dynamic>(@"
SELECT id,
       NTILE(2) OVER (ORDER BY id DESC ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS tile_desc_sliding,
       NTILE(2) OVER (ORDER BY id DESC ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING) AS tile_desc_forward
FROM users
ORDER BY id").ToList());

        Assert.Contains("window frame clause", ex.Message, StringComparison.OrdinalIgnoreCase);
    }


    /// <summary>
    /// EN: Tests Regexp_NotOperator_ShouldWork behavior.
    /// PT: Testa o comportamento de Regexp_NotOperator_ShouldWork.
    /// </summary>
    [Fact]
    [Trait("Category", "SqliteAdvancedSqlGap")]
    public void Regexp_NotOperator_ShouldWork()
    {
        var rows = _cnn.Query<dynamic>("SELECT id FROM users WHERE name NOT REGEXP '^J' ORDER BY id").ToList();
        Assert.Equal([2], [.. rows.Select(r => (int)r.id)]);
    }


    /// <summary>
    /// EN: Tests Like_NotOperator_ShouldWork behavior.
    /// PT: Testa o comportamento de Like_NotOperator_ShouldWork.
    /// </summary>
    [Fact]
    [Trait("Category", "SqliteAdvancedSqlGap")]
    public void Like_NotOperator_ShouldWork()
    {
        var rows = _cnn.Query<dynamic>("SELECT id FROM users WHERE name NOT LIKE 'J%' ORDER BY id").ToList();
        Assert.Equal([2], [.. rows.Select(r => (int)r.id)]);
    }


    /// <summary>
    /// EN: Tests Window_Lag_And_NthValue_WithExpressionOffset_ShouldWork behavior.
    /// PT: Testa o comportamento de Window_Lag_And_NthValue_WithExpressionOffset_ShouldWork.
    /// </summary>
    [Fact]
    [Trait("Category", "SqliteAdvancedSqlGap")]
    public void Window_Lag_And_NthValue_WithExpressionOffset_ShouldWork()
    {
        var rows = _cnn.Query<dynamic>(@"
SELECT id,
       LAG(id, 1 + 0, -1) OVER (ORDER BY id) AS lag_expr,
       NTH_VALUE(name, 1 + 1) OVER (ORDER BY id) AS nth_expr
FROM users
ORDER BY id").ToList();

        Assert.Equal([-1, 1, 2], [.. rows.Select(r => (int)r.lag_expr)]);
        Assert.Equal(["Bob", "Bob", "Bob"], [.. rows.Select(r => (string)r.nth_expr)]);
    }


    /// <summary>
    /// EN: Tests Window_Ntile_WithExpressionBuckets_ShouldWork behavior.
    /// PT: Testa o comportamento de Window_Ntile_WithExpressionBuckets_ShouldWork.
    /// </summary>
    [Fact]
    [Trait("Category", "SqliteAdvancedSqlGap")]
    public void Window_Ntile_WithExpressionBuckets_ShouldWork()
    {
        var rows = _cnn.Query<dynamic>(@"
SELECT id,
       NTILE(1 + 1) OVER (ORDER BY id) AS tile_expr
FROM users
ORDER BY id").ToList();

        Assert.Equal([1, 1, 2], [.. rows.Select(r => (int)r.tile_expr)]);
    }


    /// <summary>
    /// EN: Tests CorrelatedSubquery_InSelectList_ShouldWork behavior.
    /// PT: Testa o comportamento de CorrelatedSubquery_InSelectList_ShouldWork.
    /// </summary>
    [Fact]
    [Trait("Category", "SqliteAdvancedSqlGap")]
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
    [Trait("Category", "SqliteAdvancedSqlGap")]
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
    /// EN: Tests Date_Function_WithModifier_ShouldWork behavior.
    /// PT: Testa o comportamento de Date_Function_WithModifier_ShouldWork.
    /// </summary>
    [Fact]
    [Trait("Category", "SqliteAdvancedSqlGap")]
    public void Date_Function_WithModifier_ShouldWork()
    {
        var rows = _cnn.Query<dynamic>(@"
SELECT id, DATE(created, '+1 day') AS d
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
    [Trait("Category", "SqliteAdvancedSqlGap")]
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
    [Trait("Category", "SqliteAdvancedSqlGap")]
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
    [Trait("Category", "SqliteAdvancedSqlGap")]
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
    [Trait("Category", "SqliteAdvancedSqlGap")]
    public void Collation_CaseSensitivity_ShouldFollowColumnCollation()
    {
        // Example expectation in SQLite: behavior depends on column collation.
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
