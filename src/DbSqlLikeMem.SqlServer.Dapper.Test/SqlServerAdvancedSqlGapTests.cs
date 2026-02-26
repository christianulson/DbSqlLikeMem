namespace DbSqlLikeMem.SqlServer.Dapper.Test;

/// <summary>
/// These are TDD "gap" tests for MySQL features that are NOT implemented yet in the in-memory mock.
/// They are intentionally skipped so they don't break your build until you decide to implement them.
/// When you implement a feature, remove the Skip and make it green.
/// </summary>
public sealed class SqlServerAdvancedSqlGapTests : XUnitTestBase
{
    private readonly SqlServerConnectionMock _cnn;

    /// <summary>
    /// EN: Tests SqlServerAdvancedSqlGapTests behavior.
    /// PT: Testa o comportamento de SqlServerAdvancedSqlGapTests.
    /// </summary>
    public SqlServerAdvancedSqlGapTests(ITestOutputHelper helper) : base(helper)
    {
        var db = new SqlServerDbMock();
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

        _cnn = new SqlServerConnectionMock(db);
        _cnn.Open();
    }

    /// <summary>
    /// EN: Tests Window_RowNumber_PartitionBy_ShouldWork behavior.
    /// PT: Testa o comportamento de Window_RowNumber_PartitionBy_ShouldWork.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlServerAdvancedSqlGap")]
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
    [Trait("Category", "SqlServerAdvancedSqlGap")]
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
    [Trait("Category", "SqlServerAdvancedSqlGap")]
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
    [Trait("Category", "SqlServerAdvancedSqlGap")]
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
    [Trait("Category", "SqlServerAdvancedSqlGap")]
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
    [Trait("Category", "SqlServerAdvancedSqlGap")]
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
    /// EN: Tests Window_NthValue_ShouldWork behavior.
    /// PT: Testa o comportamento de Window_NthValue_ShouldWork.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlServerAdvancedSqlGap")]
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
    /// EN: Tests Window_Lag_Lead_WithZeroOffset_ShouldReturnCurrentRow behavior.
    /// PT: Testa o comportamento de Window_Lag_Lead_WithZeroOffset_ShouldReturnCurrentRow.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlServerAdvancedSqlGap")]
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
    /// EN: Tests Regexp_NotOperator_ShouldWork behavior.
    /// PT: Testa o comportamento de Regexp_NotOperator_ShouldWork.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlServerAdvancedSqlGap")]
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
    [Trait("Category", "SqlServerAdvancedSqlGap")]
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
    [Trait("Category", "SqlServerAdvancedSqlGap")]
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
    [Trait("Category", "SqlServerAdvancedSqlGap")]
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
    /// EN: Tests RANGE frame execution for ranking/distribution and lead-lag defaults with numeric ORDER BY.
    /// PT: Testa execução de frame RANGE para ranking/distribuição e defaults de lead-lag com ORDER BY numérico.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlServerAdvancedSqlGap")]
    public void Window_RangeFrame_ShouldWorkForRankingDistributionAndLagLead()
    {
        var rows = _cnn.Query<dynamic>(@"
SELECT id,
       RANK() OVER (ORDER BY id RANGE BETWEEN 1 PRECEDING AND CURRENT ROW) AS rk_range,
       DENSE_RANK() OVER (ORDER BY id RANGE BETWEEN 1 PRECEDING AND CURRENT ROW) AS dr_range,
       PERCENT_RANK() OVER (ORDER BY id RANGE BETWEEN 1 PRECEDING AND CURRENT ROW) AS pr_range,
       CUME_DIST() OVER (ORDER BY id RANGE BETWEEN 1 PRECEDING AND CURRENT ROW) AS cd_range,
       NTILE(2) OVER (ORDER BY id RANGE BETWEEN 1 PRECEDING AND CURRENT ROW) AS ntile_range,
       LAG(id, 1, -1) OVER (ORDER BY id RANGE BETWEEN CURRENT ROW AND 1 FOLLOWING) AS lag_range,
       LEAD(id, 1, 99) OVER (ORDER BY id RANGE BETWEEN 1 PRECEDING AND CURRENT ROW) AS lead_range
FROM users
ORDER BY id").ToList();

        Assert.Equal([1, 2, 2], [.. rows.Select(r => (int)r.rk_range)]);
        Assert.Equal([1, 2, 2], [.. rows.Select(r => (int)r.dr_range)]);
        Assert.Equal([0d, 1d, 1d], [.. rows.Select(r => Convert.ToDouble(r.pr_range))]);
        Assert.Equal([1d, 1d, 1d], [.. rows.Select(r => Convert.ToDouble(r.cd_range))]);
        Assert.Equal([1, 1, 2], [.. rows.Select(r => (int)r.ntile_range)]);
        Assert.Equal([-1, -1, -1], [.. rows.Select(r => (int)r.lag_range)]);
        Assert.Equal([99, 99, 99], [.. rows.Select(r => (int)r.lead_range)]);
    }


    /// <summary>
    /// EN: Tests GROUPS frame execution aligns with peer-group boundaries.
    /// PT: Testa se execução de frame GROUPS respeita os limites de grupos de peers.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlServerAdvancedSqlGap")]
    public void Window_GroupsFrame_ShouldRespectPeerGroups()
    {
        var rows = _cnn.Query<dynamic>(@"
SELECT id,
       RANK() OVER (ORDER BY tenantid GROUPS BETWEEN CURRENT ROW AND CURRENT ROW) AS rk_groups,
       DENSE_RANK() OVER (ORDER BY tenantid GROUPS BETWEEN CURRENT ROW AND CURRENT ROW) AS dr_groups,
       PERCENT_RANK() OVER (ORDER BY tenantid GROUPS BETWEEN CURRENT ROW AND CURRENT ROW) AS pr_groups,
       CUME_DIST() OVER (ORDER BY tenantid GROUPS BETWEEN CURRENT ROW AND CURRENT ROW) AS cd_groups,
       NTILE(2) OVER (ORDER BY tenantid GROUPS BETWEEN CURRENT ROW AND CURRENT ROW) AS ntile_groups
FROM users
ORDER BY id").ToList();

        Assert.Equal([1, 1, 1], [.. rows.Select(r => (int)r.rk_groups)]);
        Assert.Equal([1, 1, 1], [.. rows.Select(r => (int)r.dr_groups)]);
        Assert.Equal([0d, 0d, 0d], [.. rows.Select(r => Convert.ToDouble(r.pr_groups))]);
        Assert.Equal([1d, 1d, 1d], [.. rows.Select(r => Convert.ToDouble(r.cd_groups))]);
        Assert.Equal([1, 2, 1], [.. rows.Select(r => (int)r.ntile_groups)]);
    }


    /// <summary>
    /// EN: Tests RANGE offset with composite ORDER BY throws a clear runtime validation error.
    /// PT: Testa se RANGE com offset e ORDER BY composto lança erro claro de validação em runtime.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlServerAdvancedSqlGap")]
    public void Window_RangeOffset_WithCompositeOrder_ShouldThrowClearError()
    {
        var ex = Assert.Throws<InvalidOperationException>(() =>
            _cnn.Query<dynamic>(@"
SELECT id,
       RANK() OVER (ORDER BY tenantid, id RANGE BETWEEN 1 PRECEDING AND CURRENT ROW) AS rk_bad
FROM users
ORDER BY id").ToList());

        Assert.Contains("single ORDER BY expression", ex.Message, StringComparison.OrdinalIgnoreCase);
    }


    /// <summary>
    /// EN: Tests RANGE CURRENT ROW with composite ORDER BY works without offset-specific numeric requirement.
    /// PT: Testa se RANGE CURRENT ROW com ORDER BY composto funciona sem exigir regra numérica específica de offset.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlServerAdvancedSqlGap")]
    public void Window_RangeCurrentRow_WithCompositeOrder_ShouldWork()
    {
        var rows = _cnn.Query<dynamic>(@"
SELECT id,
       RANK() OVER (ORDER BY tenantid, id RANGE BETWEEN CURRENT ROW AND CURRENT ROW) AS rk_range_current,
       DENSE_RANK() OVER (ORDER BY tenantid, id RANGE BETWEEN CURRENT ROW AND CURRENT ROW) AS dr_range_current,
       CUME_DIST() OVER (ORDER BY tenantid, id RANGE BETWEEN CURRENT ROW AND CURRENT ROW) AS cd_range_current
FROM users
ORDER BY id").ToList();

        Assert.Equal([1, 1, 1], [.. rows.Select(r => (int)r.rk_range_current)]);
        Assert.Equal([1, 1, 1], [.. rows.Select(r => (int)r.dr_range_current)]);
        Assert.Equal([1d, 1d, 1d], [.. rows.Select(r => Convert.ToDouble(r.cd_range_current))]);
    }


    /// <summary>
    /// EN: Tests RANGE CURRENT ROW with non-numeric ORDER BY remains valid because it is peer-based.
    /// PT: Testa se RANGE CURRENT ROW com ORDER BY não numérico continua válido por ser baseado em peers.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlServerAdvancedSqlGap")]
    public void Window_RangeCurrentRow_WithTextOrder_ShouldWork()
    {
        var rows = _cnn.Query<dynamic>(@"
SELECT id,
       RANK() OVER (ORDER BY name RANGE BETWEEN CURRENT ROW AND CURRENT ROW) AS rk_name_range,
       DENSE_RANK() OVER (ORDER BY name RANGE BETWEEN CURRENT ROW AND CURRENT ROW) AS dr_name_range,
       NTILE(2) OVER (ORDER BY name RANGE BETWEEN CURRENT ROW AND CURRENT ROW) AS ntile_name_range
FROM users
ORDER BY id").ToList();

        Assert.Equal([1, 1, 1], [.. rows.Select(r => (int)r.rk_name_range)]);
        Assert.Equal([1, 1, 1], [.. rows.Select(r => (int)r.dr_name_range)]);
        Assert.Equal([1, 2, 1], [.. rows.Select(r => (int)r.ntile_name_range)]);
    }


    /// <summary>
    /// EN: Tests CorrelatedSubquery_InSelectList_ShouldWork behavior.
    /// PT: Testa o comportamento de CorrelatedSubquery_InSelectList_ShouldWork.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlServerAdvancedSqlGap")]
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
    [Trait("Category", "SqlServerAdvancedSqlGap")]
    public void DateAdd_IntervalDay_ShouldWork()
    {
        var rows = _cnn.Query<dynamic>(@"
SELECT id, DATEADD(day, 1, created) AS d
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
    [Trait("Category", "SqlServerAdvancedSqlGap")]
    public void Cast_StringToInt_ShouldWork()
    {
        var rows = _cnn.Query<dynamic>("SELECT CAST('42' AS INT) AS v").ToList();
        Assert.Single(rows);
        Assert.Equal(42, (int)rows[0].v);
    }

    /// <summary>
    /// EN: Tests Regexp_Operator_ShouldWork behavior.
    /// PT: Testa o comportamento de Regexp_Operator_ShouldWork.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlServerAdvancedSqlGap")]
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
    [Trait("Category", "SqlServerAdvancedSqlGap")]
    public void OrderBy_Field_Function_ShouldWork()
    {
        var rows = _cnn.Query<dynamic>("SELECT id FROM users ORDER BY CASE id WHEN 3 THEN 1 WHEN 1 THEN 2 WHEN 2 THEN 3 ELSE 4 END").ToList();
        Assert.Equal([3, 1, 2], [.. rows.Select(r => (int)r.id)]);
    }

    /// <summary>
    /// EN: Tests Collation_CaseSensitivity_ShouldFollowColumnCollation behavior.
    /// PT: Testa o comportamento de Collation_CaseSensitivity_ShouldFollowColumnCollation.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlServerAdvancedSqlGap")]
    public void Collation_CaseSensitivity_ShouldFollowColumnCollation()
    {
        // Example expectation in MySQL: behavior depends on column collation.
        // This is intentionally a gap test — decide the mock rule, then implement it consistently.
        var rows = _cnn.Query<dynamic>("SELECT id FROM users WHERE name = 'john' ORDER BY id").ToList();
        Assert.Equal([1], [.. rows.Select(r => (int)r.id)]);
    }



    /// <summary>
    /// EN: Tests Pivot_Count_ByTenant_ShouldWork behavior.
    /// PT: Testa o comportamento de Pivot_Count_ByTenant_ShouldWork.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlServerAdvancedSqlGap")]
    public void Pivot_Count_ByTenant_ShouldWork()
    {
        var row = _cnn.QuerySingle<dynamic>(@"
SELECT t10, t20
FROM (
    SELECT tenantid, id
    FROM users
) src
PIVOT (
    COUNT(id)
    FOR tenantid IN (10 AS t10, 20 AS t20)
) p");

        Assert.Equal(2, (int)row.t10);
        Assert.Equal(1, (int)row.t20);
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
