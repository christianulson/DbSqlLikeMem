namespace DbSqlLikeMem.Db2.Dapper.Test;

/// <summary>
/// These are TDD "gap" tests for DB2 features that are NOT implemented yet in the in-memory mock.
/// They are intentionally skipped so they don't break your build until you decide to implement them.
/// When you implement a feature, remove the Skip and make it green.
/// </summary>
public sealed class Db2AdvancedSqlGapTests : XUnitTestBase
{
    private readonly Db2ConnectionMock _cnn;

    /// <summary>
    /// EN: Tests Db2AdvancedSqlGapTests behavior.
    /// PT: Testa o comportamento de Db2AdvancedSqlGapTests.
    /// </summary>
    public Db2AdvancedSqlGapTests(ITestOutputHelper helper) : base(helper)
    {
        var db = new Db2DbMock();
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

        var eventsTable = db.AddTable("events");
        eventsTable.AddColumn("id", DbType.Int32, false);
        eventsTable.AddColumn("occurred", DbType.DateTime, false);
        eventsTable.Add(new Dictionary<int, object?> { [0] = 1, [1] = new DateTime(2020, 1, 1, 12, 0, 0, DateTimeKind.Local) });
        eventsTable.Add(new Dictionary<int, object?> { [0] = 2, [1] = new DateTime(2020, 1, 1, 12, 1, 0, DateTimeKind.Local) });
        eventsTable.Add(new Dictionary<int, object?> { [0] = 3, [1] = new DateTime(2020, 1, 1, 12, 2, 0, DateTimeKind.Local) });

        _cnn = new Db2ConnectionMock(db);
        _cnn.Open();
    }

    /// <summary>
    /// EN: Tests Window_RowNumber_PartitionBy_ShouldWork behavior.
    /// PT: Testa o comportamento de Window_RowNumber_PartitionBy_ShouldWork.
    /// </summary>
    [Fact]
    [Trait("Category", "Db2AdvancedSqlGap")]
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
    [Trait("Category", "Db2AdvancedSqlGap")]
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
    [Trait("Category", "Db2AdvancedSqlGap")]
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
    [Trait("Category", "Db2AdvancedSqlGap")]
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
    [Trait("Category", "Db2AdvancedSqlGap")]
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
    [Trait("Category", "Db2AdvancedSqlGap")]
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
    [Trait("Category", "Db2AdvancedSqlGap")]
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
    [Trait("Category", "Db2AdvancedSqlGap")]
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
    [Trait("Category", "Db2AdvancedSqlGap")]
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
    [Trait("Category", "Db2AdvancedSqlGap")]
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
    [Trait("Category", "Db2AdvancedSqlGap")]
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
    [Trait("Category", "Db2AdvancedSqlGap")]
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
    /// EN: Tests ranking and distribution window functions on size-1 ROWS frames with composite mixed-direction ORDER BY.
    /// PT: Testa funções de ranking e distribuição em frames ROWS de tamanho 1 com ORDER BY composto e direções mistas.
    /// </summary>
    [Fact]
    [Trait("Category", "Db2AdvancedSqlGap")]
    public void Window_RankingDistribution_WithCompositeOrder_AndCurrentRowFrame_ShouldReturnSingleRowSemantics()
    {
        var rows = _cnn.Query<dynamic>(@"
SELECT id,
       RANK() OVER (ORDER BY tenantid DESC, id ASC ROWS BETWEEN CURRENT ROW AND CURRENT ROW) AS rk,
       DENSE_RANK() OVER (ORDER BY tenantid DESC, id ASC ROWS BETWEEN CURRENT ROW AND CURRENT ROW) AS dr,
       PERCENT_RANK() OVER (ORDER BY tenantid DESC, id ASC ROWS BETWEEN CURRENT ROW AND CURRENT ROW) AS pr,
       CUME_DIST() OVER (ORDER BY tenantid DESC, id ASC ROWS BETWEEN CURRENT ROW AND CURRENT ROW) AS cd
FROM users
ORDER BY id").ToList();

        Assert.Equal([1, 1, 1], [.. rows.Select(r => (int)r.rk)]);
        Assert.Equal([1, 1, 1], [.. rows.Select(r => (int)r.dr)]);

        var pr = rows.Select(r => Convert.ToDouble(r.pr)).ToArray();
        var cd = rows.Select(r => Convert.ToDouble(r.cd)).ToArray();

        Assert.All(pr, v => Assert.True(Math.Abs(v - 0d) <= 1e-9));
        Assert.All(cd, v => Assert.True(Math.Abs(v - 1d) <= 1e-9));
    }


    /// <summary>
    /// EN: Tests LAG/LEAD defaults with composite mixed-direction ORDER BY and frame-limited visibility.
    /// PT: Testa defaults de LAG/LEAD com ORDER BY composto e direções mistas com visibilidade limitada por frame.
    /// </summary>
    [Fact]
    [Trait("Category", "Db2AdvancedSqlGap")]
    public void Window_LagLead_WithCompositeOrder_AndFrameLimit_ShouldApplyDefaults()
    {
        var rows = _cnn.Query<dynamic>(@"
SELECT id,
       LAG(id, 1, -1) OVER (ORDER BY tenantid DESC, id ASC ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING) AS lag_forward,
       LEAD(id, 1, 99) OVER (ORDER BY tenantid DESC, id ASC ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS lead_sliding
FROM users
ORDER BY id").ToList();

        Assert.Equal([-1, -1, -1], [.. rows.Select(r => (int)r.lag_forward)]);
        Assert.Equal([99, 99, 99], [.. rows.Select(r => (int)r.lead_sliding)]);
    }


    /// <summary>
    /// EN: Tests ranking/distribution and NTILE return NULL when the ROWS frame excludes the current row.
    /// PT: Testa se ranking/distribuição e NTILE retornam NULL quando o frame ROWS exclui a linha atual.
    /// </summary>
    [Fact]
    [Trait("Category", "Db2AdvancedSqlGap")]
    public void Window_RankingDistribution_AndNtile_WithFrameExcludingCurrentRow_ShouldReturnNull()
    {
        var rows = _cnn.Query<dynamic>(@"
SELECT id,
       RANK() OVER (ORDER BY tenantid DESC, id ASC ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING) AS rk_excluded,
       DENSE_RANK() OVER (ORDER BY tenantid DESC, id ASC ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING) AS dr_excluded,
       PERCENT_RANK() OVER (ORDER BY tenantid DESC, id ASC ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING) AS pr_excluded,
       CUME_DIST() OVER (ORDER BY tenantid DESC, id ASC ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING) AS cd_excluded,
       NTILE(2) OVER (ORDER BY tenantid DESC, id ASC ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING) AS ntile_excluded
FROM users
ORDER BY id").ToList();

        Assert.Equal([null, null, null], [.. rows.Select(r => (int?)r.rk_excluded)]);
        Assert.Equal([null, null, null], [.. rows.Select(r => (int?)r.dr_excluded)]);
        Assert.Equal([null, null, null], [.. rows.Select(r => (double?)r.pr_excluded)]);
        Assert.Equal([null, null, null], [.. rows.Select(r => (double?)r.cd_excluded)]);
        Assert.Equal([null, null, null], [.. rows.Select(r => (int?)r.ntile_excluded)]);
    }

    /// <summary>
    /// EN: Tests RANGE CURRENT ROW with DESC ordering keeps peer semantics across ranking/distribution/NTILE.
    /// PT: Testa se RANGE CURRENT ROW com ordenação DESC preserva semântica de peers em ranking/distribuição/NTILE.
    /// </summary>
    [Fact]
    [Trait("Category", "Db2AdvancedSqlGap")]
    public void Window_RangeCurrentRow_WithDescOrderAndPeers_ShouldRespectPeerSemantics()
    {
        var rows = _cnn.Query<dynamic>(@"
SELECT id,
       RANK() OVER (ORDER BY tenantid DESC RANGE BETWEEN CURRENT ROW AND CURRENT ROW) AS rk_desc_range,
       DENSE_RANK() OVER (ORDER BY tenantid DESC RANGE BETWEEN CURRENT ROW AND CURRENT ROW) AS dr_desc_range,
       CUME_DIST() OVER (ORDER BY tenantid DESC RANGE BETWEEN CURRENT ROW AND CURRENT ROW) AS cd_desc_range,
       NTILE(2) OVER (ORDER BY tenantid DESC RANGE BETWEEN CURRENT ROW AND CURRENT ROW) AS ntile_desc_range
FROM users
ORDER BY id").ToList();

        Assert.Equal([1, 1, 1], [.. rows.Select(r => (int)r.rk_desc_range)]);
        Assert.Equal([1, 1, 1], [.. rows.Select(r => (int)r.dr_desc_range)]);
        Assert.Equal([1d, 1d, 1d], [.. rows.Select(r => Convert.ToDouble(r.cd_desc_range))]);
        Assert.Equal([1, 2, 1], [.. rows.Select(r => (int)r.ntile_desc_range)]);
    }

    /// <summary>
    /// EN: Tests RANGE offset with DateTime ORDER BY values using tick-based offsets.
    /// PT: Testa RANGE com offset em ORDER BY DateTime usando offsets baseados em ticks.
    /// </summary>
    [Fact]
    [Trait("Category", "Db2AdvancedSqlGap")]
    public void Window_RangeOffset_WithDateTimeOrder_ShouldWork()
    {
        var rows = _cnn.Query<dynamic>(@"
SELECT id,
       FIRST_VALUE(id) OVER (ORDER BY occurred RANGE BETWEEN 900000000 PRECEDING AND CURRENT ROW) AS first_id_90s,
       LAST_VALUE(id) OVER (ORDER BY occurred RANGE BETWEEN 900000000 PRECEDING AND CURRENT ROW) AS last_id_90s
FROM events
ORDER BY id").ToList();

        Assert.Equal([1, 1, 2], [.. rows.Select(r => (int)r.first_id_90s)]);
        Assert.Equal([1, 2, 3], [.. rows.Select(r => (int)r.last_id_90s)]);
    }

    /// <summary>
    /// EN: Tests GROUPS frame with composite mixed-direction ORDER BY preserves peer-group boundaries.
    /// PT: Testa frame GROUPS com ORDER BY composto e direções mistas preservando limites de grupos de peers.
    /// </summary>
    [Fact]
    [Trait("Category", "Db2AdvancedSqlGap")]
    public void Window_GroupsFrame_WithCompositeMixedDirectionOrder_ShouldRespectPeerGroups()
    {
        var rows = _cnn.Query<dynamic>(@"
SELECT id,
       RANK() OVER (ORDER BY tenantid DESC, tenantid ASC GROUPS BETWEEN CURRENT ROW AND CURRENT ROW) AS rk_groups_mix,
       NTILE(2) OVER (ORDER BY tenantid DESC, tenantid ASC GROUPS BETWEEN CURRENT ROW AND CURRENT ROW) AS ntile_groups_mix
FROM users
ORDER BY id").ToList();

        Assert.Equal([1, 1, 1], [.. rows.Select(r => (int)r.rk_groups_mix)]);
        Assert.Equal([1, 2, 1], [.. rows.Select(r => (int)r.ntile_groups_mix)]);
    }

    /// <summary>
    /// EN: Tests RANGE frame that excludes current row yields NULL ranking/distribution/NTILE and applies LAG/LEAD defaults.
    /// PT: Testa se frame RANGE que exclui a linha atual retorna NULL em ranking/distribuição/NTILE e aplica defaults de LAG/LEAD.
    /// </summary>
    [Fact]
    [Trait("Category", "Db2AdvancedSqlGap")]
    public void Window_RangeFrame_ExcludingCurrentRow_ShouldReturnNullOrDefault()
    {
        var rows = _cnn.Query<dynamic>(@"
SELECT id,
       RANK() OVER (ORDER BY tenantid DESC RANGE BETWEEN 1 FOLLOWING AND 1 FOLLOWING) AS rk_excluded_range,
       DENSE_RANK() OVER (ORDER BY tenantid DESC RANGE BETWEEN 1 FOLLOWING AND 1 FOLLOWING) AS dr_excluded_range,
       PERCENT_RANK() OVER (ORDER BY tenantid DESC RANGE BETWEEN 1 FOLLOWING AND 1 FOLLOWING) AS pr_excluded_range,
       CUME_DIST() OVER (ORDER BY tenantid DESC RANGE BETWEEN 1 FOLLOWING AND 1 FOLLOWING) AS cd_excluded_range,
       NTILE(2) OVER (ORDER BY tenantid DESC RANGE BETWEEN 1 FOLLOWING AND 1 FOLLOWING) AS ntile_excluded_range,
       LAG(id, 1, -1) OVER (ORDER BY tenantid DESC RANGE BETWEEN 1 FOLLOWING AND 1 FOLLOWING) AS lag_excluded_range,
       LEAD(id, 1, 99) OVER (ORDER BY tenantid DESC RANGE BETWEEN 1 FOLLOWING AND 1 FOLLOWING) AS lead_excluded_range
FROM users
ORDER BY id").ToList();

        Assert.Equal([null, null, null], [.. rows.Select(r => (int?)r.rk_excluded_range)]);
        Assert.Equal([null, null, null], [.. rows.Select(r => (int?)r.dr_excluded_range)]);
        Assert.Equal([null, null, null], [.. rows.Select(r => (double?)r.pr_excluded_range)]);
        Assert.Equal([null, null, null], [.. rows.Select(r => (double?)r.cd_excluded_range)]);
        Assert.Equal([null, null, null], [.. rows.Select(r => (int?)r.ntile_excluded_range)]);
        Assert.Equal([-1, -1, -1], [.. rows.Select(r => (int)r.lag_excluded_range)]);
        Assert.Equal([99, 99, 99], [.. rows.Select(r => (int)r.lead_excluded_range)]);
    }

    /// <summary>
    /// EN: Tests RANGE offset with non-numeric ORDER BY throws a clear runtime type message.
    /// PT: Testa se RANGE com offset e ORDER BY não numérico lança mensagem clara com tipo em runtime.
    /// </summary>
    [Fact]
    [Trait("Category", "Db2AdvancedSqlGap")]
    public void Window_RangeOffset_WithTextOrder_ShouldThrowClearTypeError()
    {
        var ex = Assert.Throws<InvalidOperationException>(() =>
            _cnn.Query<dynamic>(@"
SELECT id,
       RANK() OVER (ORDER BY name RANGE BETWEEN 1 PRECEDING AND CURRENT ROW) AS rk_bad_type
FROM users
ORDER BY id").ToList());

        Assert.Contains("numeric/date ORDER BY values", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("String", ex.Message, StringComparison.OrdinalIgnoreCase);
    }


    /// <summary>
    /// EN: Tests CorrelatedSubquery_InSelectList_ShouldWork behavior.
    /// PT: Testa o comportamento de CorrelatedSubquery_InSelectList_ShouldWork.
    /// </summary>
    [Fact]
    [Trait("Category", "Db2AdvancedSqlGap")]
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
    [Trait("Category", "Db2AdvancedSqlGap")]
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
    /// EN: Tests TimestampAdd_Day_ShouldWork behavior.
    /// PT: Testa o comportamento de TimestampAdd_Day_ShouldWork.
    /// </summary>
    [Fact]
    [Trait("Category", "Db2AdvancedSqlGap")]
    public void TimestampAdd_Day_ShouldWork()
    {
        var rows = _cnn.Query<dynamic>(@"
SELECT id, TIMESTAMPADD(DAY, 1, created) AS d
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
    [Trait("Category", "Db2AdvancedSqlGap")]
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
    [Trait("Category", "Db2AdvancedSqlGap")]
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
    [Trait("Category", "Db2AdvancedSqlGap")]
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
    [Trait("Category", "Db2AdvancedSqlGap")]
    public void Collation_CaseSensitivity_ShouldFollowColumnCollation()
    {
        // Example expectation in DB2: behavior depends on column collation.
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
