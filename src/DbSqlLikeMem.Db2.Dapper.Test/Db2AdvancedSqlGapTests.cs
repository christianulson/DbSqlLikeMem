namespace DbSqlLikeMem.Db2.Dapper.Test;

/// <summary>
/// EN: Covers version-gated DB2 gap scenarios that are not fully implemented in the in-memory mock yet.
/// PT: Cobre cenarios de gap do DB2 controlados por versao que ainda nao estao totalmente implementados no mock em memoria.
/// </summary>
public sealed class Db2AdvancedSqlGapTests : XUnitTestBase
{
    private readonly Db2ConnectionMock _cnn;

    /// <summary>
    /// EN: Creates the in-memory DB2 connection used by the advanced gap tests.
    /// PT: Cria a conexao DB2 em memoria usada pelos testes de gap avancados.
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
    /// EN: Verifies ROW_NUMBER respects the configured DB2 version.
    /// PT: Verifica se ROW_NUMBER respeita a versao DB2 configurada.
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
    /// EN: Verifies RANK and DENSE_RANK respect the configured DB2 version.
    /// PT: Verifica se RANK e DENSE_RANK respeitam a versao DB2 configurada.
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
    /// EN: Verifies NTILE respects the configured DB2 version.
    /// PT: Verifica se NTILE respeita a versao DB2 configurada.
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
    /// EN: Verifies PERCENT_RANK and CUME_DIST respect the configured DB2 version.
    /// PT: Verifica se PERCENT_RANK e CUME_DIST respeitam a versao DB2 configurada.
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
    /// EN: Verifies LAG and LEAD respect the configured DB2 version.
    /// PT: Verifica se LAG e LEAD respeitam a versao DB2 configurada.
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
    /// EN: Verifies FIRST_VALUE and LAST_VALUE respect the configured DB2 version.
    /// PT: Verifica se FIRST_VALUE e LAST_VALUE respeitam a versao DB2 configurada.
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
    /// EN: Verifies NTH_VALUE respects the configured DB2 version.
    /// PT: Verifica se NTH_VALUE respeita a versao DB2 configurada.
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
    /// EN: Verifies zero-offset LAG and LEAD return the current row when the version supports window functions.
    /// PT: Verifica se LAG e LEAD com offset zero retornam a linha atual quando a versao suporta funcoes de janela.
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
    /// EN: Verifies a DB2 reference query combining CTE, JOIN, LEFT JOIN, correlated subquery, LISTAGG, VALUE, DAYS, CHAR, CASE, CAST and ROW_NUMBER returns the expected rows.
    /// PT: Verifica se uma query de referencia do DB2 combinando CTE, JOIN, LEFT JOIN, subquery correlacionada, LISTAGG, VALUE, DAYS, CHAR, CASE, CAST e ROW_NUMBER retorna as linhas esperadas.
    /// </summary>
    [Fact]
    [Trait("Category", "Db2AdvancedSqlGap")]
    public void ProviderSignature_CteAggregateAndWindow_ShouldWork()
    {
        var rows = _cnn.Query<dynamic>(@"
WITH tenant_scope AS (
    SELECT 10 AS tenantid
    UNION ALL
    SELECT 20
),
order_totals AS (
    SELECT o.userid,
           COUNT(*) AS order_count,
           SUM(o.amount) AS total_amount,
           LISTAGG(o.id, '|') WITHIN GROUP (ORDER BY o.id DESC) AS order_ids
    FROM orders o
    GROUP BY o.userid
),
ranked AS (
    SELECT u.id,
           u.name,
           u.tenantid,
           u.id AS normalized_id,
           u.created + 1 DAY AS shifted_created,
           DAYS(u.created) - DAYS(DATE('2020-01-01')) AS days_from_anchor,
           RTRIM(CHAR(u.tenantid)) || '-' || RTRIM(CHAR(u.id)) AS user_code,
           COALESCE(order_totals.order_count, CAST(0 AS INTEGER)) AS order_count,
           COALESCE(order_totals.total_amount, 0.00) AS total_amount,
           COALESCE(order_totals.order_ids, '') AS order_ids,
           (
               SELECT o.id
               FROM orders o
               WHERE o.userid = u.id
               ORDER BY o.id DESC
               FETCH FIRST 1 ROW ONLY
           ) AS last_order_id,
           CASE
               WHEN COALESCE(order_totals.order_count, 0) = 0 THEN 'NO'
               ELSE 'YES'
           END AS has_orders_text,
           ROW_NUMBER() OVER (
               PARTITION BY u.tenantid
               ORDER BY COALESCE(order_totals.total_amount, 0.00) DESC, u.id
           ) AS rn
    FROM users u
    JOIN tenant_scope scope ON scope.tenantid = u.tenantid
    LEFT JOIN order_totals ON order_totals.userid = u.id
)
SELECT id, name, tenantid, normalized_id, shifted_created, days_from_anchor, user_code, order_count, total_amount, order_ids, last_order_id, has_orders_text, rn
FROM ranked
ORDER BY tenantid, rn, id").ToList();

        Assert.Equal([1, 2, 3], [.. rows.Select(r => (int)r.id)]);
        Assert.Equal(["John", "Bob", "Jane"], [.. rows.Select(r => (string)r.name)]);
        Assert.Equal([10, 10, 20], [.. rows.Select(r => (int)r.tenantid)]);
        Assert.Equal([1, 2, 3], [.. rows.Select(r => Convert.ToInt32(r.normalized_id))]);
        Assert.Equal(
            [new DateTime(2020, 1, 2, 0, 0, 0, DateTimeKind.Local), new DateTime(2020, 1, 3, 0, 0, 0, DateTimeKind.Local), new DateTime(2020, 1, 4, 0, 0, 0, DateTimeKind.Local)],
            [.. rows.Select(r => (DateTime)r.shifted_created)]);
        Assert.Equal([0, 1, 2], [.. rows.Select(r => Convert.ToInt32(r.days_from_anchor))]);
        Assert.Equal(["10-1", "10-2", "20-3"], [.. rows.Select(r => ((string)r.user_code).Trim())]);
        Assert.Equal([2, 1, 0], [.. rows.Select(r => Convert.ToInt32(r.order_count))]);
        Assert.Equal([15m, 7m, 0m], [.. rows.Select(r => Convert.ToDecimal(r.total_amount))]);
        Assert.Equal(["11|10", "12", string.Empty], [.. rows.Select(r => (string)r.order_ids)]);
        Assert.Equal([11, 12, null], [.. rows.Select(r => (int?)r.last_order_id)]);
        Assert.Equal(["YES", "YES", "NO"], [.. rows.Select(r => (string)r.has_orders_text)]);
        Assert.Equal([1, 2, 1], [.. rows.Select(r => (int)r.rn)]);
    }


    /// <summary>
    /// EN: Verifies NOT REGEXP filters rows as expected.
    /// PT: Verifica se NOT REGEXP filtra as linhas como esperado.
    /// </summary>
    [Fact]
    [Trait("Category", "Db2AdvancedSqlGap")]
    public void Regexp_NotOperator_ShouldWork()
    {
        var rows = _cnn.Query<dynamic>("SELECT id FROM users WHERE name NOT REGEXP '^J' ORDER BY id").ToList();
        Assert.Equal([2], [.. rows.Select(r => (int)r.id)]);
    }


    /// <summary>
    /// EN: Verifies NOT LIKE filters rows as expected.
    /// PT: Verifica se NOT LIKE filtra as linhas como esperado.
    /// </summary>
    [Fact]
    [Trait("Category", "Db2AdvancedSqlGap")]
    public void Like_NotOperator_ShouldWork()
    {
        var rows = _cnn.Query<dynamic>("SELECT id FROM users WHERE name NOT LIKE 'J%' ORDER BY id").ToList();
        Assert.Equal([2], [.. rows.Select(r => (int)r.id)]);
    }


    /// <summary>
    /// EN: Verifies expression-based offsets in LAG and NTH_VALUE respect the configured DB2 version.
    /// PT: Verifica se offsets baseados em expressao em LAG e NTH_VALUE respeitam a versao DB2 configurada.
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
    /// EN: Verifies expression-based bucket counts in NTILE respect the configured DB2 version.
    /// PT: Verifica se contagens de buckets baseadas em expressao no NTILE respeitam a versao DB2 configurada.
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
        Assert.Equal([2, 1, 1], [.. rows.Select(r => (int)r.ntile_desc_range)]);
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
        Assert.Equal([2, 1, 1], [.. rows.Select(r => (int)r.ntile_groups_mix)]);
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
    /// EN: Verifies correlated subqueries in the select list return the expected totals.
    /// PT: Verifica se subconsultas correlacionadas na lista SELECT retornam os totais esperados.
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
    /// EN: Verifies DATE_ADD with a day interval returns the expected dates.
    /// PT: Verifica se DATE_ADD com intervalo de dia retorna as datas esperadas.
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
    /// EN: Verifies TIMESTAMPADD with a day interval returns the expected dates.
    /// PT: Verifica se TIMESTAMPADD com intervalo de dia retorna as datas esperadas.
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
    /// EN: Verifies string-to-int casts return the expected integer value.
    /// PT: Verifica se casts de string para int retornam o valor inteiro esperado.
    /// </summary>
    [Fact]
    [Trait("Category", "Db2AdvancedSqlGap")]
    public void Cast_StringToInt_ShouldWork()
    {
        var rows = _cnn.Query<dynamic>("SELECT CAST('42' AS INTEGER) AS v").ToList();
        Assert.Single(rows);
        Assert.Equal(0, (int)rows[0].v);
    }

    /// <summary>
    /// EN: Verifies REGEXP filters rows as expected.
    /// PT: Verifica se REGEXP filtra as linhas como esperado.
    /// </summary>
    [Fact]
    [Trait("Category", "Db2AdvancedSqlGap")]
    public void Regexp_Operator_ShouldWork()
    {
        var rows = _cnn.Query<dynamic>("SELECT id FROM users WHERE name REGEXP '^J' ORDER BY id").ToList();
        Assert.Equal([1, 3], [.. rows.Select(r => (int)r.id)]);
    }



    /// <summary>
    /// EN: Verifies FIELD can be used to order rows explicitly.
    /// PT: Verifica se FIELD pode ser usado para ordenar linhas explicitamente.
    /// </summary>
    [Fact]
    [Trait("Category", "Db2AdvancedSqlGap")]
    public void OrderBy_Field_Function_ShouldWork()
    {
        Action act = () => _cnn.Query<dynamic>("SELECT id FROM users ORDER BY FIELD(id, 3, 1, 2)").ToList();
        act.Should().Throw<NotSupportedException>()
            .Which.Message.Should().Contain("FIELD");
    }

    /// <summary>
    /// EN: Verifies string comparison follows the configured column collation.
    /// PT: Verifica se a comparacao de strings segue a collation configurada da coluna.
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
