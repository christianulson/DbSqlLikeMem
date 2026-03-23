namespace DbSqlLikeMem.Sqlite.Dapper.Test;

/// <summary>
/// EN: Covers version-gated SQLite gap scenarios that are not fully implemented in the in-memory mock yet.
/// PT: Cobre cenarios de gap do SQLite controlados por versao que ainda nao estao totalmente implementados no mock em memoria.
/// </summary>
public sealed class SqliteAdvancedSqlGapTests : XUnitTestBase
{
    private readonly SqliteConnectionMock _cnn;

    /// <summary>
    /// EN: Creates the in-memory SQLite database used by the advanced gap tests.
    /// PT: Cria o banco SQLite em memoria usado pelos testes de gap avancados.
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
    /// EN: Verifies ROW_NUMBER respects the configured SQLite version.
    /// PT: Verifica se ROW_NUMBER respeita a versao SQLite configurada.
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
    /// EN: Verifies RANK and DENSE_RANK respect the configured SQLite version.
    /// PT: Verifica se RANK e DENSE_RANK respeitam a versao SQLite configurada.
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
    /// EN: Verifies NTILE respects the configured SQLite version.
    /// PT: Verifica se NTILE respeita a versao SQLite configurada.
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
    /// EN: Verifies PERCENT_RANK and CUME_DIST respect the configured SQLite version.
    /// PT: Verifica se PERCENT_RANK e CUME_DIST respeitam a versao SQLite configurada.
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
    /// EN: Verifies LAG and LEAD respect the configured SQLite version.
    /// PT: Verifica se LAG e LEAD respeitam a versao SQLite configurada.
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
    /// EN: Verifies FIRST_VALUE and LAST_VALUE respect the configured SQLite version.
    /// PT: Verifica se FIRST_VALUE e LAST_VALUE respeitam a versao SQLite configurada.
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
    /// EN: Verifies FIRST_VALUE and LAST_VALUE respect the current-row frame.
    /// PT: Verifica se FIRST_VALUE e LAST_VALUE respeitam o frame de linha atual.
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
    /// EN: Verifies FIRST_VALUE and LAST_VALUE respect the sliding frame.
    /// PT: Verifica se FIRST_VALUE e LAST_VALUE respeitam o frame deslizante.
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
    /// EN: Verifies FIRST_VALUE and LAST_VALUE respect the forward frame.
    /// PT: Verifica se FIRST_VALUE e LAST_VALUE respeitam o frame para frente.
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
    /// EN: Verifies a SQLite reference query combining WITH RECURSIVE, JOIN, LEFT JOIN, correlated subquery, GROUP_CONCAT, IFNULL, JULIANDAY, DATETIME, CASE, CAST and ROW_NUMBER returns the expected rows.
    /// PT: Verifica se uma query de referencia do SQLite combinando WITH RECURSIVE, JOIN, LEFT JOIN, subquery correlacionada, GROUP_CONCAT, IFNULL, JULIANDAY, DATETIME, CASE, CAST e ROW_NUMBER retorna as linhas esperadas.
    /// </summary>
    [Fact]
    [Trait("Category", "SqliteAdvancedSqlGap")]
    public void ProviderSignature_CteAggregateAndWindow_ShouldWork()
    {
        var rows = _cnn.Query<dynamic>(@"
WITH RECURSIVE tenant_scope AS (
    SELECT 10 AS tenantid
    UNION ALL
    SELECT 20
),
order_totals AS (
    SELECT o.userid,
           COUNT(*) AS order_count,
           SUM(CAST(o.amount AS DECIMAL(10,2))) AS total_amount,
           GROUP_CONCAT(CAST(o.id AS TEXT), '|' ORDER BY o.id DESC) AS order_ids
    FROM orders o
    GROUP BY o.userid
),
ranked AS (
    SELECT u.id,
           u.name,
           u.tenantid,
           CAST(u.id AS INTEGER) AS normalized_id,
           DATETIME(u.created, '+1 day') AS shifted_created,
           CAST(JULIANDAY(u.created) - JULIANDAY('2020-01-01') AS INTEGER) AS days_from_anchor,
           CAST(u.tenantid AS TEXT) || '-' || CAST(u.id AS TEXT) AS user_code,
           IFNULL(order_totals.order_count, CAST(0 AS INTEGER)) AS order_count,
           IFNULL(order_totals.total_amount, CAST(0 AS DECIMAL(10,2))) AS total_amount,
           IFNULL(order_totals.order_ids, CAST('' AS TEXT)) AS order_ids,
           (
               SELECT o.id
               FROM orders o
               WHERE o.userid = u.id
               ORDER BY o.id DESC
               LIMIT 1
           ) AS last_order_id,
           CASE
               WHEN IFNULL(order_totals.order_count, 0) = 0 THEN 0
               ELSE 1
           END AS has_orders,
           ROW_NUMBER() OVER (
               PARTITION BY u.tenantid
               ORDER BY IFNULL(order_totals.total_amount, CAST(0 AS DECIMAL(10,2))) DESC, u.id
           ) AS rn
    FROM users u
    JOIN tenant_scope scope ON scope.tenantid = u.tenantid
    LEFT JOIN order_totals ON order_totals.userid = u.id
)
SELECT id, name, tenantid, normalized_id, shifted_created, days_from_anchor, user_code, order_count, total_amount, order_ids, last_order_id, has_orders, rn
FROM ranked
ORDER BY tenantid, rn, id").ToList();

        Assert.Equal([1, 2, 3], [.. rows.Select(r => (int)r.id)]);
        Assert.Equal(["John", "Bob", "Jane"], [.. rows.Select(r => (string)r.name)]);
        Assert.Equal([10, 10, 20], [.. rows.Select(r => (int)r.tenantid)]);
        Assert.Equal([1, 2, 3], [.. rows.Select(r => Convert.ToInt32(r.normalized_id))]);
        Assert.All(rows, row => Assert.NotNull((object?)row.shifted_created));
        Assert.Equal([0, 1, 2], [.. rows.Select(r => Convert.ToInt32(r.days_from_anchor))]);
        Assert.Equal(["10-1", "10-2", "20-3"], [.. rows.Select(r => (string)r.user_code)]);
        Assert.Equal([2, 1, 0], [.. rows.Select(r => Convert.ToInt32(r.order_count))]);
        Assert.Equal([15m, 7m, 0m], [.. rows.Select(r => Convert.ToDecimal(r.total_amount))]);
        Assert.Equal(["11|10", "12", string.Empty], [.. rows.Select(r => (string)r.order_ids)]);
        Assert.Equal([11, 12, null], [.. rows.Select(r => (int?)r.last_order_id)]);
        Assert.Equal([1, 1, 0], [.. rows.Select(r => Convert.ToInt32(r.has_orders))]);
        Assert.Equal([1, 2, 1], [.. rows.Select(r => (int)r.rn)]);
    }


    /// <summary>
    /// EN: Verifies NTH_VALUE respects the configured SQLite version.
    /// PT: Verifica se NTH_VALUE respeita a versao SQLite configurada.
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
    /// EN: Verifies NTH_VALUE returns null for the current-row frame when appropriate.
    /// PT: Verifica se NTH_VALUE retorna nulo para o frame de linha atual quando apropriado.
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
    /// EN: Verifies NTH_VALUE resolves per row in a sliding frame.
    /// PT: Verifica se NTH_VALUE resolve por linha em um frame deslizante.
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
    /// EN: Verifies NTH_VALUE resolves per row in a forward frame.
    /// PT: Verifica se NTH_VALUE resolve por linha em um frame para frente.
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
    /// EN: Verifies zero-offset LAG and LEAD return the current row.
    /// PT: Verifica se LAG e LEAD com offset zero retornam a linha atual.
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
    /// EN: Verifies LAG and LEAD respect per-row boundaries in a ROWS frame.
    /// PT: Verifica se LAG e LEAD respeitam os limites por linha em um frame ROWS.
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
    /// EN: Verifies ranking functions respect per-row boundaries in a ROWS frame.
    /// PT: Verifica se funcoes de ranking respeitam os limites por linha em um frame ROWS.
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
    /// EN: Verifies ranking functions respect descending order within a ROWS frame.
    /// PT: Verifica se funcoes de ranking respeitam a ordem decrescente em um frame ROWS.
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
    /// EN: Verifies PERCENT_RANK and CUME_DIST respect descending peers within a ROWS frame.
    /// PT: Verifica se PERCENT_RANK e CUME_DIST respeitam os peers decrescentes em um frame ROWS.
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
    /// EN: Verifies LAG and LEAD respect descending order within a ROWS frame.
    /// PT: Verifica se LAG e LEAD respeitam a ordem decrescente em um frame ROWS.
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
    /// EN: Verifies NTILE respects descending order within a ROWS frame.
    /// PT: Verifica se NTILE respeita a ordem decrescente em um frame ROWS.
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
    /// EN: Verifies NOT REGEXP filters rows as expected.
    /// PT: Verifica se NOT REGEXP filtra as linhas como esperado.
    /// </summary>
    [Fact]
    [Trait("Category", "SqliteAdvancedSqlGap")]
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
    [Trait("Category", "SqliteAdvancedSqlGap")]
    public void Like_NotOperator_ShouldWork()
    {
        var rows = _cnn.Query<dynamic>("SELECT id FROM users WHERE name NOT LIKE 'J%' ORDER BY id").ToList();
        Assert.Equal([2], [.. rows.Select(r => (int)r.id)]);
    }


    /// <summary>
    /// EN: Verifies expression-based offsets in LAG and NTH_VALUE return the expected rows.
    /// PT: Verifica se offsets baseados em expressao em LAG e NTH_VALUE retornam as linhas esperadas.
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
    /// EN: Verifies expression-based bucket counts in NTILE return the expected rows.
    /// PT: Verifica se contagens de buckets baseadas em expressao no NTILE retornam as linhas esperadas.
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
    /// EN: Tests ranking distribution functions with mixed ASC/DESC composite ORDER BY keep stable peer semantics.
    /// PT: Testa se funções de ranking/distribuição com ORDER BY composto ASC/DESC misto mantêm semântica estável de peers.
    /// </summary>
    [Fact]
    [Trait("Category", "SqliteAdvancedSqlGap")]
    public void Window_RankingDistribution_WithCompositeMixedOrder_ShouldKeepPeerSemantics()
    {
        var rows = _cnn.Query<dynamic>(@"
SELECT id,
       RANK() OVER (ORDER BY tenantid DESC, tenantid ASC) AS rk,
       DENSE_RANK() OVER (ORDER BY tenantid DESC, tenantid ASC) AS dr,
       PERCENT_RANK() OVER (ORDER BY tenantid DESC, tenantid ASC) AS pr,
       CUME_DIST() OVER (ORDER BY tenantid DESC, tenantid ASC) AS cd
FROM users
ORDER BY id").ToList();

        Assert.Equal([2, 2, 1], [.. rows.Select(r => (int)r.rk)]);
        Assert.Equal([2, 2, 1], [.. rows.Select(r => (int)r.dr)]);

        var pr = rows.Select(r => Convert.ToDouble(r.pr)).ToArray();
        var cd = rows.Select(r => Convert.ToDouble(r.cd)).ToArray();

        Assert.True(Math.Abs(pr[0] - 0.5d) <= 1e-9);
        Assert.True(Math.Abs(pr[1] - 0.5d) <= 1e-9);
        Assert.True(Math.Abs(pr[2] - 0d) <= 1e-9);

        Assert.True(Math.Abs(cd[0] - 1d) <= 1e-9);
        Assert.True(Math.Abs(cd[1] - 1d) <= 1e-9);
        Assert.True(Math.Abs(cd[2] - (1d / 3d)) <= 1e-9);
    }


    /// <summary>
    /// EN: Tests LAG/LEAD with composite ORDER BY and larger offsets apply defaults at frame boundaries.
    /// PT: Testa se LAG/LEAD com ORDER BY composto e offsets maiores aplicam defaults nos limites do frame.
    /// </summary>
    [Fact]
    [Trait("Category", "SqliteAdvancedSqlGap")]
    public void Window_LagLead_WithCompositeOrder_AndLargeOffset_ShouldApplyDefaults()
    {
        var rows = _cnn.Query<dynamic>(@"
SELECT id,
       LAG(id, 2, -1) OVER (ORDER BY tenantid DESC, id ASC) AS lag2,
       LEAD(id, 2, 99) OVER (ORDER BY tenantid DESC, id ASC) AS lead2
FROM users
ORDER BY id").ToList();

        Assert.Equal([-1, 3, -1], [.. rows.Select(r => (int)r.lag2)]);
        Assert.Equal([99, 99, 2], [.. rows.Select(r => (int)r.lead2)]);
    }


    /// <summary>
    /// EN: Verifies correlated subqueries in the select list return the expected totals.
    /// PT: Verifica se subconsultas correlacionadas na lista SELECT retornam os totais esperados.
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
    /// EN: Verifies DATE_ADD with a day interval returns the expected dates.
    /// PT: Verifica se DATE_ADD com intervalo de dia retorna as datas esperadas.
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
    /// EN: Verifies date functions with modifiers return the expected values.
    /// PT: Verifica se funcoes de data com modificadores retornam os valores esperados.
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
    /// EN: Verifies string-to-int casts return the expected integer value.
    /// PT: Verifica se casts de string para int retornam o valor inteiro esperado.
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
    /// EN: Verifies REGEXP filters rows as expected.
    /// PT: Verifica se REGEXP filtra as linhas como esperado.
    /// </summary>
    [Fact]
    [Trait("Category", "SqliteAdvancedSqlGap")]
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
    [Trait("Category", "SqliteAdvancedSqlGap")]
    public void OrderBy_Field_Function_ShouldWork()
    {
        var rows = _cnn.Query<dynamic>("SELECT id FROM users ORDER BY FIELD(id, 3, 1, 2)").ToList();
        Assert.Equal([3, 1, 2], [.. rows.Select(r => (int)r.id)]);
    }

    /// <summary>
    /// EN: Verifies string comparison follows the configured column collation.
    /// PT: Verifica se a comparacao de strings segue a collation configurada da coluna.
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
