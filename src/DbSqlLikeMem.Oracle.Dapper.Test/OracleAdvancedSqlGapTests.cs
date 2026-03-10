namespace DbSqlLikeMem.Oracle.Test;

/// <summary>
/// These are TDD "gap" tests for MySQL features that are NOT implemented yet in the in-memory mock.
/// They are intentionally skipped so they don't break your build until you decide to implement them.
/// When you implement a feature, remove the Skip and make it green.
/// </summary>
public sealed class OracleAdvancedSqlGapTests : XUnitTestBase
{
    private readonly OracleConnectionMock _cnn;

    /// <summary>
    /// EN: Initializes a new instance of OracleAdvancedSqlGapTests.
    /// PT: Inicializa uma nova instância de OracleAdvancedSqlGapTests.
    /// </summary>
    public OracleAdvancedSqlGapTests(ITestOutputHelper helper) : base(helper)
    {
        var db = new OracleDbMock();
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

        _cnn = new OracleConnectionMock(db);
        _cnn.Open();
    }

    /// <summary>
    /// EN: Tests Window_RowNumber_PartitionBy_ShouldWork behavior.
    /// PT: Testa o comportamento de Window_RowNumber_PartitionBy_ShouldWork.
    /// </summary>
    [Fact]
    [Trait("Category", "OracleAdvancedSqlGap")]
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
    [Trait("Category", "OracleAdvancedSqlGap")]
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
    [Trait("Category", "OracleAdvancedSqlGap")]
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
    [Trait("Category", "OracleAdvancedSqlGap")]
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
    [Trait("Category", "OracleAdvancedSqlGap")]
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
    [Trait("Category", "OracleAdvancedSqlGap")]
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
    [Trait("Category", "OracleAdvancedSqlGap")]
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
    [Trait("Category", "OracleAdvancedSqlGap")]
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
    /// EN: Verifies an Oracle reference query combining CTE, JOIN, LEFT JOIN, EXISTS, LISTAGG, NVL, NVL2, DECODE, INTERVAL, CAST and ROW_NUMBER returns the expected rows.
    /// PT: Verifica se uma query de referencia do Oracle combinando CTE, JOIN, LEFT JOIN, EXISTS, LISTAGG, NVL, NVL2, DECODE, INTERVAL, CAST e ROW_NUMBER retorna as linhas esperadas.
    /// </summary>
    [Fact]
    [Trait("Category", "OracleAdvancedSqlGap")]
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
           SUM(CAST(o.amount AS NUMBER(10,2))) AS total_amount,
           LISTAGG(CAST(o.id AS VARCHAR2(20)), '|') WITHIN GROUP (ORDER BY o.id DESC) AS order_ids
    FROM orders o
    GROUP BY o.userid
),
ranked AS (
    SELECT u.id,
           u.name,
           u.tenantid,
           CAST(u.id AS NUMBER(10)) AS normalized_id,
           u.created + INTERVAL '1' DAY AS shifted_created,
           TRUNC(u.created) - DATE '2020-01-01' AS days_from_anchor,
           TO_CHAR(u.tenantid) || '-' || TO_CHAR(u.id) AS user_code,
           NVL(order_totals.order_count, CAST(0 AS NUMBER(10))) AS order_count,
           NVL(order_totals.total_amount, CAST(0 AS NUMBER(10,2))) AS total_amount,
           NVL(order_totals.order_ids, CAST('' AS VARCHAR2(20))) AS order_ids,
           DECODE(NVL(order_totals.order_count, 0), 0, 'NO', 'YES') AS has_orders_text,
           CASE
               WHEN EXISTS (SELECT 1 FROM orders ox WHERE ox.userid = u.id AND ox.amount >= CAST(10 AS NUMBER(10,2))) THEN 1
               ELSE 0
           END AS has_big_order,
           NVL2(order_totals.order_ids, LENGTH(order_totals.order_ids), 0) AS order_ids_length,
           ROW_NUMBER() OVER (
               PARTITION BY u.tenantid
               ORDER BY NVL(order_totals.total_amount, CAST(0 AS NUMBER(10,2))) DESC, u.id
           ) AS rn
    FROM users u
    JOIN tenant_scope scope ON scope.tenantid = u.tenantid
    LEFT JOIN order_totals ON order_totals.userid = u.id
)
SELECT id, name, tenantid, normalized_id, shifted_created, days_from_anchor, user_code, order_count, total_amount, order_ids, has_orders_text, has_big_order, order_ids_length, rn
FROM ranked
ORDER BY tenantid, rn, id").ToList();

        Assert.Equal([1, 2, 3], [.. rows.Select(r => (int)r.id)]);
        Assert.Equal(["John", "Bob", "Jane"], [.. rows.Select(r => (string)r.name)]);
        Assert.Equal([10, 10, 20], [.. rows.Select(r => (int)r.tenantid)]);
        Assert.Equal([1, 2, 3], [.. rows.Select(r => Convert.ToInt32(r.normalized_id))]);
        Assert.Equal(
            [new DateTime(2020, 1, 2, 0, 0, 0, DateTimeKind.Local), new DateTime(2020, 1, 3, 0, 0, 0, DateTimeKind.Local), new DateTime(2020, 1, 4, 0, 0, 0, DateTimeKind.Local)],
            [.. rows.Select(r => (DateTime)r.shifted_created)]);
        Assert.Equal([0m, 1m, 2m], [.. rows.Select(r => Convert.ToDecimal(r.days_from_anchor))]);
        Assert.Equal(["10-1", "10-2", "20-3"], [.. rows.Select(r => (string)r.user_code)]);
        Assert.Equal([2, 1, 0], [.. rows.Select(r => Convert.ToInt32(r.order_count))]);
        Assert.Equal([15m, 7m, 0m], [.. rows.Select(r => Convert.ToDecimal(r.total_amount))]);
        Assert.Equal(["11|10", "12", string.Empty], [.. rows.Select(r => (string)r.order_ids)]);
        Assert.Equal(["YES", "YES", "NO"], [.. rows.Select(r => (string)r.has_orders_text)]);
        Assert.Equal([1, 0, 0], [.. rows.Select(r => Convert.ToInt32(r.has_big_order))]);
        Assert.Equal([5, 2, 0], [.. rows.Select(r => Convert.ToInt32(r.order_ids_length))]);
        Assert.Equal([1, 2, 1], [.. rows.Select(r => (int)r.rn)]);
    }


    /// <summary>
    /// EN: Tests Regexp_NotOperator_ShouldWork behavior.
    /// PT: Testa o comportamento de Regexp_NotOperator_ShouldWork.
    /// </summary>
    [Fact]
    [Trait("Category", "OracleAdvancedSqlGap")]
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
    [Trait("Category", "OracleAdvancedSqlGap")]
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
    [Trait("Category", "OracleAdvancedSqlGap")]
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
    [Trait("Category", "OracleAdvancedSqlGap")]
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
    [Trait("Category", "OracleAdvancedSqlGap")]
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
    [Trait("Category", "OracleAdvancedSqlGap")]
    public void DateAdd_IntervalDay_ShouldWork()
    {
        var rows = _cnn.Query<dynamic>(@"
SELECT id, (created + INTERVAL \'1\' DAY) AS d
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
    [Trait("Category", "OracleAdvancedSqlGap")]
    public void Cast_StringToInt_ShouldWork()
    {
        var rows = _cnn.Query<dynamic>("SELECT TO_NUMBER('42') AS v").ToList();
        Assert.Single(rows);
        Assert.Equal(42, (int)rows[0].v);
    }



    /// <summary>
    /// EN: Ensures NUMBER cast target is treated as integer-compatible in Oracle behavior.
    /// PT: Garante que o alvo de cast NUMBER seja tratado como compatível com inteiro no comportamento Oracle.
    /// </summary>
    [Fact]
    [Trait("Category", "OracleAdvancedSqlGap")]
    public void Cast_StringToInt_NumberType_ShouldWork()
    {
        var rows = _cnn.Query<dynamic>("SELECT CAST('42' AS NUMBER) AS v").ToList();
        Assert.Single(rows);
        Assert.Equal(42, (int)rows[0].v);
    }

    /// <summary>
    /// EN: Tests Regexp_Operator_ShouldWork behavior.
    /// PT: Testa o comportamento de Regexp_Operator_ShouldWork.
    /// </summary>
    [Fact]
    [Trait("Category", "OracleAdvancedSqlGap")]
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
    [Trait("Category", "OracleAdvancedSqlGap")]
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
    [Trait("Category", "OracleAdvancedSqlGap")]
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
    [Trait("Category", "OracleAdvancedSqlGap")]
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
    /// EN: Verifies PIVOT supports SUM, MIN, MAX and AVG for Oracle buckets.
    /// PT: Verifica se o PIVOT suporta SUM, MIN, MAX e AVG para buckets do Oracle.
    /// </summary>
    [Fact]
    [Trait("Category", "OracleAdvancedSqlGap")]
    public void Pivot_CommonNumericAggregates_ByTenant_ShouldWork()
    {
        var sumRow = _cnn.QuerySingle<dynamic>(@"
SELECT t10, t20
FROM (
    SELECT tenantid, id
    FROM users
) src
PIVOT (
    SUM(id)
    FOR tenantid IN (10 AS t10, 20 AS t20)
) p");

        Assert.Equal(3m, (decimal)sumRow.t10);
        Assert.Equal(3m, (decimal)sumRow.t20);

        var minRow = _cnn.QuerySingle<dynamic>(@"
SELECT t10, t20
FROM (
    SELECT tenantid, id
    FROM users
) src
PIVOT (
    MIN(id)
    FOR tenantid IN (10 AS t10, 20 AS t20)
) p");

        var maxRow = _cnn.QuerySingle<dynamic>(@"
SELECT t10, t20
FROM (
    SELECT tenantid, id
    FROM users
) src
PIVOT (
    MAX(id)
    FOR tenantid IN (10 AS t10, 20 AS t20)
) p");

        var avgRow = _cnn.QuerySingle<dynamic>(@"
SELECT t10, t20
FROM (
    SELECT tenantid, id
    FROM users
) src
PIVOT (
    AVG(id)
    FOR tenantid IN (10 AS t10, 20 AS t20)
) p");

        Assert.Equal(1m, (decimal)minRow.t10);
        Assert.Equal(3m, (decimal)minRow.t20);
        Assert.Equal(2m, (decimal)maxRow.t10);
        Assert.Equal(3m, (decimal)maxRow.t20);
        Assert.Equal(1.5m, (decimal)avgRow.t10);
        Assert.Equal(3m, (decimal)avgRow.t20);
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
