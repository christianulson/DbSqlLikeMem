namespace DbSqlLikeMem.Oracle.Test;

/// <summary>
/// EN: Covers version-gated Oracle gap scenarios that are not fully implemented in the in-memory mock yet.
/// PT: Cobre cenarios de gap do Oracle controlados por versao que ainda nao estao totalmente implementados no mock em memoria.
/// </summary>
/// <remarks>
/// EN: Creates the in-memory Oracle connection used by the advanced gap tests.
/// PT: Cria a conexao Oracle em memoria usada pelos testes de gap avancados.
/// </remarks>
public sealed class OracleAdvancedSqlGapTests(ITestOutputHelper helper) : XUnitTestBase(helper)
{
    private readonly OracleConnectionMock _cnn = CreateOpenConnection();

    private static OracleConnectionMock CreateOpenConnection(int? version = null)
    {
        var db = new OracleDbMock(version);
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

        var connection = new OracleConnectionMock(db);
        connection.Open();
        return connection;
    }

    /// <summary>
    /// EN: Verifies ROW_NUMBER respects the configured Oracle version.
    /// PT: Verifica se ROW_NUMBER respeita a versao Oracle configurada.
    /// </summary>
    [Theory]
    [MemberDataOracleVersion]
    [Trait("Category", "OracleAdvancedSqlGap")]
    public void Window_RowNumber_PartitionBy_ShouldWork(int version)
    {
        using var connection = CreateOpenConnection(version);
        const string sql = @"
SELECT id, tenantid,
       ROW_NUMBER() OVER (PARTITION BY tenantid ORDER BY id) AS rn
FROM users
ORDER BY tenantid, id";

        if (version < OracleDialect.WindowFunctionsMinVersion)
        {
            Assert.Throws<NotSupportedException>(() => connection.Query<dynamic>(sql).ToList());
            return;
        }

        var rows = connection.Query<dynamic>(sql).ToList();

        Assert.Equal([1, 2, 1], [.. rows.Select(r => (int)r.rn)]);
    }

    /// <summary>
    /// EN: Verifies RANK and DENSE_RANK respect the configured Oracle version.
    /// PT: Verifica se RANK e DENSE_RANK respeitam a versao Oracle configurada.
    /// </summary>
    [Theory]
    [MemberDataOracleVersion]
    [Trait("Category", "OracleAdvancedSqlGap")]
    public void Window_Rank_And_DenseRank_ShouldWork(int version)
    {
        using var connection = CreateOpenConnection(version);
        const string sql = @"
SELECT id,
       RANK() OVER (ORDER BY tenantid) AS rk,
       DENSE_RANK() OVER (ORDER BY tenantid) AS dr
FROM users
ORDER BY id";

        if (version < OracleDialect.WindowFunctionsMinVersion)
        {
            Assert.Throws<NotSupportedException>(() => connection.Query<dynamic>(sql).ToList());
            return;
        }

        var rows = connection.Query<dynamic>(sql).ToList();

        Assert.Equal([1, 1, 3], [.. rows.Select(r => (int)r.rk)]);
        Assert.Equal([1, 1, 2], [.. rows.Select(r => (int)r.dr)]);
    }


    /// <summary>
    /// EN: Verifies NTILE respects the configured Oracle version.
    /// PT: Verifica se NTILE respeita a versao Oracle configurada.
    /// </summary>
    [Theory]
    [MemberDataOracleVersion]
    [Trait("Category", "OracleAdvancedSqlGap")]
    public void Window_Ntile_ShouldWork(int version)
    {
        using var connection = CreateOpenConnection(version);
        const string sql = @"
SELECT id,
       NTILE(2) OVER (ORDER BY id) AS tile
FROM users
ORDER BY id";

        if (version < OracleDialect.WindowFunctionsMinVersion)
        {
            Assert.Throws<NotSupportedException>(() => connection.Query<dynamic>(sql).ToList());
            return;
        }

        var rows = connection.Query<dynamic>(sql).ToList();

        Assert.Equal([1, 1, 2], [.. rows.Select(r => (int)r.tile)]);
    }


    /// <summary>
    /// EN: Verifies PERCENT_RANK and CUME_DIST respect the configured Oracle version.
    /// PT: Verifica se PERCENT_RANK e CUME_DIST respeitam a versao Oracle configurada.
    /// </summary>
    [Theory]
    [MemberDataOracleVersion]
    [Trait("Category", "OracleAdvancedSqlGap")]
    public void Window_PercentRank_And_CumeDist_ShouldWork(int version)
    {
        using var connection = CreateOpenConnection(version);
        const string sql = @"
SELECT id,
       PERCENT_RANK() OVER (ORDER BY tenantid) AS pr,
       CUME_DIST() OVER (ORDER BY tenantid) AS cd
FROM users
ORDER BY id";

        if (version < OracleDialect.WindowFunctionsMinVersion)
        {
            Assert.Throws<NotSupportedException>(() => connection.Query<dynamic>(sql).ToList());
            return;
        }

        var rows = connection.Query<dynamic>(sql).ToList();

        var pr = rows.Select(r => Convert.ToDouble(r.pr)).ToArray();
        var cd = rows.Select(r => Convert.ToDouble(r.cd)).ToArray();

        Assert.Equal([0d, 0d, 1d], pr);
        Assert.True(Math.Abs(cd[0] - (2d / 3d)) <= 1e-9);
        Assert.True(Math.Abs(cd[1] - (2d / 3d)) <= 1e-9);
        Assert.True(Math.Abs(cd[2] - 1d) <= 1e-9);
    }


    /// <summary>
    /// EN: Verifies LAG and LEAD respect the configured Oracle version.
    /// PT: Verifica se LAG e LEAD respeitam a versao Oracle configurada.
    /// </summary>
    [Theory]
    [MemberDataOracleVersion]
    [Trait("Category", "OracleAdvancedSqlGap")]
    public void Window_Lag_And_Lead_ShouldWork(int version)
    {
        using var connection = CreateOpenConnection(version);
        const string sql = @"
SELECT id,
       LAG(id) OVER (ORDER BY id) AS prev_id,
       LEAD(id, 1, 99) OVER (ORDER BY id) AS next_id
FROM users
ORDER BY id";

        if (version < OracleDialect.WindowFunctionsMinVersion)
        {
            Assert.Throws<NotSupportedException>(() => connection.Query<dynamic>(sql).ToList());
            return;
        }

        var rows = connection.Query<dynamic>(sql).ToList();

        Assert.Equal([null, 1, 2], [.. rows.Select(r => (int?)r.prev_id)]);
        Assert.Equal([2, 3, 99], [.. rows.Select(r => (int)r.next_id)]);
    }


    /// <summary>
    /// EN: Verifies FIRST_VALUE and LAST_VALUE respect the configured Oracle version.
    /// PT: Verifica se FIRST_VALUE e LAST_VALUE respeitam a versao Oracle configurada.
    /// </summary>
    [Theory]
    [MemberDataOracleVersion]
    [Trait("Category", "OracleAdvancedSqlGap")]
    public void Window_FirstValue_And_LastValue_ShouldWork(int version)
    {
        using var connection = CreateOpenConnection(version);
        const string sql = @"
SELECT id,
       FIRST_VALUE(name) OVER (ORDER BY id) AS first_name,
       LAST_VALUE(name) OVER (
           ORDER BY id
           ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
       ) AS last_name
FROM users
ORDER BY id";

        if (version < OracleDialect.WindowFunctionsMinVersion)
        {
            Assert.Throws<NotSupportedException>(() => connection.Query<dynamic>(sql).ToList());
            return;
        }

        var rows = connection.Query<dynamic>(sql).ToList();

        Assert.Equal(["John", "John", "John"], [.. rows.Select(r => (string)r.first_name)]);
        Assert.Equal(["Jane", "Jane", "Jane"], [.. rows.Select(r => (string)r.last_name)]);
    }


    /// <summary>
    /// EN: Verifies NTH_VALUE respects the configured Oracle version.
    /// PT: Verifica se NTH_VALUE respeita a versao Oracle configurada.
    /// </summary>
    [Theory]
    [MemberDataOracleVersion]
    [Trait("Category", "OracleAdvancedSqlGap")]
    public void Window_NthValue_ShouldWork(int version)
    {
        using var connection = CreateOpenConnection(version);
        const string sql = @"
SELECT id,
       NTH_VALUE(name, 2) OVER (
           ORDER BY id
           ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
       ) AS second_name
FROM users
ORDER BY id";

        if (version < OracleDialect.WindowFunctionsMinVersion)
        {
            Assert.Throws<NotSupportedException>(() => connection.Query<dynamic>(sql).ToList());
            return;
        }

        var rows = connection.Query<dynamic>(sql).ToList();

        Assert.Equal(["Bob", "Bob", "Bob"], [.. rows.Select(r => (string)r.second_name)]);
    }


    /// <summary>
    /// EN: Verifies zero-offset LAG and LEAD return the current row when the version supports window functions.
    /// PT: Verifica se LAG e LEAD com offset zero retornam a linha atual quando a versao suporta funcoes de janela.
    /// </summary>
    [Theory]
    [MemberDataOracleVersion]
    [Trait("Category", "OracleAdvancedSqlGap")]
    public void Window_Lag_Lead_WithZeroOffset_ShouldReturnCurrentRow(int version)
    {
        using var connection = CreateOpenConnection(version);
        const string sql = @"
SELECT id,
       LAG(id, 0, -1) OVER (ORDER BY id) AS lag0,
       LEAD(id, 0, -1) OVER (ORDER BY id) AS lead0
FROM users
ORDER BY id";

        if (version < OracleDialect.WindowFunctionsMinVersion)
        {
            Assert.Throws<NotSupportedException>(() => connection.Query<dynamic>(sql).ToList());
            return;
        }

        var rows = connection.Query<dynamic>(sql).ToList();

        Assert.Equal([1, 2, 3], [.. rows.Select(r => (int)r.lag0)]);
        Assert.Equal([1, 2, 3], [.. rows.Select(r => (int)r.lead0)]);
    }

    /// <summary>
    /// EN: Verifies an Oracle reference query combining CTE, JOIN, LEFT JOIN, EXISTS, LISTAGG, NVL, NVL2, DECODE, INTERVAL, CAST and ROW_NUMBER returns the expected rows.
    /// PT: Verifica se uma query de referencia do Oracle combinando CTE, JOIN, LEFT JOIN, EXISTS, LISTAGG, NVL, NVL2, DECODE, INTERVAL, CAST e ROW_NUMBER retorna as linhas esperadas.
    /// </summary>
    [Theory]
    [MemberDataOracleVersion]
    [Trait("Category", "OracleAdvancedSqlGap")]
    public void ProviderSignature_CteAggregateAndWindow_ShouldWork(int version)
    {
        using var connection = CreateOpenConnection(version);
        const string sql = @"
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
ORDER BY tenantid, rn, id";

        if (version < OracleDialect.WithCteMinVersion || version < OracleDialect.WindowFunctionsMinVersion)
        {
            Assert.Throws<NotSupportedException>(() => connection.Query<dynamic>(sql).ToList());
            return;
        }

        var rows = connection.Query<dynamic>(sql).ToList();

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
    /// EN: Verifies NOT REGEXP filters rows as expected.
    /// PT: Verifica se NOT REGEXP filtra as linhas como esperado.
    /// </summary>
    [Fact]
    [Trait("Category", "OracleAdvancedSqlGap")]
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
    [Trait("Category", "OracleAdvancedSqlGap")]
    public void Like_NotOperator_ShouldWork()
    {
        var rows = _cnn.Query<dynamic>("SELECT id FROM users WHERE name NOT LIKE 'J%' ORDER BY id").ToList();
        Assert.Equal([2], [.. rows.Select(r => (int)r.id)]);
    }


    /// <summary>
    /// EN: Verifies expression-based offsets in LAG and NTH_VALUE respect the configured Oracle version.
    /// PT: Verifica se offsets baseados em expressao em LAG e NTH_VALUE respeitam a versao Oracle configurada.
    /// </summary>
    [Theory]
    [MemberDataOracleVersion]
    [Trait("Category", "OracleAdvancedSqlGap")]
    public void Window_Lag_And_NthValue_WithExpressionOffset_ShouldWork(int version)
    {
        using var connection = CreateOpenConnection(version);
        const string sql = @"
SELECT id,
       LAG(id, 1 + 0, -1) OVER (ORDER BY id) AS lag_expr,
       NTH_VALUE(name, 1 + 1) OVER (
           ORDER BY id
           ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
       ) AS nth_expr
FROM users
ORDER BY id";

        if (version < OracleDialect.WindowFunctionsMinVersion)
        {
            Assert.Throws<NotSupportedException>(() => connection.Query<dynamic>(sql).ToList());
            return;
        }

        var rows = connection.Query<dynamic>(sql).ToList();

        Assert.Equal([-1, 1, 2], [.. rows.Select(r => (int)r.lag_expr)]);
        Assert.Equal(["Bob", "Bob", "Bob"], [.. rows.Select(r => (string)r.nth_expr)]);
    }


    /// <summary>
    /// EN: Verifies expression-based bucket counts in NTILE respect the configured Oracle version.
    /// PT: Verifica se contagens de buckets baseadas em expressao no NTILE respeitam a versao Oracle configurada.
    /// </summary>
    [Theory]
    [MemberDataOracleVersion]
    [Trait("Category", "OracleAdvancedSqlGap")]
    public void Window_Ntile_WithExpressionBuckets_ShouldWork(int version)
    {
        using var connection = CreateOpenConnection(version);
        const string sql = @"
SELECT id,
       NTILE(1 + 1) OVER (ORDER BY id) AS tile_expr
FROM users
ORDER BY id";

        if (version < OracleDialect.WindowFunctionsMinVersion)
        {
            Assert.Throws<NotSupportedException>(() => connection.Query<dynamic>(sql).ToList());
            return;
        }

        var rows = connection.Query<dynamic>(sql).ToList();

        Assert.Equal([1, 1, 2], [.. rows.Select(r => (int)r.tile_expr)]);
    }


    /// <summary>
    /// EN: Verifies correlated subqueries in the select list return the expected totals.
    /// PT: Verifica se subconsultas correlacionadas na lista SELECT retornam os totais esperados.
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

        Assert.Equal([15m, 7m, 0m], [.. rows.Select(r => Convert.ToDecimal(GetValueIgnoreCase(r, "total") ?? 0m))]);
    }

    /// <summary>
    /// EN: Verifies DATE_ADD with a day interval returns the expected dates.
    /// PT: Verifica se DATE_ADD com intervalo de dia retorna as datas esperadas.
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
            [.. rows.Select(r => (DateTime)GetValueIgnoreCase(r, "d")!)]);
    }

    /// <summary>
    /// EN: Verifies string-to-int casts return the expected integer value.
    /// PT: Verifica se casts de string para int retornam o valor inteiro esperado.
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
    /// EN: Verifies REGEXP filters rows as expected.
    /// PT: Verifica se REGEXP filtra as linhas como esperado.
    /// </summary>
    [Fact]
    [Trait("Category", "OracleAdvancedSqlGap")]
    public void Regexp_Operator_ShouldWork()
    {
        var rows = _cnn.Query<dynamic>("SELECT id FROM users WHERE name REGEXP '^J' ORDER BY id").ToList();
        Assert.Equal([1, 3], [.. rows.Select(r => (int)r.id)]);
    }

    private static object? GetValueIgnoreCase(object row, string name)
    {
        if (row is IDictionary<string, object?> values)
        {
            foreach (var pair in values)
            {
                if (string.Equals(pair.Key, name, StringComparison.OrdinalIgnoreCase))
                    return pair.Value;
            }
        }

        return null;
    }



    /// <summary>
    /// EN: Verifies FIELD can be used to order rows explicitly.
    /// PT: Verifica se FIELD pode ser usado para ordenar linhas explicitamente.
    /// </summary>
    [Fact]
    [Trait("Category", "OracleAdvancedSqlGap")]
    public void OrderBy_Field_Function_ShouldWork()
    {
        var rows = _cnn.Query<dynamic>("SELECT id FROM users ORDER BY CASE id WHEN 3 THEN 1 WHEN 1 THEN 2 WHEN 2 THEN 3 ELSE 4 END").ToList();
        Assert.Equal([3, 1, 2], [.. rows.Select(r => (int)r.id)]);
    }

    /// <summary>
    /// EN: Verifies string comparison follows the configured column collation.
    /// PT: Verifica se a comparacao de strings segue a collation configurada da coluna.
    /// </summary>
    [Fact]
    [Trait("Category", "OracleAdvancedSqlGap")]
    public void Collation_CaseSensitivity_ShouldFollowColumnCollation()
    {
        // Oracle uses binary comparison by default here because the column does not declare a collation.
        var rows = _cnn.Query<dynamic>("SELECT id FROM users WHERE name = 'john' ORDER BY id").ToList();
        Assert.Empty(rows);
    }



    /// <summary>
    /// EN: Verifies PIVOT counting by tenant returns the expected rows.
    /// PT: Verifica se o PIVOT de contagem por tenant retorna as linhas esperadas.
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
