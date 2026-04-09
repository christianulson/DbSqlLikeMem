namespace DbSqlLikeMem.MySql.Dapper.Test;

/// <summary>
/// EN: Covers version-gated MySQL gap scenarios that are not fully implemented in the in-memory mock yet.
/// PT: Cobre cenarios de gap do MySQL controlados por versao que ainda nao estao totalmente implementados no mock em memoria.
/// </summary>
public sealed class MySqlAdvancedSqlGapTests : XUnitTestBase
{
    private readonly MySqlConnectionMock _cnn;
    private const int MySqlWindowFunctionsMinVersion = 80;

    /// <summary>
    /// EN: Creates the in-memory MySQL connection used by the advanced gap tests.
    /// PT: Cria a conexao MySQL em memoria usada pelos testes de gap avancados.
    /// </summary>
    public MySqlAdvancedSqlGapTests(ITestOutputHelper helper) : base(helper)
    {
        _cnn = CreateConnection();
        _cnn.Open();
    }

    private static MySqlConnectionMock CreateConnection(int? version = null)
    {
        var db = new MySqlDbMock(version);
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

        return new MySqlConnectionMock(db);
    }

    /// <summary>
    /// EN: Verifies ROW_NUMBER respects the configured MySQL version.
    /// PT: Verifica se ROW_NUMBER respeita a versao MySQL configurada.
    /// </summary>
    [Theory]
    [Trait("Category", "MySqlAdvancedSqlGap")]
    [MemberDataMySqlVersion]
    public void Window_RowNumber_PartitionBy_ShouldRespectVersion(int version)
    {
        using var cnn = CreateConnection(version);
        cnn.Open();

        if (version < MySqlWindowFunctionsMinVersion)
        {
            Assert.Throws<NotSupportedException>(() =>
                cnn.Query<dynamic>(@"
SELECT id, tenantid,
       ROW_NUMBER() OVER (PARTITION BY tenantid ORDER BY id) AS rn
FROM users
ORDER BY tenantid, id").ToList());
            return;
        }

        var rows = cnn.Query<dynamic>(@"
SELECT id, tenantid,
       ROW_NUMBER() OVER (PARTITION BY tenantid ORDER BY id) AS rn
FROM users
ORDER BY tenantid, id").ToList();

        Assert.Equal([1, 2, 1], [.. rows.Select(r => (int)r.rn)]);
    }

    /// <summary>
    /// EN: Verifies RANK and DENSE_RANK respect the configured MySQL version.
    /// PT: Verifica se RANK e DENSE_RANK respeitam a versao MySQL configurada.
    /// </summary>
    [Theory]
    [Trait("Category", "MySqlAdvancedSqlGap")]
    [MemberDataMySqlVersion]
    public void Window_Rank_And_DenseRank_ShouldRespectVersion(int version)
    {
        using var cnn = CreateConnection(version);
        cnn.Open();

        if (version < MySqlWindowFunctionsMinVersion)
        {
            Assert.Throws<NotSupportedException>(() =>
                cnn.Query<dynamic>(@"
SELECT id,
       RANK() OVER (ORDER BY tenantid) AS rk,
       DENSE_RANK() OVER (ORDER BY tenantid) AS dr
FROM users
ORDER BY id").ToList());
            return;
        }

        var rows = cnn.Query<dynamic>(@"
SELECT id,
       RANK() OVER (ORDER BY tenantid) AS rk,
       DENSE_RANK() OVER (ORDER BY tenantid) AS dr
FROM users
ORDER BY id").ToList();

        Assert.Equal([1, 1, 3], [.. rows.Select(r => (int)r.rk)]);
        Assert.Equal([1, 1, 2], [.. rows.Select(r => (int)r.dr)]);
    }


    /// <summary>
    /// EN: Verifies NTILE respects the configured MySQL version.
    /// PT: Verifica se NTILE respeita a versao MySQL configurada.
    /// </summary>
    [Theory]
    [Trait("Category", "MySqlAdvancedSqlGap")]
    [MemberDataMySqlVersion]
    public void Window_Ntile_ShouldRespectVersion(int version)
    {
        using var cnn = CreateConnection(version);
        cnn.Open();

        if (version < MySqlWindowFunctionsMinVersion)
        {
            Assert.Throws<NotSupportedException>(() =>
                cnn.Query<dynamic>(@"
SELECT id,
       NTILE(2) OVER (ORDER BY id) AS tile
FROM users
ORDER BY id").ToList());
            return;
        }

        var rows = cnn.Query<dynamic>(@"
SELECT id,
       NTILE(2) OVER (ORDER BY id) AS tile
FROM users
ORDER BY id").ToList();

        Assert.Equal([1, 1, 2], [.. rows.Select(r => (int)r.tile)]);
    }


    /// <summary>
    /// EN: Verifies PERCENT_RANK and CUME_DIST respect the configured MySQL version.
    /// PT: Verifica se PERCENT_RANK e CUME_DIST respeitam a versao MySQL configurada.
    /// </summary>
    [Theory]
    [Trait("Category", "MySqlAdvancedSqlGap")]
    [MemberDataMySqlVersion]
    public void Window_PercentRank_And_CumeDist_ShouldRespectVersion(int version)
    {
        using var cnn = CreateConnection(version);
        cnn.Open();

        if (version < MySqlWindowFunctionsMinVersion)
        {
            Assert.Throws<NotSupportedException>(() =>
                cnn.Query<dynamic>(@"
SELECT id,
       PERCENT_RANK() OVER (ORDER BY tenantid) AS pr,
       CUME_DIST() OVER (ORDER BY tenantid) AS cd
FROM users
ORDER BY id").ToList());
            return;
        }

        var rows = cnn.Query<dynamic>(@"
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
    /// EN: Verifies LAG and LEAD respect the configured MySQL version.
    /// PT: Verifica se LAG e LEAD respeitam a versao MySQL configurada.
    /// </summary>
    [Theory]
    [Trait("Category", "MySqlAdvancedSqlGap")]
    [MemberDataMySqlVersion]
    public void Window_Lag_And_Lead_ShouldRespectVersion(int version)
    {
        using var cnn = CreateConnection(version);
        cnn.Open();

        if (version < MySqlWindowFunctionsMinVersion)
        {
            Assert.Throws<NotSupportedException>(() =>
                cnn.Query<dynamic>(@"
SELECT id,
       LAG(id) OVER (ORDER BY id) AS prev_id,
       LEAD(id, 1, 99) OVER (ORDER BY id) AS next_id
FROM users
ORDER BY id").ToList());
            return;
        }

        var rows = cnn.Query<dynamic>(@"
SELECT id,
       LAG(id) OVER (ORDER BY id) AS prev_id,
       LEAD(id, 1, 99) OVER (ORDER BY id) AS next_id
FROM users
ORDER BY id").ToList();

        Assert.Equal([null, 1, 2], [.. rows.Select(r => (int?)r.prev_id)]);
        Assert.Equal([2, 3, 99], [.. rows.Select(r => (int)r.next_id)]);
    }


    /// <summary>
    /// EN: Verifies FIRST_VALUE and LAST_VALUE respect the configured MySQL version.
    /// PT: Verifica se FIRST_VALUE e LAST_VALUE respeitam a versao MySQL configurada.
    /// </summary>
    [Theory]
    [Trait("Category", "MySqlAdvancedSqlGap")]
    [MemberDataMySqlVersion]
    public void Window_FirstValue_And_LastValue_ShouldRespectVersion(int version)
    {
        using var cnn = CreateConnection(version);
        cnn.Open();

        if (version < MySqlWindowFunctionsMinVersion)
        {
            Assert.Throws<NotSupportedException>(() =>
                cnn.Query<dynamic>(@"
SELECT id,
       FIRST_VALUE(name) OVER (ORDER BY id) AS first_name,
       LAST_VALUE(name) OVER (ORDER BY id) AS last_name
FROM users
ORDER BY id").ToList());
            return;
        }

        var rows = cnn.Query<dynamic>(@"
SELECT id,
       FIRST_VALUE(name) OVER (ORDER BY id) AS first_name,
       LAST_VALUE(name) OVER (ORDER BY id) AS last_name
FROM users
ORDER BY id").ToList();

        Assert.Equal(["John", "John", "John"], [.. rows.Select(r => (string)r.first_name)]);
        Assert.Equal(["Jane", "Jane", "Jane"], [.. rows.Select(r => (string)r.last_name)]);
    }


    /// <summary>
    /// EN: Verifies NTH_VALUE respects the configured MySQL version.
    /// PT: Verifica se NTH_VALUE respeita a versao MySQL configurada.
    /// </summary>
    [Theory]
    [Trait("Category", "MySqlAdvancedSqlGap")]
    [MemberDataMySqlVersion]
    public void Window_NthValue_ShouldRespectVersion(int version)
    {
        using var cnn = CreateConnection(version);
        cnn.Open();

        if (version < MySqlWindowFunctionsMinVersion)
        {
            Assert.Throws<NotSupportedException>(() =>
                cnn.Query<dynamic>(@"
SELECT id,
       NTH_VALUE(name, 2) OVER (ORDER BY id) AS second_name
FROM users
ORDER BY id").ToList());
            return;
        }

        var rows = cnn.Query<dynamic>(@"
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
    [Theory]
    [Trait("Category", "MySqlAdvancedSqlGap")]
    [MemberDataMySqlVersion]
    public void Window_Lag_Lead_WithZeroOffset_ShouldRespectVersion(int version)
    {
        using var cnn = CreateConnection(version);
        cnn.Open();

        if (version < MySqlWindowFunctionsMinVersion)
        {
            Assert.Throws<NotSupportedException>(() =>
                cnn.Query<dynamic>(@"
SELECT id,
       LAG(id, 0, -1) OVER (ORDER BY id) AS lag0,
       LEAD(id, 0, -1) OVER (ORDER BY id) AS lead0
FROM users
ORDER BY id").ToList());
            return;
        }

        var rows = cnn.Query<dynamic>(@"
SELECT id,
       LAG(id, 0, -1) OVER (ORDER BY id) AS lag0,
       LEAD(id, 0, -1) OVER (ORDER BY id) AS lead0
FROM users
ORDER BY id").ToList();

        Assert.Equal([1, 2, 3], [.. rows.Select(r => (int)r.lag0)]);
        Assert.Equal([1, 2, 3], [.. rows.Select(r => (int)r.lead0)]);
    }

    /// <summary>
    /// EN: Verifies a combined MySQL reference query respects the configured version.
    /// PT: Verifica se uma query combinada de referencia do MySQL respeita a versao configurada.
    /// </summary>
    [Theory]
    [Trait("Category", "MySqlAdvancedSqlGap")]
    [MemberDataMySqlVersion]
    public void ProviderSignature_CteAggregateRegexpAndWindow_ShouldRespectVersion(int version)
    {
        using var cnn = CreateConnection(version);
        cnn.Open();

        const string sql = @"
WITH tenant_scope AS (
    SELECT 10 AS tenantid
    UNION ALL
    SELECT 20
),
order_totals AS (
    SELECT o.userid,
           COUNT(*) AS order_count,
           SUM(CAST(o.amount AS DECIMAL(10,2))) AS total_amount,
           GROUP_CONCAT(CAST(o.id AS CHAR) ORDER BY o.id DESC SEPARATOR '|') AS order_ids
    FROM orders o
    GROUP BY o.userid
),
ranked AS (
    SELECT u.id,
           u.name,
           u.tenantid,
           CAST(u.id AS SIGNED) AS normalized_id,
           DATE_ADD(u.created, INTERVAL 1 DAY) AS shifted_created,
           TIMESTAMPDIFF(DAY, CAST('2020-01-01 00:00:00' AS DATETIME), u.created) AS days_from_anchor,
           CONCAT(CAST(u.tenantid AS CHAR), '-', CAST(u.id AS CHAR)) AS user_code,
           IFNULL(order_totals.order_count, CAST(0 AS SIGNED)) AS order_count,
           IFNULL(order_totals.total_amount, CAST(0 AS DECIMAL(10,2))) AS total_amount,
           IFNULL(order_totals.order_ids, '') AS order_ids,
           CASE
               WHEN EXISTS (SELECT 1 FROM orders ox WHERE ox.userid = u.id AND ox.amount >= CAST(10 AS DECIMAL(10,2))) THEN TRUE
               ELSE FALSE
           END AS has_big_order,
           ROW_NUMBER() OVER (
               PARTITION BY u.tenantid
               ORDER BY IFNULL(order_totals.total_amount, CAST(0 AS DECIMAL(10,2))) DESC, u.id
           ) AS rn
    FROM users u
    JOIN tenant_scope scope ON scope.tenantid = u.tenantid
    LEFT JOIN order_totals ON order_totals.userid = u.id
    WHERE u.name REGEXP '^(John|Bob|Jane)$'
)
SELECT id, name, tenantid, normalized_id, shifted_created, days_from_anchor, user_code, order_count, total_amount, order_ids, has_big_order, rn
FROM ranked
ORDER BY tenantid, rn, id";

        if (version < MySqlWindowFunctionsMinVersion)
        {
            Assert.Throws<NotSupportedException>(() => cnn.Query<dynamic>(sql).ToList());
            return;
        }

        var rows = cnn.Query<dynamic>(sql).ToList();

        Assert.Equal([1, 2, 3], [.. rows.Select(r => (int)r.id)]);
        Assert.Equal(["John", "Bob", "Jane"], [.. rows.Select(r => (string)r.name)]);
        Assert.Equal([10, 10, 20], [.. rows.Select(r => (int)r.tenantid)]);
        Assert.Equal([1, 2, 3], [.. rows.Select(r => (int)r.normalized_id)]);
        Assert.Equal(
            [new DateTime(2020, 1, 2, 0, 0, 0, DateTimeKind.Local), new DateTime(2020, 1, 3, 0, 0, 0, DateTimeKind.Local), new DateTime(2020, 1, 4, 0, 0, 0, DateTimeKind.Local)],
            [.. rows.Select(r => (DateTime)r.shifted_created)]);
        Assert.Equal([0, 1, 2], [.. rows.Select(r => Convert.ToInt32(r.days_from_anchor))]);
        Assert.Equal(["10-1", "10-2", "20-3"], [.. rows.Select(r => (string)r.user_code)]);
        Assert.Equal([2, 1, 0], [.. rows.Select(r => Convert.ToInt32(r.order_count))]);
        Assert.Equal([15m, 7m, 0m], [.. rows.Select(r => Convert.ToDecimal(r.total_amount))]);
        Assert.Equal(["11|10", "12", string.Empty], [.. rows.Select(r => (string)r.order_ids)]);
        Assert.Equal([true, false, false], [.. rows.Select(r => Convert.ToBoolean(r.has_big_order))]);
        Assert.Equal([1, 2, 1], [.. rows.Select(r => (int)r.rn)]);
    }


    /// <summary>
    /// EN: Verifies NOT REGEXP filters rows as expected.
    /// PT: Verifica se NOT REGEXP filtra as linhas como esperado.
    /// </summary>
    [Fact]
    [Trait("Category", "MySqlAdvancedSqlGap")]
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
    [Trait("Category", "MySqlAdvancedSqlGap")]
    public void Like_NotOperator_ShouldWork()
    {
        var rows = _cnn.Query<dynamic>("SELECT id FROM users WHERE name NOT LIKE 'J%' ORDER BY id").ToList();
        Assert.Equal([2], [.. rows.Select(r => (int)r.id)]);
    }


    /// <summary>
    /// EN: Verifies expression-based offsets in LAG and NTH_VALUE respect the configured MySQL version.
    /// PT: Verifica se offsets baseados em expressao em LAG e NTH_VALUE respeitam a versao MySQL configurada.
    /// </summary>
    [Theory]
    [Trait("Category", "MySqlAdvancedSqlGap")]
    [MemberDataMySqlVersion]
    public void Window_Lag_And_NthValue_WithExpressionOffset_ShouldRespectVersion(int version)
    {
        using var cnn = CreateConnection(version);
        cnn.Open();

        if (version < MySqlWindowFunctionsMinVersion)
        {
            Assert.Throws<NotSupportedException>(() =>
                cnn.Query<dynamic>(@"
SELECT id,
       LAG(id, 1 + 0, -1) OVER (ORDER BY id) AS lag_expr,
       NTH_VALUE(name, 1 + 1) OVER (ORDER BY id) AS nth_expr
FROM users
ORDER BY id").ToList());
            return;
        }

        var rows = cnn.Query<dynamic>(@"
SELECT id,
       LAG(id, 1 + 0, -1) OVER (ORDER BY id) AS lag_expr,
       NTH_VALUE(name, 1 + 1) OVER (ORDER BY id) AS nth_expr
FROM users
ORDER BY id").ToList();

        Assert.Equal([-1, 1, 2], [.. rows.Select(r => (int)r.lag_expr)]);
        Assert.Equal(["Bob", "Bob", "Bob"], [.. rows.Select(r => (string)r.nth_expr)]);
    }


    /// <summary>
    /// EN: Verifies expression-based bucket counts in NTILE respect the configured MySQL version.
    /// PT: Verifica se contagens de buckets baseadas em expressao no NTILE respeitam a versao MySQL configurada.
    /// </summary>
    [Theory]
    [Trait("Category", "MySqlAdvancedSqlGap")]
    [MemberDataMySqlVersion]
    public void Window_Ntile_WithExpressionBuckets_ShouldRespectVersion(int version)
    {
        using var cnn = CreateConnection(version);
        cnn.Open();

        if (version < MySqlWindowFunctionsMinVersion)
        {
            Assert.Throws<NotSupportedException>(() =>
                cnn.Query<dynamic>(@"
SELECT id,
       NTILE(1 + 1) OVER (ORDER BY id) AS tile_expr
FROM users
ORDER BY id").ToList());
            return;
        }

        var rows = cnn.Query<dynamic>(@"
SELECT id,
       NTILE(1 + 1) OVER (ORDER BY id) AS tile_expr
FROM users
ORDER BY id").ToList();

        Assert.Equal([1, 1, 2], [.. rows.Select(r => (int)r.tile_expr)]);
    }


    /// <summary>
    /// EN: Verifies correlated subqueries in the select list return the expected totals.
    /// PT: Verifica se subconsultas correlacionadas na lista SELECT retornam os totais esperados.
    /// </summary>
    [Fact]
    [Trait("Category", "MySqlAdvancedSqlGap")]
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
    [Trait("Category", "MySqlAdvancedSqlGap")]
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
    /// EN: Verifies string-to-int casts return the expected integer value.
    /// PT: Verifica se casts de string para int retornam o valor inteiro esperado.
    /// </summary>
    [Fact]
    [Trait("Category", "MySqlAdvancedSqlGap")]
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
    [Trait("Category", "MySqlAdvancedSqlGap")]
    public void Regexp_Operator_ShouldWork()
    {
        var rows = _cnn.Query<dynamic>("SELECT id FROM users WHERE name REGEXP '^J' ORDER BY id").ToList();
        Assert.Equal([1, 3], [.. rows.Select(r => (int)r.id)]);
    }

    /// <summary>
    /// EN: Verifies REGEXP respects the configured dialect case sensitivity.
    /// PT: Verifica se REGEXP respeita a sensibilidade a maiusculas e minusculas do dialeto configurado.
    /// </summary>
    [Fact]
    [Trait("Category", "MySqlAdvancedSqlGap")]
    public void Regexp_Operator_ShouldRespectDialectCaseSensitivity()
    {
        var rows = _cnn.Query<dynamic>("SELECT id FROM users WHERE name REGEXP '^j' ORDER BY id").ToList();
        Assert.Equal([1, 3], [.. rows.Select(r => (int)r.id)]);
    }


    /// <summary>
    /// EN: Verifies FIELD can be used to order rows explicitly.
    /// PT: Verifica se FIELD pode ser usado para ordenar linhas explicitamente.
    /// </summary>
    [Fact]
    [Trait("Category", "MySqlAdvancedSqlGap")]
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
    [Trait("Category", "MySqlAdvancedSqlGap")]
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
