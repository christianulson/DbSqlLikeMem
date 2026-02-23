namespace DbSqlLikeMem.MySql.Dapper.Test;

/// <summary>
/// TDD guard-rail tests for SQL features where this in-memory MySql mock commonly diverges from real MySQL.
/// These tests are EXPECTED TO FAIL until the corresponding functionality is implemented in the parser/executor.
/// </summary>
public sealed class MySqlSqlCompatibilityGapTests : XUnitTestBase
{
    private readonly MySqlConnectionMock _cnn;
    private const int MySqlCteMinVersion = 8;

    /// <summary>
    /// EN: Tests MySqlSqlCompatibilityGapTests behavior.
    /// PT: Testa o comportamento de MySqlSqlCompatibilityGapTests.
    /// </summary>
    public MySqlSqlCompatibilityGapTests(ITestOutputHelper helper) : base(helper)
    {
        _cnn = CreateConnection();
        _cnn.Open();
    }

    private static MySqlConnectionMock CreateConnection(int? version = null)
    {
        // users
        var db = new MySqlDbMock(version);
        var users = db.AddTable("users");
        users.AddColumn("id", DbType.Int32, false);
        users.AddColumn("name", DbType.String, false);
        users.AddColumn("email", DbType.String, true);

        users.Add(new Dictionary<int, object?> { [0] = 1, [1] = "John", [2] = "john@x.com" });
        users.Add(new Dictionary<int, object?> { [0] = 2, [1] = "Bob",  [2] = null });
        users.Add(new Dictionary<int, object?> { [0] = 3, [1] = "Jane", [2] = "jane@x.com" });

        // orders
        var orders = db.AddTable("orders");
        orders.AddColumn("id", DbType.Int32, false);
        orders.AddColumn("userId", DbType.Int32, false);
        orders.AddColumn("amount", DbType.Decimal, false, decimalPlaces: 2);

        orders.Add(new Dictionary<int, object?> { [0] = 10, [1] = 1, [2] = 50m });
        orders.Add(new Dictionary<int, object?> { [0] = 11, [1] = 2, [2] = 200m });
        orders.Add(new Dictionary<int, object?> { [0] = 12, [1] = 2, [2] = 10m });

        return new MySqlConnectionMock(db);
    }

    /// <summary>
    /// EN: Tests Where_Precedence_AND_ShouldBindStrongerThan_OR behavior.
    /// PT: Testa o comportamento de Where_Precedence_AND_ShouldBindStrongerThan_OR.
    /// </summary>
    [Fact]
    [Trait("Category", "MySqlSqlCompatibilityGap")]
    public void Where_Precedence_AND_ShouldBindStrongerThan_OR()
    {
        // MySQL precedence: AND binds stronger than OR.
        // Equivalent to: id = 1 OR (id = 2 AND name = 'Bob')
        var rows = _cnn.Query<dynamic>("SELECT id FROM users WHERE id = 1 OR id = 2 AND name = 'Bob'").ToList();
        Assert.Equal([1, 2], [.. rows.Select(r => (int)r.id).OrderBy(_ => _)]);
    }

    /// <summary>
    /// EN: Tests Where_OR_ShouldWork behavior.
    /// PT: Testa o comportamento de Where_OR_ShouldWork.
    /// </summary>
    [Fact]
    [Trait("Category", "MySqlSqlCompatibilityGap")]
    public void Where_OR_ShouldWork()
    {
        var rows = _cnn.Query<dynamic>("SELECT id FROM users WHERE id = 1 OR id = 3").ToList();
        Assert.Equal([1, 3], [.. rows.Select(r => (int)r.id).OrderBy(_ => _)]);
    }

    /// <summary>
    /// EN: Tests Where_ParenthesesGrouping_ShouldWork behavior.
    /// PT: Testa o comportamento de Where_ParenthesesGrouping_ShouldWork.
    /// </summary>
    [Fact]
    [Trait("Category", "MySqlSqlCompatibilityGap")]
    public void Where_ParenthesesGrouping_ShouldWork()
    {
        // (id=1 OR id=2) AND email IS NULL => only user 2
        var rows = _cnn.Query<dynamic>("SELECT id FROM users WHERE (id = 1 OR id = 2) AND email IS NULL").ToList();
        Assert.Single(rows);
        Assert.Equal(2, (int)rows[0].id);
    }

    /// <summary>
    /// EN: Tests Select_Expressions_Arithmetic_ShouldWork behavior.
    /// PT: Testa o comportamento de Select_Expressions_Arithmetic_ShouldWork.
    /// </summary>
    [Fact]
    [Trait("Category", "MySqlSqlCompatibilityGap")]
    public void Select_Expressions_Arithmetic_ShouldWork()
    {
        var rows = _cnn.Query<dynamic>("SELECT id, id + 1 AS nextId FROM users ORDER BY id").ToList();
        Assert.Equal([2, 3, 4], [.. rows.Select(r => (int)r.nextId)]);
    }

    /// <summary>
    /// EN: Tests Select_Expressions_CASE_WHEN_ShouldWork behavior.
    /// PT: Testa o comportamento de Select_Expressions_CASE_WHEN_ShouldWork.
    /// </summary>
    [Fact]
    [Trait("Category", "MySqlSqlCompatibilityGap")]
    public void Select_Expressions_CASE_WHEN_ShouldWork()
    {
        var rows = _cnn.Query<dynamic>("SELECT id, CASE WHEN email IS NULL THEN 0 ELSE 1 END AS hasEmail FROM users ORDER BY id").ToList();
        Assert.Equal([1, 0, 1], [.. rows.Select(r => (int)r.hasEmail)]);
    }

    /// <summary>
    /// EN: Tests Select_Expressions_IF_ShouldWork behavior.
    /// PT: Testa o comportamento de Select_Expressions_IF_ShouldWork.
    /// </summary>
    [Fact]
    [Trait("Category", "MySqlSqlCompatibilityGap")]
    public void Select_Expressions_IF_ShouldWork()
    {
        // MySQL: IF(cond, then, else)
        var rows = _cnn.Query<dynamic>("SELECT id, IF(email IS NULL, 'no', 'yes') AS flag FROM users ORDER BY id").ToList();
        Assert.Equal(["yes", "no", "yes"], [.. rows.Select(r => (string)r.flag)]);
    }

    /// <summary>
    /// EN: Tests Select_Expressions_IIF_ShouldWork_AsAliasForIF behavior.
    /// PT: Testa o comportamento de Select_Expressions_IIF_ShouldWork_AsAliasForIF.
    /// </summary>
    [Fact]
    [Trait("Category", "MySqlSqlCompatibilityGap")]
    public void Select_Expressions_IIF_ShouldWork_AsAliasForIF()
    {
        // Not native MySQL, but requested as convenience.
        var rows = _cnn.Query<dynamic>("SELECT id, IIF(email IS NULL, 0, 1) AS hasEmail FROM users ORDER BY id").ToList();
        Assert.Equal([1, 0, 1], [.. rows.Select(r => (int)r.hasEmail)]);
    }

    /// <summary>
    /// EN: Tests Functions_COALESCE_ShouldWork behavior.
    /// PT: Testa o comportamento de Functions_COALESCE_ShouldWork.
    /// </summary>
    [Fact]
    [Trait("Category", "MySqlSqlCompatibilityGap")]
    public void Functions_COALESCE_ShouldWork()
    {
        var rows = _cnn.Query<dynamic>("SELECT id, COALESCE(NULL, email, 'none') AS em FROM users ORDER BY id").ToList();
        Assert.Equal(["john@x.com", "none", "jane@x.com"], [.. rows.Select(r => (string)r.em)]);
    }

    /// <summary>
    /// EN: Tests Functions_IFNULL_ShouldWork behavior.
    /// PT: Testa o comportamento de Functions_IFNULL_ShouldWork.
    /// </summary>
    [Fact]
    [Trait("Category", "MySqlSqlCompatibilityGap")]
    public void Functions_IFNULL_ShouldWork()
    {
        var rows = _cnn.Query<dynamic>("SELECT id, IFNULL(email, 'none') AS em FROM users ORDER BY id").ToList();
        Assert.Equal(["john@x.com", "none", "jane@x.com"], [.. rows.Select(r => (string)r.em)]);
    }

    /// <summary>
    /// EN: Tests Functions_CONCAT_ShouldWork behavior.
    /// PT: Testa o comportamento de Functions_CONCAT_ShouldWork.
    /// </summary>
    [Fact]
    [Trait("Category", "MySqlSqlCompatibilityGap")]
    public void Functions_CONCAT_ShouldWork()
    {
        var rows = _cnn.Query<dynamic>("SELECT id, CONCAT(name, '#', id) AS tag FROM users ORDER BY id").ToList();
        Assert.Equal(["John#1", "Bob#2", "Jane#3"], [.. rows.Select(r => (string)r.tag)]);
    }

    /// <summary>
    /// EN: Tests Distinct_ShouldBeConsistent behavior.
    /// PT: Testa o comportamento de Distinct_ShouldBeConsistent.
    /// </summary>
    [Fact]
    [Trait("Category", "MySqlSqlCompatibilityGap")]
    public void Distinct_ShouldBeConsistent()
    {
        // duplicate names
        _cnn.Execute("INSERT INTO users (id,name,email) VALUES (4,'john','j2@x.com')");
        var rows = _cnn.Query<dynamic>("SELECT DISTINCT name FROM users ORDER BY name").ToList();
        Assert.Equal(["Bob", "Jane", "John"], [.. rows.Select(r => (string)r.name)]);
    }

    /// <summary>
    /// EN: Tests Join_ComplexOn_WithOr_ShouldWork behavior.
    /// PT: Testa o comportamento de Join_ComplexOn_WithOr_ShouldWork.
    /// </summary>
    [Fact]
    [Trait("Category", "MySqlSqlCompatibilityGap")]
    public void Join_ComplexOn_WithOr_ShouldWork()
    {
        // include orders joined when (o.userId = u.id OR o.userId = 0)
        // We'll add a global order with userId=0 and expect it to join to ALL users.
        _cnn.Execute("INSERT INTO orders (id,userId,amount) VALUES (13,0,1)");
        var rows = _cnn.Query<dynamic>(
            "SELECT u.id AS uid, o.id AS oid FROM users u " +
            "JOIN orders o ON (o.userId = u.id OR o.userId = 0) " +
            "WHERE u.id IN (1,2) ORDER BY u.id, o.id").ToList();

        // For uid=1: orders 10 and 13; for uid=2: orders 11,12,13
        Assert.Equal([(1,10),(1,13),(2,11),(2,12),(2,13)],
            [.. rows.Select(r => ((int)r.uid,(int)r.oid))]);
    }

    /// <summary>
    /// EN: Tests GroupBy_Having_ShouldSupportAggregates behavior.
    /// PT: Testa o comportamento de GroupBy_Having_ShouldSupportAggregates.
    /// </summary>
    [Fact]
    [Trait("Category", "MySqlSqlCompatibilityGap")]
    public void GroupBy_Having_ShouldSupportAggregates()
    {
        var rows = _cnn.Query<dynamic>(
            "SELECT userId, SUM(amount) AS total " +
            "FROM orders GROUP BY userId HAVING SUM(amount) > 100 " +
            "ORDER BY userId").ToList();

        Assert.Single(rows);
        Assert.Equal(2, (int)rows[0].userId);
        Assert.Equal(210m, (decimal)rows[0].total);
    }

    /// <summary>
    /// EN: Tests OrderBy_ShouldSupportAlias_And_Ordinal behavior.
    /// PT: Testa o comportamento de OrderBy_ShouldSupportAlias_And_Ordinal.
    /// </summary>
    [Fact]
    [Trait("Category", "MySqlSqlCompatibilityGap")]
    public void OrderBy_ShouldSupportAlias_And_Ordinal()
    {
        var rows1 = _cnn.Query<dynamic>("SELECT id, id + 1 AS x FROM users ORDER BY x DESC").ToList();
        Assert.Equal([3,2,1], [.. rows1.Select(r => (int)r.id)]);

        var rows2 = _cnn.Query<dynamic>("SELECT id, name FROM users ORDER BY 2 ASC, 1 DESC").ToList();
        // order by name asc, then id desc
        Assert.Equal([(2,"Bob"),(3,"Jane"),(1,"John")], [.. rows2.Select(r => ((int)r.id,(string)r.name))]);
    }

    /// <summary>
    /// EN: Tests Union_ShouldWork behavior.
    /// PT: Testa o comportamento de Union_ShouldWork.
    /// </summary>
    [Fact]
    [Trait("Category", "MySqlSqlCompatibilityGap")]
    public void Union_ShouldWork()
    {
        var rows = _cnn.Query<dynamic>(
            "SELECT id FROM users WHERE id = 1 " +
            "UNION " +
            "SELECT id FROM users WHERE id = 2 " +
            "ORDER BY id").ToList();
        Assert.Equal([1,2], [.. rows.Select(r => (int)r.id)]);
    }

    /// <summary>
    /// EN: Tests Union_All_ShouldWork behavior.
    /// PT: Testa o comportamento de Union_All_ShouldWork.
    /// </summary>
    [Fact]
    [Trait("Category", "MySqlSqlCompatibilityGap")]
    public void Union_All_ShouldWork()
    {
        var rows = _cnn.Query<dynamic>(
            "SELECT id FROM users WHERE id = 1 " +
            "UNION ALL " +
            "SELECT id FROM users WHERE id = 1 " +
            "ORDER BY id").ToList();

        Assert.Equal([1, 1], [.. rows.Select(r => (int)r.id)]);
    }


    /// <summary>
    /// EN: Tests Union_Inside_SubSelect_ShouldWork behavior.
    /// PT: Testa o comportamento de Union_Inside_SubSelect_ShouldWork.
    /// </summary>
    [Fact]
    [Trait("Category", "MySqlSqlCompatibilityGap")]
    public void Union_Inside_SubSelect_ShouldWork()
    {
        var rows = _cnn.Query<dynamic>(@"
SELECT * FROM (
SELECT id FROM users WHERE id = 1
UNION
SELECT id FROM users WHERE id = 2
) X
ORDER BY id
").ToList();
        Assert.Equal([1, 2], [.. rows.Select(r => (int)r.id)]);
    }

    /// <summary>
    /// EN: Tests Union_All_Inside_SubSelect_ShouldWork behavior.
    /// PT: Testa o comportamento de Union_All_Inside_SubSelect_ShouldWork.
    /// </summary>
    [Fact]
    [Trait("Category", "MySqlSqlCompatibilityGap")]
    public void Union_All_Inside_SubSelect_ShouldWork()
    {
        var rows = _cnn.Query<dynamic>(@"
SELECT * FROM (
SELECT id FROM users WHERE id = 1
UNION ALL
SELECT id FROM users WHERE id = 1
) X
ORDER BY id
").ToList();
        Assert.Equal([1, 1], [.. rows.Select(r => (int)r.id)]);
    }


    /// <summary>
    /// EN: Tests Cte_With_ShouldWork behavior.
    /// PT: Testa o comportamento de Cte_With_ShouldWork.
    /// </summary>
    [Theory]
    [Trait("Category", "MySqlSqlCompatibilityGap")]
    [MemberDataMySqlVersion]
    public void Cte_With_ShouldRespectVersion(int version)
    {
        using var cnn = CreateConnection(version);
        cnn.Open();

        if (version < MySqlCteMinVersion)
        {
            Assert.Throws<NotSupportedException>(() =>
                cnn.Query<dynamic>(
                    "WITH u AS (SELECT id, name FROM users WHERE id <= 2) " +
                    "SELECT id FROM u ORDER BY id DESC").ToList());
            return;
        }

        var rows = cnn.Query<dynamic>(
            "WITH u AS (SELECT id, name FROM users WHERE id <= 2) " +
            "SELECT id FROM u ORDER BY id DESC").ToList();
        Assert.Equal([2,1], [.. rows.Select(r => (int)r.id)]);
    }

    /// <summary>
    /// EN: Tests Typing_ImplicitCasts_And_Collation_ShouldMatchMySqlDefault behavior.
    /// PT: Testa o comportamento de Typing_ImplicitCasts_And_Collation_ShouldMatchMySqlDefault.
    /// </summary>
    [Fact]
    [Trait("Category", "MySqlSqlCompatibilityGap")]
    public void Typing_ImplicitCasts_And_Collation_ShouldMatchMySqlDefault()
    {
        // Many MySQL installations use case-insensitive collations by default.
        var rows1 = _cnn.Query<dynamic>("SELECT id FROM users WHERE name = 'john'").ToList();
        Assert.Single(rows1);
        Assert.Equal(1, (int)rows1[0].id);

        // Implicit cast string->int for comparison
        var rows2 = _cnn.Query<dynamic>("SELECT id FROM users WHERE id = '2'").ToList();
        Assert.Single(rows2);
        Assert.Equal(2, (int)rows2[0].id);
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
