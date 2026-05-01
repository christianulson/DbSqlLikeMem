namespace DbSqlLikeMem.Firebird.Dapper.Test;

/// <summary>
/// EN: Covers Firebird WHERE parser and executor scenarios over a direct mock connection.
/// PT-br: Cobre cenarios do parser e executor de WHERE Firebird sobre uma conexao mock direta.
/// </summary>
public sealed class FirebirdWhereParserAndExecutorTests(
    ITestOutputHelper helper
) : XUnitTestBase(helper)
{
    /// <summary>
    /// EN: Creates the in-memory Firebird database used by the WHERE parser and executor coverage tests.
    /// PT-br: Cria o banco Firebird em memoria usado pelos testes de cobertura do parser e executor de WHERE.
    /// </summary>
    private static FirebirdConnectionMock CreateOpenConnection()
    {
        var db = new FirebirdDbMock();
        var users = db.AddTable("users");
        users.AddColumn("id", DbType.Int32, false);
        users.AddColumn("name", DbType.String, false);
        users.AddColumn("email", DbType.String, true);
        users.AddColumn("tags", DbType.String, true);

        users.Add(new Dictionary<int, object?> { [0] = 1, [1] = "John", [2] = "john@x.com", [3] = "a,b" });
        users.Add(new Dictionary<int, object?> { [0] = 2, [1] = "Jane", [2] = null, [3] = "b,c" });
        users.Add(new Dictionary<int, object?> { [0] = 3, [1] = "Bob", [2] = "bob@x.com", [3] = null });

        var orders = db.AddTable("orders");
        orders.AddColumn("id", DbType.Int32, false);
        orders.AddColumn("userid", DbType.Int32, false);
        orders.AddColumn("amount", DbType.Decimal, false, decimalPlaces: 2);
        orders.Add(new Dictionary<int, object?> { [0] = 10, [1] = 1, [2] = 50m });
        orders.Add(new Dictionary<int, object?> { [0] = 11, [1] = 2, [2] = 200m });
        orders.Add(new Dictionary<int, object?> { [0] = 12, [1] = 2, [2] = 10m });

        var connection = new FirebirdConnectionMock(db);
        connection.Open();
        return connection;
    }

    /// <summary>
    /// EN: Verifies AND binds stronger than OR in Firebird WHERE coverage.
    /// PT-br: Verifica se AND tem maior precedencia que OR na cobertura de WHERE do Firebird.
    /// </summary>
    [Fact]
    [Trait("Category", "FirebirdWhereParserAndExecutor")]
    public void Where_Precedence_AND_ShouldBindStrongerThan_OR()
    {
        using var connection = CreateOpenConnection();

        var rows = connection.Query<dynamic>(
            "SELECT id FROM users WHERE id = 1 OR id = 2 AND name = 'Jane' ORDER BY id")
            .ToList();

        Assert.Equal([1, 2], [.. rows.Select(r => Convert.ToInt32(GetValueIgnoreCase((object)r, "id")) )]);
    }

    /// <summary>
    /// EN: Verifies parentheses grouping works in Firebird WHERE coverage.
    /// PT-br: Verifica se o agrupamento com parenteses funciona na cobertura de WHERE do Firebird.
    /// </summary>
    [Fact]
    [Trait("Category", "FirebirdWhereParserAndExecutor")]
    public void Where_ParenthesesGrouping_ShouldWork()
    {
        using var connection = CreateOpenConnection();

        var rows = connection.Query<dynamic>(
            "SELECT id FROM users WHERE (id = 1 OR id = 2) AND email IS NULL")
            .ToList();

        Assert.Single(rows);
        Assert.Equal(2, Convert.ToInt32(GetValueIgnoreCase((object)rows[0], "id")));
    }

    /// <summary>
    /// EN: Verifies arithmetic expressions, CASE, and COALESCE work in Firebird SELECT projections.
    /// PT-br: Verifica se expressoes aritmeticas, CASE e COALESCE funcionam em projecoes SELECT do Firebird.
    /// </summary>
    [Fact]
    [Trait("Category", "FirebirdWhereParserAndExecutor")]
    public void Select_Expressions_ArithmeticCaseAndCoalesce_ShouldWork()
    {
        using var connection = CreateOpenConnection();

        var rows = connection.Query<dynamic>(
            """
SELECT id,
       id + 1 AS next_id,
       CASE WHEN email IS NULL THEN 0 ELSE 1 END AS has_email,
       COALESCE(email, 'none') AS email_or_none
FROM users
        ORDER BY id
""")
            .ToList();

        Assert.Equal([2, 3, 4], [.. rows.Select(r => Convert.ToInt32(GetValueIgnoreCase((object)r, "next_id")) )]);
        Assert.Equal([1, 0, 1], [.. rows.Select(r => Convert.ToInt32(GetValueIgnoreCase((object)r, "has_email")) )]);
        Assert.Equal(["john@x.com", "none", "bob@x.com"], [.. rows.Select(r => Convert.ToString(GetValueIgnoreCase((object)r, "email_or_none")) )!]);
    }

    /// <summary>
    /// EN: Verifies JOIN predicates with OR and grouped conditions work in Firebird coverage.
    /// PT-br: Verifica se predicados JOIN com OR e condicoes agrupadas funcionam na cobertura do Firebird.
    /// </summary>
    [Fact]
    [Trait("Category", "FirebirdWhereParserAndExecutor")]
    public void Join_ComplexOn_WithOr_ShouldWork()
    {
        using var connection = CreateOpenConnection();

        connection.Execute("INSERT INTO orders (id, userid, amount) VALUES (13, 0, 1)");

        var rows = connection.Query<dynamic>(
            """
SELECT u.id AS uid, o.id AS oid
FROM users u
JOIN orders o ON (o.userid = u.id OR o.userid = 0)
WHERE u.id IN (1, 2)
        ORDER BY u.id, o.id
""")
            .ToList();

        Assert.Equal([(1, 10), (1, 13), (2, 11), (2, 12), (2, 13)],
            [.. rows.Select(r => (Convert.ToInt32(GetValueIgnoreCase((object)r, "uid")), Convert.ToInt32(GetValueIgnoreCase((object)r, "oid"))))]);
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
}
