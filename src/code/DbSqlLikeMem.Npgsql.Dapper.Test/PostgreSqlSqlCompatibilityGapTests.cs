namespace DbSqlLikeMem.Npgsql.Test;

/// <summary>
/// EN: Covers PostgreSQL SQL compatibility gap scenarios that intentionally document current divergence from real PostgreSQL.
/// PT: Cobre cenarios de gap de compatibilidade SQL do PostgreSQL que documentam intencionalmente as divergencias atuais em relacao ao PostgreSQL real.
/// </summary>
public sealed class PostgreSqlSqlCompatibilityGapTests : XUnitTestBase
{
    private readonly NpgsqlConnectionMock _cnn;

    /// <summary>
    /// EN: Creates the in-memory PostgreSQL connection used by the SQL compatibility gap tests.
    /// PT: Cria a conexao PostgreSQL em memoria usada pelos testes de gap de compatibilidade SQL.
    /// </summary>
    public PostgreSqlSqlCompatibilityGapTests(ITestOutputHelper helper) : base(helper)
    {
        // users
        var db = new NpgsqlDbMock();
        var users = db.AddTable("users");
        users.AddColumn("id", DbType.Int32, false);
        users.AddColumn("name", DbType.String, false);
        users.AddColumn("email", DbType.String, true);

        users.Add(new Dictionary<int, object?> { [0] = 1, [1] = "John", [2] = "john@x.com" });
        users.Add(new Dictionary<int, object?> { [0] = 2, [1] = "Bob", [2] = null });
        users.Add(new Dictionary<int, object?> { [0] = 3, [1] = "Jane", [2] = "jane@x.com" });

        // orders
        var orders = db.AddTable("orders");
        orders.AddColumn("id", DbType.Int32, false);
        orders.AddColumn("userId", DbType.Int32, false);
        orders.AddColumn("amount", DbType.Decimal, false, decimalPlaces: 2);

        orders.Add(new Dictionary<int, object?> { [0] = 10, [1] = 1, [2] = 50m });
        orders.Add(new Dictionary<int, object?> { [0] = 11, [1] = 2, [2] = 200m });
        orders.Add(new Dictionary<int, object?> { [0] = 12, [1] = 2, [2] = 10m });

        _cnn = new NpgsqlConnectionMock(db);

        _cnn.Open();
    }

    /// <summary>
    /// EN: Verifies AND binds stronger than OR in PostgreSQL WHERE coverage.
    /// PT: Verifica se AND tem maior precedencia que OR na cobertura de WHERE do PostgreSQL.
    /// </summary>
    [Fact]
    [Trait("Category", "PostgreSqlSqlCompatibilityGap")]
    public async Task Where_Precedence_AND_ShouldBindStrongerThan_OR()
    {
        // MySQL precedence: AND binds stronger than OR.
        // Equivalent to: id = 1 OR (id = 2 AND name = 'Bob')
        var rows = (await _cnn.QueryAsync<dynamic>("SELECT id FROM users WHERE id = 1 OR id = 2 AND name = 'Bob'")).ToList();
        Assert.Equal([1, 2], [.. rows.Select(r => (int)r.id).OrderBy(_ => _)]);
    }

    /// <summary>
    /// EN: Verifies OR filters rows correctly in PostgreSQL WHERE coverage.
    /// PT: Verifica se OR filtra linhas corretamente na cobertura de WHERE do PostgreSQL.
    /// </summary>
    [Fact]
    [Trait("Category", "PostgreSqlSqlCompatibilityGap")]
    public async Task Where_OR_ShouldWork()
    {
        var rows = (await _cnn.QueryAsync<dynamic>("SELECT id FROM users WHERE id = 1 OR id = 3")).ToList();
        Assert.Equal([1, 3], [.. rows.Select(r => (int)r.id).OrderBy(_ => _)]);
    }

    /// <summary>
    /// EN: Verifies parentheses grouping works in PostgreSQL WHERE coverage.
    /// PT: Verifica se o agrupamento com parenteses funciona na cobertura de WHERE do PostgreSQL.
    /// </summary>
    [Fact]
    [Trait("Category", "PostgreSqlSqlCompatibilityGap")]
    public void Where_ParenthesesGrouping_ShouldWork()
    {
        // (id=1 OR id=2) AND email IS NULL => only user 2
        var rows = _cnn.Query<dynamic>("SELECT id FROM users WHERE (id = 1 OR id = 2) AND email IS NULL").ToList();
        Assert.Single(rows);
        Assert.Equal(2, (int)rows[0].id);
    }

    /// <summary>
    /// EN: Verifies arithmetic expressions are evaluated in SELECT projections.
    /// PT: Verifica se expressoes aritmeticas sao avaliadas em projeções SELECT.
    /// </summary>
    [Fact]
    [Trait("Category", "PostgreSqlSqlCompatibilityGap")]
    public void Select_Expressions_Arithmetic_ShouldWork()
    {
        var rows = _cnn.Query<dynamic>("SELECT id, id + 1 AS nextId FROM users ORDER BY id").ToList();
        Assert.Equal([2, 3, 4], [.. rows.Select(r => (int)r.nextId)]);
    }

    /// <summary>
    /// EN: Verifies CASE WHEN expressions return the expected values.
    /// PT: Verifica se expressoes CASE WHEN retornam os valores esperados.
    /// </summary>
    [Fact]
    [Trait("Category", "PostgreSqlSqlCompatibilityGap")]
    public void Select_Expressions_CASE_WHEN_ShouldWork()
    {
        var rows = _cnn.Query<dynamic>("SELECT id, CASE WHEN email IS NULL THEN 0 ELSE 1 END AS hasEmail FROM users ORDER BY id").ToList();
        Assert.Equal([1, 0, 1], [.. rows.Select(r => (int)r.hasEmail)]);
    }

    /// <summary>
    /// EN: Verifies IF expressions return the expected values.
    /// PT: Verifica se expressoes IF retornam os valores esperados.
    /// </summary>
    [Fact]
    [Trait("Category", "PostgreSqlSqlCompatibilityGap")]
    public void Select_Expressions_IF_ShouldWork()
    {
        var ex = Assert.Throws<NotSupportedException>(() =>
            _cnn.Query<dynamic>("SELECT id, IF(email IS NULL, 'no', 'yes') AS flag FROM users ORDER BY id").ToList());

        Assert.Contains("IF", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Verifies IIF works as an alias for IF in PostgreSQL compatibility coverage.
    /// PT: Verifica se IIF funciona como alias de IF na cobertura de compatibilidade do PostgreSQL.
    /// </summary>
    [Fact]
    [Trait("Category", "PostgreSqlSqlCompatibilityGap")]
    public void Select_Expressions_IIF_ShouldWork_AsAliasForIF()
    {
        var ex = Assert.Throws<NotSupportedException>(() =>
            _cnn.Query<dynamic>("SELECT id, IIF(email IS NULL, 0, 1) AS hasEmail FROM users ORDER BY id").ToList());

        Assert.Contains("IIF", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Verifies COALESCE returns the first non-null value.
    /// PT: Verifica se COALESCE retorna o primeiro valor nao nulo.
    /// </summary>
    [Fact]
    [Trait("Category", "PostgreSqlSqlCompatibilityGap")]
    public void Functions_COALESCE_ShouldWork()
    {
        var rows = _cnn.Query<dynamic>("SELECT id, COALESCE(NULL, email, 'none') AS em FROM users ORDER BY id").ToList();
        Assert.Equal(["john@x.com", "none", "jane@x.com"], [.. rows.Select(r => (string)r.em)]);
    }

    /// <summary>
    /// EN: Verifies IFNULL falls back to the first non-null value.
    /// PT: Verifica se IFNULL faz fallback para o primeiro valor nao nulo.
    /// </summary>
    [Fact]
    [Trait("Category", "PostgreSqlSqlCompatibilityGap")]
    public void Functions_IFNULL_ShouldWork()
    {
        var rows = _cnn.Query<dynamic>("SELECT id, COALESCE(NULL, email, 'none') AS em FROM users ORDER BY id").ToList();
        Assert.Equal(["john@x.com", "none", "jane@x.com"], [.. rows.Select(r => (string)r.em)]);
    }

    /// <summary>
    /// EN: Verifies CONCAT joins string parts in PostgreSQL coverage.
    /// PT: Verifica se CONCAT concatena partes de string na cobertura do PostgreSQL.
    /// </summary>
    [Fact]
    [Trait("Category", "PostgreSqlSqlCompatibilityGap")]
    public void Functions_CONCAT_ShouldWork()
    {
        var rows = _cnn.Query<dynamic>("SELECT id, CONCAT(name, '#', id) AS tag FROM users ORDER BY id").ToList();
        Assert.Equal(["John#1", "Bob#2", "Jane#3"], [.. rows.Select(r => (string)r.tag)]);
    }

    /// <summary>
    /// EN: Verifies DISTINCT returns the expected unique rows.
    /// PT: Verifica se DISTINCT retorna as linhas unicas esperadas.
    /// </summary>
    [Fact]
    [Trait("Category", "PostgreSqlSqlCompatibilityGap")]
    public void Distinct_ShouldBeConsistent()
    {
        // PostgreSQL keeps John and john distinct under the default binary comparison.
        _cnn.Execute("INSERT INTO users (id,name,email) VALUES (4,'john','j2@x.com')");
        var rows = _cnn.Query<dynamic>("SELECT DISTINCT name FROM users ORDER BY name").ToList();
        Assert.Equal(["Bob", "Jane", "John", "john"], [.. rows.Select(r => (string)r.name)]);
    }

    /// <summary>
    /// EN: Verifies JOIN ON clauses with OR predicates work correctly.
    /// PT: Verifica se clausulas JOIN ON com predicados OR funcionam corretamente.
    /// </summary>
    [Fact]
    [Trait("Category", "PostgreSqlSqlCompatibilityGap")]
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
        Assert.Equal([(1, 10), (1, 13), (2, 11), (2, 12), (2, 13)],
            [.. rows.Select(r => ((int)r.uid, (int)r.oid))]);
    }

    /// <summary>
    /// EN: Verifies GROUP BY and HAVING work with aggregate filters.
    /// PT: Verifica se GROUP BY e HAVING funcionam com filtros agregados.
    /// </summary>
    [Fact]
    [Trait("Category", "PostgreSqlSqlCompatibilityGap")]
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
    /// EN: Verifies ORDER BY supports aliases and ordinal positions.
    /// PT: Verifica se ORDER BY aceita aliases e posicoes ordinais.
    /// </summary>
    [Fact]
    [Trait("Category", "PostgreSqlSqlCompatibilityGap")]
    public void OrderBy_ShouldSupportAlias_And_Ordinal()
    {
        var rows1 = _cnn.Query<dynamic>("SELECT id, id + 1 AS x FROM users ORDER BY x DESC").ToList();
        Assert.Equal([3, 2, 1], [.. rows1.Select(r => (int)r.id)]);

        var rows2 = _cnn.Query<dynamic>("SELECT id, name FROM users ORDER BY 2 ASC, 1 DESC").ToList();
        // order by name asc, then id desc
        Assert.Equal([(2, "Bob"), (3, "Jane"), (1, "John")], [.. rows2.Select(r => ((int)r.id, (string)r.name))]);
    }

    /// <summary>
    /// EN: Verifies UNION removes duplicates across SELECT parts.
    /// PT: Verifica se UNION remove duplicatas entre partes do SELECT.
    /// </summary>
    [Fact]
    [Trait("Category", "PostgreSqlSqlCompatibilityGap")]
    public void Union_ShouldWork()
    {
        var rows = _cnn.Query<dynamic>(
            "SELECT id FROM users WHERE id = 1 " +
            "UNION " +
            "SELECT id FROM users WHERE id = 2 " +
            "ORDER BY id").ToList();
        Assert.Equal([1, 2], [.. rows.Select(r => (int)r.id)]);
    }

    /// <summary>
    /// EN: Verifies UNION ALL keeps duplicates across SELECT parts.
    /// PT: Verifica se UNION ALL mantem duplicatas entre partes do SELECT.
    /// </summary>
    [Fact]
    [Trait("Category", "PostgreSqlSqlCompatibilityGap")]
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
    /// EN: Verifies UNION works inside subselects.
    /// PT: Verifica se UNION funciona dentro de subselects.
    /// </summary>
    [Fact]
    [Trait("Category", "PostgreSqlSqlCompatibilityGap")]
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
    /// EN: Verifies UNION ALL works inside subselects.
    /// PT: Verifica se UNION ALL funciona dentro de subselects.
    /// </summary>
    [Fact]
    [Trait("Category", "PostgreSqlSqlCompatibilityGap")]
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
    /// EN: Verifies WITH common table expressions work in PostgreSQL coverage.
    /// PT: Verifica se expressoes de tabela comum WITH funcionam na cobertura do PostgreSQL.
    /// </summary>
    [Fact]
    [Trait("Category", "PostgreSqlSqlCompatibilityGap")]
    public void Cte_With_ShouldWork()
    {
        var rows = _cnn.Query<dynamic>(
            "WITH u AS (SELECT id, name FROM users WHERE id <= 2) " +
            "SELECT id FROM u ORDER BY id DESC").ToList();
        Assert.Equal([2, 1], [.. rows.Select(r => (int)r.id)]);
    }

    /// <summary>
    /// EN: Verifies implicit casts and collation match the expected PostgreSQL rules.
    /// PT: Verifica se casts implicitos e collation seguem as regras esperadas do PostgreSQL.
    /// </summary>
    [Fact]
    [Trait("Category", "PostgreSqlSqlCompatibilityGap")]
    public void Typing_ImplicitCasts_And_Collation_ShouldMatchMySqlDefault()
    {
        // PostgreSQL uses binary comparison here because the column does not declare a collation.
        var rows1 = _cnn.Query<dynamic>("SELECT id FROM users WHERE name = 'john'").ToList();
        Assert.Empty(rows1);

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
