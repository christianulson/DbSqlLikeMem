namespace DbSqlLikeMem.Sqlite.Dapper.Test;

/// <summary>
/// EN: Covers extra SQLite SELECT and WHERE scenarios over a direct mock connection.
/// PT-br: Cobre cenarios extras de SELECT e WHERE SQLite sobre uma conexao mock direta.
/// </summary>
public sealed class SqliteSelectAndWhereMoreCoverageTests : XUnitTestBase
{
    private readonly SqliteConnectionMock _cnn;

    /// <summary>
    /// EN: Creates the in-memory SQLite database used by the extra SELECT and WHERE coverage tests.
    /// PT-br: Cria o banco SQLite em memoria usado pelos testes extras de cobertura de SELECT e WHERE.
    /// </summary>
    public SqliteSelectAndWhereMoreCoverageTests(ITestOutputHelper helper) : base(helper)
    {
        var db = new SqliteDbMock();
        var users = db.AddTable("users");
        users.AddColumn("id", DbType.Int32, false);
        users.AddColumn("name", DbType.String, false);
        users.AddColumn("email", DbType.String, true);

        users.Add(new Dictionary<int, object?> { [0] = 1, [1] = "John", [2] = "john@x.com" });
        users.Add(new Dictionary<int, object?> { [0] = 2, [1] = "Bob", [2] = null });
        users.Add(new Dictionary<int, object?> { [0] = 3, [1] = "Jane", [2] = "jane@x.com" });

        var orders = db.AddTable("orders");
        orders.AddColumn("id", DbType.Int32, false);
        orders.AddColumn("userId", DbType.Int32, false);
        orders.AddColumn("amount", DbType.Decimal, false, decimalPlaces: 2);

        orders.Add(new Dictionary<int, object?> { [0] = 10, [1] = 1, [2] = 50m });
        orders.Add(new Dictionary<int, object?> { [0] = 11, [1] = 2, [2] = 200m });
        orders.Add(new Dictionary<int, object?> { [0] = 12, [1] = 2, [2] = 10m });

        var pairs = db.AddTable("pairs");
        pairs.AddColumn("a", DbType.Int32, false);
        pairs.AddColumn("b", DbType.Int32, false);
        pairs.Add(new Dictionary<int, object?> { [0] = 1, [1] = 10 });
        pairs.Add(new Dictionary<int, object?> { [0] = 1, [1] = 20 });
        pairs.Add(new Dictionary<int, object?> { [0] = 2, [1] = 10 });
        pairs.Add(new Dictionary<int, object?> { [0] = 3, [1] = 30 });

        var allowedPairs = db.AddTable("allowed_pairs");
        allowedPairs.AddColumn("a", DbType.Int32, false);
        allowedPairs.AddColumn("b", DbType.Int32, false);
        allowedPairs.Add(new Dictionary<int, object?> { [0] = 1, [1] = 10 });
        allowedPairs.Add(new Dictionary<int, object?> { [0] = 2, [1] = 10 });
        allowedPairs.Add(new Dictionary<int, object?> { [0] = 4, [1] = 40 });

        _cnn = new SqliteConnectionMock(db);

        _cnn.Open();
    }

    /// <summary>
    /// EN: Verifies BETWEEN filters rows in the expected range.
    /// PT-br: Verifica se BETWEEN filtra as linhas no intervalo esperado.
    /// </summary>
    [Fact]
    [Trait("Category", "SqliteSelectAndWhereMoreCoverage")]
    public void Where_Between_ShouldWork()
    {
        var rows = _cnn.Query<dynamic>("SELECT id FROM users WHERE id BETWEEN 2 AND 3 ORDER BY id").ToList();
        Assert.Equal([2, 3], [.. rows.Select(r => (int)r.id)]);
    }

    /// <summary>
    /// EN: Verifies NOT IN filters rows as expected.
    /// PT-br: Verifica se NOT IN filtra as linhas como esperado.
    /// </summary>
    [Fact]
    [Trait("Category", "SqliteSelectAndWhereMoreCoverage")]
    public void Where_NotIn_ShouldWork()
    {
        var rows = _cnn.Query<dynamic>("SELECT id FROM users WHERE id NOT IN (1,3)").ToList();
        Assert.Single(rows);
        Assert.Equal(2, (int)rows[0].id);
    }

    /// <summary>
    /// EN: Verifies EXISTS subqueries filter rows as expected.
    /// PT-br: Verifica se subconsultas EXISTS filtram as linhas como esperado.
    /// </summary>
    [Fact]
    [Trait("Category", "SqliteSelectAndWhereMoreCoverage")]
    public void Where_ExistsSubquery_ShouldWork()
    {
        var rows = _cnn.Query<dynamic>(@"
SELECT u.id
FROM users u
WHERE EXISTS (
    SELECT 1
    FROM orders o
    WHERE o.userId = u.id
      AND o.amount > 100
)
ORDER BY u.id").ToList();

        Assert.Equal([2], [.. rows.Select(r => (int)r.id)]);
    }

    /// <summary>
    /// EN: Verifies row-value IN subqueries return the expected rows.
    /// PT-br: Verifica se subconsultas IN com valor de linha retornam as linhas esperadas.
    /// </summary>
    [Fact]
    [Trait("Category", "SqliteSelectAndWhereMoreCoverage")]
    public void Where_RowInSubquery_ShouldWork()
    {
        var rows = _cnn.Query<dynamic>(@"
SELECT a, b
FROM pairs
WHERE (a, b) IN (
    SELECT a, b
    FROM allowed_pairs
)
ORDER BY a, b").ToList();

        Assert.Equal(2, rows.Count);
        Assert.Equal(1, (int)rows[0].a);
        Assert.Equal(10, (int)rows[0].b);
        Assert.Equal(2, (int)rows[1].a);
        Assert.Equal(10, (int)rows[1].b);
    }

    /// <summary>
    /// EN: Verifies correlated row-value IN subqueries return the expected rows.
    /// PT-br: Verifica se subconsultas IN com valor de linha correlacionadas retornam as linhas esperadas.
    /// </summary>
    [Fact]
    [Trait("Category", "SqliteSelectAndWhereMoreCoverage")]
    public void Where_CorrelatedRowInSubquery_ShouldWork()
    {
        var rows = _cnn.Query<dynamic>(@"
SELECT p.a, p.b
FROM pairs p
WHERE (p.a, p.b) IN (
    SELECT ap.a, ap.b
    FROM allowed_pairs ap
    WHERE ap.a = p.a
)
ORDER BY p.a, p.b").ToList();

        Assert.Equal(2, rows.Count);
        Assert.Equal(1, (int)rows[0].a);
        Assert.Equal(10, (int)rows[0].b);
        Assert.Equal(2, (int)rows[1].a);
        Assert.Equal(10, (int)rows[1].b);
    }

    /// <summary>
    /// EN: Verifies CASE WHEN projections return the expected values.
    /// PT-br: Verifica se projeções CASE WHEN retornam os valores esperados.
    /// </summary>
    [Fact]
    [Trait("Category", "SqliteSelectAndWhereMoreCoverage")]
    public void Select_CaseWhen_ShouldWork()
    {
        var rows = _cnn.Query<dynamic>(@"
SELECT id,
       CASE WHEN email IS NULL THEN 'N' ELSE 'Y' END AS hasEmail
FROM users
ORDER BY id").ToList();

        Assert.Equal(3, rows.Count);
        Assert.Equal("Y", (string)rows[0].hasEmail);
        Assert.Equal("N", (string)rows[1].hasEmail);
        Assert.Equal("Y", (string)rows[2].hasEmail);
    }

    /// <summary>
    /// EN: Verifies IFNULL projections return the expected fallback value.
    /// PT-br: Verifica se projeções IFNULL retornam o valor de fallback esperado.
    /// </summary>
    [Fact]
    [Trait("Category", "SqliteSelectAndWhereMoreCoverage")]
    public void Select_IfNull_ShouldWork()
    {
        var row = _cnn.QuerySingle<dynamic>("SELECT IFNULL(email,'(none)') AS em FROM users WHERE id = 2");
        Assert.Equal("(none)", (string)row.em);
    }

    /// <summary>
    /// EN: Disposes test resources.
    /// PT-br: Descarta os recursos do teste.
    /// </summary>
    /// <param name="disposing">EN: True to dispose managed resources. PT-br: True para descartar recursos gerenciados.</param>
    protected override void Dispose(bool disposing)
    {
        _cnn?.Dispose();
        base.Dispose(disposing);
    }
}
