namespace DbSqlLikeMem.Firebird.Dapper.Test;

/// <summary>
/// EN: Covers extra Firebird SELECT and WHERE scenarios over a direct mock connection.
/// PT-br: Cobre cenarios extras de SELECT e WHERE Firebird sobre uma conexao mock direta.
/// </summary>
public sealed class FirebirdSelectAndWhereMoreCoverageTests : XUnitTestBase
{
    private readonly FirebirdConnectionMock _cnn;

    /// <summary>
    /// EN: Creates the in-memory Firebird database used by the extra SELECT and WHERE coverage tests.
    /// PT-br: Cria o banco Firebird em memoria usado pelos testes extras de cobertura de SELECT e WHERE.
    /// </summary>
    public FirebirdSelectAndWhereMoreCoverageTests(ITestOutputHelper helper) : base(helper)
    {
        var db = new FirebirdDbMock();
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

        _cnn = new FirebirdConnectionMock(db);
        _cnn.Open();
    }

    /// <summary>
    /// EN: Verifies BETWEEN filters rows in Firebird SELECT and WHERE coverage.
    /// PT-br: Verifica se BETWEEN filtra linhas na cobertura de SELECT e WHERE do Firebird.
    /// </summary>
    [Fact]
    [Trait("Category", "FirebirdSelectAndWhereMoreCoverage")]
    public void Where_Between_ShouldWork()
    {
        var rows = _cnn.Query<dynamic>("SELECT id FROM users WHERE id BETWEEN 2 AND 3 ORDER BY id").ToList();
        Assert.Equal([2, 3], [.. rows.Select(r => Convert.ToInt32(GetValueIgnoreCase((object)r, "id")))]); 
    }

    /// <summary>
    /// EN: Verifies NOT IN filters rows in Firebird SELECT and WHERE coverage.
    /// PT-br: Verifica se NOT IN filtra linhas na cobertura de SELECT e WHERE do Firebird.
    /// </summary>
    [Fact]
    [Trait("Category", "FirebirdSelectAndWhereMoreCoverage")]
    public void Where_NotIn_ShouldWork()
    {
        var rows = _cnn.Query<dynamic>("SELECT id FROM users WHERE id NOT IN (1,3)").ToList();
        Assert.Single(rows);
        Assert.Equal(2, Convert.ToInt32(GetValueIgnoreCase((object)rows[0], "id")));
    }

    /// <summary>
    /// EN: Verifies EXISTS subqueries filter rows in Firebird coverage.
    /// PT-br: Verifica se subconsultas EXISTS filtram linhas na cobertura do Firebird.
    /// </summary>
    [Fact]
    [Trait("Category", "FirebirdSelectAndWhereMoreCoverage")]
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

        Assert.Equal([2], [.. rows.Select(r => Convert.ToInt32(GetValueIgnoreCase((object)r, "id")))]); 
    }

    /// <summary>
    /// EN: Verifies CASE WHEN projections return the expected values.
    /// PT-br: Verifica se projeções CASE WHEN retornam os valores esperados.
    /// </summary>
    [Fact]
    [Trait("Category", "FirebirdSelectAndWhereMoreCoverage")]
    public void Select_CaseWhen_ShouldWork()
    {
        var rows = _cnn.Query<dynamic>(@"
SELECT id,
       CASE WHEN email IS NULL THEN 'N' ELSE 'Y' END AS hasEmail
FROM users
ORDER BY id").ToList();

        Assert.Collection(rows,
            _ => { },
            _ => { },
            _ => { });
        Assert.Equal("Y", Convert.ToString(GetValueIgnoreCase((object)rows[0], "hasEmail")));
        Assert.Equal("N", Convert.ToString(GetValueIgnoreCase((object)rows[1], "hasEmail")));
        Assert.Equal("Y", Convert.ToString(GetValueIgnoreCase((object)rows[2], "hasEmail")));
    }

    /// <summary>
    /// EN: Verifies COALESCE-based null fallback in Firebird SELECT coverage.
    /// PT-br: Verifica o fallback de nulos com COALESCE na cobertura de SELECT do Firebird.
    /// </summary>
    [Fact]
    [Trait("Category", "FirebirdSelectAndWhereMoreCoverage")]
    public void Select_Coalesce_ShouldWork()
    {
        var row = _cnn.QuerySingle<dynamic>("SELECT COALESCE(email,'(none)') AS em FROM users WHERE id = 2");
        Assert.Equal("(none)", Convert.ToString(GetValueIgnoreCase((object)row, "em")));
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
