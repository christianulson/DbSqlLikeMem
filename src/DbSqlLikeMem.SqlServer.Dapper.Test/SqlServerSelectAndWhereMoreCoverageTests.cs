namespace DbSqlLikeMem.SqlServer.Dapper.Test;

/// <summary>
/// EN: Defines the class SqlServerSelectAndWhereMoreCoverageTests.
/// PT: Define a classe SqlServerSelectAndWhereMoreCoverageTests.
/// </summary>
public sealed class SqlServerSelectAndWhereMoreCoverageTests : XUnitTestBase
{
    private readonly SqlServerConnectionMock _cnn;

    /// <summary>
    /// EN: Tests SqlServerSelectAndWhereMoreCoverageTests behavior.
    /// PT: Testa o comportamento de SqlServerSelectAndWhereMoreCoverageTests.
    /// </summary>
    public SqlServerSelectAndWhereMoreCoverageTests(ITestOutputHelper helper) : base(helper)
    {
        var db = new SqlServerDbMock();
        var users = db.AddTable("users");
        users.AddColumn("id", DbType.Int32, false);
        users.AddColumn("name", DbType.String, false);
        users.AddColumn("email", DbType.String, true);

        users.Add(new Dictionary<int, object?> { [0] = 1, [1] = "John", [2] = "john@x.com" });
        users.Add(new Dictionary<int, object?> { [0] = 2, [1] = "Bob",  [2] = null });
        users.Add(new Dictionary<int, object?> { [0] = 3, [1] = "Jane", [2] = "jane@x.com" });

        var orders = db.AddTable("orders");
        orders.AddColumn("id", DbType.Int32, false);
        orders.AddColumn("userId", DbType.Int32, false);
        orders.AddColumn("amount", DbType.Decimal, false, decimalPlaces: 2);

        orders.Add(new Dictionary<int, object?> { [0] = 10, [1] = 1, [2] = 50m });
        orders.Add(new Dictionary<int, object?> { [0] = 11, [1] = 2, [2] = 200m });
        orders.Add(new Dictionary<int, object?> { [0] = 12, [1] = 2, [2] = 10m });

        _cnn = new SqlServerConnectionMock(db);

        _cnn.Open();
    }

    /// <summary>
    /// EN: Tests Where_Between_ShouldWork behavior.
    /// PT: Testa o comportamento de Where_Between_ShouldWork.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlServerSelectAndWhereMoreCoverage")]
    public void Where_Between_ShouldWork()
    {
        var rows = _cnn.Query<dynamic>("SELECT id FROM users WHERE id BETWEEN 2 AND 3 ORDER BY id").ToList();
        Assert.Equal([2, 3], [.. rows.Select(r => (int)r.id)]);
    }

    /// <summary>
    /// EN: Tests Where_NotIn_ShouldWork behavior.
    /// PT: Testa o comportamento de Where_NotIn_ShouldWork.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlServerSelectAndWhereMoreCoverage")]
    public void Where_NotIn_ShouldWork()
    {
        var rows = _cnn.Query<dynamic>("SELECT id FROM users WHERE id NOT IN (1,3)").ToList();
        Assert.Single(rows);
        Assert.Equal(2, (int)rows[0].id);
    }

    /// <summary>
    /// EN: Tests Where_ExistsSubquery_ShouldWork behavior.
    /// PT: Testa o comportamento de Where_ExistsSubquery_ShouldWork.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlServerSelectAndWhereMoreCoverage")]
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
    /// EN: Tests Select_CaseWhen_ShouldWork behavior.
    /// PT: Testa o comportamento de Select_CaseWhen_ShouldWork.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlServerSelectAndWhereMoreCoverage")]
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
    /// EN: Tests Select_IfNull_ShouldWork behavior.
    /// PT: Testa o comportamento de Select_IfNull_ShouldWork.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlServerSelectAndWhereMoreCoverage")]
    public void Select_IfNull_ShouldWork()
    {
        var row = _cnn.QuerySingle<dynamic>("SELECT ISNULL(email,'(none)') AS em FROM users WHERE id = 2");
        Assert.Equal("(none)", (string)row.em);
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
