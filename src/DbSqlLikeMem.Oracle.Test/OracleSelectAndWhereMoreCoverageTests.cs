namespace DbSqlLikeMem.Oracle.Test;

/// <summary>
/// EN: Defines the class OracleSelectAndWhereMoreCoverageTests.
/// PT: Define o(a) class OracleSelectAndWhereMoreCoverageTests.
/// </summary>
public sealed class OracleSelectAndWhereMoreCoverageTests : XUnitTestBase
{
    private readonly OracleConnectionMock _cnn;

    /// <summary>
    /// EN: Initializes a new instance of OracleSelectAndWhereMoreCoverageTests.
    /// PT: Inicializa uma nova instância de OracleSelectAndWhereMoreCoverageTests.
    /// </summary>
    public OracleSelectAndWhereMoreCoverageTests(ITestOutputHelper helper) : base(helper)
    {
        var db = new OracleDbMock();
        var users = db.AddTable("users");
        users.Columns["id"] = new(0, DbType.Int32, false);
        users.Columns["name"] = new(1, DbType.String, false);
        users.Columns["email"] = new(2, DbType.String, true);

        users.Add(new Dictionary<int, object?> { [0] = 1, [1] = "John", [2] = "john@x.com" });
        users.Add(new Dictionary<int, object?> { [0] = 2, [1] = "Bob",  [2] = null });
        users.Add(new Dictionary<int, object?> { [0] = 3, [1] = "Jane", [2] = "jane@x.com" });

        var orders = db.AddTable("orders");
        orders.Columns["id"] = new(0, DbType.Int32, false);
        orders.Columns["userId"] = new(1, DbType.Int32, false);
        orders.Columns["amount"] = new(2, DbType.Decimal, false);

        orders.Add(new Dictionary<int, object?> { [0] = 10, [1] = 1, [2] = 50m });
        orders.Add(new Dictionary<int, object?> { [0] = 11, [1] = 2, [2] = 200m });
        orders.Add(new Dictionary<int, object?> { [0] = 12, [1] = 2, [2] = 10m });

        _cnn = new OracleConnectionMock(db);

        _cnn.Open();
    }

    /// <summary>
    /// EN: Tests Where_Between_ShouldWork behavior.
    /// PT: Testa o comportamento de Where_Between_ShouldWork.
    /// </summary>
    [Fact]
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
    public void Select_IfNull_ShouldWork()
    {
        var row = _cnn.QuerySingle<dynamic>("SELECT COALESCE(email,'(none)') AS em FROM users WHERE id = 2");
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
