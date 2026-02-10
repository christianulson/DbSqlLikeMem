namespace DbSqlLikeMem.Sqlite.Test;

/// <summary>
/// Auto-generated summary.
/// </summary>
public sealed class SqliteJoinTests : XUnitTestBase
{
    private readonly SqliteConnectionMock _cnn;

    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public SqliteJoinTests(ITestOutputHelper helper) : base(helper)
    {
        var db = new SqliteDbMock();
        var users = db.AddTable("users");
        users.Columns["id"] = new(0, DbType.Int32, false);
        users.Columns["name"] = new(1, DbType.String, false);

        users.Add(new Dictionary<int, object?> { [0] = 1, [1] = "John" });
        users.Add(new Dictionary<int, object?> { [0] = 2, [1] = "Jane" });

        var orders = db.AddTable("orders");
        orders.Columns["id"] = new(0, DbType.Int32, false);
        orders.Columns["userId"] = new(1, DbType.Int32, false);
        orders.Columns["amount"] = new(2, DbType.Decimal, false);
        orders.Columns["status"] = new(3, DbType.String, false);

        orders.Add(new Dictionary<int, object?> { [0] = 10, [1] = 1, [2] = 100m, [3] = "paid" });
        orders.Add(new Dictionary<int, object?> { [0] = 11, [1] = 1, [2] = 50m, [3] = "open" });
        orders.Add(new Dictionary<int, object?> { [0] = 12, [1] = 99, [2] = 7m, [3] = "paid" }); // sem user

        _cnn = new SqliteConnectionMock(db);
        _cnn.Open();
    }

    /// <summary>
    /// EN: Tests LeftJoin_ShouldKeepAllLeftRows behavior.
    /// PT: Testa o comportamento de LeftJoin_ShouldKeepAllLeftRows.
    /// </summary>
    [Fact]
    public void LeftJoin_ShouldKeepAllLeftRows()
    {
        const string sql = """
                  SELECT u.id, o.id AS orderId
                  FROM users u
                  LEFT JOIN orders o ON u.id = o.userId
                  ORDER BY u.id
                  """;

        var rows = _cnn.Query<dynamic>(sql).ToList();

        // Jane (id=2) não tem orders => precisa aparecer ao menos uma vez (orderId null)
        Assert.Contains(rows, r => (int)r.id == 2);
    }

    /// <summary>
    /// EN: Tests RightJoin_ShouldKeepAllRightRows behavior.
    /// PT: Testa o comportamento de RightJoin_ShouldKeepAllRightRows.
    /// </summary>
    [Fact]
    public void RightJoin_ShouldKeepAllRightRows()
    {
        const string sql = """
                  SELECT u.id, o.id AS orderId
                  FROM users u
                  RIGHT JOIN orders o ON u.id = o.userId
                  """;

        var rows = _cnn.Query<dynamic>(sql).ToList();

        // order userId=99 precisa aparecer (u.id null)
        Assert.Contains(rows, r => r.id is null && (int)r.orderId == 12);
    }

    //[Fact]
    //public void FullJoin_ShouldKeepBothSides()
    //{
    //    var sql = """
    //              SELECT u.id, o.id AS orderId
    //              FROM users u
    //              FULL JOIN orders o ON u.id = o.userId
    //              """;

    //    var rows = _cnn.Query<dynamic>(sql).ToList();

    //    Assert.Contains(rows, r => (int?)r.id == 2);                // Jane
    //    Assert.Contains(rows, r => r.id is null && (int)r.orderId == 12); // órfão
    //}

    /// <summary>
    /// EN: Tests Join_ON_WithMultipleConditions_AND_ShouldWork behavior.
    /// PT: Testa o comportamento de Join_ON_WithMultipleConditions_AND_ShouldWork.
    /// </summary>
    [Fact]
    public void Join_ON_WithMultipleConditions_AND_ShouldWork()
    {
        const string sql = """
                  SELECT u.id, o.id AS orderId
                  FROM users u
                  INNER JOIN orders o ON u.id = o.userId AND o.status = 'paid'
                  """;

        var rows = _cnn.Query<dynamic>(sql).ToList();
        Assert.Single(rows);           // só order 10
        Assert.Equal(10, (int)rows[0].orderId);
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
