namespace DbSqlLikeMem.Oracle.Test;

/// <summary>
/// EN: Defines the class OracleJoinTests.
/// PT: Define o(a) class OracleJoinTests.
/// </summary>
public sealed class OracleJoinTests : XUnitTestBase
{
    private readonly OracleConnectionMock _cnn;

    /// <summary>
    /// EN: Initializes a new instance of OracleJoinTests.
    /// PT: Inicializa uma nova instância de OracleJoinTests.
    /// </summary>
    public OracleJoinTests(ITestOutputHelper helper) : base(helper)
    {
        var db = new OracleDbMock();
        var users = db.AddTable("users");
        users.AddColumn("id", DbType.Int32, false);
        users.AddColumn("name", DbType.String, false);

        users.Add(new Dictionary<int, object?> { [0] = 1, [1] = "John" });
        users.Add(new Dictionary<int, object?> { [0] = 2, [1] = "Jane" });

        var orders = db.AddTable("orders");
        orders.AddColumn("id", DbType.Int32, false);
        orders.AddColumn("userId", DbType.Int32, false);
        orders.AddColumn("amount", DbType.Decimal, false, decimalPlaces: 2);
        orders.AddColumn("status", DbType.String, false);

        orders.Add(new Dictionary<int, object?> { [0] = 10, [1] = 1, [2] = 100m, [3] = "paid" });
        orders.Add(new Dictionary<int, object?> { [0] = 11, [1] = 1, [2] = 50m, [3] = "open" });
        orders.Add(new Dictionary<int, object?> { [0] = 12, [1] = 99, [2] = 7m, [3] = "paid" }); // sem user

        _cnn = new OracleConnectionMock(db);
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
