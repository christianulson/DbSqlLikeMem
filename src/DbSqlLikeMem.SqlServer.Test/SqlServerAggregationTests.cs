namespace DbSqlLikeMem.SqlServer.Test;

/// <summary>
/// Auto-generated summary.
/// </summary>
public sealed class SqlServerAggregationTests : XUnitTestBase
{
    private readonly SqlServerConnectionMock _cnn;

    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public SqlServerAggregationTests(ITestOutputHelper helper) : base(helper)
    {
        var db = new SqlServerDbMock();
        var orders = db.AddTable("orders");
        orders.AddColumn("id", DbType.Int32, false);
        orders.AddColumn("userId", DbType.Int32, false);
        orders.AddColumn("amount", DbType.Decimal, false, decimalPlaces: 2);

        orders.Add(new Dictionary<int, object?> { [0] = 1, [1] = 1, [2] = 10m });
        orders.Add(new Dictionary<int, object?> { [0] = 2, [1] = 1, [2] = 30m });
        orders.Add(new Dictionary<int, object?> { [0] = 3, [1] = 2, [2] = 5m });

        _cnn = new SqlServerConnectionMock(db);
        _cnn.Open();
    }

    /// <summary>
    /// EN: Tests GroupBy_WithCountAndSum_ShouldWork behavior.
    /// PT: Testa o comportamento de GroupBy_WithCountAndSum_ShouldWork.
    /// </summary>
    [Fact]
    public void GroupBy_WithCountAndSum_ShouldWork()
    {
        const string sql = """
                  SELECT userId, COUNT(id) AS total, SUM(amount) AS sumAmount
                  FROM orders
                  GROUP BY userId
                  ORDER BY userId
                  """;

        var rows = _cnn.Query<dynamic>(sql).ToList();
        Assert.Equal(2, rows.Count);

        Assert.Equal(1, (int)rows[0].userId);
        Assert.Equal(2, (int)rows[0].total);
        Assert.Equal(40m, (decimal)rows[0].sumAmount);

        Assert.Equal(2, (int)rows[1].userId);
        Assert.Equal(1, (int)rows[1].total);
        Assert.Equal(5m, (decimal)rows[1].sumAmount);
    }

    /// <summary>
    /// EN: Tests Having_ShouldFilterAggregates behavior.
    /// PT: Testa o comportamento de Having_ShouldFilterAggregates.
    /// </summary>
    [Fact]
    public void Having_ShouldFilterAggregates()
    {
        const string sql = """
                  SELECT userId, SUM(amount) AS sumAmount
                  FROM orders
                  GROUP BY userId
                  HAVING sumAmount >= 10
                  """;

        var rows = _cnn.Query<dynamic>(sql).ToList();
        Assert.Single(rows);
        Assert.Equal(1, (int)rows[0].userId);
    }

    /// <summary>
    /// EN: Tests Distinct_Order_Limit_Offset_ShouldWork behavior.
    /// PT: Testa o comportamento de Distinct_Order_Limit_Offset_ShouldWork.
    /// </summary>
    [Fact]
    public void Distinct_Order_Limit_Offset_ShouldWork()
    {
        const string sql = """
                  SELECT DISTINCT userId
                  FROM orders
                  ORDER BY userId
                  OFFSET 1 ROWS FETCH NEXT 1 ROWS ONLY
                  """;

        var rows = _cnn.Query<dynamic>(sql).ToList();
        Assert.Single(rows);
        Assert.Equal(2, (int)rows[0].userId);
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
