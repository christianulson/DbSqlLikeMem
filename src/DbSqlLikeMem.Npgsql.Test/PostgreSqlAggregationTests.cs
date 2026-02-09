namespace DbSqlLikeMem.Npgsql.Test;

public sealed class PostgreSqlAggregationTests : XUnitTestBase
{
    private readonly NpgsqlConnectionMock _cnn;

    public PostgreSqlAggregationTests(ITestOutputHelper helper) : base(helper)
    {
        var db = new NpgsqlDbMock();
        var orders = db.AddTable("orders");
        orders.Columns["id"] = new(0, DbType.Int32, false);
        orders.Columns["userId"] = new(1, DbType.Int32, false);
        orders.Columns["amount"] = new(2, DbType.Decimal, false);

        orders.Add(new Dictionary<int, object?> { [0] = 1, [1] = 1, [2] = 10m });
        orders.Add(new Dictionary<int, object?> { [0] = 2, [1] = 1, [2] = 30m });
        orders.Add(new Dictionary<int, object?> { [0] = 3, [1] = 2, [2] = 5m });

        _cnn = new NpgsqlConnectionMock(db);
        _cnn.Open();
    }

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

    [Fact]
    public void Distinct_Order_Limit_Offset_ShouldWork()
    {
        const string sql = """
                  SELECT DISTINCT userId
                  FROM orders
                  ORDER BY userId
                  LIMIT 1 OFFSET 1
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
