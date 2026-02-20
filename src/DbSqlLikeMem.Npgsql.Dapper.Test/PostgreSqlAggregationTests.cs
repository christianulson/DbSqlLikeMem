namespace DbSqlLikeMem.Npgsql.Test;

/// <summary>
/// Auto-generated summary.
/// </summary>
public sealed class PostgreSqlAggregationTests : XUnitTestBase
{
    private readonly NpgsqlConnectionMock _cnn;

    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public PostgreSqlAggregationTests(ITestOutputHelper helper) : base(helper)
    {
        var db = new NpgsqlDbMock();
        var orders = db.AddTable("orders");
        orders.AddColumn("id", DbType.Int32, false);
        orders.AddColumn("userId", DbType.Int32, false);
        orders.AddColumn("amount", DbType.Decimal, false, decimalPlaces: 2);

        orders.Add(new Dictionary<int, object?> { [0] = 1, [1] = 1, [2] = 10m });
        orders.Add(new Dictionary<int, object?> { [0] = 2, [1] = 1, [2] = 30m });
        orders.Add(new Dictionary<int, object?> { [0] = 3, [1] = 2, [2] = 5m });

        _cnn = new NpgsqlConnectionMock(db);
        _cnn.Open();
    }

    /// <summary>
    /// EN: Tests GroupBy_WithCountAndSum_ShouldWork behavior.
    /// PT: Testa o comportamento de GroupBy_WithCountAndSum_ShouldWork.
    /// </summary>
    [Fact]
    [Trait("Category", "PostgreSqlAggregation")]
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
    [Trait("Category", "PostgreSqlAggregation")]
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
    /// EN: Ensures HAVING aggregate alias can be combined with ORDER BY ordinal in grouped execution.
    /// PT: Garante que alias de agregação no HAVING possa ser combinado com ORDER BY ordinal na execução agrupada.
    /// </summary>
    [Fact]
    [Trait("Category", "PostgreSqlAggregation")]
    public void Having_AggregateAlias_WithOrderByOrdinal_ShouldWork()
    {
        const string sql = """
                  SELECT userId, SUM(amount) AS sumAmount
                  FROM orders
                  GROUP BY userId
                  HAVING sumAmount > 0
                  ORDER BY 2 DESC
                  """;

        var rows = _cnn.Query<dynamic>(sql).ToList();

        Assert.Equal(2, rows.Count);
        Assert.Equal(1, (int)rows[0].userId);
        Assert.Equal(40m, (decimal)rows[0].sumAmount);
        Assert.Equal(2, (int)rows[1].userId);
        Assert.Equal(5m, (decimal)rows[1].sumAmount);
    }

    /// <summary>
    /// EN: Ensures invalid HAVING alias in grouped execution throws a clear validation error.
    /// PT: Garante que alias inválido no HAVING em execução agrupada lance erro de validação claro.
    /// </summary>
    [Fact]
    [Trait("Category", "PostgreSqlAggregation")]
    public void Having_InvalidAlias_ShouldThrow()
    {
        const string sql = """
                  SELECT userId, SUM(amount) AS sumAmount
                  FROM orders
                  GROUP BY userId
                  HAVING missing_alias > 0
                  """;

        var ex = Assert.Throws<InvalidOperationException>(() => _cnn.Query<dynamic>(sql).ToList());
        Assert.Contains("HAVING reference", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures HAVING ordinal expression resolves to the corresponding projected select item.
    /// PT: Garante que expressão ordinal no HAVING resolva para o item projetado correspondente no SELECT.
    /// </summary>
    [Fact]
    [Trait("Category", "PostgreSqlAggregation")]
    public void Having_OrdinalExpression_ShouldResolveSelectedColumn()
    {
        const string sql = """
                  SELECT userId, SUM(amount) AS sumAmount
                  FROM orders
                  GROUP BY userId
                  HAVING 2 > 0
                  ORDER BY userId
                  """;

        var rows = _cnn.Query<dynamic>(sql).ToList();
        Assert.Equal(2, rows.Count);
        Assert.Equal(1, (int)rows[0].userId);
        Assert.Equal(2, (int)rows[1].userId);
    }


    /// <summary>
    /// EN: Ensures HAVING mixed with ordinal and aggregate resolves ordinal to the select-item expression.
    /// PT: Garante que HAVING misto com ordinal e agregação resolva o ordinal para a expressão do item do SELECT.
    /// </summary>
    [Fact]
    [Trait("Category", "PostgreSqlAggregation")]
    public void Having_MixedOrdinalAndAggregate_ShouldResolveOrdinal()
    {
        const string sql = """
                  SELECT userId, SUM(amount) AS sumAmount
                  FROM orders
                  GROUP BY userId
                  HAVING 2 > 10 AND SUM(amount) > 0
                  ORDER BY userId
                  """;

        var rows = _cnn.Query<dynamic>(sql).ToList();
        Assert.Single(rows);
        Assert.Equal(1, (int)rows[0].userId);
    }


    /// <summary>
    /// EN: Ensures HAVING CASE expression resolves ordinal references correctly.
    /// PT: Garante que expressão CASE no HAVING resolva corretamente referências ordinais.
    /// </summary>
    [Fact]
    [Trait("Category", "PostgreSqlAggregation")]
    public void Having_CaseWithOrdinal_ShouldResolveOrdinal()
    {
        const string sql = """
                  SELECT userId, SUM(amount) AS sumAmount
                  FROM orders
                  GROUP BY userId
                  HAVING CASE WHEN 2 > 10 THEN 1 ELSE 0 END = 1
                  ORDER BY userId
                  """;

        var rows = _cnn.Query<dynamic>(sql).ToList();
        Assert.Single(rows);
        Assert.Equal(1, (int)rows[0].userId);
    }


    /// <summary>
    /// EN: Ensures HAVING BETWEEN expression resolves ordinal references correctly.
    /// PT: Garante que expressão BETWEEN no HAVING resolva corretamente referências ordinais.
    /// </summary>
    [Fact]
    [Trait("Category", "PostgreSqlAggregation")]
    public void Having_BetweenWithOrdinal_ShouldResolveOrdinal()
    {
        const string sql = """
                  SELECT userId, SUM(amount) AS sumAmount
                  FROM orders
                  GROUP BY userId
                  HAVING 2 BETWEEN 35 AND 45
                  ORDER BY userId
                  """;

        var rows = _cnn.Query<dynamic>(sql).ToList();
        Assert.Single(rows);
        Assert.Equal(1, (int)rows[0].userId);
    }


    /// <summary>
    /// EN: Ensures HAVING IN expression resolves ordinal references correctly.
    /// PT: Garante que expressão IN no HAVING resolva corretamente referências ordinais.
    /// </summary>
    [Fact]
    [Trait("Category", "PostgreSqlAggregation")]
    public void Having_InWithOrdinal_ShouldResolveOrdinal()
    {
        const string sql = """
                  SELECT userId, SUM(amount) AS sumAmount
                  FROM orders
                  GROUP BY userId
                  HAVING 2 IN (40)
                  ORDER BY userId
                  """;

        var rows = _cnn.Query<dynamic>(sql).ToList();
        Assert.Single(rows);
        Assert.Equal(1, (int)rows[0].userId);
    }


    /// <summary>
    /// EN: Ensures numeric thresholds in HAVING aggregate comparisons are treated as constants, not ordinals.
    /// PT: Garante que limites numéricos em comparações de agregação no HAVING sejam tratados como constantes, não ordinais.
    /// </summary>
    [Fact]
    [Trait("Category", "PostgreSqlAggregation")]
    public void Having_AggregateThresholdConstant_ShouldNotBeTreatedAsOrdinal()
    {
        const string sql = """
                  SELECT userId, SUM(amount) AS sumAmount
                  FROM orders
                  GROUP BY userId
                  HAVING SUM(amount) > 10
                  ORDER BY userId
                  """;

        var rows = _cnn.Query<dynamic>(sql).ToList();
        Assert.Single(rows);
        Assert.Equal(1, (int)rows[0].userId);
    }


    /// <summary>
    /// EN: Ensures HAVING ordinal out of range throws a clear validation error.
    /// PT: Garante que ordinal fora do intervalo no HAVING lance um erro de validação claro.
    /// </summary>
    [Fact]
    [Trait("Category", "PostgreSqlAggregation")]
    public void Having_OrdinalOutOfRange_ShouldThrow()
    {
        const string sql = """
                  SELECT userId, SUM(amount) AS sumAmount
                  FROM orders
                  GROUP BY userId
                  HAVING 3 > 0
                  """;

        var ex = Assert.Throws<InvalidOperationException>(() => _cnn.Query<dynamic>(sql).ToList());
        Assert.Contains("HAVING ordinal", ex.Message, StringComparison.OrdinalIgnoreCase);
    }
    /// <summary>
    /// EN: Tests Distinct_Order_Limit_Offset_ShouldWork behavior.
    /// PT: Testa o comportamento de Distinct_Order_Limit_Offset_ShouldWork.
    /// </summary>
    [Fact]
    [Trait("Category", "PostgreSqlAggregation")]
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
