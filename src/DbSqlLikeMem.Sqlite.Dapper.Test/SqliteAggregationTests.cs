namespace DbSqlLikeMem.Sqlite.Dapper.Test;

/// <summary>
/// EN: Runs shared aggregation/HAVING scenarios for SQLite and keeps SQLite-specific coverage.
/// PT: Executa cenários compartilhados de agregação/HAVING para SQLite e mantém cobertura específica de SQLite.
/// </summary>
public sealed class SqliteAggregationTests : AggregationHavingOrdinalTestsBase<SqliteDbMock, SqliteConnectionMock>
{
    /// <summary>
    /// EN: Initializes SQLite aggregation tests.
    /// PT: Inicializa os testes de agregação do SQLite.
    /// </summary>
    /// <param name="helper">EN: Output helper. PT: Helper de saída.</param>
    public SqliteAggregationTests(ITestOutputHelper helper) : base(helper)
    {
    }

    /// <inheritdoc />
    protected override SqliteDbMock CreateDb() => new();

    /// <inheritdoc />
    protected override SqliteConnectionMock CreateConnection(SqliteDbMock db) => new(db);

    /// <inheritdoc />
    protected override List<dynamic> Query(string sql) => Connection.Query<dynamic>(sql).ToList();

    /// <summary>
    /// EN: Tests Distinct_Order_WithPagination_ShouldWork behavior.
    /// PT: Testa o comportamento de Distinct_Order_WithPagination_ShouldWork.
    /// </summary>
    [Fact]
    [Trait("Category", "SqliteAggregation")]
    public void Distinct_Order_WithPagination_ShouldWork()
    {
        AssertDistinctOrderPagination("LIMIT 1 OFFSET 1");
    }

    /// <summary>
    /// EN: Tests provider string aggregation with custom separator.
    /// PT: Testa agregação textual do provedor com separador customizado.
    /// </summary>
    [Fact]
    [Trait("Category", "SqliteAggregation")]
    public void StringAggregation_WithCustomSeparator_ShouldWork()
    {
        const string sql = """
                  SELECT userId, GROUP_CONCAT(amount, '|') AS joined
                  FROM orders
                  GROUP BY userId
                  ORDER BY userId
                  """;

        var rows = Query(sql);
        Assert.Equal(2, rows.Count);

        var first = Convert.ToString(rows[0].joined) ?? string.Empty;
        var second = Convert.ToString(rows[1].joined) ?? string.Empty;

        Assert.Contains("|", first);
        Assert.Contains("10", first, StringComparison.Ordinal);
        Assert.Contains("30", first, StringComparison.Ordinal);
        Assert.Contains("5", second, StringComparison.Ordinal);
    }


    /// <summary>
    /// EN: Ensures string aggregation with DISTINCT ignores NULL values and deduplicates text.
    /// PT: Garante que agregação textual com DISTINCT ignore NULL e remova duplicidade de texto.
    /// </summary>
    [Fact]
    [Trait("Category", "SqliteAggregation")]
    public void StringAggregation_Distinct_ShouldIgnoreNullValues()
    {
        Connection.Execute("CREATE TABLE textagg_data (grp INT, val VARCHAR(20) NULL)");
        Connection.Execute("INSERT INTO textagg_data (grp, val) VALUES (1, 'a')");
        Connection.Execute("INSERT INTO textagg_data (grp, val) VALUES (1, NULL)");
        Connection.Execute("INSERT INTO textagg_data (grp, val) VALUES (1, 'a')");
        Connection.Execute("INSERT INTO textagg_data (grp, val) VALUES (1, 'b')");

        var rows = Query("SELECT GROUP_CONCAT(DISTINCT val, '|') AS joined FROM textagg_data WHERE grp = 1");
        Assert.Single(rows);

        var joined = Convert.ToString(rows[0].joined) ?? string.Empty;
        Assert.Contains("a", joined, StringComparison.Ordinal);
        Assert.Contains("b", joined, StringComparison.Ordinal);
        Assert.Contains("|", joined, StringComparison.Ordinal);
    }


    /// <summary>
    /// EN: Ensures zero-arg temporal function works in WHERE filter and projection.
    /// PT: Garante que função temporal sem argumentos funcione em filtro WHERE e projeção.
    /// </summary>
    [Fact]
    [Trait("Category", "SqliteAggregation")]
    public void TemporalFunction_InWhere_ShouldWork()
    {
        var rows = Query("SELECT CURRENT_TIMESTAMP AS nowValue FROM orders WHERE CURRENT_TIMESTAMP IS NOT NULL");

        Assert.NotEmpty(rows);
        Assert.NotNull(rows[0].nowValue);
    }


    /// <summary>
    /// EN: Ensures zero-arg temporal function works in INSERT values and can be read back.
    /// PT: Garante que função temporal sem argumentos funcione em valores de INSERT e possa ser lida depois.
    /// </summary>
    [Fact]
    [Trait("Category", "SqliteAggregation")]
    public void TemporalFunction_InInsertValues_ShouldWork()
    {
        Connection.Execute("CREATE TABLE temporal_data (id INT, created_at DATETIME NULL)");
        Connection.Execute("INSERT INTO temporal_data (id, created_at) VALUES (1, CURRENT_TIMESTAMP)");

        var rows = Query("SELECT created_at FROM temporal_data WHERE id = 1");

        Assert.Single(rows);
        Assert.NotNull(rows[0].created_at);
    }


    /// <summary>
    /// EN: Ensures zero-arg temporal function works in UPDATE set expression.
    /// PT: Garante que função temporal sem argumentos funcione em expressão de UPDATE.
    /// </summary>
    [Fact]
    [Trait("Category", "SqliteAggregation")]
    public void TemporalFunction_InUpdateSet_ShouldWork()
    {
        Connection.Execute("CREATE TABLE temporal_update_data (id INT, updated_at DATETIME NULL)");
        Connection.Execute("INSERT INTO temporal_update_data (id, updated_at) VALUES (1, NULL)");
        Connection.Execute("UPDATE temporal_update_data SET updated_at = CURRENT_TIMESTAMP WHERE id = 1");

        var rows = Query("SELECT updated_at FROM temporal_update_data WHERE id = 1");

        Assert.Single(rows);
        Assert.NotNull(rows[0].updated_at);
    }

}
