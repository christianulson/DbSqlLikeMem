namespace DbSqlLikeMem.SqlServer.Dapper.Test;

/// <summary>
/// EN: Runs shared aggregation/HAVING scenarios for SQL Server and keeps SQL Server-specific coverage.
/// PT: Executa cenários compartilhados de agregação/HAVING para SQL Server e mantém cobertura específica de SQL Server.
/// </summary>
public sealed class SqlServerAggregationTests : AggregationHavingOrdinalTestsBase<SqlServerDbMock, SqlServerConnectionMock>
{
    /// <summary>
    /// EN: Initializes SQL Server aggregation tests.
    /// PT: Inicializa os testes de agregação do SQL Server.
    /// </summary>
    /// <param name="helper">EN: Output helper. PT: Helper de saída.</param>
    public SqlServerAggregationTests(ITestOutputHelper helper) : base(helper)
    {
    }

    /// <inheritdoc />
    protected override SqlServerDbMock CreateDb() => new();

    /// <inheritdoc />
    protected override SqlServerConnectionMock CreateConnection(SqlServerDbMock db) => new(db);

    /// <inheritdoc />
    protected override List<dynamic> Query(string sql) => Connection.Query<dynamic>(sql).ToList();

    /// <summary>
    /// EN: Tests Distinct_Order_WithPagination_ShouldWork behavior.
    /// PT: Testa o comportamento de Distinct_Order_WithPagination_ShouldWork.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlServerAggregation")]
    public void Distinct_Order_WithPagination_ShouldWork()
    {
        AssertDistinctOrderPagination("OFFSET 1 ROWS FETCH NEXT 1 ROWS ONLY");
    }

    /// <summary>
    /// EN: Tests provider string aggregation with custom separator ignoring NULL values.
    /// PT: Testa agregação textual do provedor com separador customizado ignorando valores NULL.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlServerAggregation")]
    public void StringAggregation_WithCustomSeparator_ShouldWork()
    {
        const string sql = """
                  SELECT userId, STRING_AGG(amount, '|') AS joined
                  FROM orders
                  GROUP BY userId
                  ORDER BY userId
                  """;

        AssertStringAggregationWithCustomSeparator(sql);
    }


    /// <summary>
    /// EN: Ensures mixed projection with string aggregation and NULL literal works consistently.
    /// PT: Garante que projeção mista com agregação textual e literal NULL funcione de forma consistente.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlServerAggregation")]
    public void StringAggregation_WithNullProjection_ShouldWork()
    {
        const string sql = """
                  SELECT userId, STRING_AGG(amount, '|') AS joined, NULL AS note
                  FROM orders
                  GROUP BY userId
                  ORDER BY userId
                  """;

        AssertStringAggregationWithNullProjection(sql);
    }


    /// <summary>
    /// EN: Ensures CASE projection returning NULL stays stable with grouped string aggregation.
    /// PT: Garante que projeção CASE retornando NULL permaneça estável com agregação textual agrupada.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlServerAggregation")]
    public void StringAggregation_WithCaseNullProjection_ShouldWork()
    {
        const string sql = """
                  SELECT userId, STRING_AGG(amount, '|') AS joined,
                         CASE WHEN userId > 0 THEN NULL ELSE 'unexpected' END AS note
                  FROM orders
                  GROUP BY userId
                  ORDER BY userId
                  """;

        AssertStringAggregationWithCaseNullProjection(sql);
    }


    /// <summary>
    /// EN: Ensures CASE projection with mixed text/NULL branches remains stable with grouped string aggregation.
    /// PT: Garante que projeção CASE com ramos mistos texto/NULL permaneça estável com agregação textual agrupada.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlServerAggregation")]
    public void StringAggregation_WithCaseMixedProjection_ShouldWork()
    {
        const string sql = """
                  SELECT userId, STRING_AGG(amount, '|') AS joined,
                         CASE WHEN userId = 1 THEN 'ok' ELSE NULL END AS note
                  FROM orders
                  GROUP BY userId
                  ORDER BY userId
                  """;

        AssertStringAggregationWithCaseMixedProjection(sql);
    }


    /// <summary>
    /// EN: Ensures multi-branch CASE projection remains stable with grouped string aggregation.
    /// PT: Garante que projeção CASE de múltiplos ramos permaneça estável com agregação textual agrupada.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlServerAggregation")]
    public void StringAggregation_WithCaseMultiBranchProjection_ShouldWork()
    {
        const string sql = """
                  SELECT userId, STRING_AGG(amount, '|') AS joined,
                         CASE WHEN userId = 1 THEN 'primary'
                              WHEN userId = 2 THEN 'secondary'
                              ELSE NULL
                         END AS note
                  FROM orders
                  GROUP BY userId
                  ORDER BY userId
                  """;

        AssertStringAggregationWithCaseMultiBranchProjection(sql);
    }


    /// <summary>
    /// EN: Ensures numeric multi-branch CASE projection remains stable with grouped string aggregation.
    /// PT: Garante que projeção CASE numérica multibranch permaneça estável com agregação textual agrupada.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlServerAggregation")]
    public void StringAggregation_WithCaseNumericMultiBranchProjection_ShouldWork()
    {
        const string sql = """
                  SELECT userId, STRING_AGG(amount, '|') AS joined,
                         CASE WHEN userId = 1 THEN 100
                              WHEN userId = 2 THEN 200
                              ELSE 0
                         END AS note
                  FROM orders
                  GROUP BY userId
                  ORDER BY userId
                  """;

        AssertStringAggregationWithCaseNumericMultiBranchProjection(sql);
    }

    /// <summary>
    /// EN: Ensures string aggregation with DISTINCT ignores NULL values and deduplicates text.
    /// PT: Garante que agregação textual com DISTINCT ignore NULL e remova duplicidade de texto.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlServerAggregation")]
    public void StringAggregation_Distinct_ShouldIgnoreNullValues()
    {
        AssertStringAggregationDistinctIgnoresNullValues("SELECT STRING_AGG(DISTINCT val, '|') AS joined FROM textagg_data WHERE grp = 1");
    }


    /// <summary>
    /// EN: Ensures ordered-set syntax WITHIN GROUP produces actionable not-supported error.
    /// PT: Garante que a sintaxe ordered-set WITHIN GROUP gere erro claro de não suportado.
    /// </summary>
    [Fact]
    [Trait("Category", "Aggregation")]
    public void StringAggregation_WithinGroup_ShouldThrowNotSupported()
    {
        AssertWithinGroupNotSupported("SELECT STRING_AGG(amount, '|') WITHIN GROUP (ORDER BY amount DESC) AS joined FROM orders");
    }


    /// <summary>
    /// EN: Ensures zero-arg temporal function works in WHERE filter and projection.
    /// PT: Garante que função temporal sem argumentos funcione em filtro WHERE e projeção.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlServerAggregation")]
    public void TemporalFunction_InWhere_ShouldWork()
    {
        var rows = Query("SELECT CURRENT_TIMESTAMP AS nowValue FROM orders WHERE CURRENT_TIMESTAMP IS NOT NULL");

        Assert.NotEmpty(rows);
        Assert.NotNull(rows[0].nowValue);
    }




    /// <summary>
    /// EN: Ensures GETDATE and SYSDATETIME functions work in WHERE filter and projection.
    /// PT: Garante que funções GETDATE e SYSDATETIME funcionem em filtro WHERE e projeção.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlServerAggregation")]
    public void TemporalFunction_GetDateAndSysDateTime_InWhere_ShouldWork()
    {
        var rows = Query("SELECT GETDATE() AS currentDate, SYSDATETIME() AS currentTime FROM orders WHERE GETDATE() IS NOT NULL AND SYSDATETIME() IS NOT NULL");

        Assert.NotEmpty(rows);
        Assert.NotNull(rows[0].currentDate);
        Assert.NotNull(rows[0].currentTime);
    }



    /// <summary>
    /// EN: Ensures GETDATE and SYSDATETIME functions work in HAVING and ORDER BY grouped queries.
    /// PT: Garante que funções GETDATE e SYSDATETIME funcionem em HAVING e ORDER BY com agrupamento.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlServerAggregation")]
    public void TemporalFunction_GetDateAndSysDateTime_InHavingAndOrderBy_ShouldWork()
    {
        var rows = Query("""
            SELECT userId, COUNT(*) AS total
            FROM orders
            GROUP BY userId
            HAVING GETDATE() IS NOT NULL
            ORDER BY SYSDATETIME(), userId
            """);

        Assert.Equal(2, rows.Count);
        Assert.Equal(1, Convert.ToInt32(rows[0].userId));
        Assert.Equal(2, Convert.ToInt32(rows[1].userId));
    }

    /// <summary>
    /// EN: Ensures zero-arg temporal function works in INSERT values and can be read back.
    /// PT: Garante que função temporal sem argumentos funcione em valores de INSERT e possa ser lida depois.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlServerAggregation")]
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
    [Trait("Category", "SqlServerAggregation")]
    public void TemporalFunction_InUpdateSet_ShouldWork()
    {
        Connection.Execute("CREATE TABLE temporal_update_data (id INT, updated_at DATETIME NULL)");
        Connection.Execute("INSERT INTO temporal_update_data (id, updated_at) VALUES (1, NULL)");
        Connection.Execute("UPDATE temporal_update_data SET updated_at = CURRENT_TIMESTAMP WHERE id = 1");

        var rows = Query("SELECT updated_at FROM temporal_update_data WHERE id = 1");

        Assert.Single(rows);
        Assert.NotNull(rows[0].updated_at);
    }

    /// <summary>
    /// EN: Ensures zero-arg temporal function works in HAVING and ORDER BY grouped queries.
    /// PT: Garante que função temporal sem argumentos funcione em HAVING e ORDER BY com agrupamento.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlServerAggregation")]
    public void TemporalFunction_InHavingAndOrderBy_ShouldWork()
    {
        var rows = Query("""
            SELECT userId, COUNT(*) AS total
            FROM orders
            GROUP BY userId
            HAVING CURRENT_TIMESTAMP IS NOT NULL
            ORDER BY CURRENT_TIMESTAMP, userId
            """);

        Assert.Equal(2, rows.Count);
        Assert.Equal(1, Convert.ToInt32(rows[0].userId));
        Assert.Equal(2, Convert.ToInt32(rows[1].userId));
    }




    /// <summary>
    /// EN: Ensures GETDATE()/SYSDATETIME() call-style temporal functions work in HAVING and ORDER BY grouped queries.
    /// PT: Garante que funções temporais GETDATE()/SYSDATETIME() (call-style) funcionem em HAVING e ORDER BY com agrupamento.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlServerAggregation")]
    public void TemporalFunction_GetDateAndSysDateTimeCall_InHavingAndOrderBy_ShouldWork()
    {
        var rows = Query("""
            SELECT userId, COUNT(*) AS total
            FROM orders
            GROUP BY userId
            HAVING GETDATE() IS NOT NULL
            ORDER BY SYSDATETIME(), userId
            """);

        Assert.Equal(2, rows.Count);
        Assert.Equal(1, Convert.ToInt32(rows[0].userId));
        Assert.Equal(2, Convert.ToInt32(rows[1].userId));
    }



    /// <summary>
    /// EN: Ensures GETDATE() call-style temporal function keeps consistent behavior across SELECT/WHERE/HAVING/ORDER BY/INSERT/UPDATE.
    /// PT: Garante que a função temporal GETDATE() (call-style) mantenha comportamento consistente em SELECT/WHERE/HAVING/ORDER BY/INSERT/UPDATE.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlServerAggregation")]
    public void TemporalFunction_GetDateCall_ShouldBeConsistentAcrossContexts()
    {
        var projectionRows = Query("SELECT GETDATE() AS nowValue FROM orders");
        Assert.NotEmpty(projectionRows);
        Assert.NotNull(projectionRows[0].nowValue);

        var whereRows = Query("SELECT id FROM orders WHERE GETDATE() IS NOT NULL");
        Assert.NotEmpty(whereRows);

        var groupedRows = Query("""
            SELECT userId, COUNT(*) AS total
            FROM orders
            GROUP BY userId
            HAVING GETDATE() IS NOT NULL
            ORDER BY GETDATE(), userId
            """);
        Assert.Equal(2, groupedRows.Count);

        Connection.Execute("CREATE TABLE temporal_ctx_insert_getdate (id INT, created_at DATETIME NULL)");
        Connection.Execute("INSERT INTO temporal_ctx_insert_getdate (id, created_at) VALUES (1, GETDATE())");
        var insertedRows = Query("SELECT created_at FROM temporal_ctx_insert_getdate WHERE id = 1");
        Assert.Single(insertedRows);
        Assert.NotNull(insertedRows[0].created_at);

        Connection.Execute("CREATE TABLE temporal_ctx_update_getdate (id INT, updated_at DATETIME NULL)");
        Connection.Execute("INSERT INTO temporal_ctx_update_getdate (id, updated_at) VALUES (1, NULL)");
        Connection.Execute("UPDATE temporal_ctx_update_getdate SET updated_at = GETDATE() WHERE id = 1");
        var updatedRows = Query("SELECT updated_at FROM temporal_ctx_update_getdate WHERE id = 1");
        Assert.Single(updatedRows);
        Assert.NotNull(updatedRows[0].updated_at);
    }



    /// <summary>
    /// EN: Ensures SYSDATETIME() call-style temporal function keeps consistent behavior across SELECT/WHERE/HAVING/ORDER BY/INSERT/UPDATE.
    /// PT: Garante que a função temporal SYSDATETIME() (call-style) mantenha comportamento consistente em SELECT/WHERE/HAVING/ORDER BY/INSERT/UPDATE.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlServerAggregation")]
    public void TemporalFunction_SysDateTimeCall_ShouldBeConsistentAcrossContexts()
    {
        var projectionRows = Query("SELECT SYSDATETIME() AS nowValue FROM orders");
        Assert.NotEmpty(projectionRows);
        Assert.NotNull(projectionRows[0].nowValue);

        var whereRows = Query("SELECT id FROM orders WHERE SYSDATETIME() IS NOT NULL");
        Assert.NotEmpty(whereRows);

        var groupedRows = Query("""
            SELECT userId, COUNT(*) AS total
            FROM orders
            GROUP BY userId
            HAVING SYSDATETIME() IS NOT NULL
            ORDER BY SYSDATETIME(), userId
            """);
        Assert.Equal(2, groupedRows.Count);

        Connection.Execute("CREATE TABLE temporal_ctx_insert_sysdatetime (id INT, created_at DATETIME NULL)");
        Connection.Execute("INSERT INTO temporal_ctx_insert_sysdatetime (id, created_at) VALUES (1, SYSDATETIME())");
        var insertedRows = Query("SELECT created_at FROM temporal_ctx_insert_sysdatetime WHERE id = 1");
        Assert.Single(insertedRows);
        Assert.NotNull(insertedRows[0].created_at);

        Connection.Execute("CREATE TABLE temporal_ctx_update_sysdatetime (id INT, updated_at DATETIME NULL)");
        Connection.Execute("INSERT INTO temporal_ctx_update_sysdatetime (id, updated_at) VALUES (1, NULL)");
        Connection.Execute("UPDATE temporal_ctx_update_sysdatetime SET updated_at = SYSDATETIME() WHERE id = 1");
        var updatedRows = Query("SELECT updated_at FROM temporal_ctx_update_sysdatetime WHERE id = 1");
        Assert.Single(updatedRows);
        Assert.NotNull(updatedRows[0].updated_at);
    }

    /// <summary>
    /// EN: Ensures unsupported temporal function from another dialect reports a clear error message.
    /// PT: Garante que função temporal de outro dialeto gere mensagem de erro clara.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlServerAggregation")]
    public void TemporalFunction_UnsupportedFunctionFromOtherDialect_ShouldThrowClearError()
    {
        var ex = Assert.ThrowsAny<Exception>(() =>
            Query("SELECT NOW() AS invalidNow FROM orders"));

        Assert.Contains("NOW", ex.Message, StringComparison.OrdinalIgnoreCase);
    }




    /// <summary>
    /// EN: Ensures token-only temporal function called with parentheses reports clear error.
    /// PT: Garante que função temporal no formato token chamada com parênteses gere erro claro.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlServerAggregation")]
    public void TemporalFunction_TokenCalledAsFunction_ShouldThrowClearError()
    {
        var ex = Assert.ThrowsAny<Exception>(() =>
            Query("SELECT CURRENT_TIMESTAMP() AS invalidNow FROM orders"));

        Assert.Contains("CURRENT_TIMESTAMP", ex.Message, StringComparison.OrdinalIgnoreCase);
    }



    /// <summary>
    /// EN: Ensures call-only temporal function used without parentheses reports clear error.
    /// PT: Garante que função temporal apenas-invocável usada sem parênteses gere erro claro.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlServerAggregation")]
    public void TemporalFunction_CallOnlyIdentifierWithoutParentheses_ShouldThrowClearError()
    {
        var ex = Assert.ThrowsAny<Exception>(() =>
            Query("SELECT GETDATE AS invalidNow FROM orders"));

        Assert.Contains("GETDATE", ex.Message, StringComparison.OrdinalIgnoreCase);
    }


    /// <summary>
    /// EN: Ensures SYSDATETIME call-style temporal function used without parentheses reports clear error.
    /// PT: Garante que a função temporal call-style SYSDATETIME usada sem parênteses gere erro claro.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlServerAggregation")]
    public void TemporalFunction_SysDateTimeCallOnlyIdentifierWithoutParentheses_ShouldThrowClearError()
    {
        var ex = Assert.ThrowsAny<Exception>(() =>
            Query("SELECT SYSDATETIME AS invalidNow FROM orders"));

        Assert.Contains("SYSDATETIME", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

}