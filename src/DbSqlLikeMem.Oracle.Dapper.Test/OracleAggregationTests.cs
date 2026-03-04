namespace DbSqlLikeMem.Oracle.Test;

/// <summary>
/// EN: Runs shared aggregation/HAVING scenarios for Oracle and keeps Oracle-specific coverage.
/// PT: Executa cenários compartilhados de agregação/HAVING para Oracle e mantém cobertura específica de Oracle.
/// </summary>
public sealed class OracleAggregationTests : AggregationHavingOrdinalTestsBase<OracleDbMock, OracleConnectionMock>
{
    /// <summary>
    /// EN: Initializes Oracle aggregation tests.
    /// PT: Inicializa os testes de agregação do Oracle.
    /// </summary>
    /// <param name="helper">EN: Output helper. PT: Helper de saída.</param>
    public OracleAggregationTests(ITestOutputHelper helper) : base(helper)
    {
    }

    /// <inheritdoc />
    protected override OracleDbMock CreateDb() => new();

    /// <inheritdoc />
    protected override OracleConnectionMock CreateConnection(OracleDbMock db) => new(db);

    /// <inheritdoc />
    protected override List<dynamic> Query(string sql) => Connection.Query<dynamic>(sql).ToList();

    /// <summary>
    /// EN: Tests Distinct_Order_WithPagination_ShouldWork behavior.
    /// PT: Testa o comportamento de Distinct_Order_WithPagination_ShouldWork.
    /// </summary>
    [Fact]
    [Trait("Category", "OracleAggregation")]
    public void Distinct_Order_WithPagination_ShouldWork()
    {
        AssertDistinctOrderPagination("OFFSET 1 ROWS FETCH NEXT 1 ROWS ONLY");
    }

    /// <summary>
    /// EN: Tests provider string aggregation with custom separator ignoring NULL values.
    /// PT: Testa agregação textual do provedor com separador customizado ignorando valores NULL.
    /// </summary>
    [Fact]
    [Trait("Category", "OracleAggregation")]
    public void StringAggregation_WithCustomSeparator_ShouldWork()
    {
        const string sql = """
                  SELECT userId, LISTAGG(amount, '|') AS joined
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
    [Trait("Category", "OracleAggregation")]
    public void StringAggregation_WithNullProjection_ShouldWork()
    {
        const string sql = """
                  SELECT userId, LISTAGG(amount, '|') AS joined, NULL AS note
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
    [Trait("Category", "OracleAggregation")]
    public void StringAggregation_WithCaseNullProjection_ShouldWork()
    {
        const string sql = """
                  SELECT userId, LISTAGG(amount, '|') AS joined,
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
    [Trait("Category", "OracleAggregation")]
    public void StringAggregation_WithCaseMixedProjection_ShouldWork()
    {
        const string sql = """
                  SELECT userId, LISTAGG(amount, '|') AS joined,
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
    [Trait("Category", "OracleAggregation")]
    public void StringAggregation_WithCaseMultiBranchProjection_ShouldWork()
    {
        const string sql = """
                  SELECT userId, LISTAGG(amount, '|') AS joined,
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
    [Trait("Category", "OracleAggregation")]
    public void StringAggregation_WithCaseNumericMultiBranchProjection_ShouldWork()
    {
        const string sql = """
                  SELECT userId, LISTAGG(amount, '|') AS joined,
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
    /// EN: Tests LISTAGG default separator behavior (empty string when separator is omitted).
    /// PT: Testa o comportamento padrão do separador do LISTAGG (string vazia quando omitido).
    /// </summary>
    [Fact]
    [Trait("Category", "OracleAggregation")]
    public void ListAgg_WithoutSeparator_ShouldConcatenateWithoutDelimiter()
    {
        const string sql = """
                  SELECT userId, LISTAGG(amount) AS joined
                  FROM orders
                  GROUP BY userId
                  ORDER BY userId
                  """;

        var rows = Query(sql);
        Assert.Equal(2, rows.Count);

        Assert.Equal("1030", Convert.ToString(rows[0].joined));
        Assert.Equal("5", Convert.ToString(rows[1].joined));
    }


    /// <summary>
    /// EN: Ensures string aggregation with DISTINCT ignores NULL values and deduplicates text.
    /// PT: Garante que agregação textual com DISTINCT ignore NULL e remova duplicidade de texto.
    /// </summary>
    [Fact]
    [Trait("Category", "OracleAggregation")]
    public void StringAggregation_Distinct_ShouldIgnoreNullValues()
    {
        AssertStringAggregationDistinctIgnoresNullValues("SELECT LISTAGG(DISTINCT val, '|') AS joined FROM textagg_data WHERE grp = 1");
    }


    /// <summary>
    /// EN: Ensures ordered-set syntax WITHIN GROUP applies ORDER BY to string aggregation output.
    /// PT: Garante que a sintaxe ordered-set WITHIN GROUP aplique ORDER BY na saída da agregação textual.
    /// </summary>
    [Fact]
    [Trait("Category", "Aggregation")]
    public void StringAggregation_WithinGroup_ShouldApplyOrderBy()
    {
        AssertWithinGroupOrdersAggregation("SELECT LISTAGG(amount, '|') WITHIN GROUP (ORDER BY amount DESC) AS joined FROM orders", "30|10|5");
    }

    /// <summary>
    /// EN: Ensures WITHIN GROUP ascending order is applied by string aggregation.
    /// PT: Garante que a ordenação ascendente do WITHIN GROUP seja aplicada na agregação textual.
    /// </summary>
    [Fact]
    [Trait("Category", "Aggregation")]
    public void StringAggregation_WithinGroupAscending_ShouldApplyOrderBy()
    {
        AssertWithinGroupOrdersAggregation("SELECT LISTAGG(amount, '|') WITHIN GROUP (ORDER BY amount ASC) AS joined FROM orders", "5|10|30");
    }

    /// <summary>
    /// EN: Ensures WITHIN GROUP supports composite ORDER BY expressions.
    /// PT: Garante que WITHIN GROUP suporte expressões compostas no ORDER BY.
    /// </summary>
    [Fact]
    [Trait("Category", "Aggregation")]
    public void StringAggregation_WithinGroupCompositeOrder_ShouldApplyOrderBy()
    {
        AssertWithinGroupCompositeOrdering("SELECT LISTAGG(val, '|') WITHIN GROUP (ORDER BY ord1 ASC, ord2 ASC) AS joined FROM textagg_order WHERE grp = 1", "b|a|c");
    }

    /// <summary>
    /// EN: Ensures DISTINCT respects WITHIN GROUP ordering semantics.
    /// PT: Garante que DISTINCT respeite a semântica de ordenação do WITHIN GROUP.
    /// </summary>
    [Fact]
    [Trait("Category", "Aggregation")]
    public void StringAggregation_DistinctWithinGroupCompositeOrder_ShouldApplyOrderBy()
    {
        AssertWithinGroupDistinctOrdering("SELECT LISTAGG(DISTINCT val, '|') WITHIN GROUP (ORDER BY ord1 ASC, ord2 ASC) AS joined FROM textagg_distinct_order WHERE grp = 1", "b|a");
    }


    /// <summary>
    /// EN: Ensures zero-arg temporal function works in WHERE filter and projection.
    /// PT: Garante que função temporal sem argumentos funcione em filtro WHERE e projeção.
    /// </summary>
    [Fact]
    [Trait("Category", "OracleAggregation")]
    public void TemporalFunction_InWhere_ShouldWork()
    {
        var rows = Query("SELECT SYSDATE AS nowValue FROM orders WHERE SYSDATE IS NOT NULL");

        Assert.NotEmpty(rows);
        Assert.NotNull(rows[0].nowValue);
    }




    /// <summary>
    /// EN: Ensures CURRENT_DATE and SYSTIMESTAMP tokens work in WHERE filter and projection.
    /// PT: Garante que tokens CURRENT_DATE e SYSTIMESTAMP funcionem em filtro WHERE e projeção.
    /// </summary>
    [Fact]
    [Trait("Category", "OracleAggregation")]
    public void TemporalFunction_CurrentDateAndSysTimestamp_InWhere_ShouldWork()
    {
        var rows = Query("SELECT CURRENT_DATE AS currentDate, SYSTIMESTAMP AS currentTime FROM orders WHERE CURRENT_DATE IS NOT NULL AND SYSTIMESTAMP IS NOT NULL");

        Assert.NotEmpty(rows);
        Assert.NotNull(rows[0].currentDate);
        Assert.NotNull(rows[0].currentTime);
    }



    /// <summary>
    /// EN: Ensures CURRENT_DATE and SYSTIMESTAMP tokens work in HAVING and ORDER BY grouped queries.
    /// PT: Garante que tokens CURRENT_DATE e SYSTIMESTAMP funcionem em HAVING e ORDER BY com agrupamento.
    /// </summary>
    [Fact]
    [Trait("Category", "OracleAggregation")]
    public void TemporalFunction_CurrentDateAndSysTimestamp_InHavingAndOrderBy_ShouldWork()
    {
        var rows = Query("""
            SELECT userId, COUNT(*) AS total
            FROM orders
            GROUP BY userId
            HAVING CURRENT_DATE IS NOT NULL
            ORDER BY SYSTIMESTAMP, userId
            """);

        Assert.Equal(2, rows.Count);
        Assert.Equal(1, Convert.ToInt32(rows[0].userId));
        Assert.Equal(2, Convert.ToInt32(rows[1].userId));
    }

    /// <summary>
    /// EN: Ensures SYSTIMESTAMP token works explicitly in HAVING and ORDER BY grouped queries.
    /// PT: Garante explicitamente que o token SYSTIMESTAMP funcione em HAVING e ORDER BY com agrupamento.
    /// </summary>
    [Fact]
    [Trait("Category", "OracleAggregation")]
    public void TemporalFunction_SysTimestamp_InHavingAndOrderBy_ShouldWork()
    {
        var rows = Query("""
            SELECT userId, COUNT(*) AS total
            FROM orders
            GROUP BY userId
            HAVING SYSTIMESTAMP IS NOT NULL
            ORDER BY SYSTIMESTAMP, userId
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
    [Trait("Category", "OracleAggregation")]
    public void TemporalFunction_InInsertValues_ShouldWork()
    {
        Connection.Execute("CREATE TABLE temporal_data (id INT, created_at DATETIME NULL)");
        Connection.Execute("INSERT INTO temporal_data (id, created_at) VALUES (1, SYSDATE)");

        var rows = Query("SELECT created_at FROM temporal_data WHERE id = 1");

        Assert.Single(rows);
        Assert.NotNull(rows[0].created_at);
    }


    /// <summary>
    /// EN: Ensures zero-arg temporal function works in UPDATE set expression.
    /// PT: Garante que função temporal sem argumentos funcione em expressão de UPDATE.
    /// </summary>
    [Fact]
    [Trait("Category", "OracleAggregation")]
    public void TemporalFunction_InUpdateSet_ShouldWork()
    {
        Connection.Execute("CREATE TABLE temporal_update_data (id INT, updated_at DATE NULL)");
        Connection.Execute("INSERT INTO temporal_update_data (id, updated_at) VALUES (1, NULL)");
        Connection.Execute("UPDATE temporal_update_data SET updated_at = SYSDATE WHERE id = 1");

        var rows = Query("SELECT updated_at FROM temporal_update_data WHERE id = 1");

        Assert.Single(rows);
        Assert.NotNull(rows[0].updated_at);
    }

    /// <summary>
    /// EN: Ensures SYSDATE token works explicitly in HAVING and ORDER BY grouped queries.
    /// PT: Garante explicitamente que o token SYSDATE funcione em HAVING e ORDER BY com agrupamento.
    /// </summary>
    [Fact]
    [Trait("Category", "OracleAggregation")]
    public void TemporalFunction_SysDate_InHavingAndOrderBy_ShouldWork()
    {
        var rows = Query("""
            SELECT userId, COUNT(*) AS total
            FROM orders
            GROUP BY userId
            HAVING SYSDATE IS NOT NULL
            ORDER BY SYSDATE, userId
            """);

        Assert.Equal(2, rows.Count);
        Assert.Equal(1, Convert.ToInt32(rows[0].userId));
        Assert.Equal(2, Convert.ToInt32(rows[1].userId));
    }




    /// <summary>
    /// EN: Ensures SYSDATE token keeps consistent behavior across SELECT/WHERE/HAVING/ORDER BY/INSERT/UPDATE.
    /// PT: Garante que o token SYSDATE mantenha comportamento consistente em SELECT/WHERE/HAVING/ORDER BY/INSERT/UPDATE.
    /// </summary>
    [Fact]
    [Trait("Category", "OracleAggregation")]
    public void TemporalFunction_SysDate_ShouldBeConsistentAcrossContexts()
    {
        var projectionRows = Query("SELECT SYSDATE AS nowValue FROM orders");
        Assert.NotEmpty(projectionRows);
        Assert.NotNull(projectionRows[0].nowValue);

        var whereRows = Query("SELECT id FROM orders WHERE SYSDATE IS NOT NULL");
        Assert.NotEmpty(whereRows);

        var groupedRows = Query("""
            SELECT userId, COUNT(*) AS total
            FROM orders
            GROUP BY userId
            HAVING SYSDATE IS NOT NULL
            ORDER BY SYSDATE, userId
            """);
        Assert.Equal(2, groupedRows.Count);

        Connection.Execute("CREATE TABLE temporal_ctx_insert_sysdate (id INT, created_at DATETIME NULL)");
        Connection.Execute("INSERT INTO temporal_ctx_insert_sysdate (id, created_at) VALUES (1, SYSDATE)");
        var insertedRows = Query("SELECT created_at FROM temporal_ctx_insert_sysdate WHERE id = 1");
        Assert.Single(insertedRows);
        Assert.NotNull(insertedRows[0].created_at);

        Connection.Execute("CREATE TABLE temporal_ctx_update_sysdate (id INT, updated_at DATE NULL)");
        Connection.Execute("INSERT INTO temporal_ctx_update_sysdate (id, updated_at) VALUES (1, NULL)");
        Connection.Execute("UPDATE temporal_ctx_update_sysdate SET updated_at = SYSDATE WHERE id = 1");
        var updatedRows = Query("SELECT updated_at FROM temporal_ctx_update_sysdate WHERE id = 1");
        Assert.Single(updatedRows);
        Assert.NotNull(updatedRows[0].updated_at);
    }



    /// <summary>
    /// EN: Ensures SYSTIMESTAMP token keeps consistent behavior across SELECT/WHERE/HAVING/ORDER BY/INSERT/UPDATE.
    /// PT: Garante que o token SYSTIMESTAMP mantenha comportamento consistente em SELECT/WHERE/HAVING/ORDER BY/INSERT/UPDATE.
    /// </summary>
    [Fact]
    [Trait("Category", "OracleAggregation")]
    public void TemporalFunction_SysTimestamp_ShouldBeConsistentAcrossContexts()
    {
        var projectionRows = Query("SELECT SYSTIMESTAMP AS nowValue FROM orders");
        Assert.NotEmpty(projectionRows);
        Assert.NotNull(projectionRows[0].nowValue);

        var whereRows = Query("SELECT id FROM orders WHERE SYSTIMESTAMP IS NOT NULL");
        Assert.NotEmpty(whereRows);

        var groupedRows = Query("""
            SELECT userId, COUNT(*) AS total
            FROM orders
            GROUP BY userId
            HAVING SYSTIMESTAMP IS NOT NULL
            ORDER BY SYSTIMESTAMP, userId
            """);
        Assert.Equal(2, groupedRows.Count);

        Connection.Execute("CREATE TABLE temporal_ctx_insert_systimestamp (id INT, created_at DATETIME NULL)");
        Connection.Execute("INSERT INTO temporal_ctx_insert_systimestamp (id, created_at) VALUES (1, SYSTIMESTAMP)");
        var insertedRows = Query("SELECT created_at FROM temporal_ctx_insert_systimestamp WHERE id = 1");
        Assert.Single(insertedRows);
        Assert.NotNull(insertedRows[0].created_at);

        Connection.Execute("CREATE TABLE temporal_ctx_update_systimestamp (id INT, updated_at DATETIME NULL)");
        Connection.Execute("INSERT INTO temporal_ctx_update_systimestamp (id, updated_at) VALUES (1, NULL)");
        Connection.Execute("UPDATE temporal_ctx_update_systimestamp SET updated_at = SYSTIMESTAMP WHERE id = 1");
        var updatedRows = Query("SELECT updated_at FROM temporal_ctx_update_systimestamp WHERE id = 1");
        Assert.Single(updatedRows);
        Assert.NotNull(updatedRows[0].updated_at);
    }



    /// <summary>
    /// EN: Ensures CURRENT_DATE token keeps consistent behavior across SELECT/WHERE/HAVING/ORDER BY/INSERT/UPDATE.
    /// PT: Garante que o token CURRENT_DATE mantenha comportamento consistente em SELECT/WHERE/HAVING/ORDER BY/INSERT/UPDATE.
    /// </summary>
    [Fact]
    [Trait("Category", "OracleAggregation")]
    public void TemporalFunction_CurrentDate_ShouldBeConsistentAcrossContexts()
    {
        var projectionRows = Query("SELECT CURRENT_DATE AS nowValue FROM orders");
        Assert.NotEmpty(projectionRows);
        Assert.NotNull(projectionRows[0].nowValue);

        var whereRows = Query("SELECT id FROM orders WHERE CURRENT_DATE IS NOT NULL");
        Assert.NotEmpty(whereRows);

        var groupedRows = Query("""
            SELECT userId, COUNT(*) AS total
            FROM orders
            GROUP BY userId
            HAVING CURRENT_DATE IS NOT NULL
            ORDER BY CURRENT_DATE, userId
            """);
        Assert.Equal(2, groupedRows.Count);

        Connection.Execute("CREATE TABLE temporal_ctx_insert_current_date (id INT, created_at DATE NULL)");
        Connection.Execute("INSERT INTO temporal_ctx_insert_current_date (id, created_at) VALUES (1, CURRENT_DATE)");
        var insertedRows = Query("SELECT created_at FROM temporal_ctx_insert_current_date WHERE id = 1");
        Assert.Single(insertedRows);
        Assert.NotNull(insertedRows[0].created_at);

        Connection.Execute("CREATE TABLE temporal_ctx_update_current_date (id INT, updated_at DATE NULL)");
        Connection.Execute("INSERT INTO temporal_ctx_update_current_date (id, updated_at) VALUES (1, NULL)");
        Connection.Execute("UPDATE temporal_ctx_update_current_date SET updated_at = CURRENT_DATE WHERE id = 1");
        var updatedRows = Query("SELECT updated_at FROM temporal_ctx_update_current_date WHERE id = 1");
        Assert.Single(updatedRows);
        Assert.NotNull(updatedRows[0].updated_at);
    }



    /// <summary>
    /// EN: Ensures CURRENT_TIMESTAMP token keeps consistent behavior across SELECT/WHERE/HAVING/ORDER BY/INSERT/UPDATE.
    /// PT: Garante que o token CURRENT_TIMESTAMP mantenha comportamento consistente em SELECT/WHERE/HAVING/ORDER BY/INSERT/UPDATE.
    /// </summary>
    [Fact]
    [Trait("Category", "OracleAggregation")]
    public void TemporalFunction_CurrentTimestamp_ShouldBeConsistentAcrossContexts()
    {
        var projectionRows = Query("SELECT CURRENT_TIMESTAMP AS nowValue FROM orders");
        Assert.NotEmpty(projectionRows);
        Assert.NotNull(projectionRows[0].nowValue);

        var whereRows = Query("SELECT id FROM orders WHERE CURRENT_TIMESTAMP IS NOT NULL");
        Assert.NotEmpty(whereRows);

        var groupedRows = Query("""
            SELECT userId, COUNT(*) AS total
            FROM orders
            GROUP BY userId
            HAVING CURRENT_TIMESTAMP IS NOT NULL
            ORDER BY CURRENT_TIMESTAMP, userId
            """);
        Assert.Equal(2, groupedRows.Count);

        Connection.Execute("CREATE TABLE temporal_ctx_insert_current_timestamp (id INT, created_at DATETIME NULL)");
        Connection.Execute("INSERT INTO temporal_ctx_insert_current_timestamp (id, created_at) VALUES (1, CURRENT_TIMESTAMP)");
        var insertedRows = Query("SELECT created_at FROM temporal_ctx_insert_current_timestamp WHERE id = 1");
        Assert.Single(insertedRows);
        Assert.NotNull(insertedRows[0].created_at);

        Connection.Execute("CREATE TABLE temporal_ctx_update_current_timestamp (id INT, updated_at DATETIME NULL)");
        Connection.Execute("INSERT INTO temporal_ctx_update_current_timestamp (id, updated_at) VALUES (1, NULL)");
        Connection.Execute("UPDATE temporal_ctx_update_current_timestamp SET updated_at = CURRENT_TIMESTAMP WHERE id = 1");
        var updatedRows = Query("SELECT updated_at FROM temporal_ctx_update_current_timestamp WHERE id = 1");
        Assert.Single(updatedRows);
        Assert.NotNull(updatedRows[0].updated_at);
    }

    /// <summary>
    /// EN: Ensures unsupported temporal function from another dialect reports a clear error message.
    /// PT: Garante que função temporal de outro dialeto gere mensagem de erro clara.
    /// </summary>
    [Fact]
    [Trait("Category", "OracleAggregation")]
    public void TemporalFunction_UnsupportedFunctionFromOtherDialect_ShouldThrowClearError()
    {
        var ex = Assert.ThrowsAny<Exception>(() =>
            Query("SELECT GETDATE() AS invalidNow FROM orders"));

        Assert.Contains("GETDATE", ex.Message, StringComparison.OrdinalIgnoreCase);
    }




    /// <summary>
    /// EN: Ensures token-only temporal function called with parentheses reports clear error.
    /// PT: Garante que função temporal no formato token chamada com parênteses gere erro claro.
    /// </summary>
    [Fact]
    [Trait("Category", "OracleAggregation")]
    public void TemporalFunction_TokenCalledAsFunction_ShouldThrowClearError()
    {
        var ex = Assert.ThrowsAny<Exception>(() =>
            Query("SELECT CURRENT_TIMESTAMP() AS invalidNow FROM orders"));

        Assert.Contains("CURRENT_TIMESTAMP", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures SYSTIMESTAMP token called with parentheses reports clear error.
    /// PT: Garante que o token SYSTIMESTAMP chamado com parênteses gere erro claro.
    /// </summary>
    [Fact]
    [Trait("Category", "OracleAggregation")]
    public void TemporalFunction_SysTimestampCalledAsFunction_ShouldThrowClearError()
    {
        var ex = Assert.ThrowsAny<Exception>(() =>
            Query("SELECT SYSTIMESTAMP() AS invalidNow FROM orders"));

        Assert.Contains("SYSTIMESTAMP", ex.Message, StringComparison.OrdinalIgnoreCase);
    }


    /// <summary>
    /// EN: Ensures SYSDATE token called with parentheses reports clear error.
    /// PT: Garante que o token SYSDATE chamado com parênteses gere erro claro.
    /// </summary>
    [Fact]
    [Trait("Category", "OracleAggregation")]
    public void TemporalFunction_SysDateCalledAsFunction_ShouldThrowClearError()
    {
        var ex = Assert.ThrowsAny<Exception>(() =>
            Query("SELECT SYSDATE() AS invalidNow FROM orders"));

        Assert.Contains("SYSDATE", ex.Message, StringComparison.OrdinalIgnoreCase);
    }


    /// <summary>
    /// EN: Ensures CURRENT_DATE token called with parentheses reports clear error.
    /// PT: Garante que o token CURRENT_DATE chamado com parênteses gere erro claro.
    /// </summary>
    [Fact]
    [Trait("Category", "OracleAggregation")]
    public void TemporalFunction_CurrentDateCalledAsFunction_ShouldThrowClearError()
    {
        var ex = Assert.ThrowsAny<Exception>(() =>
            Query("SELECT CURRENT_DATE() AS invalidDate FROM orders"));

        Assert.Contains("CURRENT_DATE", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

}
