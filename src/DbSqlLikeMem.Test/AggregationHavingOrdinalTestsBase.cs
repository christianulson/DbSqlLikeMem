using System.Data.Common;

namespace DbSqlLikeMem.Test;

/// <summary>
/// EN: Defines shared aggregation and HAVING ordinal scenarios across providers.
/// PT: Define cenários compartilhados de agregação e ordinal em HAVING entre provedores.
/// </summary>
/// <typeparam name="TDbMock">EN: Provider database mock type. PT: Tipo do mock de banco do provedor.</typeparam>
/// <typeparam name="TConnection">EN: Provider connection type. PT: Tipo de conexão do provedor.</typeparam>
public abstract class AggregationHavingOrdinalTestsBase<TDbMock, TConnection> : XUnitTestBase
    where TDbMock : DbMock
    where TConnection : DbConnection
{
    private readonly TConnection _connection;

    /// <summary>
    /// EN: Gets the provider connection used by derived classes.
    /// PT: Obtém a conexão do provedor usada pelas classes derivadas.
    /// </summary>
    protected DbConnection Connection => _connection;

    /// <summary>
    /// EN: Initializes shared aggregation/HAVING tests.
    /// PT: Inicializa testes compartilhados de agregação/HAVING.
    /// </summary>
    /// <param name="helper">EN: Output helper. PT: Helper de saída.</param>
    protected AggregationHavingOrdinalTestsBase(ITestOutputHelper helper) : base(helper)
    {
        var db = CreateDb();
        SeedOrders(db);

        _connection = CreateConnection(db);
        if (_connection.State != System.Data.ConnectionState.Open)
        {
            _connection.Open();
        }
    }

    /// <summary>
    /// EN: Creates the provider-specific database mock used by shared tests.
    /// PT: Cria o simulado de banco específico do provedor usado pelos testes compartilhados.
    /// </summary>
    /// <returns>EN: Provider-specific database mock. PT: Mock de banco específico do provedor.</returns>
    protected abstract TDbMock CreateDb();

    /// <summary>
    /// EN: Creates the provider-specific connection used by shared tests.
    /// PT: Cria a conexão específica do provedor usada pelos testes compartilhados.
    /// </summary>
    /// <param name="db">EN: Provider database mock. PT: Mock de banco do provedor.</param>
    /// <returns>EN: Provider-specific connection. PT: Conexão específica do provedor.</returns>
    protected abstract TConnection CreateConnection(TDbMock db);

    /// <summary>
    /// EN: Executes SQL and returns dynamic rows.
    /// PT: Executa SQL e retorna linhas dinâmicas.
    /// </summary>
    /// <param name="sql">EN: SQL to execute. PT: SQL para executar.</param>
    /// <returns>EN: Materialized result rows. PT: Linhas do resultado materializadas.</returns>
    protected abstract List<dynamic> Query(string sql);

    /// <summary>
    /// EN: Validates DISTINCT + ORDER BY + provider pagination syntax over grouped seed data.
    /// PT: Valida sintaxe de DISTINCT + ORDER BY + paginação do provedor sobre os dados semeados.
    /// </summary>
    /// <param name="paginationClause">EN: Provider pagination clause appended after ORDER BY. PT: Cláusula de paginação do provedor anexada após ORDER BY.</param>
    protected void AssertDistinctOrderPagination(string paginationClause)
    {
        if (string.IsNullOrWhiteSpace(paginationClause))
        {
            throw new ArgumentException("Pagination clause cannot be null, empty, or whitespace.", nameof(paginationClause));
        }

        var sql = $"""
                  SELECT DISTINCT userId
                  FROM orders
                  ORDER BY userId
                  {paginationClause}
                  """;

        var rows = Query(sql);
        Assert.Single(rows);
        Assert.Equal(2, (int)rows[0].userId);
    }


    /// <summary>
    /// EN: Validates grouped string aggregation with custom separator and stable ordering by user id.
    /// PT: Valida agregação textual agrupada com separador customizado e ordenação estável por user id.
    /// </summary>
    /// <param name="sql">EN: Provider-specific SQL using string aggregation. PT: SQL específico do provedor usando agregação textual.</param>
    protected void AssertStringAggregationWithCustomSeparator(string sql)
    {
        if (string.IsNullOrWhiteSpace(sql))
        {
            throw new ArgumentException("SQL cannot be null, empty, or whitespace.", nameof(sql));
        }

        var rows = Query(sql);
        Assert.Equal(2, rows.Count);

        var first = Convert.ToString(rows[0].joined) ?? string.Empty;
        var second = Convert.ToString(rows[1].joined) ?? string.Empty;

        Assert.Contains("|", first, StringComparison.Ordinal);
        Assert.Contains("10", first, StringComparison.Ordinal);
        Assert.Contains("30", first, StringComparison.Ordinal);
        Assert.Contains("5", second, StringComparison.Ordinal);
    }





    /// <summary>
    /// EN: Validates mixed projection with string aggregation and explicit NULL literal remains stable.
    /// PT: Valida que projeção mista com agregação textual e literal NULL explícito permaneça estável.
    /// </summary>
    /// <param name="sql">EN: Provider-specific SQL that projects aggregation + NULL literal. PT: SQL específico do provedor que projeta agregação + literal NULL.</param>
    protected void AssertStringAggregationWithNullProjection(string sql)
    {
        if (string.IsNullOrWhiteSpace(sql))
        {
            throw new ArgumentException("SQL cannot be null, empty, or whitespace.", nameof(sql));
        }

        var rows = Query(sql);
        Assert.Equal(2, rows.Count);

        Assert.Null(rows[0].note);
        Assert.Null(rows[1].note);

        var firstJoined = Convert.ToString(rows[0].joined) ?? string.Empty;
        var secondJoined = Convert.ToString(rows[1].joined) ?? string.Empty;

        Assert.NotEmpty(firstJoined);
        Assert.NotEmpty(secondJoined);
    }



    /// <summary>
    /// EN: Validates CASE expression that returns NULL in grouped projection remains consistent with string aggregation.
    /// PT: Valida que expressão CASE retornando NULL em projeção agrupada permaneça consistente com agregação textual.
    /// </summary>
    /// <param name="sql">EN: Provider-specific SQL that projects aggregation + CASE NULL expression. PT: SQL específico do provedor que projeta agregação + expressão CASE NULL.</param>
    protected void AssertStringAggregationWithCaseNullProjection(string sql)
    {
        if (string.IsNullOrWhiteSpace(sql))
        {
            throw new ArgumentException("SQL cannot be null, empty, or whitespace.", nameof(sql));
        }

        var rows = Query(sql);
        Assert.Equal(2, rows.Count);

        Assert.Null(rows[0].note);
        Assert.Null(rows[1].note);

        var firstJoined = Convert.ToString(rows[0].joined) ?? string.Empty;
        var secondJoined = Convert.ToString(rows[1].joined) ?? string.Empty;

        Assert.NotEmpty(firstJoined);
        Assert.NotEmpty(secondJoined);
    }



    /// <summary>
    /// EN: Validates CASE expression with mixed text/NULL branches in grouped projection stays deterministic.
    /// PT: Valida que expressão CASE com ramos mistos texto/NULL em projeção agrupada permaneça determinística.
    /// </summary>
    /// <param name="sql">EN: Provider-specific SQL that projects aggregation + CASE mixed branches. PT: SQL específico do provedor com agregação + CASE de ramos mistos.</param>
    protected void AssertStringAggregationWithCaseMixedProjection(string sql)
    {
        if (string.IsNullOrWhiteSpace(sql))
        {
            throw new ArgumentException("SQL cannot be null, empty, or whitespace.", nameof(sql));
        }

        var rows = Query(sql);
        Assert.Equal(2, rows.Count);

        Assert.Equal("ok", Convert.ToString(rows[0].note));
        Assert.Null(rows[1].note);

        var firstJoined = Convert.ToString(rows[0].joined) ?? string.Empty;
        var secondJoined = Convert.ToString(rows[1].joined) ?? string.Empty;

        Assert.NotEmpty(firstJoined);
        Assert.NotEmpty(secondJoined);
    }



    /// <summary>
    /// EN: Validates multi-branch CASE projection (text/text) remains stable with grouped string aggregation.
    /// PT: Valida que projeção CASE de múltiplos ramos (texto/texto) permaneça estável com agregação textual agrupada.
    /// </summary>
    /// <param name="sql">EN: Provider-specific SQL with aggregation + multi-branch CASE projection. PT: SQL específico do provedor com agregação + projeção CASE de múltiplos ramos.</param>
    protected void AssertStringAggregationWithCaseMultiBranchProjection(string sql)
    {
        if (string.IsNullOrWhiteSpace(sql))
        {
            throw new ArgumentException("SQL cannot be null, empty, or whitespace.", nameof(sql));
        }

        var rows = Query(sql);
        Assert.Equal(2, rows.Count);

        Assert.Equal("primary", Convert.ToString(rows[0].note));
        Assert.Equal("secondary", Convert.ToString(rows[1].note));

        var firstJoined = Convert.ToString(rows[0].joined) ?? string.Empty;
        var secondJoined = Convert.ToString(rows[1].joined) ?? string.Empty;

        Assert.NotEmpty(firstJoined);
        Assert.NotEmpty(secondJoined);
    }



    /// <summary>
    /// EN: Validates numeric CASE multi-branch projection remains stable with grouped string aggregation.
    /// PT: Valida que projeção CASE numérica de múltiplos ramos permaneça estável com agregação textual agrupada.
    /// </summary>
    /// <param name="sql">EN: Provider-specific SQL with aggregation + numeric CASE multi-branch projection. PT: SQL específico do provedor com agregação + projeção CASE numérica multibranch.</param>
    protected void AssertStringAggregationWithCaseNumericMultiBranchProjection(string sql)
    {
        if (string.IsNullOrWhiteSpace(sql))
        {
            throw new ArgumentException("SQL cannot be null, empty, or whitespace.", nameof(sql));
        }

        var rows = Query(sql);
        Assert.Equal(2, rows.Count);

        Assert.Equal(100, Convert.ToInt32(rows[0].note));
        Assert.Equal(200, Convert.ToInt32(rows[1].note));

        var firstJoined = Convert.ToString(rows[0].joined) ?? string.Empty;
        var secondJoined = Convert.ToString(rows[1].joined) ?? string.Empty;

        Assert.NotEmpty(firstJoined);
        Assert.NotEmpty(secondJoined);
    }

    /// <summary>
    /// EN: Validates ordered-set aggregate syntax WITHIN GROUP applies ORDER BY semantics to string aggregation.
    /// PT: Valida que a sintaxe ordered-set WITHIN GROUP aplique semântica de ORDER BY na agregação textual.
    /// </summary>
    /// <param name="sql">EN: Provider-specific SQL using WITHIN GROUP. PT: SQL específico do provedor usando WITHIN GROUP.</param>
    /// <param name="expected">EN: Expected aggregated text. PT: Texto agregado esperado.</param>
    protected void AssertWithinGroupOrdersAggregation(string sql, string expected)
    {
        if (string.IsNullOrWhiteSpace(sql))
        {
            throw new ArgumentException("SQL cannot be null, empty, or whitespace.", nameof(sql));
        }

        var rows = Query(sql);
        Assert.Single(rows);

        var joined = Convert.ToString(rows[0].joined) ?? string.Empty;
        Assert.Equal(expected, joined);
    }

    /// <summary>
    /// EN: Validates ordered-set aggregate syntax WITHIN GROUP is rejected with an actionable message.
    /// PT: Valida que a sintaxe de agregação ordered-set WITHIN GROUP seja rejeitada com mensagem acionável.
    /// </summary>
    /// <param name="sql">EN: Provider-specific SQL using WITHIN GROUP. PT: SQL específico do provedor usando WITHIN GROUP.</param>
    protected void AssertWithinGroupNotSupported(string sql)
    {
        if (string.IsNullOrWhiteSpace(sql))
        {
            throw new ArgumentException("SQL cannot be null, empty, or whitespace.", nameof(sql));
        }

        SqlNotSupportedAssert.ThrowsWithFeature(() => Query(sql), "WITHIN GROUP");
    }

    /// <summary>
    /// EN: Validates DISTINCT string aggregation ignores NULL values and preserves expected tokens.
    /// PT: Valida que agregação textual com DISTINCT ignore valores NULL e preserve os tokens esperados.
    /// </summary>
    /// <param name="querySql">EN: Provider-specific aggregate query over textagg_data. PT: Query agregada específica do provedor sobre textagg_data.</param>
    protected void AssertStringAggregationDistinctIgnoresNullValues(string querySql)
    {
        if (string.IsNullOrWhiteSpace(querySql))
        {
            throw new ArgumentException("Query SQL cannot be null, empty, or whitespace.", nameof(querySql));
        }

        ExecuteNonQuery("CREATE TABLE textagg_data (grp INT, val VARCHAR(20) NULL)");
        ExecuteNonQuery("INSERT INTO textagg_data (grp, val) VALUES (1, 'a')");
        ExecuteNonQuery("INSERT INTO textagg_data (grp, val) VALUES (1, NULL)");
        ExecuteNonQuery("INSERT INTO textagg_data (grp, val) VALUES (1, 'a')");
        ExecuteNonQuery("INSERT INTO textagg_data (grp, val) VALUES (1, 'b')");

        var rows = Query(querySql);
        Assert.Single(rows);

        var joined = Convert.ToString(rows[0].joined) ?? string.Empty;
        Assert.Contains("a", joined, StringComparison.Ordinal);
        Assert.Contains("b", joined, StringComparison.Ordinal);
        Assert.Contains("|", joined, StringComparison.Ordinal);
    }

    /// <summary>
    /// EN: Validates WITHIN GROUP ordering with composite keys (multiple ORDER BY expressions).
    /// PT: Valida ordenação WITHIN GROUP com chaves compostas (múltiplas expressões em ORDER BY).
    /// </summary>
    /// <param name="querySql">EN: Provider-specific aggregate query over textagg_order. PT: Query agregada específica do provedor sobre textagg_order.</param>
    /// <param name="expected">EN: Expected aggregated text. PT: Texto agregado esperado.</param>
    protected void AssertWithinGroupCompositeOrdering(string querySql, string expected)
    {
        if (string.IsNullOrWhiteSpace(querySql))
            throw new ArgumentException("Query SQL cannot be null, empty, or whitespace.", nameof(querySql));

        ExecuteNonQuery("CREATE TABLE textagg_order (grp INT, val VARCHAR(20) NULL, ord1 INT, ord2 INT)");
        ExecuteNonQuery("INSERT INTO textagg_order (grp, val, ord1, ord2) VALUES (1, 'a', 1, 2)");
        ExecuteNonQuery("INSERT INTO textagg_order (grp, val, ord1, ord2) VALUES (1, 'b', 1, 1)");
        ExecuteNonQuery("INSERT INTO textagg_order (grp, val, ord1, ord2) VALUES (1, 'c', 2, 1)");

        var rows = Query(querySql);
        Assert.Single(rows);

        var joined = Convert.ToString(rows[0].joined) ?? string.Empty;
        Assert.Equal(expected, joined);
    }

    /// <summary>
    /// EN: Validates DISTINCT + WITHIN GROUP ordering to ensure deduplication follows ORDER BY sequence.
    /// PT: Valida DISTINCT + WITHIN GROUP para garantir que a deduplicação siga a sequência do ORDER BY.
    /// </summary>
    /// <param name="querySql">EN: Provider-specific aggregate query over textagg_distinct_order. PT: Query agregada específica do provedor sobre textagg_distinct_order.</param>
    /// <param name="expected">EN: Expected aggregated text after ordering and distinct. PT: Texto agregado esperado após ordenação e distinct.</param>
    protected void AssertWithinGroupDistinctOrdering(string querySql, string expected)
    {
        if (string.IsNullOrWhiteSpace(querySql))
            throw new ArgumentException("Query SQL cannot be null, empty, or whitespace.", nameof(querySql));

        ExecuteNonQuery("CREATE TABLE textagg_distinct_order (grp INT, val VARCHAR(20) NULL, ord1 INT, ord2 INT)");
        ExecuteNonQuery("INSERT INTO textagg_distinct_order (grp, val, ord1, ord2) VALUES (1, 'b', 1, 1)");
        ExecuteNonQuery("INSERT INTO textagg_distinct_order (grp, val, ord1, ord2) VALUES (1, 'a', 1, 2)");
        ExecuteNonQuery("INSERT INTO textagg_distinct_order (grp, val, ord1, ord2) VALUES (1, 'a', 2, 1)");
        ExecuteNonQuery("INSERT INTO textagg_distinct_order (grp, val, ord1, ord2) VALUES (1, NULL, 0, 0)");

        var rows = Query(querySql);
        Assert.Single(rows);

        var joined = Convert.ToString(rows[0].joined) ?? string.Empty;
        Assert.Equal(expected, joined);
    }



    private void ExecuteNonQuery(string sql)
    {
        using var command = Connection.CreateCommand();
        command.CommandText = sql;
        command.ExecuteNonQuery();
    }

    private static void SeedOrders(TDbMock db)
    {
        var orders = db.AddTable("orders");
        orders.AddColumn("id", DbType.Int32, false);
        orders.AddColumn("userId", DbType.Int32, false);
        orders.AddColumn("amount", DbType.Decimal, false, decimalPlaces: 2);

        orders.Add(new Dictionary<int, object?> { [0] = 1, [1] = 1, [2] = 10m });
        orders.Add(new Dictionary<int, object?> { [0] = 2, [1] = 1, [2] = 30m });
        orders.Add(new Dictionary<int, object?> { [0] = 3, [1] = 2, [2] = 5m });
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

        var rows = Query(sql);
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

        var rows = Query(sql);
        Assert.Single(rows);
        Assert.Equal(1, (int)rows[0].userId);
    }

    /// <summary>
    /// EN: Ensures HAVING aggregate alias can be combined with ORDER BY ordinal in grouped execution.
    /// PT: Garante que alias de agregação no HAVING possa ser combinado com ORDER BY ordinal na execução agrupada.
    /// </summary>
    [Fact]
    public void Having_AggregateAlias_WithOrderByOrdinal_ShouldWork()
    {
        const string sql = """
                  SELECT userId, SUM(amount) AS sumAmount
                  FROM orders
                  GROUP BY userId
                  HAVING sumAmount > 0
                  ORDER BY 2 DESC
                  """;

        var rows = Query(sql);

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
    public void Having_InvalidAlias_ShouldThrow()
    {
        const string sql = """
                  SELECT userId, SUM(amount) AS sumAmount
                  FROM orders
                  GROUP BY userId
                  HAVING missing_alias > 0
                  """;

        var ex = Assert.Throws<InvalidOperationException>(() => Query(sql));
        Assert.Contains("HAVING reference", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures HAVING ordinal expression resolves to the corresponding projected select item.
    /// PT: Garante que expressão ordinal no HAVING resolva para o item projetado correspondente no SELECT.
    /// </summary>
    [Fact]
    public void Having_OrdinalExpression_ShouldResolveSelectedColumn()
    {
        const string sql = """
                  SELECT userId, SUM(amount) AS sumAmount
                  FROM orders
                  GROUP BY userId
                  HAVING 2 > 0
                  ORDER BY userId
                  """;

        var rows = Query(sql);
        Assert.Equal(2, rows.Count);
        Assert.Equal(1, (int)rows[0].userId);
        Assert.Equal(2, (int)rows[1].userId);
    }

    /// <summary>
    /// EN: Ensures HAVING mixed with ordinal and aggregate resolves ordinal to the select-item expression.
    /// PT: Garante que HAVING misto com ordinal e agregação resolva o ordinal para a expressão do item do SELECT.
    /// </summary>
    [Fact]
    public void Having_MixedOrdinalAndAggregate_ShouldResolveOrdinal()
    {
        const string sql = """
                  SELECT userId, SUM(amount) AS sumAmount
                  FROM orders
                  GROUP BY userId
                  HAVING 2 > 10 AND SUM(amount) > 0
                  ORDER BY userId
                  """;

        var rows = Query(sql);
        Assert.Single(rows);
        Assert.Equal(1, (int)rows[0].userId);
    }

    /// <summary>
    /// EN: Ensures HAVING CASE expression resolves ordinal references correctly.
    /// PT: Garante que expressão CASE no HAVING resolva corretamente referências ordinais.
    /// </summary>
    [Fact]
    public void Having_CaseWithOrdinal_ShouldResolveOrdinal()
    {
        const string sql = """
                  SELECT userId, SUM(amount) AS sumAmount
                  FROM orders
                  GROUP BY userId
                  HAVING CASE WHEN 2 > 10 THEN 1 ELSE 0 END = 1
                  ORDER BY userId
                  """;

        var rows = Query(sql);
        Assert.Single(rows);
        Assert.Equal(1, (int)rows[0].userId);
    }

    /// <summary>
    /// EN: Ensures HAVING BETWEEN expression resolves ordinal references correctly.
    /// PT: Garante que expressão BETWEEN no HAVING resolva corretamente referências ordinais.
    /// </summary>
    [Fact]
    public void Having_BetweenWithOrdinal_ShouldResolveOrdinal()
    {
        const string sql = """
                  SELECT userId, SUM(amount) AS sumAmount
                  FROM orders
                  GROUP BY userId
                  HAVING 2 BETWEEN 35 AND 45
                  ORDER BY userId
                  """;

        var rows = Query(sql);
        Assert.Single(rows);
        Assert.Equal(1, (int)rows[0].userId);
    }

    /// <summary>
    /// EN: Ensures HAVING IN expression resolves ordinal references correctly.
    /// PT: Garante que expressão IN no HAVING resolva corretamente referências ordinais.
    /// </summary>
    [Fact]
    public void Having_InWithOrdinal_ShouldResolveOrdinal()
    {
        const string sql = """
                  SELECT userId, SUM(amount) AS sumAmount
                  FROM orders
                  GROUP BY userId
                  HAVING 2 IN (40)
                  ORDER BY userId
                  """;

        var rows = Query(sql);
        Assert.Single(rows);
        Assert.Equal(1, (int)rows[0].userId);
    }

    /// <summary>
    /// EN: Ensures numeric thresholds in HAVING aggregate comparisons are treated as constants, not ordinals.
    /// PT: Garante que limites numéricos em comparações de agregação no HAVING sejam tratados como constantes, não ordinais.
    /// </summary>
    [Fact]
    public void Having_AggregateThresholdConstant_ShouldNotBeTreatedAsOrdinal()
    {
        const string sql = """
                  SELECT userId, SUM(amount) AS sumAmount
                  FROM orders
                  GROUP BY userId
                  HAVING SUM(amount) > 10
                  ORDER BY userId
                  """;

        var rows = Query(sql);
        Assert.Single(rows);
        Assert.Equal(1, (int)rows[0].userId);
    }

    /// <summary>
    /// EN: Ensures HAVING ordinal out of range throws a clear validation error.
    /// PT: Garante que ordinal fora do intervalo no HAVING lance um erro de validação claro.
    /// </summary>
    [Fact]
    public void Having_OrdinalOutOfRange_ShouldThrow()
    {
        const string sql = """
                  SELECT userId, SUM(amount) AS sumAmount
                  FROM orders
                  GROUP BY userId
                  HAVING 3 > 0
                  """;

        var ex = Assert.Throws<InvalidOperationException>(() => Query(sql));
        Assert.Contains("HAVING ordinal", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures non-positive ordinals in HAVING are rejected even when mixed with aggregate predicates.
    /// PT: Garante que ordinais não positivos no HAVING sejam rejeitados mesmo quando combinados com predicados de agregação.
    /// </summary>
    [Fact]
    public void Having_NonPositiveOrdinal_WithAggregate_ShouldThrow()
    {
        const string sql = """
                  SELECT userId, SUM(amount) AS sumAmount
                  FROM orders
                  GROUP BY userId
                  HAVING 0 > 0 AND SUM(amount) > 0
                  """;

        var ex = Assert.Throws<InvalidOperationException>(() => Query(sql));
        Assert.Contains("HAVING ordinal", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures out-of-range ordinals in HAVING are rejected even when mixed with aggregate predicates.
    /// PT: Garante que ordinais fora do intervalo no HAVING sejam rejeitados mesmo quando combinados com predicados de agregação.
    /// </summary>
    [Fact]
    public void Having_OrdinalOutOfRange_WithAggregate_ShouldThrow()
    {
        const string sql = """
                  SELECT userId, SUM(amount) AS sumAmount
                  FROM orders
                  GROUP BY userId
                  HAVING 3 > 0 AND SUM(amount) > 0
                  """;

        var ex = Assert.Throws<InvalidOperationException>(() => Query(sql));
        Assert.Contains("HAVING ordinal", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Disposes test resources.
    /// PT: Descarta os recursos do teste.
    /// </summary>
    /// <param name="disposing">EN: True to dispose managed resources. PT: True para descartar recursos gerenciados.</param>
    protected override void Dispose(bool disposing)
    {
        _connection.Dispose();
        base.Dispose(disposing);
    }
}
