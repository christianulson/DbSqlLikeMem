using Dapper;
using System.Data.Common;

namespace DbSqlLikeMem.Test;

/// <summary>
/// EN: Defines shared Dapper-oriented provider contract tests for mock connections.
/// PT: Define testes de contrato compartilhados orientados a Dapper para conexões mock.
/// </summary>
public abstract class DapperSupportTestsBase(
    ITestOutputHelper helper
) : XUnitTestBase(helper)
{
    /// <summary>
    /// EN: Creates and opens the provider-specific connection under test.
    /// PT: Cria e abre a conexão específica do provedor sob teste.
    /// </summary>
    protected abstract DbConnection CreateOpenConnection();

    /// <summary>
    /// EN: Verifies Dapper can execute parameterized CASE WHEN aggregates with grouped ordering.
    /// PT: Verifica se o Dapper executa agregações CASE WHEN parametrizadas com ordenação agrupada.
    /// </summary>
    [Fact]
    [Trait("Category", "DapperContract")]
    public void Dapper_Connection_ShouldSupportGroupedCaseWhenAggregateProjection()
    {
        using var connection = CreateOpenConnection();
        connection.Execute("CREATE TABLE dapper_case_group (id INT PRIMARY KEY, category VARCHAR(10), amount INT)");
        connection.Execute("INSERT INTO dapper_case_group (id, category, amount) VALUES (@id, @category, @amount)", new[]
        {
            new { id = 1, category = "A", amount = 120 },
            new { id = 2, category = "A", amount = 30 },
            new { id = 3, category = "B", amount = 80 },
            new { id = 4, category = "B", amount = 10 }
        });

        var rows = connection.Query<(string category, int score)>(@"SELECT
  category,
  SUM(CASE WHEN amount >= @cutoff THEN 1 ELSE 0 END) score
FROM dapper_case_group
GROUP BY category
ORDER BY category", new { cutoff = 50 }).ToList();

        Assert.Equal(2, rows.Count);
        Assert.Equal(("A", 1), rows[0]);
        Assert.Equal(("B", 1), rows[1]);
    }

    /// <summary>
    /// EN: Verifies Dapper supports parameterized LIKE composition with OR and wildcard variants.
    /// PT: Verifica se o Dapper suporta composição de LIKE parametrizado com OR e variações de curingas.
    /// </summary>
    [Fact]
    [Trait("Category", "DapperContract")]
    public void Dapper_Connection_ShouldSupportParameterizedLikeComposition()
    {
        using var connection = CreateOpenConnection();
        connection.Execute("CREATE TABLE dapper_like_users (id INT PRIMARY KEY, name VARCHAR(100))");
        connection.Execute("INSERT INTO dapper_like_users (id, name) VALUES (@id, @name)", new[]
        {
            new { id = 1, name = "Alice" },
            new { id = 2, name = "Aline" },
            new { id = 3, name = "Bruno" },
            new { id = 4, name = "Ana" }
        });

        var count = connection.ExecuteScalar<int>(
            "SELECT COUNT(*) FROM dapper_like_users WHERE name LIKE @contains OR name LIKE @fixed1",
            new { contains = "%li%", fixed1 = "A__" });

        Assert.Equal(3, count);
    }

    /// <summary>
    /// EN: Verifies Dapper keeps deterministic pagination windows disjoint across pages 1, 2 and 3.
    /// PT: Verifica se o Dapper mantém janelas de paginação determinísticas e disjuntas entre as páginas 1, 2 e 3.
    /// </summary>
    [Fact]
    [Trait("Category", "DapperContract")]
    public void Dapper_Connection_PaginationWithDeterministicOrder_ShouldReturnDisjointPages()
    {
        using var connection = CreateOpenConnection();
        connection.Execute("CREATE TABLE dapper_page_users (id INT PRIMARY KEY, grp INT)");

        for (var i = 1; i <= 9; i++)
            connection.Execute("INSERT INTO dapper_page_users (id, grp) VALUES (@id, @grp)", new { id = i, grp = i <= 3 ? 1 : i <= 6 ? 2 : 3 });

        List<int> ReadPage(int offset) => connection.Query<int>($"SELECT id FROM dapper_page_users ORDER BY grp, id OFFSET {offset} ROWS FETCH NEXT 3 ROWS ONLY").ToList();

        var p1 = ReadPage(0);
        var p2 = ReadPage(3);
        var p3 = ReadPage(6);

        Assert.Equal([1, 2, 3], p1);
        Assert.Equal([4, 5, 6], p2);
        Assert.Equal([7, 8, 9], p3);
        Assert.Empty(p1.Intersect(p2));
        Assert.Empty(p2.Intersect(p3));
    }


    /// <summary>
    /// EN: Verifies Dapper preserves precedence for composite IS NULL, IN and OR filters with explicit grouping.
    /// PT: Verifica se o Dapper preserva a precedência para filtros compostos com IS NULL, IN e OR com agrupamento explícito.
    /// </summary>
    [Fact]
    [Trait("Category", "DapperContract")]
    public void Dapper_Connection_ShouldSupportCompositeNullInOrFilter()
    {
        using var connection = CreateOpenConnection();
        connection.Execute("CREATE TABLE dapper_null_in_or (id INT PRIMARY KEY, nickname VARCHAR(100) NULL, kind VARCHAR(20))");
        connection.Execute("INSERT INTO dapper_null_in_or (id, nickname, kind) VALUES (@id, @nickname, @kind)", new[]
        {
            new { id = 1, nickname = (string?)null, kind = "staff" },
            new { id = 2, nickname = (string?)"Ana", kind = "staff" },
            new { id = 3, nickname = (string?)"Bob", kind = "staff" },
            new { id = 4, nickname = (string?)"Bia", kind = "guest" }
        });

        var count = connection.ExecuteScalar<int>(
            "SELECT COUNT(*) FROM dapper_null_in_or WHERE (nickname IS NULL OR nickname IN (@a, @b)) AND kind = @kind",
            new { a = "Ana", b = "Bob", kind = "staff" });

        Assert.Equal(3, count);
    }

    /// <summary>
    /// EN: Verifies scalar subquery projections keep current mock behavior when inner query returns multiple rows.
    /// PT: Verifica se projeções de subquery escalar mantêm o comportamento atual do mock quando a consulta interna retorna múltiplas linhas.
    /// </summary>
    [Fact]
    [Trait("Category", "DapperContract")]
    public void Dapper_Connection_ScalarSubqueryProjectionWithMultipleRows_ShouldUseFirstInnerCell()
    {
        using var connection = CreateOpenConnection();
        connection.Execute("CREATE TABLE dapper_scalar_users (id INT PRIMARY KEY, name VARCHAR(100))");
        connection.Execute("CREATE TABLE dapper_scalar_orders (id INT PRIMARY KEY, user_id INT, amount INT)");

        connection.Execute("INSERT INTO dapper_scalar_users (id, name) VALUES (1, 'Alice')");
        connection.Execute("INSERT INTO dapper_scalar_orders (id, user_id, amount) VALUES (10, 1, 25)");
        connection.Execute("INSERT INTO dapper_scalar_orders (id, user_id, amount) VALUES (11, 1, 30)");

        var row = connection.QuerySingle<(string name, int firstAmount)>(@"SELECT
  u.name,
  (SELECT o.amount FROM dapper_scalar_orders o WHERE o.user_id = u.id ORDER BY o.id) firstAmount
FROM dapper_scalar_users u
WHERE u.id = 1");

        Assert.Equal("Alice", row.name);
        Assert.Equal(25, row.firstAmount);
    }

    /// <summary>
    /// EN: Verifies Dapper transaction scope restores rollback state and persists a later committed sequence.
    /// PT: Verifica se o escopo transacional do Dapper restaura o estado após rollback e persiste uma sequência posterior com commit.
    /// </summary>
    [Fact]
    [Trait("Category", "DapperContract")]
    public void Dapper_Connection_TransactionSequence_ShouldRollbackThenCommitExpectedState()
    {
        using var connection = CreateOpenConnection();
        connection.Execute("CREATE TABLE dapper_tx_sequence (id INT PRIMARY KEY, value INT)");

        using (var tx = connection.BeginTransaction())
        {
            connection.Execute("INSERT INTO dapper_tx_sequence (id, value) VALUES (1, 10), (2, 20)", transaction: tx);
            connection.Execute("UPDATE dapper_tx_sequence SET value = value + 5 WHERE id = 1", transaction: tx);
            connection.Execute("DELETE FROM dapper_tx_sequence WHERE id = 2", transaction: tx);
            tx.Rollback();
        }

        Assert.Equal(0, connection.ExecuteScalar<int>("SELECT COUNT(*) FROM dapper_tx_sequence"));

        using (var tx = connection.BeginTransaction())
        {
            connection.Execute("INSERT INTO dapper_tx_sequence (id, value) VALUES (1, 10), (2, 20)", transaction: tx);
            connection.Execute("UPDATE dapper_tx_sequence SET value = value + 5 WHERE id = 1", transaction: tx);
            connection.Execute("DELETE FROM dapper_tx_sequence WHERE id = 2", transaction: tx);
            tx.Commit();
        }

        var values = connection.Query<int>("SELECT value FROM dapper_tx_sequence ORDER BY id").ToList();
        Assert.Equal([15], values);
    }



    /// <summary>
    /// EN: Verifies Dapper supports grouped CASE WHEN with HAVING thresholds using shared parameterized conditions.
    /// PT: Verifica se o Dapper suporta CASE WHEN agrupado com limiares em HAVING usando condições parametrizadas compartilhadas.
    /// </summary>
    [Fact]
    [Trait("Category", "DapperContract")]
    public void Dapper_Connection_ShouldSupportCaseWhenWithGroupedHavingThreshold()
    {
        using var connection = CreateOpenConnection();
        connection.Execute("CREATE TABLE dapper_case_having (id INT PRIMARY KEY, category VARCHAR(10), amount INT)");
        connection.Execute("INSERT INTO dapper_case_having (id, category, amount) VALUES (@id, @category, @amount)", new[]
        {
            new { id = 1, category = "A", amount = 110 },
            new { id = 2, category = "A", amount = 60 },
            new { id = 3, category = "B", amount = 55 },
            new { id = 4, category = "B", amount = 10 }
        });

        var rows = connection.Query<(string category, int score)>(@"SELECT
  category,
  SUM(CASE WHEN amount >= @high THEN 2 WHEN amount >= @mid THEN 1 ELSE 0 END) score
FROM dapper_case_having
GROUP BY category
HAVING SUM(CASE WHEN amount >= @high THEN 2 WHEN amount >= @mid THEN 1 ELSE 0 END) >= @minScore
ORDER BY category", new { high = 100, mid = 50, minScore = 2 }).ToList();

        Assert.Single(rows);
        Assert.Equal(("A", 3), rows[0]);
    }

    /// <summary>
    /// EN: Verifies Dapper parameterized LIKE supports contains, fixed-length and suffix patterns in one scenario.
    /// PT: Verifica se LIKE parametrizado no Dapper suporta padrões de contém, tamanho fixo e sufixo em um único cenário.
    /// </summary>
    [Fact]
    [Trait("Category", "DapperContract")]
    public void Dapper_Connection_ShouldSupportParameterizedLikePatternVariants()
    {
        using var connection = CreateOpenConnection();
        connection.Execute("CREATE TABLE dapper_like_variants (id INT PRIMARY KEY, name VARCHAR(100))");
        connection.Execute("INSERT INTO dapper_like_variants (id, name) VALUES (@id, @name)", new[]
        {
            new { id = 1, name = "Alpha" },
            new { id = 2, name = "Aline" },
            new { id = 3, name = "Grail" },
            new { id = 4, name = "Alphabet" }
        });

        int countContains = connection.ExecuteScalar<int>("SELECT COUNT(*) FROM dapper_like_variants WHERE name LIKE @p", new { p = "%ph%" });
        int countFixed = connection.ExecuteScalar<int>("SELECT COUNT(*) FROM dapper_like_variants WHERE name LIKE @p", new { p = "A____" });
        int countSuffix = connection.ExecuteScalar<int>("SELECT COUNT(*) FROM dapper_like_variants WHERE name LIKE @p", new { p = "%bet" });

        Assert.Equal(2, countContains);
        Assert.Equal(2, countFixed);
        Assert.Equal(1, countSuffix);
    }

    /// <summary>
    /// EN: Verifies scalar subquery projections return null when no inner rows exist for an outer row.
    /// PT: Verifica se projeções com subquery escalar retornam nulo quando não existem linhas internas para uma linha externa.
    /// </summary>
    [Fact]
    [Trait("Category", "DapperContract")]
    public void Dapper_Connection_ScalarSubqueryProjectionWithoutMatches_ShouldReturnNull()
    {
        using var connection = CreateOpenConnection();
        connection.Execute("CREATE TABLE dapper_scalar_null_users (id INT PRIMARY KEY, name VARCHAR(100))");
        connection.Execute("CREATE TABLE dapper_scalar_null_orders (id INT PRIMARY KEY, user_id INT, amount INT)");
        connection.Execute("INSERT INTO dapper_scalar_null_users (id, name) VALUES (1, 'Alice')");

        var row = connection.QuerySingle<(int id, object? totalAmount)>(@"SELECT
  u.id,
  (SELECT SUM(o.amount) FROM dapper_scalar_null_orders o WHERE o.user_id = u.id) totalAmount
FROM dapper_scalar_null_users u
WHERE u.id = 1");

        Assert.Equal(1, row.id);
        Assert.Null(row.totalAmount);
    }

    /// <summary>
    /// EN: Verifies deterministic pagination pages are repeatable across consecutive executions.
    /// PT: Verifica se páginas de paginação determinística são repetíveis em execuções consecutivas.
    /// </summary>
    [Fact]
    [Trait("Category", "DapperContract")]
    public void Dapper_Connection_PaginationWithDeterministicOrder_ShouldBeRepeatableAcrossExecutions()
    {
        using var connection = CreateOpenConnection();
        connection.Execute("CREATE TABLE dapper_page_repeat (id INT PRIMARY KEY, grp INT)");

        for (var i = 1; i <= 9; i++)
            connection.Execute("INSERT INTO dapper_page_repeat (id, grp) VALUES (@id, @grp)", new { id = i, grp = i <= 3 ? 1 : i <= 6 ? 2 : 3 });

        List<int> ReadPage(int offset) => connection.Query<int>($"SELECT id FROM dapper_page_repeat ORDER BY grp, id OFFSET {offset} ROWS FETCH NEXT 3 ROWS ONLY").ToList();

        var p1a = ReadPage(0); var p2a = ReadPage(3); var p3a = ReadPage(6);
        var p1b = ReadPage(0); var p2b = ReadPage(3); var p3b = ReadPage(6);

        Assert.Equal(p1a, p1b);
        Assert.Equal(p2a, p2b);
        Assert.Equal(p3a, p3b);
    }

    /// <summary>
    /// EN: Verifies a Dapper transaction observes intermediate write state before rollback.
    /// PT: Verifica se uma transação Dapper observa estado intermediário de escrita antes do rollback.
    /// </summary>
    [Fact]
    [Trait("Category", "DapperContract")]
    public void Dapper_Connection_TransactionReadAfterWrite_ShouldObserveCurrentScopeBeforeRollback()
    {
        using var connection = CreateOpenConnection();
        connection.Execute("CREATE TABLE dapper_tx_read_write (id INT PRIMARY KEY, value INT)");
        connection.Execute("INSERT INTO dapper_tx_read_write (id, value) VALUES (1, 10)");

        using var tx = connection.BeginTransaction();
        connection.Execute("UPDATE dapper_tx_read_write SET value = value + 7 WHERE id = 1", transaction: tx);
        var inside = connection.ExecuteScalar<int>("SELECT value FROM dapper_tx_read_write WHERE id = 1", transaction: tx);
        tx.Rollback();

        var outside = connection.ExecuteScalar<int>("SELECT value FROM dapper_tx_read_write WHERE id = 1");
        Assert.Equal(17, inside);
        Assert.Equal(10, outside);
    }

    /// <summary>
    /// EN: Verifies NULL-comparison parameter handling in composite OR filter returns only null rows when parameter is null.
    /// PT: Verifica se o tratamento de parâmetro de comparação nulo em filtro OR composto retorna apenas linhas nulas quando o parâmetro é nulo.
    /// </summary>
    [Fact]
    [Trait("Category", "DapperContract")]
    public void Dapper_Connection_CompositeNullOrFilter_WithNullParameter_ShouldOnlyMatchNullRows()
    {
        using var connection = CreateOpenConnection();
        connection.Execute("CREATE TABLE dapper_null_param (id INT PRIMARY KEY, nickname VARCHAR(100) NULL)");
        connection.Execute("INSERT INTO dapper_null_param (id, nickname) VALUES (1, NULL)");
        connection.Execute("INSERT INTO dapper_null_param (id, nickname) VALUES (2, 'Ana')");

        var count = connection.ExecuteScalar<int>(
            "SELECT COUNT(*) FROM dapper_null_param WHERE nickname IS NULL OR nickname = @nickname",
            new { nickname = (string?)null });

        Assert.Equal(1, count);
    }

    /// <summary>
    /// EN: Verifies Dapper transactions persist inserted rows after commit.
    /// PT: Verifica se transações Dapper persistem linhas inseridas após commit.
    /// </summary>
    [Fact]
    [Trait("Category", "DapperContract")]
    public void Dapper_Connection_TransactionCommit_ShouldPersistInsert()
    {
        using var connection = CreateOpenConnection();
        connection.Execute("CREATE TABLE dapper_tx_commit_users (id INT PRIMARY KEY, name VARCHAR(100))");

        using (var tx = connection.BeginTransaction())
        {
            connection.Execute(
                "INSERT INTO dapper_tx_commit_users (id, name) VALUES (@id, @name)",
                new { id = 1, name = "Committed" },
                transaction: tx);
            tx.Commit();
        }

        var persistedName = connection.ExecuteScalar<string>("SELECT name FROM dapper_tx_commit_users WHERE id = @id", new { id = 1 });
        Assert.Equal("Committed", persistedName);
    }

    /// <summary>
    /// EN: Verifies Dapper supports typed decimal and datetime parameters in filtering and projection.
    /// PT: Verifica se Dapper suporta parâmetros tipados de decimal e datetime em filtros e projeções.
    /// </summary>
    [Fact]
    [Trait("Category", "DapperContract")]
    public void Dapper_Connection_ShouldSupportTypedDecimalAndDateTimeParameters()
    {
        using var connection = CreateOpenConnection();
        connection.Execute("CREATE TABLE dapper_typed_params (id INT PRIMARY KEY, amount DECIMAL(10,2), created_at DATETIME)");

        var baselineDate = new DateTime(2026, 01, 15, 10, 30, 00, DateTimeKind.Utc);
        connection.Execute(
            "INSERT INTO dapper_typed_params (id, amount, created_at) VALUES (@id, @amount, @createdAt)",
            new { id = 1, amount = 12.75m, createdAt = baselineDate });
        connection.Execute(
            "INSERT INTO dapper_typed_params (id, amount, created_at) VALUES (@id, @amount, @createdAt)",
            new { id = 2, amount = 25.50m, createdAt = baselineDate.AddDays(1) });

        var rows = connection.Query<(int id, decimal amount)>(
            "SELECT id, amount FROM dapper_typed_params WHERE amount >= @minimumAmount AND created_at >= @minimumDate ORDER BY id",
            new { minimumAmount = 20.00m, minimumDate = baselineDate }).ToList();

        Assert.Single(rows);
        Assert.Equal((2, 25.50m), rows[0]);
    }

    /// <summary>
    /// EN: Verifies Dapper executes INNER JOIN with ORDER BY and preserves deterministic ordering.
    /// PT: Verifica se Dapper executa INNER JOIN com ORDER BY e preserva ordenação determinística.
    /// </summary>
    [Fact]
    [Trait("Category", "DapperContract")]
    public void Dapper_Connection_ShouldSupportInnerJoinWithDeterministicOrder()
    {
        using var connection = CreateOpenConnection();
        connection.Execute("CREATE TABLE dapper_join_departments (id INT PRIMARY KEY, name VARCHAR(100))");
        connection.Execute("CREATE TABLE dapper_join_users (id INT PRIMARY KEY, department_id INT, name VARCHAR(100))");

        connection.Execute("INSERT INTO dapper_join_departments (id, name) VALUES (1, 'Engineering')");
        connection.Execute("INSERT INTO dapper_join_departments (id, name) VALUES (2, 'Sales')");
        connection.Execute("INSERT INTO dapper_join_users (id, department_id, name) VALUES (1, 1, 'Alice')");
        connection.Execute("INSERT INTO dapper_join_users (id, department_id, name) VALUES (2, 1, 'Bob')");
        connection.Execute("INSERT INTO dapper_join_users (id, department_id, name) VALUES (3, 2, 'Carol')");

        var rows = connection.Query<(string department, string userName)>(@"SELECT
  d.name department,
  u.name userName
FROM dapper_join_users u
INNER JOIN dapper_join_departments d ON d.id = u.department_id
ORDER BY d.name, u.name").ToList();

        Assert.Equal(3, rows.Count);
        Assert.Equal(("Engineering", "Alice"), rows[0]);
        Assert.Equal(("Engineering", "Bob"), rows[1]);
        Assert.Equal(("Sales", "Carol"), rows[2]);
    }

    /// <summary>
    /// EN: Verifies Dapper handles LEFT JOIN ... IS NULL anti-join filters.
    /// PT: Verifica se Dapper lida com filtros anti-join de LEFT JOIN ... IS NULL.
    /// </summary>
    [Fact]
    [Trait("Category", "DapperContract")]
    public void Dapper_Connection_ShouldSupportLeftJoinIsNullFilter()
    {
        using var connection = CreateOpenConnection();
        connection.Execute("CREATE TABLE dapper_left_users (id INT PRIMARY KEY, name VARCHAR(100))");
        connection.Execute("CREATE TABLE dapper_left_orders (id INT PRIMARY KEY, user_id INT)");

        connection.Execute("INSERT INTO dapper_left_users (id, name) VALUES (1, 'Alice')");
        connection.Execute("INSERT INTO dapper_left_users (id, name) VALUES (2, 'Bob')");
        connection.Execute("INSERT INTO dapper_left_orders (id, user_id) VALUES (10, 1)");

        var userIdsWithoutOrders = connection.Query<int>(@"SELECT u.id
FROM dapper_left_users u
LEFT JOIN dapper_left_orders o ON o.user_id = u.id
WHERE o.id IS NULL
ORDER BY u.id").ToList();

        Assert.Equal([2], userIdsWithoutOrders);
    }

    /// <summary>
    /// EN: Verifies Dapper supports EXISTS subqueries combined with IN and ORDER BY filters.
    /// PT: Verifica se Dapper suporta subqueries EXISTS combinadas com filtros IN e ORDER BY.
    /// </summary>
    [Fact]
    [Trait("Category", "DapperContract")]
    public void Dapper_Connection_ShouldSupportExistsWithInAndOrderBy()
    {
        using var connection = CreateOpenConnection();
        connection.Execute("CREATE TABLE dapper_exists_users (id INT PRIMARY KEY, name VARCHAR(100))");
        connection.Execute("CREATE TABLE dapper_exists_orders (id INT PRIMARY KEY, user_id INT, amount INT)");

        connection.Execute("INSERT INTO dapper_exists_users (id, name) VALUES (1, 'Alice')");
        connection.Execute("INSERT INTO dapper_exists_users (id, name) VALUES (2, 'Bob')");
        connection.Execute("INSERT INTO dapper_exists_users (id, name) VALUES (3, 'Carol')");
        connection.Execute("INSERT INTO dapper_exists_orders (id, user_id, amount) VALUES (10, 1, 100)");
        connection.Execute("INSERT INTO dapper_exists_orders (id, user_id, amount) VALUES (11, 3, 200)");

        var rows = connection.Query<(int id, string name)>(@"SELECT u.id, u.name
FROM dapper_exists_users u
WHERE u.id IN (@a, @b, @c)
  AND EXISTS (SELECT 1 FROM dapper_exists_orders o WHERE o.user_id = u.id)
ORDER BY u.id", new { a = 1, b = 2, c = 3 }).ToList();

        Assert.Equal(2, rows.Count);
        Assert.Equal((1, "Alice"), rows[0]);
        Assert.Equal((3, "Carol"), rows[1]);
    }

}
