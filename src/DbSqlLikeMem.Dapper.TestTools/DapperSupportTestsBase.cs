using Dapper;
using System.Linq.Expressions;
using System.Reflection;
using System.Text.RegularExpressions;

namespace DbSqlLikeMem.TestTools;

internal static class DbMockFactory
{
    public static TDb Create<TDb>(int? version = null)
        where TDb : DbMock
    {
        foreach (var ctor in typeof(TDb).GetConstructors(BindingFlags.Public | BindingFlags.Instance).OrderBy(static c => c.GetParameters().Length))
        {
            var parameters = ctor.GetParameters();
            var args = new object?[parameters.Length];
            var supported = true;

            for (var i = 0; i < parameters.Length; i++)
            {
                var parameter = parameters[i];

                if (parameter.ParameterType == typeof(int?) || parameter.ParameterType == typeof(int))
                {
                    if (version.HasValue)
                    {
                        args[i] = version.Value;
                        continue;
                    }

                    if (parameter.IsOptional)
                    {
                        args[i] = parameter.DefaultValue is DBNull ? null : parameter.DefaultValue;
                        continue;
                    }

                    if (parameter.ParameterType == typeof(int?))
                    {
                        args[i] = null;
                        continue;
                    }
                }
                else if (parameter.IsOptional)
                {
                    args[i] = parameter.DefaultValue is DBNull ? null : parameter.DefaultValue;
                    continue;
                }

                supported = false;
                break;
            }

            if (supported)
                return (TDb)ctor.Invoke(args);
        }

        throw new InvalidOperationException($"No supported public constructor was found for {typeof(TDb).FullName}.");
    }
}

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

    /// <summary>
    /// EN: Verifies Dapper QueryMultiple returns independent ordered result sets from a single command.
    /// PT: Verifica se o Dapper QueryMultiple retorna conjuntos de resultados ordenados e independentes a partir de um único comando.
    /// </summary>
    [Fact]
    [Trait("Category", "DapperContract")]
    public void Dapper_Connection_QueryMultiple_ShouldReturnOrderedIndependentResultSets()
    {
        using var connection = CreateOpenConnection();
        connection.Execute("CREATE TABLE dapper_qm_users (id INT PRIMARY KEY, name VARCHAR(100))");
        connection.Execute("CREATE TABLE dapper_qm_orders (id INT PRIMARY KEY, user_id INT, amount INT)");

        connection.Execute("INSERT INTO dapper_qm_users (id, name) VALUES (1, 'Alice')");
        connection.Execute("INSERT INTO dapper_qm_users (id, name) VALUES (2, 'Bob')");
        connection.Execute("INSERT INTO dapper_qm_orders (id, user_id, amount) VALUES (10, 1, 30)");
        connection.Execute("INSERT INTO dapper_qm_orders (id, user_id, amount) VALUES (11, 2, 15)");
        connection.Execute("INSERT INTO dapper_qm_orders (id, user_id, amount) VALUES (12, 1, 45)");

        using var multi = connection.QueryMultiple(@"
SELECT id, name FROM dapper_qm_users ORDER BY id;
SELECT user_id userId, amount FROM dapper_qm_orders WHERE amount >= @minAmount ORDER BY amount DESC;", new { minAmount = 20 });

        var users = multi.Read<(int id, string name)>().ToList();
        var orders = multi.Read<(int userId, int amount)>().ToList();

        Assert.Equal([(1, "Alice"), (2, "Bob")], users);
        Assert.Equal([(1, 45), (1, 30)], orders);
    }

    /// <summary>
    /// EN: Verifies Dapper multi-mapping with splitOn composes joined values into a single aggregate object.
    /// PT: Verifica se multi-mapping do Dapper com splitOn compõe valores de join em um único objeto agregado.
    /// </summary>
    [Fact]
    [Trait("Category", "DapperContract")]
    public void Dapper_Connection_QueryWithSplitOn_ShouldComposeAggregateFromJoin()
    {
        using var connection = CreateOpenConnection();
        connection.Execute("CREATE TABLE dapper_mm_users (id INT PRIMARY KEY, name VARCHAR(100))");
        connection.Execute("CREATE TABLE dapper_mm_user_tenants (user_id INT, tenant_id INT)");

        connection.Execute("INSERT INTO dapper_mm_users (id, name) VALUES (1, 'Alice')");
        connection.Execute("INSERT INTO dapper_mm_user_tenants (user_id, tenant_id) VALUES (1, 7)");

        var row = connection.Query<DapperMultiMapUser, int, DapperMultiMapUser>(
            @"SELECT
  u.id Id,
  u.name Name,
  ut.tenant_id TenantId
FROM dapper_mm_users u
INNER JOIN dapper_mm_user_tenants ut ON ut.user_id = u.id
WHERE u.id = @id",
            static (user, tenantId) =>
            {
                user.Tenants.Add(tenantId);
                return user;
            },
            new { id = 1 },
            splitOn: "TenantId").Single();

        Assert.Equal(1, row.Id);
        Assert.Equal("Alice", row.Name);
        Assert.Equal([7], row.Tenants);
    }

    private sealed class DapperMultiMapUser
    {
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public List<int> Tenants { get; } = [];
    }

}

/// <summary>
/// EN: Provides a provider-agnostic Dapper smoke test base that opens a connection using a parameterless constructor.
/// PT: Fornece uma base de testes smoke Dapper agnóstica de provedor que abre conexão usando construtor sem parâmetros.
/// </summary>
/// <typeparam name="TConnection">EN: Provider connection type. PT: Tipo de conexão do provedor.</typeparam>
public abstract class DapperSmokeTestsBase<TConnection>(
    ITestOutputHelper helper,
    Func<TConnection> connectionFactory
) : DapperSupportTestsBase(helper)
    where TConnection : DbConnection
{
    /// <summary>
    /// EN: Creates and opens a provider connection instance using the parameterless constructor.
    /// PT: Cria e abre uma instancia de conexao do provedor usando o construtor sem parametros.
    /// </summary>
    protected sealed override DbConnection CreateOpenConnection()
    {
        var connection = connectionFactory();
        connection.Open();
        return connection;
    }
}

/// <summary>
/// EN: Shared DTO used by Dapper CRUD support tests.
/// PT: DTO compartilhado usado pelos testes de suporte CRUD do Dapper.
/// </summary>
public sealed class UserObjectTest
{
    /// <summary>
    /// EN: Gets or sets the user identifier used in CRUD test projections.
    /// PT: Obtem ou define o identificador do usuario usado nas projecoes de teste de CRUD.
    /// </summary>
    public int Id { get; set; }
    /// <summary>
    /// EN: Gets or sets the user name used in CRUD test projections.
    /// PT: Obtem ou define o nome do usuario usado nas projecoes de teste de CRUD.
    /// </summary>
    public string Name { get; set; } = string.Empty;
    /// <summary>
    /// EN: Gets or sets the user email used in CRUD test projections.
    /// PT: Obtem ou define o email do usuario usado nas projecoes de teste de CRUD.
    /// </summary>
    public string Email { get; set; } = string.Empty;
    /// <summary>
    /// EN: Gets or sets the creation timestamp used in CRUD test projections.
    /// PT: Obtem ou define o timestamp de criacao usado nas projecoes de teste de CRUD.
    /// </summary>
    public DateTime CreatedDate { get; set; }
    /// <summary>
    /// EN: Gets or sets the optional update timestamp used in CRUD test projections.
    /// PT: Obtem ou define o timestamp opcional de atualizacao usado nas projecoes de teste de CRUD.
    /// </summary>
    public DateTime? UpdatedData { get; set; }
    /// <summary>
    /// EN: Gets or sets the non-null GUID value used in CRUD test projections.
    /// PT: Obtem ou define o valor GUID nao nulo usado nas projecoes de teste de CRUD.
    /// </summary>
    public Guid TestGuid { get; set; }
    /// <summary>
    /// EN: Gets or sets the optional GUID value used in CRUD test projections.
    /// PT: Obtem ou define o valor GUID opcional usado nas projecoes de teste de CRUD.
    /// </summary>
    public Guid? TestGuidNull { get; set; }
}

/// <summary>
/// EN: Shared CRUD and multi-result Dapper tests for provider mocks.
/// PT: Testes compartilhados de CRUD e múltiplos resultados com Dapper para mocks de provedor.
/// </summary>
public abstract class DapperCrudTestsBase(
    ITestOutputHelper helper,
    Func<DbMock> dbFactory,
    Func<DbMock, DbConnectionMockBase> connectionFactory,
    Func<DbConnectionMockBase, DbCommand> commandFactory
) : XUnitTestBase(helper)
{
    private readonly Func<DbMock> _dbFactory = dbFactory;
    private readonly Func<DbMock, DbConnectionMockBase> _connectionFactory = connectionFactory;
    private readonly Func<DbConnectionMockBase, DbCommand> _commandFactory = commandFactory;
    private readonly DbConnectionMockBase _connection = CreateSeededConnection(dbFactory, connectionFactory);

    /// <summary>
    /// EN: Verifies a basic Dapper select query returns a non-null result sequence.
    /// PT: Verifica se uma consulta select basica via Dapper retorna uma sequencia de resultados nao nula.
    /// </summary>
    [Fact]
    [Trait("Category", "Dapper")]
    public void TestSelectQuery()
    {
        var users = _connection.Query<UserObjectTest>("SELECT * FROM Users").ToList();
        Assert.NotNull(users);
    }

    /// <summary>
    /// EN: Verifies query execution returns the expected row values from the mocked table.
    /// PT: Verifica se a execucao da consulta retorna os valores de linha esperados da tabela simulada.
    /// </summary>
    [Fact]
    [Trait("Category", "Dapper")]
    public void QueryShouldReturnCorrectData()
    {
        var db = _dbFactory();
        var table = db.AddTable("users");
        table.AddColumn("id", DbType.Int32, false);
        table.AddColumn("name", DbType.String, false);
        table.AddColumn("CreatedDate", DbType.DateTime, false);

        var dt = DateTime.UtcNow;
        table.Add(new Dictionary<int, object?> { { 0, 1 }, { 1, "John Doe" }, { 2, dt } });

        using var connection = _connectionFactory(db);
        using var command = _commandFactory(connection);
        command.CommandText = "SELECT * FROM users";

        IEnumerable<dynamic> result;
        using (var reader = command.ExecuteReader())
        {
            result = [.. reader.Parse<dynamic>()];
        }

        Assert.Single(result);
        Assert.Equal(1, result.First().id);
        Assert.Equal("John Doe", result.First().name);
        Assert.Equal(dt, result.First().CreatedDate);
    }

    /// <summary>
    /// EN: Verifies Execute inserts a row and persists the expected column values.
    /// PT: Verifica se Execute insere uma linha e persiste os valores de coluna esperados.
    /// </summary>
    [Fact]
    [Trait("Category", "Dapper")]
    public void ExecuteShouldInsertData()
    {
        var db = _dbFactory();
        var table = db.AddTable("users");
        table.AddColumn("id", DbType.Int32, false);
        table.AddColumn("name", DbType.String, false);
        table.AddColumn("CreatedDate", DbType.DateTime, false);

        var dt = DateTime.UtcNow;

        using var connection = _connectionFactory(db);
        connection.Open();

        var rowsAffected = connection.Execute(
            "INSERT INTO users (id, name, createdDate) VALUES (@id, @name, @dt)",
            new { id = 1, name = "John Doe", dt });

        Assert.Equal(1, rowsAffected);
        Assert.Single(table);
        Assert.Equal(1, table[0][0]);
        Assert.Equal("John Doe", table[0][1]);
        Assert.Equal(dt, table[0][2]);
    }

    /// <summary>
    /// EN: Verifies Execute updates an existing row with the expected values.
    /// PT: Verifica se Execute atualiza uma linha existente com os valores esperados.
    /// </summary>
    [Fact]
    [Trait("Category", "Dapper")]
    public void ExecuteShouldUpdateData()
    {
        var db = _dbFactory();
        var table = db.AddTable("users");
        table.AddColumn("id", DbType.Int32, false);
        table.AddColumn("name", DbType.String, false);
        table.AddColumn("CreatedDate", DbType.DateTime, false);
        table.AddColumn("UpdatedData", DbType.DateTime, true);

        var dtInsert = DateTime.UtcNow.AddDays(-1);
        var dtUpdate = DateTime.UtcNow;

        table.AddItem(new { id = 1, name = "John Doe", CreatedDate = dtInsert });

        Assert.Single(table);
        Assert.Equal("John Doe", table[0][1]);
        Assert.Equal(dtInsert, table[0][2]);
        Assert.Null(table[0][3]);

        using var connection = _connectionFactory(db);
        connection.Open();

        var rowsAffected = connection.Execute(@"
UPDATE users 
   SET name = @name
     , UpdatedData = @dtUpdate 
 WHERE id = @id", new { id = 1, name = "Jane Doe", dtUpdate });

        Assert.Equal(1, rowsAffected);
        Assert.Single(table);
        Assert.Equal("Jane Doe", table[0][1]);
        Assert.Equal(dtInsert, table[0][2]);
        Assert.Equal(dtUpdate, table[0][3]);
    }

    /// <summary>
    /// EN: Verifies Execute deletes the targeted row from the mocked table.
    /// PT: Verifica se Execute exclui a linha alvo da tabela simulada.
    /// </summary>
    [Fact]
    [Trait("Category", "Dapper")]
    public void ExecuteShouldDeleteData()
    {
        var db = _dbFactory();
        var table = db.AddTable("users");
        table.AddColumn("id", DbType.Int32, false);
        table.AddColumn("name", DbType.String, false);

        table.Add(new Dictionary<int, object?> { { 0, 1 }, { 1, "John Doe" } });

        using var connection = _connectionFactory(db);
        connection.Open();

        var rowsAffected = connection.Execute("DELETE FROM users WHERE id = @id", new { id = 1 });

        Assert.Equal(1, rowsAffected);
        Assert.Empty(table);
    }

    /// <summary>
    /// EN: Verifies QueryMultiple returns each result set in the expected order and shape.
    /// PT: Verifica se QueryMultiple retorna cada conjunto de resultados na ordem e formato esperados.
    /// </summary>
    [Fact]
    [Trait("Category", "Dapper")]
    public void QueryMultipleShouldReturnMultipleResultSets()
    {
        var dt = DateTime.UtcNow;
        var dt2 = DateTime.UtcNow.AddDays(-1);

        var db = _dbFactory();
        var table1 = db.AddTable("users");
        table1.AddColumn("id", DbType.Int32, false);
        table1.AddColumn("name", DbType.String, false);
        table1.Add(new Dictionary<int, object?> { { 0, 1 }, { 1, "John Doe" } });

        var table2 = db.AddTable("emails");
        table2.AddColumn("id", DbType.Int32, false);
        table2.AddColumn("email", DbType.String, false);
        table2.AddColumn("CreatedDate", DbType.DateTime, false);
        table2.Add(new Dictionary<int, object?> { { 0, 1 }, { 1, "john.doe@example.com" }, { 2, dt } });
        table2.Add(new Dictionary<int, object?> { { 0, 2 }, { 1, "jane.doe@example.com" }, { 2, dt2 } });

        using var connection = _connectionFactory(db);
        using var command = _commandFactory(connection);
        command.CommandText = "SELECT * FROM users; SELECT * FROM emails ORDER BY CreatedDate DESC;";

        var resultSets = new List<IEnumerable<dynamic>>();
        using (var reader = command.ExecuteReader())
        {
            do
            {
                var resultSet = reader.Parse<dynamic>().ToList();
                resultSets.Add(resultSet);
            } while (reader.NextResult());
        }

        Assert.Equal(2, resultSets.Count);

        var users = resultSets[0].ToList();
        Assert.Single(users);
        Assert.Equal(1, users[0].id);
        Assert.Equal("John Doe", users[0].name);

        var emails = resultSets[1].ToList();
        Assert.Equal(2, emails.Count);
        Assert.Equal(1, emails[0].id);
        Assert.Equal("john.doe@example.com", emails[0].email);
        Assert.Equal(dt, emails[0].CreatedDate);
        Assert.Equal(2, emails[1].id);
        Assert.Equal("jane.doe@example.com", emails[1].email);
        Assert.Equal(dt2, emails[1].CreatedDate);
    }

    /// <inheritdoc />
    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            _connection.Dispose();
        }

        base.Dispose(disposing);
    }

    private static DbConnectionMockBase CreateSeededConnection(
        Func<DbMock> dbFactory,
        Func<DbMock, DbConnectionMockBase> connectionFactory)
    {
        var db = dbFactory();
        var table = db.AddTable("users");
        table.AddColumn("Id", DbType.Int32, false);
        table.AddColumn("Name", DbType.String, false);
        table.AddColumn("Email", DbType.String, false);
        table.AddColumn("CreatedDate", DbType.DateTime, false);
        table.AddColumn("UpdatedData", DbType.DateTime, true);
        table.AddColumn("TestGuid", DbType.Guid, false);
        table.AddColumn("TestGuidNull", DbType.Guid, true);

        var connection = connectionFactory(db);
        connection.Open();
        return connection;
    }
}

/// <summary>
/// EN: Shared user contract model for Dapper consumer tests.
/// PT: Modelo contratual compartilhado de usuário para testes consumidores com Dapper.
/// </summary>
public class DapperUserContractModel
{
    /// <summary>
    /// EN: Gets or sets the user identifier mapped from Dapper queries.
    /// PT: Obtem ou define o identificador do usuario mapeado a partir das consultas Dapper.
    /// </summary>
    public int Id { get; set; }
    /// <summary>
    /// EN: Gets or sets the user name mapped from Dapper queries.
    /// PT: Obtem ou define o nome do usuario mapeado a partir das consultas Dapper.
    /// </summary>
    public required string Name { get; set; }
    /// <summary>
    /// EN: Gets or sets the optional user email mapped from Dapper queries.
    /// PT: Obtem ou define o email opcional do usuario mapeado a partir das consultas Dapper.
    /// </summary>
    public string? Email { get; set; }
    /// <summary>
    /// EN: Gets or sets the user creation timestamp mapped from Dapper queries.
    /// PT: Obtem ou define o timestamp de criacao do usuario mapeado a partir das consultas Dapper.
    /// </summary>
    public DateTime CreatedDate { get; set; }
    /// <summary>
    /// EN: Gets or sets the optional user update timestamp mapped from Dapper queries.
    /// PT: Obtem ou define o timestamp opcional de atualizacao do usuario mapeado a partir das consultas Dapper.
    /// </summary>
    public DateTime? UpdatedData { get; set; }
    /// <summary>
    /// EN: Gets or sets the non-null GUID value mapped from Dapper queries.
    /// PT: Obtem ou define o valor GUID nao nulo mapeado a partir das consultas Dapper.
    /// </summary>
    public Guid TestGuid { get; set; }
    /// <summary>
    /// EN: Gets or sets the optional GUID value mapped from Dapper queries.
    /// PT: Obtem ou define o valor GUID opcional mapeado a partir das consultas Dapper.
    /// </summary>
    public Guid? TestGuidNull { get; set; }
}

/// <summary>
/// EN: Shared user contract model for Dapper multi-mapping tests.
/// PT: Modelo contratual compartilhado de usuário para testes de multi-mapping com Dapper.
/// </summary>
public sealed class DapperUserJoinContractModel : DapperUserContractModel
{
    /// <summary>
    /// EN: Gets or sets the tenant identifiers collected during Dapper multi-mapping joins.
    /// PT: Obtem ou define os identificadores de tenant coletados durante joins com multi-mapping do Dapper.
    /// </summary>
    public List<int> Tenants { get; set; } = [];
}

/// <summary>
/// EN: Shared Dapper user CRUD/query contract tests for provider mocks.
/// PT: Testes compartilhados de contrato Dapper para CRUD/consulta de usuário em mocks de provedor.
/// </summary>
public abstract class DapperUserTestsBase(
    ITestOutputHelper helper,
    Func<DbMock> dbFactory,
    Func<DbMock, DbConnectionMockBase> connectionFactory,
    Func<DbConnectionMockBase, DbCommand> commandFactory,
    string queryMultipleUsersSql = "SELECT * FROM Users1; SELECT * FROM Users2;",
    string queryWithJoinSql = """
                SELECT U.*, UT.TenantId 
                FROM User U
                JOIN UserTenant UT ON U.Id = UT.UserId
                WHERE U.Id = @Id
                """
) : XUnitTestBase(helper)
{
    private readonly Func<DbMock> _dbFactory = dbFactory;
    private readonly Func<DbMock, DbConnectionMockBase> _connectionFactory = connectionFactory;
    private readonly Func<DbConnectionMockBase, DbCommand> _commandFactory = commandFactory;
    private readonly string _queryMultipleUsersSql = queryMultipleUsersSql;
    private readonly string _queryWithJoinSql = queryWithJoinSql;

    /// <summary>
    /// EN: Verifies inserting a user writes the expected row into the target table.
    /// PT: Verifica se a insercao de um usuario grava a linha esperada na tabela de destino.
    /// </summary>
    [Fact]
    [Trait("Category", "DapperUser")]
    public void InsertUserShouldAddUserToTable()
    {
        var db = _dbFactory();
        var table = CreateUserTable(db, "Users");

        using var connection = _connectionFactory(db);
        connection.Open();

        var user = NewUser();
        var rowsAffected = connection.Execute(
            "INSERT INTO Users (Id, Name, Email, CreatedDate, UpdatedData, TestGuid, TestGuidNull) VALUES (@Id, @Name, @Email, @CreatedDate, @UpdatedData, @TestGuid, @TestGuidNull)",
            user);

        Assert.Equal(1, rowsAffected);
        Assert.Single(table);
        AssertUserRow(table[0], user);
    }

    /// <summary>
    /// EN: Verifies querying a user returns the expected mapped contract values.
    /// PT: Verifica se consultar um usuario retorna os valores contratuais mapeados esperados.
    /// </summary>
    [Fact]
    [Trait("Category", "DapperUser")]
    public void QueryUserShouldReturnCorrectData()
    {
        var db = _dbFactory();
        var table = CreateUserTable(db, "Users");
        var user = NewUser();
        AddUserRow(table, user);

        using var connection = _connectionFactory(db);
        var result = connection.Query<DapperUserContractModel>(
            "SELECT * FROM Users WHERE Id = @Id",
            new { user.Id }).FirstOrDefault();

        Assert.NotNull(result);
        Assert.Equal(user.Id, result!.Id);
        Assert.Equal(user.Name, result.Name);
        Assert.Equal(user.Email, result.Email);
        Assert.Equal(user.CreatedDate, result.CreatedDate);
        Assert.Equal(user.UpdatedData, result.UpdatedData);
        Assert.Equal(user.TestGuid, result.TestGuid);
        Assert.Equal(user.TestGuidNull, result.TestGuidNull);
    }

    /// <summary>
    /// EN: Verifies updating a user modifies the stored row with the new values.
    /// PT: Verifica se atualizar um usuario modifica a linha armazenada com os novos valores.
    /// </summary>
    [Fact]
    [Trait("Category", "DapperUser")]
    public void UpdateUserShouldModifyUserInTable()
    {
        var db = _dbFactory();
        var table = CreateUserTable(db, "Users");
        var user = NewUser();
        AddUserRow(table, user);

        using var connection = _connectionFactory(db);
        connection.Open();

        var updatedUser = new DapperUserContractModel
        {
            Id = 1,
            Name = "Jane Doe",
            Email = "jane.doe@example.com",
            CreatedDate = user.CreatedDate,
            UpdatedData = DateTime.Now,
            TestGuid = user.TestGuid,
            TestGuidNull = Guid.NewGuid()
        };

        var rowsAffected = connection.Execute(
            "UPDATE Users SET Name = @Name, Email = @Email, UpdatedData = @UpdatedData, TestGuidNull = @TestGuidNull WHERE Id = @Id",
            updatedUser);

        Assert.Equal(1, rowsAffected);
        Assert.Single(table);
        AssertUserRow(table[0], updatedUser);
    }

    /// <summary>
    /// EN: Verifies deleting a user removes the corresponding row from the table.
    /// PT: Verifica se excluir um usuario remove a linha correspondente da tabela.
    /// </summary>
    [Fact]
    [Trait("Category", "DapperUser")]
    public void DeleteUserShouldRemoveUserFromTable()
    {
        var db = _dbFactory();
        var table = CreateUserTable(db, "Users");
        var user = NewUser();
        AddUserRow(table, user);

        using var connection = _connectionFactory(db);
        connection.Open();

        var rowsAffected = connection.Execute("DELETE FROM Users WHERE Id = @Id", new { user.Id });

        Assert.Equal(1, rowsAffected);
        Assert.Empty(table);
    }

    /// <summary>
    /// EN: Verifies multi-result user queries return both expected user result sets.
    /// PT: Verifica se consultas de usuario com multiplos resultados retornam os dois conjuntos esperados.
    /// </summary>
    [Fact]
    [Trait("Category", "DapperUser")]
    public void QueryMultipleShouldReturnMultipleUserResultSets()
    {
        var db = _dbFactory();
        var table1 = CreateUserTable(db, "Users1");
        AddUserRow(table1, NewUser());

        var table2 = CreateUserTable(db, "Users2");
        AddUserRow(table2, new DapperUserContractModel
        {
            Id = 2,
            Name = "Jane Doe",
            Email = "jane.doe@example.com",
            CreatedDate = DateTime.Now,
            UpdatedData = null,
            TestGuid = Guid.NewGuid(),
            TestGuidNull = null
        });

        using var connection = _connectionFactory(db);
        using var command = _commandFactory(connection);
        command.CommandText = _queryMultipleUsersSql;

        var resultSets = new List<IEnumerable<DapperUserContractModel>>();
        using (var reader = command.ExecuteReader())
        {
            do
            {
                var resultSet = reader.Parse<DapperUserContractModel>().ToList();
                resultSets.Add(resultSet);
            } while (reader.NextResult());
        }

        Assert.Equal(2, resultSets.Count);

        var users1 = resultSets[0].ToList();
        Assert.Single(users1);
        Assert.Equal(1, users1[0].Id);
        Assert.Equal("John Doe", users1[0].Name);
        Assert.Equal("john.doe@example.com", users1[0].Email);

        var users2 = resultSets[1].ToList();
        Assert.Single(users2);
        Assert.Equal(2, users2[0].Id);
        Assert.Equal("Jane Doe", users2[0].Name);
        Assert.Equal("jane.doe@example.com", users2[0].Email);
    }

    /// <summary>
    /// EN: Verifies joined user queries populate the related tenant data correctly.
    /// PT: Verifica se consultas de usuario com join preenchem corretamente os dados relacionados de tenant.
    /// </summary>
    [Fact]
    [Trait("Category", "DapperUserTests2")]
    public void QueryWithJoinShouldReturnJoinedData()
    {
        var db = _dbFactory();
        var userTable = CreateUserTable(db, "user");
        AddUserRow(userTable, NewUser());

        var userTenantTable = db.AddTable("usertenant");
        userTenantTable.AddColumn("UserId", DbType.Int32, false);
        userTenantTable.AddColumn("TenantId", DbType.Int32, false);
        userTenantTable.Add(new Dictionary<int, object?> { { 0, 1 }, { 1, 1 } });

        using var connection = _connectionFactory(db);
        using var command = _commandFactory(connection);
        command.CommandText = _queryWithJoinSql;

        var result = connection.Query<DapperUserJoinContractModel, int, DapperUserJoinContractModel>(
            _queryWithJoinSql,
            static (user, tenantId) =>
            {
                user.Tenants = [tenantId];
                return user;
            },
            new { Id = 1 },
            splitOn: "TenantId").FirstOrDefault();

        Assert.NotNull(result);
        Assert.Equal(1, result!.Id);
        Assert.Equal("John Doe", result.Name);
        Assert.Equal("john.doe@example.com", result.Email);
        Assert.Single(result.Tenants);
        Assert.Equal(1, result.Tenants[0]);
    }

    private static ITableMock CreateUserTable(DbMock db, string tableName)
    {
        var table = db.AddTable(tableName);
        table.AddColumn("Id", DbType.Int32, false);
        table.AddColumn("Name", DbType.String, false);
        table.AddColumn("Email", DbType.String, false);
        table.AddColumn("CreatedDate", DbType.DateTime, false);
        table.AddColumn("UpdatedData", DbType.DateTime, true);
        table.AddColumn("TestGuid", DbType.Guid, false);
        table.AddColumn("TestGuidNull", DbType.Guid, true);
        return table;
    }

    private static DapperUserContractModel NewUser() => new()
    {
        Id = 1,
        Name = "John Doe",
        Email = "john.doe@example.com",
        CreatedDate = DateTime.Now,
        UpdatedData = null,
        TestGuid = Guid.NewGuid(),
        TestGuidNull = null
    };

    private static void AddUserRow(ITableMock table, DapperUserContractModel user)
    {
        table.Add(new Dictionary<int, object?>
        {
            { 0, user.Id },
            { 1, user.Name },
            { 2, user.Email },
            { 3, user.CreatedDate },
            { 4, user.UpdatedData },
            { 5, user.TestGuid },
            { 6, user.TestGuidNull }
        });
    }

    private static void AssertUserRow(IReadOnlyDictionary<int, object?> row, DapperUserContractModel user)
    {
        Assert.Equal(user.Id, row[0]);
        Assert.Equal(user.Name, row[1]);
        Assert.Equal(user.Email, row[2]);
        Assert.Equal(user.CreatedDate, row[3]);
        Assert.Equal(user.UpdatedData, row[4]);
        Assert.Equal(user.TestGuid, row[5]);
        Assert.Equal(user.TestGuidNull, row[6]);
    }
}

/// <summary>
/// EN: Shared stored procedure execution contract tests for Dapper-facing provider mocks.
/// PT: Testes compartilhados de contrato para execução de stored procedure em mocks voltados ao Dapper.
/// </summary>
public abstract class StoredProcedureExecutionTestsBase<TConnection, TCommand, TParameter, TException>(
    ITestOutputHelper helper
) : XUnitTestBase(helper)
    where TConnection : DbConnectionMockBase
    where TCommand : DbCommand
    where TParameter : DbParameter
    where TException : Exception
{
    /// <summary>
    /// EN: Creates and opens the provider-specific connection used by stored procedure tests.
    /// PT: Cria e abre a conexao especifica do provedor usada pelos testes de stored procedure.
    /// </summary>
    protected abstract TConnection CreateOpenConnection();

    /// <summary>
    /// EN: Creates a command configured to execute the specified stored procedure.
    /// PT: Cria um comando configurado para executar a stored procedure especificada.
    /// </summary>
    protected abstract TCommand CreateStoredProcedureCommand(TConnection connection, string procedureName);

    /// <summary>
    /// EN: Creates a text command for the supplied SQL text against the provided connection.
    /// PT: Cria um comando de texto para o SQL informado na conexao fornecida.
    /// </summary>
    protected abstract TCommand CreateTextCommand(TConnection connection, string commandText);

    /// <summary>
    /// EN: Creates a provider-specific parameter with the supplied name, value, type, and direction.
    /// PT: Cria um parametro especifico do provedor com nome, valor, tipo e direcao informados.
    /// </summary>
    protected abstract TParameter CreateParameter(string name, object? value, DbType dbType, ParameterDirection direction = ParameterDirection.Input);

    /// <summary>
    /// EN: Extracts the provider-specific error code from the thrown exception.
    /// PT: Extrai o codigo de erro especifico do provedor da excecao lancada.
    /// </summary>
    protected abstract int GetErrorCode(TException exception);

    /// <summary>
    /// EN: Verifies stored procedure execution succeeds when all required inputs are supplied.
    /// PT: Verifica se a execucao da stored procedure tem sucesso quando todos os parametros obrigatorios sao informados.
    /// </summary>
    [Fact]
    [Trait("Category", "StoredProcedureExecution")]
    public void ExecuteNonQuery_StoredProcedure_ShouldValidateRequiredInputs()
    {
        using var c = CreateOpenConnection();
        c.AddProdecure("sp_add_user", new ProcedureDef(
            RequiredIn:
            [
                new ProcParam("p_name", DbType.String),
                new ProcParam("p_email", DbType.String),
            ],
            OptionalIn: [],
            OutParams: [],
            ReturnParam: null));

        using var cmd = CreateStoredProcedureCommand(c, "sp_add_user");
        cmd.Parameters.Add(CreateParameter("p_name", "John", DbType.String));
        cmd.Parameters.Add(CreateParameter("p_email", "john@x.com", DbType.String));

        var affected = cmd.ExecuteNonQuery();

        Assert.Equal(0, affected);
    }

    /// <summary>
    /// EN: Verifies stored procedure execution throws when a required input parameter is missing.
    /// PT: Verifica se a execucao da stored procedure lanca erro quando falta um parametro de entrada obrigatorio.
    /// </summary>
    [Fact]
    [Trait("Category", "StoredProcedureExecution")]
    public void ExecuteNonQuery_StoredProcedure_ShouldThrow_WhenMissingRequiredInput()
    {
        using var c = CreateOpenConnection();
        c.AddProdecure("sp_add_user", new ProcedureDef(
            RequiredIn:
            [
                new ProcParam("p_name", DbType.String),
                new ProcParam("p_email", DbType.String),
            ],
            OptionalIn: [],
            OutParams: [],
            ReturnParam: null));

        using var cmd = CreateStoredProcedureCommand(c, "sp_add_user");
        cmd.Parameters.Add(CreateParameter("p_name", "John", DbType.String));

        var ex = Assert.Throws<TException>(() => cmd.ExecuteNonQuery());
        Assert.Equal(1318, GetErrorCode(ex));
    }

    /// <summary>
    /// EN: Verifies stored procedure execution throws when a required input parameter is null.
    /// PT: Verifica se a execucao da stored procedure lanca erro quando um parametro obrigatorio e nulo.
    /// </summary>
    [Fact]
    [Trait("Category", "StoredProcedureExecution")]
    public void ExecuteNonQuery_StoredProcedure_ShouldThrow_WhenRequiredInputIsNull()
    {
        using var c = CreateOpenConnection();
        c.AddProdecure("sp_add_user", new ProcedureDef(
            RequiredIn:
            [
                new ProcParam("p_name", DbType.String),
                new ProcParam("p_email", DbType.String),
            ],
            OptionalIn: [],
            OutParams: [],
            ReturnParam: null));

        using var cmd = CreateStoredProcedureCommand(c, "sp_add_user");
        cmd.Parameters.Add(CreateParameter("p_name", "John", DbType.String));
        cmd.Parameters.Add(CreateParameter("p_email", null, DbType.String));

        var ex = Assert.Throws<TException>(() => cmd.ExecuteNonQuery());
        Assert.Equal(1048, GetErrorCode(ex));
    }

    /// <summary>
    /// EN: Verifies stored procedure execution populates output parameters with default values.
    /// PT: Verifica se a execucao da stored procedure preenche parametros de saida com valores padrao.
    /// </summary>
    [Fact]
    [Trait("Category", "StoredProcedureExecution")]
    public void ExecuteNonQuery_StoredProcedure_ShouldPopulateOutParameters_DefaultValue()
    {
        using var c = CreateOpenConnection();
        c.AddProdecure("sp_create_token", new ProcedureDef(
            RequiredIn:
            [
                new ProcParam("p_userid", DbType.Int32),
            ],
            OptionalIn: [],
            OutParams:
            [
                new ProcParam("o_token", DbType.String),
                new ProcParam("o_status", DbType.Int32),
            ],
            ReturnParam: null));

        using var cmd = CreateStoredProcedureCommand(c, "sp_create_token");
        cmd.Parameters.Add(CreateParameter("p_userid", 1, DbType.Int32));
        cmd.Parameters.Add(CreateParameter("o_token", null, DbType.String, ParameterDirection.Output));
        cmd.Parameters.Add(CreateParameter("o_status", null, DbType.Int32, ParameterDirection.Output));

        cmd.ExecuteNonQuery();

        Assert.Equal(string.Empty, ((TParameter)cmd.Parameters["@o_token"]).Value);
        Assert.Equal(0, ((TParameter)cmd.Parameters["@o_status"]).Value);
    }

    /// <summary>
    /// EN: Verifies CALL syntax validates inputs and returns an empty result set when appropriate.
    /// PT: Verifica se a sintaxe CALL valida entradas e retorna um conjunto de resultados vazio quando apropriado.
    /// </summary>
    [Fact]
    [Trait("Category", "StoredProcedureExecution")]
    public void ExecuteReader_CallSyntax_ShouldValidateAndReturnEmptyResultset()
    {
        using var c = CreateOpenConnection();
        c.AddProdecure("sp_ping", new ProcedureDef(
            RequiredIn: [new ProcParam("p_id", DbType.Int32)],
            OptionalIn: [],
            OutParams: [],
            ReturnParam: null));

        using var cmd = CreateTextCommand(c, "CALL sp_ping(@p_id)");
        cmd.Parameters.Add(CreateParameter("p_id", 123, DbType.Int32));

        using var r = cmd.ExecuteReader();

        Assert.Equal(0, r.FieldCount);
        Assert.False(r.Read());
    }

    /// <summary>
    /// EN: Verifies stored procedure execution populates a return value parameter with the default zero value.
    /// PT: Verifica se a execucao da stored procedure preenche um parametro de retorno com o valor padrao zero.
    /// </summary>
    [Fact]
    [Trait("Category", "StoredProcedureExecution")]
    public void ExecuteNonQuery_StoredProcedure_ShouldPopulateReturnValueDefaultZero()
    {
        using var c = CreateOpenConnection();
        c.AddProdecure("sp_with_status", new ProcedureDef(
            RequiredIn: [new ProcParam("p_id", DbType.Int32)],
            OptionalIn: [],
            OutParams: [],
            ReturnParam: new ProcParam("ret", DbType.Int32)));

        using var command = CreateStoredProcedureCommand(c, "sp_with_status");
        command.Parameters.Add(CreateParameter("p_id", 1, DbType.Int32));
        command.Parameters.Add(CreateParameter("ret", null, DbType.Int32, ParameterDirection.ReturnValue));

        command.ExecuteNonQuery();

        Assert.Equal(0, command.Parameters["@ret"].Value);
    }

    /// <summary>
    /// EN: Verifies stored procedure execution throws when a required input parameter is incorrectly marked as output.
    /// PT: Verifica se a execucao da stored procedure lanca erro quando um parametro obrigatorio e marcado incorretamente como saida.
    /// </summary>
    [Fact]
    [Trait("Category", "StoredProcedureExecution")]
    public void ExecuteNonQuery_StoredProcedure_ShouldThrow_WhenRequiredInputDirectionIsOutput()
    {
        using var c = CreateOpenConnection();
        c.AddProdecure("sp_with_input", new ProcedureDef(
            RequiredIn: [new ProcParam("p_id", DbType.Int32)],
            OptionalIn: [],
            OutParams: [],
            ReturnParam: null));

        using var command = CreateStoredProcedureCommand(c, "sp_with_input");
        command.Parameters.Add(CreateParameter("p_id", 1, DbType.Int32, ParameterDirection.Output));

        var exception = Assert.Throws<TException>(() => command.ExecuteNonQuery());
        Assert.Equal(1414, GetErrorCode(exception));
    }

    /// <summary>
    /// EN: Verifies Dapper Execute works when the command type is StoredProcedure.
    /// PT: Verifica se o Dapper Execute funciona quando o tipo de comando e StoredProcedure.
    /// </summary>
    [Fact]
    [Trait("Category", "StoredProcedureExecution")]
    public void DapperExecute_CommandTypeStoredProcedure_ShouldWork()
    {
        using var c = CreateOpenConnection();
        c.AddProdecure("sp_add_user", new ProcedureDef(
            RequiredIn:
            [
                new ProcParam("p_name", DbType.String),
                new ProcParam("p_email", DbType.String),
            ],
            OptionalIn: [],
            OutParams: [],
            ReturnParam: null));

        var affected = c.Execute(
            "sp_add_user",
            new { p_name = "John", p_email = "john@x.com" },
            commandType: CommandType.StoredProcedure);

        Assert.Equal(0, affected);
    }

    /// <summary>
    /// EN: Verifies Dapper Execute throws for StoredProcedure calls when a required parameter is missing.
    /// PT: Verifica se o Dapper Execute lanca erro em chamadas StoredProcedure quando falta um parametro obrigatorio.
    /// </summary>
    [Fact]
    [Trait("Category", "StoredProcedureExecution")]
    public void DapperExecute_CommandTypeStoredProcedure_ShouldThrow_OnMissingParam()
    {
        using var c = CreateOpenConnection();
        c.AddProdecure("sp_add_user", new ProcedureDef(
            RequiredIn:
            [
                new ProcParam("p_name", DbType.String),
                new ProcParam("p_email", DbType.String),
            ],
            OptionalIn: [],
            OutParams: [],
            ReturnParam: null));

        var ex = Assert.Throws<TException>(() =>
            c.Execute(
                "sp_add_user",
                new { p_name = "John" },
                commandType: CommandType.StoredProcedure));

        Assert.Equal(1318, GetErrorCode(ex));
    }
}


/// <summary>
/// EN: Shared query-executor extras contract for Dapper providers with LINQ translation hooks.
/// PT: Contrato compartilhado de extras do executor de query para provedores Dapper com hooks de tradução LINQ.
/// </summary>
public abstract class QueryExecutorExtrasTestsBase<TDb, TConnection, TCommand, TQueryProvider, TTranslator>(
    ITestOutputHelper helper
) : XUnitTestBase(helper)
    where TDb : DbMock
    where TConnection : DbConnectionMockBase
    where TCommand : DbCommand
{
    /// <summary>
    /// EN: Creates a provider-specific connection for the supplied in-memory database.
    /// PT: Cria uma conexao especifica do provedor para o banco em memoria informado.
    /// </summary>
    protected abstract TConnection CreateConnection(TDb db);

    /// <summary>
    /// EN: Creates a provider-specific command initialized with the supplied SQL text.
    /// PT: Cria um comando especifico do provedor inicializado com o texto SQL informado.
    /// </summary>
    protected abstract TCommand CreateCommand(TConnection connection, string sql);

    /// <summary>
    /// EN: Gets the provider-specific SQL batch used to validate pagination behavior.
    /// PT: Obtem o batch SQL especifico do provedor usado para validar o comportamento de paginacao.
    /// </summary>
    protected abstract string PaginationBatchSql { get; }

    /// <summary>
    /// EN: Extracts the translator instance from the provider-specific query provider.
    /// PT: Extrai a instancia do tradutor a partir do query provider especifico do provedor.
    /// </summary>
    protected abstract object GetTranslatorFromProvider(IQueryProvider provider);

    /// <summary>
    /// EN: Translates the supplied LINQ expression into provider-specific SQL text.
    /// PT: Traduz a expressao LINQ informada para texto SQL especifico do provedor.
    /// </summary>
    protected abstract string TranslateSql(object translator, Expression expression);

    /// <summary>
    /// EN: Creates a queryable source used to validate provider translation behavior.
    /// PT: Cria uma fonte queryable usada para validar o comportamento de traducao do provedor.
    /// </summary>
    protected abstract IQueryable<QueryExecutorFoo> CreateQueryable(TConnection connection);

    /// <summary>
    /// EN: Validates provider-specific pagination fragments emitted for LINQ Skip/Take translation.
    /// PT: Valida os fragmentos de paginacao especificos do provedor gerados na traducao LINQ de Skip/Take.
    /// </summary>
    protected virtual void AssertPaginationSql(string sql)
    {
        Assert.Contains("OFFSET 2", sql, StringComparison.OrdinalIgnoreCase);
        Assert.True(
            sql.Contains("LIMIT 3", StringComparison.OrdinalIgnoreCase)
            || Regex.IsMatch(sql, @"FETCH\s+NEXT\s+3\s+ROWS\s+ONLY", RegexOptions.IgnoreCase | RegexOptions.CultureInvariant),
            $"Expected LIMIT/FETCH pagination fragment in SQL: {sql}");
    }

    /// <summary>
    /// EN: Creates the in-memory database used by query executor tests.
    /// PT: Cria o banco em memoria usado pelos testes do executor de consultas.
    /// </summary>
    protected virtual TDb CreateDb() => DbMockFactory.Create<TDb>();

    /// <summary>
    /// EN: Verifies grouping and aggregate functions compute the expected values.
    /// PT: Verifica se agrupamentos e funcoes de agregacao calculam os valores esperados.
    /// </summary>
    [Fact]
    [Trait("Category", "Query")]
    public void GroupByAndAggregationsShouldComputeCorrectly()
    {
        var db = SeedDb();
        using var c = CreateConnection(db);
        const string sql = """
SELECT grp
    , COUNT(tx.id) AS C
    , SUM(amt) AS S
    , AVG(amt) AS A
    , MIN(amt) AS MI
    , MAX(amt) AS MA 
 FROM tx 
GROUP BY grp
""";
        using var cmd = CreateCommand(c, sql);

        using var reader = cmd.ExecuteReader();
        var rows = reader.Parse<dynamic>().ToList();

        Assert.Equal(2, rows.Count);
        var a = rows.Single(r => r.grp == "A");
        Assert.Equal(2L, a.C);
        Assert.Equal(40m, a.S);
        Assert.Equal(20m, a.A);
        Assert.Equal(10m, a.MI);
        Assert.Equal(30m, a.MA);
    }

    /// <summary>
    /// EN: Verifies order, limit, and offset pagination return the expected page of rows.
    /// PT: Verifica se ordenacao, limite e offset de paginacao retornam a pagina esperada de linhas.
    /// </summary>
    [Fact]
    [Trait("Category", "Query")]
    public void OrderByLimitOffsetShouldPageCorrectly()
    {
        var db = CreateDb();
        var table = db.AddTable("t");
        table.AddColumn("id", DbType.Int32, false);
        table.AddColumn("iddesc", DbType.Int32, false);
        for (int i = 1; i <= 5; i++)
            table.Add(new Dictionary<int, object?> { { 0, i }, { 1, 4 - i } });

        using var c = CreateConnection(db);
        using var cmd = CreateCommand(c, PaginationBatchSql);

        using var reader = cmd.ExecuteReader();
        var ids = reader.Parse<dynamic>().Select(r => (int)r.id).ToList();

        Assert.Equal([4, 3], ids);
        reader.NextResult();
        var ids2 = reader.Parse<dynamic>().Select(r => (int)r.id).ToList();
        Assert.Equal([4, 3], ids2);
    }

    /// <summary>
    /// EN: Verifies basic where/order-by translation produces the expected SQL fragments.
    /// PT: Verifica se a traducao basica de where/order-by produz os fragmentos SQL esperados.
    /// </summary>
    [Fact]
    [Trait("Category", "Query")]
    public void TranslateBasicWhereAndOrderBySqlCorrect()
    {
        using var cnn = CreateConnection(CreateDb());
        var q = CreateQueryable(cnn)
                   .Where(f => f.X > 5 && f.Y == "abc")
                   .OrderBy(f => f.Y)
                   .Skip(2)
                   .Take(3);

        var translator = GetTranslatorFromProvider(q.Provider);
        var sql = TranslateSql(translator, q.Expression);

        Assert.StartsWith("SELECT * FROM QueryExecutorFoo WHERE", sql, StringComparison.OrdinalIgnoreCase);
        AssertPaginationSql(sql);
    }

    /// <summary>
    /// EN: Seeds the in-memory database with rows used by query executor tests.
    /// PT: Popula o banco em memoria com linhas usadas pelos testes do executor de consultas.
    /// </summary>
    protected virtual TDb SeedDb()
    {
        var db = CreateDb();
        var t = db.AddTable("tx");
        t.AddColumn("id", DbType.Int32, false);
        t.AddColumn("grp", DbType.String, false);
        t.AddColumn("amt", DbType.Decimal, false, decimalPlaces: 2);
        t.Add(new Dictionary<int, object?> { { 0, 1 }, { 1, "A" }, { 2, 10m } });
        t.Add(new Dictionary<int, object?> { { 0, 2 }, { 1, "B" }, { 2, 20m } });
        t.Add(new Dictionary<int, object?> { { 0, 3 }, { 1, "A" }, { 2, 30m } });
        return db;
    }

#pragma warning disable CA1812
    /// <summary>
    /// EN: Lightweight query model used by query executor translation and projection tests.
    /// PT: Modelo leve de consulta usado pelos testes de traducao e projecao do executor de consultas.
    /// </summary>
    protected sealed class QueryExecutorFoo
    {
        /// <summary>
        /// EN: Gets or sets the numeric value used in translation assertions.
        /// PT: Obtem ou define o valor numerico usado nas assercoes de traducao.
        /// </summary>
        public int X { get; set; }
        /// <summary>
        /// EN: Gets or sets the text value used in translation assertions.
        /// PT: Obtem ou define o valor textual usado nas assercoes de traducao.
        /// </summary>
        public string Y { get; set; } = string.Empty;
    }
#pragma warning restore CA1812
}

/// <summary>
/// EN: Shared Dapper join assertions for providers with identical JOIN semantics in the mock layer.
/// PT: Asserções compartilhadas de JOIN via Dapper para providers com semântica idêntica na camada mock.
/// </summary>
public abstract class DapperJoinTestsBase<TDb, TConnection>(
    ITestOutputHelper helper
) : XUnitTestBase(helper)
    where TDb : DbMock
    where TConnection : DbConnection, IDbConnection
{
    /// <summary>
    /// EN: Creates a provider-specific connection for the supplied database used in join tests.
    /// PT: Cria uma conexao especifica do provedor para o banco informado usado nos testes de join.
    /// </summary>
    protected abstract TConnection CreateConnection(TDb db);

    /// <summary>
    /// EN: Creates the in-memory database used by join tests.
    /// PT: Cria o banco em memoria usado pelos testes de join.
    /// </summary>
    protected virtual TDb CreateDb() => DbMockFactory.Create<TDb>();

    /// <summary>
    /// EN: Verifies a left join keeps all rows from the left table.
    /// PT: Verifica se um left join preserva todas as linhas da tabela da esquerda.
    /// </summary>
    protected void LeftJoin_ShouldKeepAllLeftRows()
    {
        using var connection = CreateOpenConnection();
        const string sql = """
                  SELECT u.id, o.id AS orderId
                  FROM users u
                  LEFT JOIN orders o ON u.id = o.userId
                  ORDER BY u.id
                  """;

        var rows = connection.Query<dynamic>(sql).ToList();
        Assert.Contains(rows, r => (int)r.id == 2);
    }

    /// <summary>
    /// EN: Verifies a right join keeps all rows from the right table.
    /// PT: Verifica se um right join preserva todas as linhas da tabela da direita.
    /// </summary>
    protected void RightJoin_ShouldKeepAllRightRows()
    {
        using var connection = CreateOpenConnection();
        const string sql = """
                  SELECT u.id, o.id AS orderId
                  FROM users u
                  RIGHT JOIN orders o ON u.id = o.userId
                  """;

        var rows = connection.Query<dynamic>(sql).ToList();
        Assert.Contains(rows, r => r.id is null && (int)r.orderId == 12);
    }

    /// <summary>
    /// EN: Verifies join predicates with multiple AND conditions are evaluated correctly.
    /// PT: Verifica se predicados de join com multiplas condicoes AND sao avaliados corretamente.
    /// </summary>
    protected void Join_ON_WithMultipleConditions_AND_ShouldWork()
    {
        using var connection = CreateOpenConnection();
        const string sql = """
                  SELECT u.id, o.id AS orderId
                  FROM users u
                  INNER JOIN orders o ON u.id = o.userId AND o.status = 'paid'
                  """;

        var rows = connection.Query<dynamic>(sql).ToList();
        Assert.Single(rows);
        Assert.Equal(10, (int)rows[0].orderId);
    }

    private TConnection CreateOpenConnection()
    {
        var db = CreateDb();
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
        orders.Add(new Dictionary<int, object?> { [0] = 12, [1] = 99, [2] = 7m, [3] = "paid" });

        var connection = CreateConnection(db);
        connection.Open();
        return connection;
    }
}

/// <summary>
/// EN: Shared Dapper transaction/lifecycle assertions for providers that expose the same connection API.
/// PT: Asserções compartilhadas de transação/ciclo de vida via Dapper para providers que expõem a mesma API de conexão.
/// </summary>
public abstract class DapperTransactionTestsBase<TDb, TConnection>(
    ITestOutputHelper helper
) : XUnitTestBase(helper)
    where TDb : DbMock
    where TConnection : DbConnectionMockBase
{
    /// <summary>
    /// EN: Creates a provider-specific connection for the supplied database used in transaction tests.
    /// PT: Cria uma conexao especifica do provedor para o banco informado usado nos testes de transacao.
    /// </summary>
    protected abstract TConnection CreateConnection(TDb db);

    /// <summary>
    /// EN: Creates the in-memory database used by transaction tests.
    /// PT: Cria o banco em memoria usado pelos testes de transacao.
    /// </summary>
    protected virtual TDb CreateDb() => DbMockFactory.Create<TDb>();

    /// <summary>
    /// EN: Verifies committing a transaction persists inserted data.
    /// PT: Verifica se confirmar uma transacao persiste os dados inseridos.
    /// </summary>
    protected void TransactionCommitShouldPersistData()
    {
        var db = CreateDb();
        var table = db.AddTable("Users");
        table.AddColumn("Id", DbType.Int32, false);
        table.AddColumn("Name", DbType.String, false);
        table.AddColumn("Email", DbType.String, false);

        using var connection = CreateConnection(db);
        connection.Open();
        using var transaction = connection.BeginTransaction();

        var user = new TransactionUser { Id = 1, Name = "John Doe", Email = "john.doe@example.com" };

        connection.Execute("INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)", user, transaction);
        connection.CommitTransaction();

        Assert.Single(table);
        var insertedRow = table[0];
        Assert.Equal(user.Id, insertedRow[0]);
        Assert.Equal(user.Name, insertedRow[1]);
        Assert.Equal(user.Email, insertedRow[2]);
    }

    /// <summary>
    /// EN: Verifies rolling back a transaction prevents inserted data from persisting.
    /// PT: Verifica se reverter uma transacao impede que os dados inseridos sejam persistidos.
    /// </summary>
    protected void TransactionRollbackShouldNotPersistData()
    {
        var db = CreateDb();
        var table = db.AddTable("Users");
        table.AddColumn("Id", DbType.Int32, false);
        table.AddColumn("Name", DbType.String, false);
        table.AddColumn("Email", DbType.String, false);

        using var connection = CreateConnection(db);
        connection.Open();
        using var transaction = connection.BeginTransaction();

        var user = new TransactionUser { Id = 1, Name = "John Doe", Email = "john.doe@example.com" };

        connection.Execute("INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)", user, transaction);
        connection.RollbackTransaction();

        Assert.Empty(table);
    }

    /// <summary>
    /// EN: Verifies a rollback restores connection-scoped temporary table contents.
    /// PT: Verifica se um rollback restaura o conteudo das tabelas temporarias de escopo da conexao.
    /// </summary>
    protected void TransactionRollback_ShouldRestoreConnectionTemporaryTable_Dapper()
    {
        var db = CreateDb();
        using var connection = CreateConnection(db);
        connection.Open();

        var temp = connection.AddTemporaryTable("temp_users");
        temp.AddColumn("Id", DbType.Int32, false);
        temp.AddColumn("Name", DbType.String, false);

        using var transaction = connection.BeginTransaction();
        connection.Execute(
            "INSERT INTO temp_users (Id, Name) VALUES (@Id, @Name)",
            new { Id = 1, Name = "Ana" },
            transaction);

        connection.RollbackTransaction();

        Assert.Empty(temp);
    }

    /// <summary>
    /// EN: Verifies rolling back to a savepoint restores the temporary table snapshot.
    /// PT: Verifica se reverter para um savepoint restaura o snapshot da tabela temporaria.
    /// </summary>
    protected void RollbackToSavepoint_ShouldRestoreConnectionTemporaryTableSnapshot_Dapper()
    {
        var db = CreateDb();
        using var connection = CreateConnection(db);
        connection.Open();

        var temp = connection.AddTemporaryTable("temp_users");
        temp.AddColumn("Id", DbType.Int32, false);
        temp.AddColumn("Name", DbType.String, false);

        using var transaction = connection.BeginTransaction();
        connection.Execute(
            "INSERT INTO temp_users (Id, Name) VALUES (@Id, @Name)",
            new { Id = 1, Name = "Ana" },
            transaction);

        connection.CreateSavepoint("sp_temp");
        connection.Execute(
            "INSERT INTO temp_users (Id, Name) VALUES (@Id, @Name)",
            new { Id = 2, Name = "Bob" },
            transaction);

        connection.RollbackTransaction("sp_temp");
        connection.CommitTransaction();

        Assert.Single(temp);
        Assert.Equal(1, temp[0][0]);
        Assert.Equal("Ana", temp[0][1]);
    }

    /// <summary>
    /// EN: Verifies resetting all volatile data clears rows, resets identities, and clears session temp tables.
    /// PT: Verifica se resetar todos os dados volateis limpa linhas, reinicia identidades e limpa tabelas temporarias da sessao.
    /// </summary>
    protected void ResetAllVolatileData_ShouldClearRowsAndResetIdentity_Dapper()
    {
        var db = CreateDb();
        var users = db.AddTable("users");
        users.AddColumn("id", DbType.Int32, false, identity: true);
        users.AddColumn("name", DbType.String, false);

        using var connection = CreateConnection(db);
        connection.Open();

        var temp = connection.AddTemporaryTable("temp_users");
        temp.AddColumn("id", DbType.Int32, false, identity: true);
        temp.AddColumn("name", DbType.String, false);

        users.Add(new Dictionary<int, object?> { [1] = "Ana" });
        users.Add(new Dictionary<int, object?> { [1] = "Bia" });
        temp.Add(new Dictionary<int, object?> { [1] = "Tmp-A" });
        temp.Add(new Dictionary<int, object?> { [1] = "Tmp-B" });

        using var transaction = connection.BeginTransaction();

        connection.ResetAllVolatileData();

        Assert.False(connection.HasActiveTransaction);
        Assert.Empty(users);
        Assert.Equal(1, users.NextIdentity);
        Assert.Empty(temp);
        Assert.Equal(1, temp.NextIdentity);
        Assert.False(connection.TryGetTemporaryTable("temp_users", out var _));
    }

    /// <summary>
    /// EN: Verifies database volatile-data reset respects the global temporary table inclusion flag.
    /// PT: Verifica se o reset de dados volateis no banco respeita a flag de inclusao de tabelas temporarias globais.
    /// </summary>
    protected void ResetVolatileData_OnDb_ShouldRespectGlobalTemporaryTablesFlag_Dapper()
    {
        var db = CreateDb();
        var users = db.AddTable("users");
        users.AddColumn("id", DbType.Int32, false);
        users.AddColumn("name", DbType.String, false);
        users.Add(new Dictionary<int, object?> { [0] = 1, [1] = "Ana" });

        using var connection = CreateConnection(db);
        connection.Open();
        connection.Execute("CREATE GLOBAL TEMPORARY TABLE gtmp_users AS SELECT id, name FROM users");

        var globalTemp = connection.GetTable("gtmp_users");
        Assert.Single(globalTemp);

        db.ResetVolatileData(includeGlobalTemporaryTables: false);
        Assert.Empty(users);
        Assert.Single(globalTemp);

        db.ResetVolatileData(includeGlobalTemporaryTables: true);
        Assert.Empty(globalTemp);
        Assert.Equal(2, globalTemp.Columns.Count);
    }

    /// <summary>
    /// EN: Verifies resetting volatile data on the database preserves table definitions.
    /// PT: Verifica se resetar dados volateis no banco preserva as definicoes das tabelas.
    /// </summary>
    protected void ResetVolatileData_OnDb_ShouldKeepTableDefinitions_Dapper()
    {
        var db = CreateDb();
        var users = db.AddTable("users");
        users.AddColumn("id", DbType.Int32, false, identity: true);
        users.AddColumn("name", DbType.String, false);
        users.Add(new Dictionary<int, object?> { [1] = "Ana" });
        users.Add(new Dictionary<int, object?> { [1] = "Bia" });

        db.ResetVolatileData();

        Assert.True(db.ContainsTable("users"));
        Assert.Equal(2, users.Columns.Count);
        Assert.Empty(users);
        Assert.Equal(1, users.NextIdentity);
    }

    /// <summary>
    /// EN: Verifies database volatile-data reset does not affect connection-scoped temporary tables.
    /// PT: Verifica se o reset de dados volateis no banco nao afeta tabelas temporarias de escopo da conexao.
    /// </summary>
    protected void ResetVolatileData_OnDb_ShouldNotAffectConnectionTemporaryTables_Dapper()
    {
        var db = CreateDb();
        using var connection = CreateConnection(db);
        connection.Open();

        var temp = connection.AddTemporaryTable("temp_users");
        temp.AddColumn("id", DbType.Int32, false);
        temp.AddColumn("name", DbType.String, false);
        connection.Execute("INSERT INTO temp_users (id, name) VALUES (@id, @name)", new { id = 1, name = "Tmp-A" });

        db.ResetVolatileData(includeGlobalTemporaryTables: true);

        Assert.True(connection.TryGetTemporaryTable("temp_users", out var tempAfter));
        Assert.Single(tempAfter!);
        Assert.Equal("Tmp-A", tempAfter![0][1]);
    }

    /// <summary>
    /// EN: Verifies resetting all volatile data clears rows from global temporary tables.
    /// PT: Verifica se resetar todos os dados volateis limpa as linhas das tabelas temporarias globais.
    /// </summary>
    protected void ResetAllVolatileData_ShouldClearGlobalTemporaryTableRows_Dapper()
    {
        var db = CreateDb();
        var users = db.AddTable("users");
        users.AddColumn("id", DbType.Int32, false);
        users.AddColumn("name", DbType.String, false);
        users.Add(new Dictionary<int, object?> { [0] = 1, [1] = "Ana" });

        using var connection = CreateConnection(db);
        connection.Open();
        connection.Execute("CREATE GLOBAL TEMPORARY TABLE gtmp_users AS SELECT id, name FROM users");

        var globalTemp = connection.GetTable("gtmp_users");
        Assert.Single(globalTemp);

        connection.ResetAllVolatileData();

        Assert.Empty(globalTemp);
        Assert.Equal(2, globalTemp.Columns.Count);
    }

    /// <summary>
    /// EN: Verifies resetting all volatile data invalidates existing savepoints.
    /// PT: Verifica se resetar todos os dados volateis invalida os savepoints existentes.
    /// </summary>
    protected void ResetAllVolatileData_ShouldInvalidateSavepoints_Dapper()
    {
        var db = CreateDb();
        using var connection = CreateConnection(db);
        connection.Open();

        using var transaction = connection.BeginTransaction();
        connection.CreateSavepoint("sp_reset");

        connection.ResetAllVolatileData();

        Assert.False(connection.HasActiveTransaction);
        var ex = Assert.Throws<InvalidOperationException>(() => connection.RollbackTransaction("sp_reset"));
        Assert.Contains("No active transaction", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Verifies connection-scoped temporary tables remain isolated between different connections.
    /// PT: Verifica se tabelas temporarias de escopo da conexao permanecem isoladas entre conexoes diferentes.
    /// </summary>
    protected void ConnectionTemporaryTables_ShouldBeIsolatedBetweenConnections_Dapper()
    {
        var db = CreateDb();
        using var connA = CreateConnection(db);
        using var connB = CreateConnection(db);
        connA.Open();
        connB.Open();

        var tempA = connA.AddTemporaryTable("temp_users");
        tempA.AddColumn("id", DbType.Int32, false);
        tempA.AddColumn("name", DbType.String, false);

        var tempB = connB.AddTemporaryTable("temp_users");
        tempB.AddColumn("id", DbType.Int32, false);
        tempB.AddColumn("name", DbType.String, false);

        connA.Execute("INSERT INTO temp_users (id, name) VALUES (@id, @name)", new { id = 1, name = "Ana" });
        connB.Execute("INSERT INTO temp_users (id, name) VALUES (@id, @name)", new { id = 2, name = "Bob" });

        Assert.Single(tempA);
        Assert.Single(tempB);
        Assert.Equal("Ana", tempA[0][1]);
        Assert.Equal("Bob", tempB[0][1]);
    }

    /// <summary>
    /// EN: Verifies closing a connection clears session-scoped transactional and temporary state.
    /// PT: Verifica se fechar uma conexao limpa o estado transacional e temporario de escopo da sessao.
    /// </summary>
    protected void Close_ShouldClearConnectionSessionState_Dapper()
    {
        var db = CreateDb();
        using var connection = CreateConnection(db);
        connection.Open();

        var temp = connection.AddTemporaryTable("temp_users");
        temp.AddColumn("id", DbType.Int32, false);
        temp.AddColumn("name", DbType.String, false);
        connection.Execute("INSERT INTO temp_users (id, name) VALUES (@id, @name)", new { id = 1, name = "Ana" });

        using var tx = connection.BeginTransaction(IsolationLevel.Serializable);
        connection.CreateSavepoint("sp_close");

        connection.Close();

        Assert.Equal(ConnectionState.Closed, connection.State);
        Assert.False(connection.HasActiveTransaction);
        Assert.Equal(IsolationLevel.Unspecified, connection.CurrentIsolationLevel);
        Assert.False(connection.TryGetTemporaryTable("temp_users", out var _));

        var ex = Assert.Throws<InvalidOperationException>(() => connection.RollbackTransaction("sp_close"));
        Assert.Contains("No active transaction", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Verifies closing a connection preserves permanent tables and shared global temporary state.
    /// PT: Verifica se fechar uma conexao preserva tabelas permanentes e o estado compartilhado de tabelas temporarias globais.
    /// </summary>
    protected void Close_ShouldPreservePermanentAndGlobalSharedState_Dapper()
    {
        var db = CreateDb();
        var users = db.AddTable("users");
        users.AddColumn("id", DbType.Int32, false);
        users.AddColumn("name", DbType.String, false);
        users.Add(new Dictionary<int, object?> { [0] = 1, [1] = "Ana" });

        using var connA = CreateConnection(db);
        using var connB = CreateConnection(db);
        connA.Open();
        connB.Open();

        connA.Execute("CREATE GLOBAL TEMPORARY TABLE gtmp_users AS SELECT id, name FROM users");

        var tempA = connA.AddTemporaryTable("temp_users");
        tempA.AddColumn("id", DbType.Int32, false);
        tempA.AddColumn("name", DbType.String, false);
        connA.Execute("INSERT INTO temp_users (id, name) VALUES (@id, @name)", new { id = 2, name = "Tmp-A" });

        connA.Close();

        Assert.Single(users);
        var globalTempFromConnB = connB.GetTable("gtmp_users");
        Assert.Single(globalTempFromConnB);
        Assert.False(connA.TryGetTemporaryTable("temp_users", out var _));
    }

    /// <summary>
    /// EN: Verifies reopening after close starts a fresh session while preserving shared database state.
    /// PT: Verifica se reabrir apos fechar inicia uma nova sessao preservando o estado compartilhado do banco.
    /// </summary>
    protected void OpenAfterClose_ShouldStartFreshSessionPreservingSharedState_Dapper()
    {
        var db = CreateDb();
        var users = db.AddTable("users");
        users.AddColumn("id", DbType.Int32, false);
        users.AddColumn("name", DbType.String, false);
        users.Add(new Dictionary<int, object?> { [0] = 1, [1] = "Ana" });

        using var connection = CreateConnection(db);
        connection.Open();

        var temp = connection.AddTemporaryTable("temp_users");
        temp.AddColumn("id", DbType.Int32, false);
        temp.AddColumn("name", DbType.String, false);
        connection.Execute("INSERT INTO temp_users (id, name) VALUES (@id, @name)", new { id = 2, name = "Tmp-A" });

        connection.Close();
        connection.Open();

        Assert.Equal(ConnectionState.Open, connection.State);
        Assert.False(connection.HasActiveTransaction);
        Assert.False(connection.TryGetTemporaryTable("temp_users", out var _));
        Assert.Single(users);

        var tempNew = connection.AddTemporaryTable("temp_users");
        tempNew.AddColumn("id", DbType.Int32, false);
        tempNew.AddColumn("name", DbType.String, false);
        Assert.Empty(tempNew);
    }

    /// <summary>
    /// EN: Lightweight user model used by transaction persistence and rollback tests.
    /// PT: Modelo leve de usuario usado pelos testes de persistencia e rollback de transacoes.
    /// </summary>
    protected sealed class TransactionUser
    {
        /// <summary>
        /// EN: Gets or sets the transaction test user identifier.
        /// PT: Obtem ou define o identificador do usuario de teste de transacao.
        /// </summary>
        public int Id { get; set; }
        /// <summary>
        /// EN: Gets or sets the transaction test user name.
        /// PT: Obtem ou define o nome do usuario de teste de transacao.
        /// </summary>
        public required string Name { get; set; }
        /// <summary>
        /// EN: Gets or sets the optional transaction test user email.
        /// PT: Obtem ou define o email opcional do usuario de teste de transacao.
        /// </summary>
        public string? Email { get; set; }
    }
}

/// <summary>
/// EN: Shared fluent API smoke tests for providers with identical setup semantics.
/// PT: Testes smoke compartilhados da API fluente para provedores com semântica idêntica de setup.
/// </summary>
public abstract class DapperFluentTestsBase<TDb, TConnection>(
    ITestOutputHelper helper
) : XUnitTestBase(helper)
    where TDb : DbMock
    where TConnection : DbConnectionMockBase
{
    /// <summary>
    /// EN: Creates a provider-specific connection for fluent API tests.
    /// PT: Cria uma conexao especifica do provedor para testes da API fluente.
    /// </summary>
    protected abstract TConnection CreateConnection(TDb db);

    /// <summary>
    /// EN: Creates the in-memory database used by fluent API tests.
    /// PT: Cria o banco em memoria usado pelos testes da API fluente.
    /// </summary>
    protected virtual TDb CreateDb() => DbMockFactory.Create<TDb>();

    /// <summary>
    /// EN: Verifies the fluent setup supports insert, update, and delete operations with metrics tracking.
    /// PT: Verifica se o setup fluente suporta operacoes de insert, update e delete com rastreamento de metricas.
    /// </summary>
    protected void InsertUpdateDeleteFluentScenario()
    {
        using var connection = BuildConfiguredOpenConnection();

        var rows = connection.Execute(
            "INSERT INTO user (name, email, created) VALUES (@name, @email, @created)",
            new { name = "Alice", email = "alice@mail.com", created = DateTime.UtcNow });

        Assert.Equal(1, rows);
        Assert.Equal(1, connection.Metrics.Inserts);
        Assert.Single(connection.GetTable("user"));

        rows = connection.Execute(
            "UPDATE user SET name = @name WHERE id = @id",
            new { id = 1, name = "Alice Cooper" });

        Assert.Equal(1, rows);
        Assert.Equal(1, connection.Metrics.Updates);
        Assert.Equal("Alice Cooper", connection.GetTable("user")[0][1]);

        rows = connection.Execute(
            "DELETE FROM user WHERE id = @id",
            new { id = 1 });

        Assert.Equal(1, rows);
        Assert.Equal(1, connection.Metrics.Deletes);
        Assert.Empty(connection.GetTable("user"));
        Assert.Equal(1, connection.Metrics.Inserts);
        Assert.Equal(1, connection.Metrics.Updates);
        Assert.Equal(1, connection.Metrics.Deletes);
        Assert.True(connection.Metrics.Elapsed > TimeSpan.Zero);
    }

    /// <summary>
    /// EN: Verifies the fluent table-definition API creates schema and seeds rows correctly.
    /// PT: Verifica se a API fluente de definicao de tabelas cria o schema e popula linhas corretamente.
    /// </summary>
    protected void TestFluent()
    {
        using var connection = CreateConnection(CreateDb());
        connection.Open();
        connection.DefineTable("user")
           .Column<int>("id", pk: true, identity: true)
           .Column<string>("name");

        connection.GetTable("user")
           .Column<DateTime>("created");

        connection.Seed("user", null,
            [null, "Alice", DateTime.UtcNow],
            [null, "Bob", DateTime.UtcNow]);

        Assert.Equal(2, connection.GetTable("user").Count);
        var idIdx = connection.GetTable("user").Columns["id"].Index;
        Assert.Equal(0, idIdx);
        Assert.Equal(1, connection.GetTable("user")[0][idIdx]);
    }

    private TConnection BuildConfiguredOpenConnection()
    {
        var db = CreateDb();
        db.ThreadSafe = true;

        var connection = CreateConnection(db);
        connection.DefineTable("user")
           .Column<int>("id", pk: true, identity: true)
           .Column<string>("name")
           .Column<string>("email", nullable: true)
           .Column<DateTime>("created", nullable: false, identity: false);

        connection.Open();
        return connection;
    }
}

/// <summary>
/// EN: Shared extended Dapper/provider behavior tests for providers with equivalent semantics.
/// PT: Testes compartilhados de comportamento estendido Dapper/provider para provedores com semântica equivalente.
/// </summary>
public abstract class ExtendedDapperProviderTestsBase<TDb, TConnection, TException>(
    ITestOutputHelper helper
) : XUnitTestBase(helper)
    where TDb : DbMock
    where TConnection : DbConnectionMockBase
    where TException : Exception
{
    private static readonly string[] CompositeIndexColumns = ["first", "second"];

    /// <summary>
    /// EN: Creates a provider-specific connection for the supplied database in extended behavior tests.
    /// PT: Cria uma conexao especifica do provedor para o banco informado nos testes de comportamento estendido.
    /// </summary>
    protected abstract TConnection CreateConnection(TDb db);

    /// <summary>
    /// EN: Creates the in-memory database used by extended provider behavior tests.
    /// PT: Cria o banco em memoria usado pelos testes de comportamento estendido do provedor.
    /// </summary>
    protected virtual TDb CreateDb() => DbMockFactory.Create<TDb>();

    /// <summary>
    /// EN: Gets the provider-specific SQL used to validate distinct pagination behavior.
    /// PT: Obtem o SQL especifico do provedor usado para validar o comportamento de paginacao com distinct.
    /// </summary>
    protected abstract string DistinctPaginationSql { get; }

    /// <summary>
    /// EN: Verifies inserts into identity columns assign generated values when no identity is specified.
    /// PT: Verifica se insercoes em colunas identity atribuem valores gerados quando nenhuma identidade e informada.
    /// </summary>
    protected void InsertAutoIncrementShouldAssignIdentityWhenNotSpecified()
    {
        var db = CreateDb();
        var table = db.AddTable("users");
        table.AddColumn("id", DbType.Int32, false, identity: true);
        table.AddColumn("name", DbType.String, false);
        using var connection = OpenConnection(db);

        var rows1 = connection.Execute("INSERT INTO users (name) VALUES (@name)", new { name = "Alice" });
        Assert.Equal(1, rows1);
        Assert.Single(table);
        Assert.Equal(1, table[0][0]);
        Assert.Equal("Alice", table[0][1]);

        var rows2 = connection.Execute("INSERT INTO users (name) VALUES (@name)", new { name = "Bob" });
        Assert.Equal(1, rows2);
        Assert.Equal(2, table.Count);
        Assert.Equal(2, table[1][0]);
        Assert.Equal("Bob", table[1][1]);
    }

    /// <summary>
    /// EN: Verifies explicit identity values are accepted only when the scenario enables identity override.
    /// PT: Verifica se valores explícitos de identity são aceitos apenas quando o cenário habilita a sobrescrita de identity.
    /// </summary>
    protected void InsertAutoIncrementShouldRespectExplicitIdentityWhenEnabled()
    {
        var db = CreateDb();
        var table = db.AddTable("users");
        table.AddColumn("id", DbType.Int32, false, identity: true);
        table.AddColumn("name", DbType.String, false);
        using var connection = OpenConnection(db);

        connection.IdentityOf("users", nextIdentity: 1, allowInsertOverride: true);

        var rows1 = connection.Execute("INSERT INTO users (id, name) VALUES (@id, @name)", new { id = 42, name = "Alice" });
        Assert.Equal(1, rows1);
        Assert.Single(table);
        Assert.Equal(42, table[0][0]);
        Assert.Equal("Alice", table[0][1]);
        Assert.Equal(43, table.NextIdentity);

        var rows2 = connection.Execute("INSERT INTO users (name) VALUES (@name)", new { name = "Bob" });
        Assert.Equal(1, rows2);
        Assert.Equal(2, table.Count);
        Assert.Equal(43, table[1][0]);
        Assert.Equal("Bob", table[1][1]);
        Assert.Equal(44, table.NextIdentity);
    }

    /// <summary>
    /// EN: Verifies inserting null into a nullable column succeeds.
    /// PT: Verifica se inserir null em uma coluna anulavel funciona corretamente.
    /// </summary>
    protected void InsertNullIntoNullableColumnShouldSucceed()
    {
        var db = CreateDb();
        var table = db.AddTable("data");
        table.AddColumn("id", DbType.Int32, false);
        table.AddColumn("info", DbType.String, true);
        using var connection = OpenConnection(db);

        var rows = connection.Execute("INSERT INTO data (id, info) VALUES (@id, @info)", new { id = 1, info = (string?)null });
        Assert.Equal(1, rows);
        Assert.Null(table[0][1]);
    }

    /// <summary>
    /// EN: Verifies inserting null into a non-nullable column throws the expected provider exception.
    /// PT: Verifica se inserir null em uma coluna nao anulavel lanca a excecao esperada do provedor.
    /// </summary>
    protected void InsertNullIntoNonNullableColumnShouldThrow()
    {
        var db = CreateDb();
        var table = db.AddTable("data");
        table.AddColumn("id", DbType.Int32, false);
        table.AddColumn("info", DbType.String, false);
        using var connection = OpenConnection(db);

        Assert.Throws<TException>(() =>
            connection.Execute("INSERT INTO data (id, info) VALUES (@id, @info)", new { id = 1, info = (string?)null }));
    }

    /// <summary>
    /// EN: Verifies filtering on a composite index returns the expected matching rows.
    /// PT: Verifica se filtrar por um indice composto retorna as linhas correspondentes esperadas.
    /// </summary>
    protected void CompositeIndexFilterShouldReturnCorrectRows()
    {
        var db = CreateDb();
        var table = db.AddTable("t");
        table.AddColumn("first", DbType.String, false);
        table.AddColumn("second", DbType.String, false);
        table.AddColumn("value", DbType.Int32, false);
        table.Add(new Dictionary<int, object?> { { 0, "A" }, { 1, "X" }, { 2, 1 } });
        table.Add(new Dictionary<int, object?> { { 0, "A" }, { 1, "Y" }, { 2, 2 } });
        table.Add(new Dictionary<int, object?> { { 0, "B" }, { 1, "X" }, { 2, 3 } });
        table.CreateIndex("ix_fs2", CompositeIndexColumns, unique: false);
        using var connection = OpenConnection(db);

        var result = connection.Query<dynamic>("SELECT * FROM t WHERE first = @f AND second = @s", new { f = "A", s = "X" }).ToList();
        Assert.Single(result);
        Assert.Equal(1, (int)result[0].value);
    }

    /// <summary>
    /// EN: Verifies LIKE filters return the expected matching rows.
    /// PT: Verifica se filtros LIKE retornam as linhas correspondentes esperadas.
    /// </summary>
    protected void LikeFilterShouldReturnMatchingRows()
    {
        var db = CreateDb();
        var table = db.AddTable("t");
        table.AddColumn("name", DbType.String, false);
        table.Add(new Dictionary<int, object?> { { 0, "alice" } });
        table.Add(new Dictionary<int, object?> { { 0, "bob" } });
        using var connection = OpenConnection(db);

        var result = connection.Query<dynamic>("SELECT * FROM t WHERE name LIKE 'a%'").ToList();
        Assert.Single(result);
        Assert.Equal("alice", result[0].name);
    }

    /// <summary>
    /// EN: Verifies IN filters return the expected matching rows.
    /// PT: Verifica se filtros IN retornam as linhas correspondentes esperadas.
    /// </summary>
    protected void InFilterShouldReturnMatchingRows()
    {
        var db = CreateDb();
        var table = db.AddTable("t");
        table.AddColumn("id", DbType.Int32, false);
        table.Add(new Dictionary<int, object?> { { 0, 1 } });
        table.Add(new Dictionary<int, object?> { { 0, 2 } });
        table.Add(new Dictionary<int, object?> { { 0, 3 } });
        using var connection = OpenConnection(db);

        var result = connection.Query<dynamic>("SELECT * FROM t WHERE id IN (1,3)").ToList();
        var ids = result.Select(r => (int)r.id).OrderBy(static x => x).ToArray();
        Assert.Equal([1, 3], ids);
    }

    /// <summary>
    /// EN: Verifies distinct pagination with ordering returns the expected rows.
    /// PT: Verifica se a paginacao com distinct e ordenacao retorna as linhas esperadas.
    /// </summary>
    protected void OrderByLimitOffsetDistinctShouldReturnExpectedRows()
    {
        var db = CreateDb();
        var table = db.AddTable("t");
        table.AddColumn("id", DbType.Int32, false);
        table.Add(new Dictionary<int, object?> { { 0, 2 } });
        table.Add(new Dictionary<int, object?> { { 0, 1 } });
        table.Add(new Dictionary<int, object?> { { 0, 2 } });
        using var connection = OpenConnection(db);

        var result = connection.Query<dynamic>(DistinctPaginationSql).ToList();
        Assert.Single(result);
        Assert.Equal(1, (int)result[0].id);
    }

    /// <summary>
    /// EN: Verifies HAVING filters are applied after aggregation results are produced.
    /// PT: Verifica se filtros HAVING sao aplicados apos a producao dos resultados agregados.
    /// </summary>
    protected void HavingFilterShouldApplyAfterAggregation()
    {
        var db = CreateDb();
        var table = db.AddTable("t");
        table.AddColumn("grp", DbType.String, false);
        table.AddColumn("val", DbType.Int32, false);
        table.Add(new Dictionary<int, object?> { { 0, "a" }, { 1, 1 } });
        table.Add(new Dictionary<int, object?> { { 0, "a" }, { 1, 2 } });
        table.Add(new Dictionary<int, object?> { { 0, "b" }, { 1, 3 } });
        using var connection = OpenConnection(db);

        const string sql = "SELECT grp, COUNT(val) AS C FROM t GROUP BY grp HAVING C > 1";
        var result = connection.Query<dynamic>(sql).ToList();
        Assert.Single(result);
        Assert.Equal("a", result[0].grp);
        Assert.Equal(2L, result[0].C);
    }

    /// <summary>
    /// EN: Verifies deleting a referenced parent row throws the expected foreign-key exception.
    /// PT: Verifica se excluir uma linha pai referenciada lanca a excecao esperada de chave estrangeira.
    /// </summary>
    protected void ForeignKeyDeleteShouldThrowOnReferencedParentDeletion()
    {
        var db = CreateDb();
        var parent = db.AddTable("parent");
        parent.AddColumn("id", DbType.Int32, false);
        parent.Add(new Dictionary<int, object?> { { 0, 1 } });
        parent.AddPrimaryKeyIndexes("id");

        var child = db.AddTable("child");
        child.AddColumn("pid", DbType.Int32, false);
        child.AddColumn("data", DbType.String, false);
        child.CreateForeignKey("ix_parent_id", parent.TableName, [("pid", "id")]);
        child.Add(new Dictionary<int, object?> { { 0, 1 }, { 1, "x" } });

        using var connection = OpenConnection(db);

        Assert.Throws<TException>(() => connection.Execute("DELETE FROM parent WHERE id = 1"));
    }

    /// <summary>
    /// EN: Verifies deleting a referenced parent row without an explicit primary key still throws the expected exception.
    /// PT: Verifica se excluir uma linha pai referenciada sem chave primaria explicita ainda lanca a excecao esperada.
    /// </summary>
    protected void ForeignKeyDeleteShouldThrowOnReferencedParentDeletionWithouPK()
    {
        var db = CreateDb();
        var parent = db.AddTable("parent");
        parent.AddColumn("id", DbType.Int32, false);
        parent.Add(new Dictionary<int, object?> { { 0, 1 } });

        var child = db.AddTable("child");
        child.AddColumn("pid", DbType.Int32, false);
        child.AddColumn("data", DbType.String, false);
        child.CreateForeignKey("ix_parent_id", parent.TableName, [("pid", "id")]);
        child.Add(new Dictionary<int, object?> { { 0, 1 }, { 1, "x" } });

        using var connection = OpenConnection(db);

        Assert.Throws<TException>(() => connection.Execute("DELETE FROM parent WHERE id = 1"));
    }

    /// <summary>
    /// EN: Verifies Dapper inserts all rows when multiple parameter sets are supplied.
    /// PT: Verifica se o Dapper insere todas as linhas quando multiplos conjuntos de parametros sao informados.
    /// </summary>
    protected void MultipleParameterSetsInsertShouldInsertAllRows()
    {
        var db = CreateDb();
        var table = db.AddTable("users");
        table.AddColumn("id", DbType.Int32, false);
        table.AddColumn("name", DbType.String, false);
        using var connection = OpenConnection(db);

        var data = new[]
        {
            new { id = 1, name = "A" },
            new { id = 2, name = "B" }
        };

        var rows = connection.Execute("INSERT INTO users (id,name) VALUES (@id,@name)", data);
        Assert.Equal(2, rows);
        Assert.Equal(2, table.Count);
        Assert.Equal("A", table[0][1]);
        Assert.Equal("B", table[1][1]);
    }

    private TConnection OpenConnection(TDb db)
    {
        var connection = CreateConnection(db);
        connection.Open();
        return connection;
    }
}

/// <summary>
/// EN: Shared base for union/pagination/json compatibility tests that reuse the same seeded table and lifecycle.
/// PT: Base compartilhada para testes de compatibilidade de union/paginação/json que reutilizam a mesma tabela seed e ciclo de vida.
/// </summary>
public abstract class DapperUnionLimitAndJsonCompatibilityTestsBase<TDb, TConnection>
    : XUnitTestBase
    where TDb : DbMock
    where TConnection : DbConnectionMockBase
{
    private readonly TConnection _connection;

    /// <summary>
    /// EN: Initializes the shared union, pagination, and JSON compatibility test base.
    /// PT: Inicializa a base compartilhada de testes de compatibilidade de union, paginacao e JSON.
    /// </summary>
    protected DapperUnionLimitAndJsonCompatibilityTestsBase(ITestOutputHelper helper)
        : base(helper)
    {
        _connection = CreateOpenConnection(null);
    }

    /// <summary>
    /// EN: Gets the open provider connection used by the compatibility assertions.
    /// PT: Obtem a conexao aberta do provedor usada pelas assercoes de compatibilidade.
    /// </summary>
    protected TConnection Connection => _connection;

    /// <summary>
    /// EN: Creates a provider-specific connection for the supplied database.
    /// PT: Cria uma conexao especifica do provedor para o banco informado.
    /// </summary>
    protected abstract TConnection CreateConnection(TDb db);

    /// <summary>
    /// EN: Seeds the database with rows used by union and JSON compatibility tests.
    /// PT: Popula o banco com linhas usadas pelos testes de compatibilidade de union e JSON.
    /// </summary>
    protected virtual void Seed(TDb db)
    {
        var t = db.AddTable("t");
        t.AddColumn("id", DbType.Int32, false);
        t.AddColumn("payload", DbType.String, true);
        t.Add(new Dictionary<int, object?> { [0] = 1, [1] = "{\"a\":{\"b\":123}}" });
        t.Add(new Dictionary<int, object?> { [0] = 2, [1] = "{\"a\":{\"b\":456}}" });
        t.Add(new Dictionary<int, object?> { [0] = 3, [1] = null });
    }

    /// <summary>
    /// EN: Creates, seeds, and opens a connection for the requested provider version.
    /// PT: Cria, popula e abre uma conexao para a versao solicitada do provedor.
    /// </summary>
    protected TConnection CreateOpenConnection(int? version)
    {
        var db = CreateDb(version);
        Seed(db);
        var connection = CreateConnection(db);
        connection.Open();
        return connection;
    }

    /// <summary>
    /// EN: Creates the in-memory database instance for the requested provider version.
    /// PT: Cria a instancia de banco em memoria para a versao solicitada do provedor.
    /// </summary>
    protected virtual TDb CreateDb(int? version) => DbMockFactory.Create<TDb>(version);

    /// <summary>
    /// EN: Verifies UNION ALL preserves duplicates while UNION removes them.
    /// PT: Verifica se UNION ALL preserva duplicidades enquanto UNION as remove.
    /// </summary>
    protected void AssertUnionAllKeepsDuplicatesAndUnionRemovesThem()
    {
        var all = Connection.Query<dynamic>(@"
SELECT id FROM t WHERE id = 1
UNION ALL
SELECT id FROM t WHERE id = 1
").ToList();
        Assert.Equal([1, 1], [.. all.Select(r => (int)r.id)]);

        var distinct = Connection.Query<dynamic>(@"
SELECT id FROM t WHERE id = 1
UNION
SELECT id FROM t WHERE id = 1
").ToList();
        Assert.Equal([1], [.. distinct.Select(r => (int)r.id)]);
    }

    /// <summary>
    /// EN: Verifies UNION normalizes equivalent numeric column types into a compatible result.
    /// PT: Verifica se UNION normaliza tipos numericos equivalentes em um resultado compativel.
    /// </summary>
    protected void AssertUnionNormalizesEquivalentNumericTypes()
    {
        var rows = Connection.Query<dynamic>(@"
SELECT 1.0 AS v
UNION
SELECT 1 AS v
").ToList();

        Assert.Single(rows);
    }

    /// <summary>
    /// EN: Verifies UNION rejects incompatible column types across branches.
    /// PT: Verifica se UNION rejeita tipos de coluna incompativeis entre seus ramos.
    /// </summary>
    protected void AssertUnionValidatesIncompatibleColumnTypes()
    {
        Assert.Throws<InvalidOperationException>(() =>
            Connection.Query<dynamic>(@"
SELECT 1 AS v
UNION
SELECT 'x' AS v
").ToList());
    }

    /// <summary>
    /// EN: Verifies UNION normalizes the result schema using the first select alias.
    /// PT: Verifica se UNION normaliza o schema do resultado usando o alias do primeiro select.
    /// </summary>
    protected void AssertUnionNormalizesSchemaToFirstSelectAlias()
    {
        var rows = Connection.Query<dynamic>(@"
SELECT id AS v FROM t WHERE id IN (1, 2)
UNION ALL
SELECT id AS x FROM t WHERE id = 3
ORDER BY v
").ToList();

        Assert.Equal([1, 2, 3], [.. rows.Select(r => (int)r.v)]);
    }

    /// <inheritdoc />
    protected override void Dispose(bool disposing)
    {
        _connection?.Dispose();
        base.Dispose(disposing);
    }
}

/// <summary>
/// EN: Shared behavioral Dapper coverage for providers with equivalent null/join/aggregation semantics.
/// PT: Cobertura comportamental Dapper compartilhada para providers com semântica equivalente de null/join/agregação.
/// </summary>
public abstract class AdditionalBehaviorCoverageTestsBase<TDb, TConnection>
    : XUnitTestBase
    where TDb : DbMock
    where TConnection : DbConnectionMockBase
{
    private static readonly int[] Param = [1, 3];
    private static readonly int[] ParamArray = [1, 2];
    private readonly TConnection _connection;

    /// <summary>
    /// EN: Initializes the shared additional-behavior coverage test base with a seeded open connection.
    /// PT: Inicializa a base compartilhada de cobertura de comportamento adicional com uma conexao aberta e populada.
    /// </summary>
    protected AdditionalBehaviorCoverageTestsBase(ITestOutputHelper helper)
        : base(helper)
    {
        var db = CreateSeededDb();
        _connection = CreateConnection(db);
        _connection.Open();
    }

    /// <summary>
    /// EN: Gets the open provider connection used by the additional behavior assertions.
    /// PT: Obtem a conexao aberta do provedor usada pelas assercoes de comportamento adicional.
    /// </summary>
    protected TConnection Connection => _connection;

    /// <summary>
    /// EN: Creates a provider-specific connection for the supplied database.
    /// PT: Cria uma conexao especifica do provedor para o banco informado.
    /// </summary>
    protected abstract TConnection CreateConnection(TDb db);

    /// <summary>
    /// EN: Creates the in-memory database used by additional behavior coverage tests.
    /// PT: Cria o banco em memoria usado pelos testes de cobertura de comportamento adicional.
    /// </summary>
    protected virtual TDb CreateDb() => DbMockFactory.Create<TDb>();

    /// <summary>
    /// EN: Gets the SQL used to validate deletes that accept parameter lists in IN clauses.
    /// PT: Obtem o SQL usado para validar deletes que aceitam listas de parametros em clausulas IN.
    /// </summary>
    protected virtual string DeleteWithInParameterListSql => "DELETE FROM users WHERE id IN @ids";

    /// <summary>
    /// EN: Verifies IS NULL and IS NOT NULL predicates return the expected rows.
    /// PT: Verifica se os predicados IS NULL e IS NOT NULL retornam as linhas esperadas.
    /// </summary>
    protected void Where_IsNull_And_IsNotNull_ShouldWork()
    {
        var nullIds = Connection.Query<int>("SELECT id FROM users WHERE email IS NULL ORDER BY id").ToList();
        Assert.Equal([2], nullIds);

        var notNullIds = Connection.Query<int>("SELECT id FROM users WHERE email IS NOT NULL ORDER BY id").ToList();
        Assert.Equal([1, 3], notNullIds);
    }

    /// <summary>
    /// EN: Verifies equality comparisons against NULL return no rows.
    /// PT: Verifica se comparacoes de igualdade com NULL nao retornam linhas.
    /// </summary>
    protected void Where_EqualNull_ShouldReturnNoRows()
    {
        var ids = Connection.Query<int>("SELECT id FROM users WHERE email = NULL").ToList();
        Assert.Empty(ids);

        ids = [.. Connection.Query<int>("SELECT id FROM users WHERE email <> NULL")];
        Assert.Empty(ids);
    }

    /// <summary>
    /// EN: Verifies left joins preserve left-side rows even when there is no match.
    /// PT: Verifica se left joins preservam as linhas da esquerda mesmo quando nao ha correspondencia.
    /// </summary>
    protected void LeftJoin_ShouldPreserveLeftRows_WhenNoMatch()
    {
        var rows = Connection.Query<dynamic>(@"
SELECT u.id, o.amount
FROM users u
LEFT JOIN orders o ON o.userid = u.id AND o.amount > 100
ORDER BY u.id
").ToList();

        Assert.Equal(3, rows.Count);
        Assert.Equal(1, (int)rows[0].id);
        Assert.Null((object?)rows[0].amount);
        Assert.Equal(2, (int)rows[1].id);
        Assert.Equal(200m, (decimal)rows[1].amount);
        Assert.Equal(3, (int)rows[2].id);
        Assert.Null((object?)rows[2].amount);
    }

    /// <summary>
    /// EN: Verifies mixed descending and ascending ordering produces deterministic results.
    /// PT: Verifica se a ordenacao mista decrescente e crescente produz resultados deterministas.
    /// </summary>
    protected void OrderBy_Desc_ThenAsc_ShouldBeDeterministic()
    {
        var rows = Connection.Query<dynamic>(@"
SELECT id, amount
FROM orders
ORDER BY amount DESC, id ASC
").ToList();

        Assert.Equal([11, 10, 12], [.. rows.Select(r => (int)r.id)]);
        Assert.Equal([200m, 50m, 10m], [.. rows.Select(r => (decimal)r.amount)]);
    }

    /// <summary>
    /// EN: Verifies COUNT(*) and COUNT(column) differ correctly when null values are present.
    /// PT: Verifica se COUNT(*) e COUNT(coluna) diferem corretamente quando ha valores nulos.
    /// </summary>
    protected void Aggregation_CountStar_Vs_CountColumn_ShouldRespectNulls()
    {
        var row = Connection.QuerySingle<dynamic>("SELECT COUNT(*) c1, COUNT(email) c2 FROM users");
        Assert.Equal(3L, (long)row.c1);
        Assert.Equal(2L, (long)row.c2);
    }

    /// <summary>
    /// EN: Verifies HAVING filters grouped results after aggregation.
    /// PT: Verifica se HAVING filtra resultados agrupados apos a agregacao.
    /// </summary>
    protected void Having_ShouldFilterGroups()
    {
        var userIds = Connection.Query<int>(@"
SELECT userid
FROM orders
GROUP BY userid
HAVING SUM(amount) > 100
ORDER BY userid
").ToList();

        Assert.Equal([2], userIds);
    }

    /// <summary>
    /// EN: Verifies parameter lists work correctly inside IN predicates.
    /// PT: Verifica se listas de parametros funcionam corretamente dentro de predicados IN.
    /// </summary>
    protected void Where_In_WithParameterList_ShouldWork()
    {
        var ids = Connection.Query<int>("SELECT id FROM users WHERE id IN @ids ORDER BY id", new { ids = Param }).ToList();
        Assert.Equal([1, 3], ids);
    }

    /// <summary>
    /// EN: Verifies inserts with columns out of declaration order map values correctly.
    /// PT: Verifica se insercoes com colunas fora da ordem de declaracao mapeiam os valores corretamente.
    /// </summary>
    protected void Insert_WithColumnsOutOfOrder_ShouldMapCorrectly()
    {
        Connection.Execute("INSERT INTO users (name, id, email) VALUES (@name, @id, @email)", new { id = 4, name = "Zed", email = "zed@x.com" });

        var row = Connection.QuerySingle<dynamic>("SELECT id, name, email FROM users WHERE id = 4");
        Assert.Equal(4, (int)row.id);
        Assert.Equal("Zed", (string)row.name);
        Assert.Equal("zed@x.com", (string)row.email);
    }

    /// <summary>
    /// EN: Verifies deletes using an IN parameter list remove the expected rows.
    /// PT: Verifica se deletes usando uma lista de parametros em IN removem as linhas esperadas.
    /// </summary>
    protected void Delete_WithInParameterList_ShouldDeleteMatchingRows()
    {
        var deleted = Connection.Execute(DeleteWithInParameterListSql, new { ids = Param });
        Assert.Equal(2, deleted);

        var remaining = Connection.Query<int>("SELECT id FROM users ORDER BY id").ToList();
        Assert.Equal([2], remaining);
    }

    /// <summary>
    /// EN: Verifies update set expressions can reference the current column value correctly.
    /// PT: Verifica se expressoes SET em updates podem referenciar corretamente o valor atual da coluna.
    /// </summary>
    protected void Update_SetExpression_ShouldUpdateRows()
    {
        var db = CreateDb();
        var users = db.AddTable("users");
        users.AddColumn("id", DbType.Int32, false, identity: false);
        users.AddColumn("counter", DbType.Int32, false);
        users.Add(new Dictionary<int, object?> { [0] = 1, [1] = 0 });
        users.Add(new Dictionary<int, object?> { [0] = 2, [1] = 0 });
        users.Add(new Dictionary<int, object?> { [0] = 3, [1] = 0 });

        using var connection = CreateConnection(db);
        connection.Open();

        var updated = connection.Execute("UPDATE users SET counter = counter + 1 WHERE id IN @ids", new { ids = ParamArray });
        Assert.Equal(2, updated);

        var counters = connection.Query<dynamic>("SELECT id, counter FROM users ORDER BY id").ToList();
        Assert.Equal(1, (int)counters[0].counter);
        Assert.Equal(1, (int)counters[1].counter);
        Assert.Equal(0, (int)counters[2].counter);
    }

    /// <inheritdoc />
    protected override void Dispose(bool disposing)
    {
        _connection?.Dispose();
        base.Dispose(disposing);
    }

    private TDb CreateSeededDb()
    {
        var db = CreateDb();
        var users = db.AddTable("users");
        users.AddColumn("id", DbType.Int32, false, identity: false);
        users.AddColumn("name", DbType.String, false);
        users.AddColumn("email", DbType.String, true);
        users.Add(new Dictionary<int, object?> { [0] = 1, [1] = "John", [2] = "john@x.com" });
        users.Add(new Dictionary<int, object?> { [0] = 2, [1] = "Bob", [2] = null });
        users.Add(new Dictionary<int, object?> { [0] = 3, [1] = "Jane", [2] = "jane@x.com" });

        var orders = db.AddTable("orders");
        orders.AddColumn("id", DbType.Int32, false, identity: false);
        orders.AddColumn("userid", DbType.Int32, false);
        orders.AddColumn("amount", DbType.Decimal, false, decimalPlaces: 2);
        orders.Add(new Dictionary<int, object?> { [0] = 10, [1] = 1, [2] = 50m });
        orders.Add(new Dictionary<int, object?> { [0] = 11, [1] = 2, [2] = 200m });
        orders.Add(new Dictionary<int, object?> { [0] = 12, [1] = 2, [2] = 10m });

        return db;
    }
}
