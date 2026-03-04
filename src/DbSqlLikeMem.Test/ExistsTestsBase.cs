namespace DbSqlLikeMem.Test;

/// <summary>
/// EN: Shared EXISTS/NOT EXISTS behavior tests executed by provider-specific derived classes.
/// PT: Testes compartilhados de EXISTS/NOT EXISTS executados por classes derivadas de cada provedor.
/// </summary>
public abstract class ExistsTestsBase(
        ITestOutputHelper helper
    ) : XUnitTestBase(helper)
{
    /// <summary>
    /// EN: Creates a provider-specific mock connection used by shared EXISTS tests.
    /// PT: Cria uma conexão simulada específica do provedor usada pelos testes compartilhados de EXISTS.
    /// </summary>
    protected abstract DbConnectionMockBase CreateConnection();

    /// <summary>
    /// EN: Tests Exists_ShouldFilterUsersWithOrders behavior.
    /// PT: Testa o comportamento de Exists_ShouldFilterUsersWithOrders.
    /// </summary>
    [Fact]
    [Trait("Category", "Exists")]
    public void Exists_ShouldFilterUsersWithOrders()
    {
        using var cnn = CreateConnection();

        DefineUsersAndOrdersTables(cnn);

        cnn.Seed("users", null,
            [1, "Ana"],
            [2, "Bob"],
            [3, "Cid"]);

        cnn.Seed("orders", null,
            [10, 1, 50m],
            [11, 1, 60m],
            [12, 3, 10m]);

        const string sql = @"SELECT u.Id
FROM users u
WHERE EXISTS (SELECT 1 FROM orders o WHERE o.UserId = u.Id)
ORDER BY u.Id";

        var ids = ExecuteAndReadIds(cnn, sql);

        ids.Should().Equal(1, 3);
    }

    /// <summary>
    /// EN: Tests NotExists_ShouldFilterUsersWithoutOrders behavior.
    /// PT: Testa o comportamento de NotExists_ShouldFilterUsersWithoutOrders.
    /// </summary>
    [Fact]
    [Trait("Category", "Exists")]
    public void NotExists_ShouldFilterUsersWithoutOrders()
    {
        using var cnn = CreateConnection();

        DefineUsersAndOrdersTables(cnn);

        cnn.Seed("users", null,
            [1, "Ana"],
            [2, "Bob"],
            [3, "Cid"]);

        cnn.Seed("orders", null,
            [10, 1, 50m],
            [11, 3, 10m]);

        const string sql = @"SELECT u.Id
FROM users u
WHERE NOT EXISTS (SELECT 1 FROM orders o WHERE o.UserId = u.Id)
ORDER BY u.Id";

        var ids = ExecuteAndReadIds(cnn, sql);

        ids.Should().Equal(2);
    }

    /// <summary>
    /// EN: Tests Exists_WithExtraPredicate_ShouldWork behavior.
    /// PT: Testa o comportamento de Exists_WithExtraPredicate_ShouldWork.
    /// </summary>
    [Fact]
    [Trait("Category", "Exists")]
    public void Exists_WithExtraPredicate_ShouldWork()
    {
        using var cnn = CreateConnection();

        DefineUsersAndOrdersTables(cnn);

        cnn.Seed("users", null,
            [1, "Ana"],
            [2, "Bob"],
            [3, "Cid"]);

        cnn.Seed("orders", null,
            [10, 1, 99m],
            [11, 1, 100m],
            [12, 2, 10m]);

        const string sql = @"SELECT u.Id
FROM users u
WHERE EXISTS (SELECT 1 FROM orders o WHERE o.UserId = u.Id AND o.Amount >= 100)
ORDER BY u.Id";

        var ids = ExecuteAndReadIds(cnn, sql);

        ids.Should().Equal(1);
    }

    /// <summary>
    /// EN: Tests correlated EXISTS subquery reuses evaluation for duplicate outer rows, reducing repeated source access.
    /// PT: Testa que subquery correlacionada com EXISTS reutiliza avaliação para linhas externas duplicadas, reduzindo acessos repetidos à fonte.
    /// </summary>
    [Fact]
    [Trait("Category", "Exists")]
    public void Exists_CorrelatedSubquery_ShouldReuseEvaluationForDuplicateOuterRows()
    {
        using var cnn = CreateConnection();

        DefineUsersAndOrdersTables(cnn);

        for (var i = 0; i < 80; i++)
            cnn.Seed("users", null, [1, "Ana"]);

        cnn.Seed("orders", null,
            [10, 1, 50m],
            [11, 1, 60m]);

        const string sql = @"SELECT u.Id
FROM users u
WHERE EXISTS (SELECT 1 FROM orders o WHERE o.UserId = u.Id)
ORDER BY u.Id";

        var ids = ExecuteAndReadIds(cnn, sql);

        ids.Should().HaveCount(80);
        GetTableHintCount(cnn, "orders").Should().BeLessThan(10);
    }

    /// <summary>
    /// EN: Tests correlated IN-subquery reuses evaluation for duplicate outer rows, reducing repeated source access.
    /// PT: Testa que subquery correlacionada em IN reutiliza avaliação para linhas externas duplicadas, reduzindo acessos repetidos à fonte.
    /// </summary>
    [Fact]
    [Trait("Category", "Exists")]
    public void InCorrelatedSubquery_ShouldReuseEvaluationForDuplicateOuterRows()
    {
        using var cnn = CreateConnection();

        DefineUsersAndOrdersTables(cnn);

        for (var i = 0; i < 80; i++)
            cnn.Seed("users", null, [1, "Ana"]);

        cnn.Seed("orders", null,
            [10, 1, 50m],
            [11, 1, 60m]);

        const string sql = @"SELECT u.Id
FROM users u
WHERE u.Id IN (SELECT o.UserId FROM orders o WHERE o.UserId = u.Id)
ORDER BY u.Id";

        var ids = ExecuteAndReadIds(cnn, sql);

        ids.Should().HaveCount(80);
        GetTableHintCount(cnn, "orders").Should().BeLessThan(10);
    }

    /// <summary>
    /// EN: Tests correlated scalar subquery in projection reuses evaluation for duplicate outer rows, reducing repeated source access.
    /// PT: Testa que subquery escalar correlacionada na projeção reutiliza avaliação para linhas externas duplicadas, reduzindo acessos repetidos à fonte.
    /// </summary>
    [Fact]
    [Trait("Category", "Exists")]
    public void ScalarCorrelatedSubqueryInProjection_ShouldReuseEvaluationForDuplicateOuterRows()
    {
        using var cnn = CreateConnection();

        DefineUsersAndOrdersTables(cnn);

        for (var i = 0; i < 80; i++)
            cnn.Seed("users", null, [1, "Ana"]);

        cnn.Seed("orders", null,
            [10, 1, 50m],
            [11, 1, 60m]);

        const string sql = @"SELECT u.Id,
       (SELECT MAX(o.Amount) FROM orders o WHERE o.UserId = u.Id) AS MaxAmount
FROM users u
ORDER BY u.Id";

        var rowCount = ExecuteAndCountRows(cnn, sql);

        rowCount.Should().Be(80);
        GetTableHintCount(cnn, "orders").Should().BeLessThan(10);
    }

    /// <summary>
    /// EN: Tests correlated scalar subquery returning NULL still reuses evaluation for duplicate outer rows.
    /// PT: Testa que subquery escalar correlacionada com retorno NULL ainda reutiliza avaliação para linhas externas duplicadas.
    /// </summary>
    [Fact]
    [Trait("Category", "Exists")]
    public void ScalarCorrelatedSubqueryReturningNull_ShouldReuseEvaluationForDuplicateOuterRows()
    {
        using var cnn = CreateConnection();

        DefineUsersAndOrdersTables(cnn);

        for (var i = 0; i < 80; i++)
            cnn.Seed("users", null, [2, "Bob"]);

        cnn.Seed("orders", null,
            [10, 1, 50m],
            [11, 1, 60m]);

        const string sql = @"SELECT u.Id,
       (SELECT MAX(o.Amount) FROM orders o WHERE o.UserId = u.Id) AS MaxAmount
FROM users u
ORDER BY u.Id";

        var rowCount = ExecuteAndCountRows(cnn, sql);

        rowCount.Should().Be(80);
        GetTableHintCount(cnn, "orders").Should().BeLessThan(10);
    }

    /// <summary>
    /// EN: Tests correlated EXISTS cache key ignores non-referenced outer columns, allowing reuse when only unrelated fields differ.
    /// PT: Testa que a chave de cache de EXISTS correlacionado ignora colunas externas não referenciadas, permitindo reuso quando apenas campos não relacionados diferem.
    /// </summary>
    [Fact]
    [Trait("Category", "Exists")]
    public void Exists_CorrelatedSubqueryCacheKey_ShouldIgnoreNonReferencedOuterColumns()
    {
        using var cnn = CreateConnection();

        DefineUsersAndOrdersTables(cnn);

        for (var i = 0; i < 80; i++)
            cnn.Seed("users", null, [1, $"Name-{i}"]);

        cnn.Seed("orders", null,
            [10, 1, 50m],
            [11, 1, 60m]);

        const string sql = @"SELECT u.Id
FROM users u
WHERE EXISTS (SELECT 1 FROM orders o WHERE o.UserId = u.Id)
ORDER BY u.Id";

        var ids = ExecuteAndReadIds(cnn, sql);

        ids.Should().HaveCount(80);
        GetTableHintCount(cnn, "orders").Should().BeLessThan(10);
    }

    /// <summary>
    /// EN: Tests correlated scalar-subquery cache key ignores non-referenced outer columns, allowing reuse when only unrelated fields differ.
    /// PT: Testa que a chave de cache de subquery escalar correlacionada ignora colunas externas não referenciadas, permitindo reuso quando apenas campos não relacionados diferem.
    /// </summary>
    [Fact]
    [Trait("Category", "Exists")]
    public void ScalarCorrelatedSubqueryCacheKey_ShouldIgnoreNonReferencedOuterColumns()
    {
        using var cnn = CreateConnection();

        DefineUsersAndOrdersTables(cnn);

        for (var i = 0; i < 80; i++)
            cnn.Seed("users", null, [1, $"Name-{i}"]);

        cnn.Seed("orders", null,
            [10, 1, 50m],
            [11, 1, 60m]);

        const string sql = @"SELECT u.Id,
       (SELECT MAX(o.Amount) FROM orders o WHERE o.UserId = u.Id) AS MaxAmount
FROM users u
ORDER BY u.Id";

        var rowCount = ExecuteAndCountRows(cnn, sql);

        rowCount.Should().Be(80);
        GetTableHintCount(cnn, "orders").Should().BeLessThan(10);
    }

    /// <summary>
    /// EN: Tests uncorrelated EXISTS subquery is reused across outer rows and does not re-read source per row.
    /// PT: Testa que subquery EXISTS não correlacionada é reutilizada entre linhas externas e não releitura a fonte por linha.
    /// </summary>
    [Fact]
    [Trait("Category", "Exists")]
    public void Exists_UncorrelatedSubquery_ShouldReuseEvaluationAcrossOuterRows()
    {
        using var cnn = CreateConnection();

        DefineUsersAndOrdersTables(cnn);

        for (var i = 0; i < 80; i++)
            cnn.Seed("users", null, [i + 1, $"Name-{i}"]);

        cnn.Seed("orders", null,
            [10, 1, 50m],
            [11, 1, 60m]);

        const string sql = @"SELECT u.Id
FROM users u
WHERE EXISTS (SELECT 1 FROM orders o WHERE o.Amount > 0)
ORDER BY u.Id";

        var ids = ExecuteAndReadIds(cnn, sql);

        ids.Should().HaveCount(80);
        GetTableHintCount(cnn, "orders").Should().BeLessThan(10);
    }

    /// <summary>
    /// EN: Tests uncorrelated scalar subquery in projection is reused across outer rows and does not re-read source per row.
    /// PT: Testa que subquery escalar não correlacionada na projeção é reutilizada entre linhas externas e não releitura a fonte por linha.
    /// </summary>
    [Fact]
    [Trait("Category", "Exists")]
    public void ScalarUncorrelatedSubqueryInProjection_ShouldReuseEvaluationAcrossOuterRows()
    {
        using var cnn = CreateConnection();

        DefineUsersAndOrdersTables(cnn);

        for (var i = 0; i < 80; i++)
            cnn.Seed("users", null, [i + 1, $"Name-{i}"]);

        cnn.Seed("orders", null,
            [10, 1, 50m],
            [11, 1, 60m]);

        const string sql = @"SELECT u.Id,
       (SELECT MAX(o.Amount) FROM orders o WHERE o.Amount > 0) AS MaxAmount
FROM users u
ORDER BY u.Id";

        var rowCount = ExecuteAndCountRows(cnn, sql);

        rowCount.Should().Be(80);
        GetTableHintCount(cnn, "orders").Should().BeLessThan(10);
    }

    /// <summary>
    /// EN: Tests equivalent correlated EXISTS subqueries with different identifier casing share the same cache entry.
    /// PT: Testa que subqueries EXISTS correlacionadas equivalentes com casing diferente de identificadores compartilham a mesma entrada de cache.
    /// </summary>
    [Fact]
    [Trait("Category", "Exists")]
    public void Exists_EquivalentCorrelatedSubqueriesWithDifferentIdentifierCase_ShouldShareCache()
    {
        using var cnn = CreateConnection();

        DefineUsersAndOrdersTables(cnn);

        for (var i = 0; i < 80; i++)
            cnn.Seed("users", null, [1, $"Name-{i}"]);

        cnn.Seed("orders", null,
            [10, 1, 50m],
            [11, 1, 60m]);

        const string sql = @"SELECT u.Id
FROM users u
WHERE EXISTS (SELECT 1 FROM orders o WHERE o.UserId = u.Id)
  AND EXISTS (SELECT 1 FROM orders o WHERE o.userid = u.id)
ORDER BY u.Id";

        var ids = ExecuteAndReadIds(cnn, sql);

        ids.Should().HaveCount(80);
        GetTableHintCount(cnn, "orders").Should().Be(1);
    }

    /// <summary>
    /// EN: Tests equivalent correlated EXISTS subqueries with different whitespace around qualified identifiers share the same cache entry.
    /// PT: Testa que subqueries EXISTS correlacionadas equivalentes com espaços diferentes em identificadores qualificados compartilham a mesma entrada de cache.
    /// </summary>
    [Fact]
    [Trait("Category", "Exists")]
    public void Exists_EquivalentCorrelatedSubqueriesWithDifferentWhitespace_ShouldShareCache()
    {
        using var cnn = CreateConnection();

        DefineUsersAndOrdersTables(cnn);

        for (var i = 0; i < 80; i++)
            cnn.Seed("users", null, [1, $"Name-{i}"]);

        cnn.Seed("orders", null,
            [10, 1, 50m],
            [11, 1, 60m]);

        const string sql = @"SELECT u.Id
FROM users u
WHERE EXISTS (SELECT 1 FROM orders o WHERE o.UserId = u.Id)
  AND EXISTS (SELECT 1 FROM orders o WHERE o . UserId = u . Id)
ORDER BY u.Id";

        var ids = ExecuteAndReadIds(cnn, sql);

        ids.Should().HaveCount(80);
        GetTableHintCount(cnn, "orders").Should().Be(1);
    }

    /// <summary>
    /// EN: Tests equivalent correlated IN subqueries with different identifier casing share the same cache entry.
    /// PT: Testa que subqueries correlacionadas equivalentes em IN com casing diferente de identificadores compartilham a mesma entrada de cache.
    /// </summary>
    [Fact]
    [Trait("Category", "Exists")]
    public void In_EquivalentCorrelatedSubqueriesWithDifferentIdentifierCase_ShouldShareCache()
    {
        using var cnn = CreateConnection();

        DefineUsersAndOrdersTables(cnn);

        for (var i = 0; i < 80; i++)
            cnn.Seed("users", null, [1, $"Name-{i}"]);

        cnn.Seed("orders", null,
            [10, 1, 50m],
            [11, 1, 60m]);

        const string sql = @"SELECT u.Id
FROM users u
WHERE u.Id IN (SELECT o.UserId FROM orders o WHERE o.UserId = u.Id)
  AND u.id IN (SELECT o.userid FROM orders o WHERE o.userid = u.id)
ORDER BY u.Id";

        var ids = ExecuteAndReadIds(cnn, sql);

        ids.Should().HaveCount(80);
        GetTableHintCount(cnn, "orders").Should().Be(1);
    }

    /// <summary>
    /// EN: Tests equivalent correlated scalar subqueries with different identifier casing share the same cache entry.
    /// PT: Testa que subqueries escalares correlacionadas equivalentes com casing diferente de identificadores compartilham a mesma entrada de cache.
    /// </summary>
    [Fact]
    [Trait("Category", "Exists")]
    public void Scalar_EquivalentCorrelatedSubqueriesWithDifferentIdentifierCase_ShouldShareCache()
    {
        using var cnn = CreateConnection();

        DefineUsersAndOrdersTables(cnn);

        for (var i = 0; i < 80; i++)
            cnn.Seed("users", null, [1, $"Name-{i}"]);

        cnn.Seed("orders", null,
            [10, 1, 50m],
            [11, 1, 60m]);

        const string sql = @"SELECT u.Id,
       (SELECT MAX(o.Amount) FROM orders o WHERE o.UserId = u.Id) AS MaxAmountA,
       (SELECT MAX(o.Amount) FROM orders o WHERE o.userid = u.id) AS MaxAmountB
FROM users u
ORDER BY u.Id";

        var rowCount = ExecuteAndCountRows(cnn, sql);

        rowCount.Should().Be(80);
        GetTableHintCount(cnn, "orders").Should().Be(1);
    }

    /// <summary>
    /// EN: Tests equivalent correlated EXISTS subqueries with different inner aliases share the same cache entry.
    /// PT: Testa que subqueries EXISTS correlacionadas equivalentes com aliases internos diferentes compartilham a mesma entrada de cache.
    /// </summary>
    [Fact]
    [Trait("Category", "Exists")]
    public void Exists_EquivalentCorrelatedSubqueriesWithDifferentInnerAliases_ShouldShareCache()
    {
        using var cnn = CreateConnection();

        DefineUsersAndOrdersTables(cnn);

        for (var i = 0; i < 80; i++)
            cnn.Seed("users", null, [1, $"Name-{i}"]);

        cnn.Seed("orders", null,
            [10, 1, 50m],
            [11, 1, 60m]);

        const string sql = @"SELECT u.Id
FROM users u
WHERE EXISTS (SELECT 1 FROM orders o WHERE o.UserId = u.Id)
  AND EXISTS (SELECT 1 FROM orders ord WHERE ord.UserId = u.Id)
ORDER BY u.Id";

        var ids = ExecuteAndReadIds(cnn, sql);

        ids.Should().HaveCount(80);
        GetTableHintCount(cnn, "orders").Should().Be(1);
    }

    /// <summary>
    /// EN: Tests equivalent correlated IN subqueries with different inner aliases share the same cache entry.
    /// PT: Testa que subqueries correlacionadas equivalentes em IN com aliases internos diferentes compartilham a mesma entrada de cache.
    /// </summary>
    [Fact]
    [Trait("Category", "Exists")]
    public void In_EquivalentCorrelatedSubqueriesWithDifferentInnerAliases_ShouldShareCache()
    {
        using var cnn = CreateConnection();

        DefineUsersAndOrdersTables(cnn);

        for (var i = 0; i < 80; i++)
            cnn.Seed("users", null, [1, $"Name-{i}"]);

        cnn.Seed("orders", null,
            [10, 1, 50m],
            [11, 1, 60m]);

        const string sql = @"SELECT u.Id
FROM users u
WHERE u.Id IN (SELECT o.UserId FROM orders o WHERE o.UserId = u.Id)
  AND u.Id IN (SELECT ord.UserId FROM orders ord WHERE ord.UserId = u.Id)
ORDER BY u.Id";

        var ids = ExecuteAndReadIds(cnn, sql);

        ids.Should().HaveCount(80);
        GetTableHintCount(cnn, "orders").Should().Be(1);
    }

    /// <summary>
    /// EN: Tests equivalent correlated scalar subqueries with different inner aliases share the same cache entry.
    /// PT: Testa que subqueries escalares correlacionadas equivalentes com aliases internos diferentes compartilham a mesma entrada de cache.
    /// </summary>
    [Fact]
    [Trait("Category", "Exists")]
    public void Scalar_EquivalentCorrelatedSubqueriesWithDifferentInnerAliases_ShouldShareCache()
    {
        using var cnn = CreateConnection();

        DefineUsersAndOrdersTables(cnn);

        for (var i = 0; i < 80; i++)
            cnn.Seed("users", null, [1, $"Name-{i}"]);

        cnn.Seed("orders", null,
            [10, 1, 50m],
            [11, 1, 60m]);

        const string sql = @"SELECT u.Id,
       (SELECT MAX(o.Amount) FROM orders o WHERE o.UserId = u.Id) AS MaxAmountA,
       (SELECT MAX(ord.Amount) FROM orders ord WHERE ord.UserId = u.Id) AS MaxAmountB
FROM users u
ORDER BY u.Id";

        var rowCount = ExecuteAndCountRows(cnn, sql);

        rowCount.Should().Be(80);
        GetTableHintCount(cnn, "orders").Should().Be(1);
    }

    /// <summary>
    /// EN: Tests equivalent correlated EXISTS subqueries with top-level AND predicates in different order share the same cache entry.
    /// PT: Testa que subqueries EXISTS correlacionadas equivalentes com predicados AND de topo em ordem diferente compartilham a mesma entrada de cache.
    /// </summary>
    [Fact]
    [Trait("Category", "Exists")]
    public void Exists_EquivalentCorrelatedSubqueriesWithAndPredicatesReordered_ShouldShareCache()
    {
        using var cnn = CreateConnection();

        DefineUsersAndOrdersTables(cnn);

        for (var i = 0; i < 80; i++)
            cnn.Seed("users", null, [1, $"Name-{i}"]);

        cnn.Seed("orders", null,
            [10, 1, 50m],
            [11, 1, 60m]);

        const string sql = @"SELECT u.Id
FROM users u
WHERE EXISTS (SELECT 1 FROM orders o WHERE o.UserId = u.Id AND o.Amount > 0)
  AND EXISTS (SELECT 1 FROM orders o WHERE o.Amount > 0 AND o.UserId = u.Id)
ORDER BY u.Id";

        var ids = ExecuteAndReadIds(cnn, sql);

        ids.Should().HaveCount(80);
        GetTableHintCount(cnn, "orders").Should().Be(1);
    }

    /// <summary>
    /// EN: Tests equivalent correlated IN subqueries with top-level AND predicates in different order share the same cache entry.
    /// PT: Testa que subqueries correlacionadas equivalentes em IN com predicados AND de topo em ordem diferente compartilham a mesma entrada de cache.
    /// </summary>
    [Fact]
    [Trait("Category", "Exists")]
    public void In_EquivalentCorrelatedSubqueriesWithAndPredicatesReordered_ShouldShareCache()
    {
        using var cnn = CreateConnection();

        DefineUsersAndOrdersTables(cnn);

        for (var i = 0; i < 80; i++)
            cnn.Seed("users", null, [1, $"Name-{i}"]);

        cnn.Seed("orders", null,
            [10, 1, 50m],
            [11, 1, 60m]);

        const string sql = @"SELECT u.Id
FROM users u
WHERE u.Id IN (SELECT o.UserId FROM orders o WHERE o.UserId = u.Id AND o.Amount > 0)
  AND u.Id IN (SELECT o.UserId FROM orders o WHERE o.Amount > 0 AND o.UserId = u.Id)
ORDER BY u.Id";

        var ids = ExecuteAndReadIds(cnn, sql);

        ids.Should().HaveCount(80);
        GetTableHintCount(cnn, "orders").Should().Be(1);
    }

    /// <summary>
    /// EN: Tests equivalent correlated scalar subqueries with top-level AND predicates in different order share the same cache entry.
    /// PT: Testa que subqueries escalares correlacionadas equivalentes com predicados AND de topo em ordem diferente compartilham a mesma entrada de cache.
    /// </summary>
    [Fact]
    [Trait("Category", "Exists")]
    public void Scalar_EquivalentCorrelatedSubqueriesWithAndPredicatesReordered_ShouldShareCache()
    {
        using var cnn = CreateConnection();

        DefineUsersAndOrdersTables(cnn);

        for (var i = 0; i < 80; i++)
            cnn.Seed("users", null, [1, $"Name-{i}"]);

        cnn.Seed("orders", null,
            [10, 1, 50m],
            [11, 1, 60m]);

        const string sql = @"SELECT u.Id,
       (SELECT MAX(o.Amount) FROM orders o WHERE o.UserId = u.Id AND o.Amount > 0) AS MaxAmountA,
       (SELECT MAX(o.Amount) FROM orders o WHERE o.Amount > 0 AND o.UserId = u.Id) AS MaxAmountB
FROM users u
ORDER BY u.Id";

        var rowCount = ExecuteAndCountRows(cnn, sql);

        rowCount.Should().Be(80);
        GetTableHintCount(cnn, "orders").Should().Be(1);
    }

    private static void DefineUsersAndOrdersTables(
        DbConnectionMockBase cnn)
    {
        cnn.Define("users");
        cnn.Column<int>("users", "Id");
        cnn.Column<string>("users", "Name");

        cnn.Define("orders");
        cnn.Column<int>("orders", "Id");
        cnn.Column<int>("orders", "UserId");
        cnn.Column<decimal>("orders", "Amount", decimalPlaces: 2);
    }

    private static List<int> ExecuteAndReadIds(
        DbConnectionMockBase cnn,
        string sql)
    {
        using var cmd = cnn.CreateCommand();
        cmd.CommandText = sql;

        using var reader = cmd.ExecuteReader();
        var ids = new List<int>();
        while (reader.Read())
            ids.Add(reader.GetInt32(0));

        return ids;
    }

    /// <summary>
    /// EN: Gets how many times a table source was touched according to connection metrics.
    /// PT: Obtém quantas vezes uma fonte de tabela foi acessada segundo as métricas da conexão.
    /// </summary>
    private static int GetTableHintCount(DbConnectionMockBase cnn, string tableName)
        => cnn.Metrics.TableHints.TryGetValue(tableName, out var count)
            ? count
            : 0;

    /// <summary>
    /// EN: Executes query and returns total row count.
    /// PT: Executa a consulta e retorna a contagem total de linhas.
    /// </summary>
    private static int ExecuteAndCountRows(DbConnectionMockBase cnn, string sql)
    {
        using var cmd = cnn.CreateCommand();
        cmd.CommandText = sql;

        using var reader = cmd.ExecuteReader();
        var count = 0;
        while (reader.Read())
            count++;

        return count;
    }
}
