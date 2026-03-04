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
    /// EN: Tests NOT IN with a subquery containing NULL follows SQL semantics (unknown -> filtered out).
    /// PT: Testa que NOT IN com subquery contendo NULL segue semântica SQL (unknown -> filtrado).
    /// </summary>
    [Fact]
    [Trait("Category", "Exists")]
    public void NotIn_SubqueryContainingNull_ShouldFilterOutAllRows()
    {
        using var cnn = CreateConnection();

        DefineUsersAndOrdersTables(cnn);

        cnn.Seed("users", null,
            [1, "Ana"],
            [2, "Bob"],
            [3, "Cid"]);

        cnn.Seed("orders", null,
            [10, 2, 50m],
            [11, null!, 60m]);

        const string sql = @"SELECT u.Id
FROM users u
WHERE u.Id NOT IN (SELECT o.UserId FROM orders o)
ORDER BY u.Id";

        var ids = ExecuteAndReadIds(cnn, sql);

        ids.Should().BeEmpty();
    }

    /// <summary>
    /// EN: Tests NOT IN with an explicit NULL item follows SQL semantics (unknown -> filtered out).
    /// PT: Testa que NOT IN com item NULL explícito segue semântica SQL (unknown -> filtrado).
    /// </summary>
    [Fact]
    [Trait("Category", "Exists")]
    public void NotIn_ListContainingNull_ShouldFilterOutAllRows()
    {
        using var cnn = CreateConnection();

        DefineUsersAndOrdersTables(cnn);

        cnn.Seed("users", null,
            [1, "Ana"],
            [2, "Bob"],
            [3, "Cid"]);

        const string sql = @"SELECT u.Id
FROM users u
WHERE u.Id NOT IN (2, NULL)
ORDER BY u.Id";

        var ids = ExecuteAndReadIds(cnn, sql);

        ids.Should().BeEmpty();
    }

    /// <summary>
    /// EN: Tests quantified comparison with ANY subquery behaves like membership for equality.
    /// PT: Testa que comparação quantificada com subquery ANY se comporta como pertencimento para igualdade.
    /// </summary>
    [Fact]
    [Trait("Category", "Exists")]
    public void ComparisonAnySubquery_WithEquality_ShouldFilterMatchingRows()
    {
        using var cnn = CreateConnection();

        DefineUsersAndOrdersTables(cnn);

        cnn.Seed("users", null,
            [1, "Ana"],
            [2, "Bob"],
            [3, "Cid"]);

        cnn.Seed("orders", null,
            [10, 2, 50m],
            [11, 3, 60m]);

        const string sql = @"SELECT u.Id
FROM users u
WHERE u.Id = ANY (SELECT o.UserId FROM orders o)
ORDER BY u.Id";

        var ids = ExecuteAndReadIds(cnn, sql);

        ids.Should().Equal(2, 3);
    }

    /// <summary>
    /// EN: Tests quantified comparison with ALL subquery requires predicate to hold for all returned rows.
    /// PT: Testa que comparação quantificada com subquery ALL exige que o predicado seja verdadeiro para todas as linhas retornadas.
    /// </summary>
    [Fact]
    [Trait("Category", "Exists")]
    public void ComparisonAllSubquery_WithLessOperator_ShouldFilterRowsLessThanAllCandidates()
    {
        using var cnn = CreateConnection();

        DefineUsersAndOrdersTables(cnn);

        cnn.Seed("users", null,
            [1, "Ana"],
            [2, "Bob"],
            [3, "Cid"]);

        cnn.Seed("orders", null,
            [10, 2, 50m],
            [11, 3, 60m]);

        const string sql = @"SELECT u.Id
FROM users u
WHERE u.Id < ALL (SELECT o.UserId FROM orders o)
ORDER BY u.Id";

        var ids = ExecuteAndReadIds(cnn, sql);

        ids.Should().Equal(1);
    }

    /// <summary>
    /// EN: Tests quantified comparison with SOME subquery as an alias of ANY.
    /// PT: Testa comparação quantificada com subquery SOME como alias de ANY.
    /// </summary>
    [Fact]
    [Trait("Category", "Exists")]
    public void ComparisonSomeSubquery_WithEquality_ShouldBehaveLikeAny()
    {
        using var cnn = CreateConnection();

        DefineUsersAndOrdersTables(cnn);

        cnn.Seed("users", null,
            [1, "Ana"],
            [2, "Bob"],
            [3, "Cid"]);

        cnn.Seed("orders", null,
            [10, 2, 50m],
            [11, 3, 60m]);

        const string sql = @"SELECT u.Id
FROM users u
WHERE u.Id = SOME (SELECT o.UserId FROM orders o)
ORDER BY u.Id";

        var ids = ExecuteAndReadIds(cnn, sql);

        ids.Should().Equal(2, 3);
    }

    /// <summary>
    /// EN: Tests quantified comparison with ALL over an empty subquery returns true (vacuous truth).
    /// PT: Testa que comparação quantificada com ALL sobre subquery vazia retorna verdadeiro (verdade vacuamente).
    /// </summary>
    [Fact]
    [Trait("Category", "Exists")]
    public void ComparisonAllSubquery_WithEmptySet_ShouldReturnAllRows()
    {
        using var cnn = CreateConnection();

        DefineUsersAndOrdersTables(cnn);

        cnn.Seed("users", null,
            [1, "Ana"],
            [2, "Bob"],
            [3, "Cid"]);

        const string sql = @"SELECT u.Id
FROM users u
WHERE u.Id > ALL (SELECT o.UserId FROM orders o)
ORDER BY u.Id";

        var ids = ExecuteAndReadIds(cnn, sql);

        ids.Should().Equal(1, 2, 3);
    }

    /// <summary>
    /// EN: Tests quantified ANY accepts an extra parenthesis wrapper around subquery.
    /// PT: Testa que ANY quantificado aceita um parêntese extra envolvendo a subquery.
    /// </summary>
    [Fact]
    [Trait("Category", "Exists")]
    public void ComparisonAnySubquery_WithExtraParenthesisWrapper_ShouldFilterMatchingRows()
    {
        using var cnn = CreateConnection();

        DefineUsersAndOrdersTables(cnn);

        cnn.Seed("users", null,
            [1, "Ana"],
            [2, "Bob"],
            [3, "Cid"]);

        cnn.Seed("orders", null,
            [10, 2, 50m],
            [11, 3, 60m]);

        const string sql = @"SELECT u.Id
FROM users u
WHERE u.Id = ANY ((SELECT o.UserId FROM orders o))
ORDER BY u.Id";

        var ids = ExecuteAndReadIds(cnn, sql);

        ids.Should().Equal(2, 3);
    }

    /// <summary>
    /// EN: Tests quantified ALL accepts an extra parenthesis wrapper around subquery.
    /// PT: Testa que ALL quantificado aceita um parêntese extra envolvendo a subquery.
    /// </summary>
    [Fact]
    [Trait("Category", "Exists")]
    public void ComparisonAllSubquery_WithExtraParenthesisWrapper_ShouldFilterRowsLessThanAllCandidates()
    {
        using var cnn = CreateConnection();

        DefineUsersAndOrdersTables(cnn);

        cnn.Seed("users", null,
            [1, "Ana"],
            [2, "Bob"],
            [3, "Cid"]);

        cnn.Seed("orders", null,
            [10, 2, 50m],
            [11, 3, 60m]);

        const string sql = @"SELECT u.Id
FROM users u
WHERE u.Id < ALL ((SELECT o.UserId FROM orders o))
ORDER BY u.Id";

        var ids = ExecuteAndReadIds(cnn, sql);

        ids.Should().Equal(1);
    }

    /// <summary>
    /// EN: Tests quantified ANY with NULL-only candidates results in UNKNOWN and filters out rows when no TRUE match exists.
    /// PT: Testa que ANY quantificado com candidatos apenas NULL resulta em UNKNOWN e filtra linhas quando não há correspondência TRUE.
    /// </summary>
    [Fact]
    [Trait("Category", "Exists")]
    public void ComparisonAnySubquery_WithOnlyNullCandidates_ShouldFilterOutAllRows()
    {
        using var cnn = CreateConnection();

        DefineUsersAndOrdersTables(cnn);

        cnn.Seed("users", null,
            [1, "Ana"],
            [2, "Bob"],
            [3, "Cid"]);

        cnn.Seed("orders", null,
            [10, null!, 50m],
            [11, null!, 60m]);

        const string sql = @"SELECT u.Id
FROM users u
WHERE u.Id = ANY (SELECT o.UserId FROM orders o)
ORDER BY u.Id";

        var ids = ExecuteAndReadIds(cnn, sql);

        ids.Should().BeEmpty();
    }

    /// <summary>
    /// EN: Tests quantified ALL with at least one NULL candidate and no FALSE comparison yields UNKNOWN and filters out rows.
    /// PT: Testa que ALL quantificado com ao menos um candidato NULL e sem comparação FALSE gera UNKNOWN e filtra linhas.
    /// </summary>
    [Fact]
    [Trait("Category", "Exists")]
    public void ComparisonAllSubquery_WithNullCandidateAndNoFalseComparison_ShouldFilterOutRows()
    {
        using var cnn = CreateConnection();

        DefineUsersAndOrdersTables(cnn);

        cnn.Seed("users", null,
            [1, "Ana"],
            [2, "Bob"]);

        cnn.Seed("orders", null,
            [10, 5, 50m],
            [11, null!, 60m]);

        const string sql = @"SELECT u.Id
FROM users u
WHERE u.Id < ALL (SELECT o.UserId FROM orders o)
ORDER BY u.Id";

        var ids = ExecuteAndReadIds(cnn, sql);

        ids.Should().BeEmpty();
    }

    /// <summary>
    /// EN: Tests equivalent quantified ANY correlated subqueries with and without explicit projection AS alias share the same cache entry.
    /// PT: Testa que subqueries correlacionadas equivalentes com ANY quantificado com e sem alias AS explícito na projeção compartilham a mesma entrada de cache.
    /// </summary>
    [Fact]
    [Trait("Category", "Exists")]
    public void ComparisonAnyCorrelatedSubqueries_WithAndWithoutProjectionAliasAs_ShouldShareCache()
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
WHERE u.Id = ANY (SELECT o.UserId FROM orders o WHERE o.UserId = u.Id)
  AND u.Id = ANY (SELECT o.UserId AS K FROM orders o WHERE o.UserId = u.Id)
ORDER BY u.Id";

        var ids = ExecuteAndReadIds(cnn, sql);

        ids.Should().HaveCount(80);
        GetTableHintCount(cnn, "orders").Should().Be(1);
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

    /// <summary>
    /// EN: Tests equivalent correlated EXISTS subqueries with redundant outer parentheses around AND predicate share the same cache entry.
    /// PT: Testa que subqueries EXISTS correlacionadas equivalentes com parênteses externos redundantes em predicado AND compartilham a mesma entrada de cache.
    /// </summary>
    [Fact]
    [Trait("Category", "Exists")]
    public void Exists_EquivalentCorrelatedSubqueriesWithRedundantOuterParentheses_ShouldShareCache()
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
  AND EXISTS (SELECT 1 FROM orders o WHERE (o.Amount > 0 AND o.UserId = u.Id))
ORDER BY u.Id";

        var ids = ExecuteAndReadIds(cnn, sql);

        ids.Should().HaveCount(80);
        GetTableHintCount(cnn, "orders").Should().Be(1);
    }

    /// <summary>
    /// EN: Tests equivalent correlated IN subqueries with redundant outer parentheses around AND predicate share the same cache entry.
    /// PT: Testa que subqueries correlacionadas equivalentes em IN com parênteses externos redundantes em predicado AND compartilham a mesma entrada de cache.
    /// </summary>
    [Fact]
    [Trait("Category", "Exists")]
    public void In_EquivalentCorrelatedSubqueriesWithRedundantOuterParentheses_ShouldShareCache()
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
  AND u.Id IN (SELECT o.UserId FROM orders o WHERE (o.Amount > 0 AND o.UserId = u.Id))
ORDER BY u.Id";

        var ids = ExecuteAndReadIds(cnn, sql);

        ids.Should().HaveCount(80);
        GetTableHintCount(cnn, "orders").Should().Be(1);
    }

    /// <summary>
    /// EN: Tests equivalent correlated scalar subqueries with redundant outer parentheses around AND predicate share the same cache entry.
    /// PT: Testa que subqueries escalares correlacionadas equivalentes com parênteses externos redundantes em predicado AND compartilham a mesma entrada de cache.
    /// </summary>
    [Fact]
    [Trait("Category", "Exists")]
    public void Scalar_EquivalentCorrelatedSubqueriesWithRedundantOuterParentheses_ShouldShareCache()
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
       (SELECT MAX(o.Amount) FROM orders o WHERE (o.Amount > 0 AND o.UserId = u.Id)) AS MaxAmountB
FROM users u
ORDER BY u.Id";

        var rowCount = ExecuteAndCountRows(cnn, sql);

        rowCount.Should().Be(80);
        GetTableHintCount(cnn, "orders").Should().Be(1);
    }

    /// <summary>
    /// EN: Tests equivalent correlated EXISTS subqueries with inverted equality operands share the same cache entry.
    /// PT: Testa que subqueries EXISTS correlacionadas equivalentes com operandos invertidos em igualdade compartilham a mesma entrada de cache.
    /// </summary>
    [Fact]
    [Trait("Category", "Exists")]
    public void Exists_EquivalentCorrelatedSubqueriesWithInvertedEqualityOperands_ShouldShareCache()
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
  AND EXISTS (SELECT 1 FROM orders o WHERE u.Id = o.UserId AND o.Amount > 0)
ORDER BY u.Id";

        var ids = ExecuteAndReadIds(cnn, sql);

        ids.Should().HaveCount(80);
        GetTableHintCount(cnn, "orders").Should().Be(1);
    }

    /// <summary>
    /// EN: Tests equivalent correlated IN subqueries with inverted equality operands share the same cache entry.
    /// PT: Testa que subqueries correlacionadas equivalentes em IN com operandos invertidos em igualdade compartilham a mesma entrada de cache.
    /// </summary>
    [Fact]
    [Trait("Category", "Exists")]
    public void In_EquivalentCorrelatedSubqueriesWithInvertedEqualityOperands_ShouldShareCache()
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
  AND u.Id IN (SELECT o.UserId FROM orders o WHERE u.Id = o.UserId AND o.Amount > 0)
ORDER BY u.Id";

        var ids = ExecuteAndReadIds(cnn, sql);

        ids.Should().HaveCount(80);
        GetTableHintCount(cnn, "orders").Should().Be(1);
    }

    /// <summary>
    /// EN: Tests equivalent correlated scalar subqueries with inverted equality operands share the same cache entry.
    /// PT: Testa que subqueries escalares correlacionadas equivalentes com operandos invertidos em igualdade compartilham a mesma entrada de cache.
    /// </summary>
    [Fact]
    [Trait("Category", "Exists")]
    public void Scalar_EquivalentCorrelatedSubqueriesWithInvertedEqualityOperands_ShouldShareCache()
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
       (SELECT MAX(o.Amount) FROM orders o WHERE u.Id = o.UserId AND o.Amount > 0) AS MaxAmountB
FROM users u
ORDER BY u.Id";

        var rowCount = ExecuteAndCountRows(cnn, sql);

        rowCount.Should().Be(80);
        GetTableHintCount(cnn, "orders").Should().Be(1);
    }

    /// <summary>
    /// EN: Tests equivalent correlated EXISTS subqueries with and without AS in inner aliases share the same cache entry.
    /// PT: Testa que subqueries EXISTS correlacionadas equivalentes com e sem AS em aliases internos compartilham a mesma entrada de cache.
    /// </summary>
    [Fact]
    [Trait("Category", "Exists")]
    public void Exists_EquivalentCorrelatedSubqueriesWithAndWithoutAsInAliases_ShouldShareCache()
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
  AND EXISTS (SELECT 1 FROM orders AS o WHERE o.UserId = u.Id)
ORDER BY u.Id";

        var ids = ExecuteAndReadIds(cnn, sql);

        ids.Should().HaveCount(80);
        GetTableHintCount(cnn, "orders").Should().Be(1);
    }

    /// <summary>
    /// EN: Tests equivalent correlated IN subqueries with and without AS in inner aliases share the same cache entry.
    /// PT: Testa que subqueries correlacionadas equivalentes em IN com e sem AS em aliases internos compartilham a mesma entrada de cache.
    /// </summary>
    [Fact]
    [Trait("Category", "Exists")]
    public void In_EquivalentCorrelatedSubqueriesWithAndWithoutAsInAliases_ShouldShareCache()
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
  AND u.Id IN (SELECT o.UserId FROM orders AS o WHERE o.UserId = u.Id)
ORDER BY u.Id";

        var ids = ExecuteAndReadIds(cnn, sql);

        ids.Should().HaveCount(80);
        GetTableHintCount(cnn, "orders").Should().Be(1);
    }

    /// <summary>
    /// EN: Tests equivalent correlated scalar subqueries with and without AS in inner aliases share the same cache entry.
    /// PT: Testa que subqueries escalares correlacionadas equivalentes com e sem AS em aliases internos compartilham a mesma entrada de cache.
    /// </summary>
    [Fact]
    [Trait("Category", "Exists")]
    public void Scalar_EquivalentCorrelatedSubqueriesWithAndWithoutAsInAliases_ShouldShareCache()
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
       (SELECT MAX(o.Amount) FROM orders AS o WHERE o.UserId = u.Id) AS MaxAmountB
FROM users u
ORDER BY u.Id";

        var rowCount = ExecuteAndCountRows(cnn, sql);

        rowCount.Should().Be(80);
        GetTableHintCount(cnn, "orders").Should().Be(1);
    }

    /// <summary>
    /// EN: Tests equivalent correlated EXISTS subqueries with different relational-operator spacing share the same cache entry.
    /// PT: Testa que subqueries EXISTS correlacionadas equivalentes com espaçamento diferente em operadores relacionais compartilham a mesma entrada de cache.
    /// </summary>
    [Fact]
    [Trait("Category", "Exists")]
    public void Exists_EquivalentCorrelatedSubqueriesWithDifferentOperatorSpacing_ShouldShareCache()
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
  AND EXISTS (SELECT 1 FROM orders o WHERE o.UserId=u.Id AND o.Amount>0)
ORDER BY u.Id";

        var ids = ExecuteAndReadIds(cnn, sql);

        ids.Should().HaveCount(80);
        GetTableHintCount(cnn, "orders").Should().Be(1);
    }

    /// <summary>
    /// EN: Tests equivalent correlated IN subqueries with different relational-operator spacing share the same cache entry.
    /// PT: Testa que subqueries correlacionadas equivalentes em IN com espaçamento diferente em operadores relacionais compartilham a mesma entrada de cache.
    /// </summary>
    [Fact]
    [Trait("Category", "Exists")]
    public void In_EquivalentCorrelatedSubqueriesWithDifferentOperatorSpacing_ShouldShareCache()
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
  AND u.Id IN (SELECT o.UserId FROM orders o WHERE o.UserId=u.Id AND o.Amount>0)
ORDER BY u.Id";

        var ids = ExecuteAndReadIds(cnn, sql);

        ids.Should().HaveCount(80);
        GetTableHintCount(cnn, "orders").Should().Be(1);
    }

    /// <summary>
    /// EN: Tests equivalent correlated scalar subqueries with different relational-operator spacing share the same cache entry.
    /// PT: Testa que subqueries escalares correlacionadas equivalentes com espaçamento diferente em operadores relacionais compartilham a mesma entrada de cache.
    /// </summary>
    [Fact]
    [Trait("Category", "Exists")]
    public void Scalar_EquivalentCorrelatedSubqueriesWithDifferentOperatorSpacing_ShouldShareCache()
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
       (SELECT MAX(o.Amount) FROM orders o WHERE o.UserId=u.Id AND o.Amount>0) AS MaxAmountB
FROM users u
ORDER BY u.Id";

        var rowCount = ExecuteAndCountRows(cnn, sql);

        rowCount.Should().Be(80);
        GetTableHintCount(cnn, "orders").Should().Be(1);
    }

    /// <summary>
    /// EN: Tests equivalent correlated EXISTS subqueries with different projection payloads share the same cache entry.
    /// PT: Testa que subqueries EXISTS correlacionadas equivalentes com payloads de projeção diferentes compartilham a mesma entrada de cache.
    /// </summary>
    [Fact]
    [Trait("Category", "Exists")]
    public void Exists_EquivalentCorrelatedSubqueriesWithDifferentProjectionPayload_ShouldShareCache()
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
  AND EXISTS (SELECT o.Amount FROM orders o WHERE o.UserId = u.Id AND o.Amount > 0)
ORDER BY u.Id";

        var ids = ExecuteAndReadIds(cnn, sql);

        ids.Should().HaveCount(80);
        GetTableHintCount(cnn, "orders").Should().Be(1);
    }

    /// <summary>
    /// EN: Tests equivalent correlated EXISTS subqueries with DISTINCT projection payload variation share the same cache entry.
    /// PT: Testa que subqueries EXISTS correlacionadas equivalentes com variação de payload de projeção DISTINCT compartilham a mesma entrada de cache.
    /// </summary>
    [Fact]
    [Trait("Category", "Exists")]
    public void Exists_EquivalentCorrelatedSubqueriesWithDistinctProjectionPayload_ShouldShareCache()
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
WHERE EXISTS (SELECT DISTINCT o.Amount FROM orders o WHERE o.UserId = u.Id AND o.Amount > 0)
  AND EXISTS (SELECT 1 FROM orders o WHERE o.UserId = u.Id AND o.Amount > 0)
ORDER BY u.Id";

        var ids = ExecuteAndReadIds(cnn, sql);

        ids.Should().HaveCount(80);
        GetTableHintCount(cnn, "orders").Should().Be(1);
    }

    /// <summary>
    /// EN: Tests equivalent correlated IN subqueries with explicit AS projection aliases share the same cache entry.
    /// PT: Testa que subqueries correlacionadas equivalentes em IN com aliases explícitos AS na projeção compartilham a mesma entrada de cache.
    /// </summary>
    [Fact]
    [Trait("Category", "Exists")]
    public void In_EquivalentCorrelatedSubqueriesWithProjectionAliasAs_ShouldShareCache()
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
  AND u.Id IN (SELECT o.UserId AS K FROM orders o WHERE o.UserId = u.Id)
ORDER BY u.Id";

        var ids = ExecuteAndReadIds(cnn, sql);

        ids.Should().HaveCount(80);
        GetTableHintCount(cnn, "orders").Should().Be(1);
    }

    /// <summary>
    /// EN: Tests equivalent correlated scalar subqueries with explicit AS projection aliases share the same cache entry.
    /// PT: Testa que subqueries escalares correlacionadas equivalentes com aliases explícitos AS na projeção compartilham a mesma entrada de cache.
    /// </summary>
    [Fact]
    [Trait("Category", "Exists")]
    public void Scalar_EquivalentCorrelatedSubqueriesWithProjectionAliasAs_ShouldShareCache()
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
       (SELECT MAX(o.Amount) AS V FROM orders o WHERE o.UserId = u.Id) AS MaxAmountB
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
        cnn.Column<int>("orders", "UserId", nullable: true);
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
