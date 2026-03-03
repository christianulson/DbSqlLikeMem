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
}
