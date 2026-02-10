namespace DbSqlLikeMem.SqlServer.Test;

/// <summary>
/// Auto-generated summary.
/// </summary>
public sealed class ExistsTests(
        ITestOutputHelper helper
    ) : XUnitTestBase(helper)
{
    /// <summary>
    /// EN: Tests Exists_ShouldFilterUsersWithOrders behavior.
    /// PT: Testa o comportamento de Exists_ShouldFilterUsersWithOrders.
    /// </summary>
    [Fact]
    public void Exists_ShouldFilterUsersWithOrders()
    {
        using var cnn = new SqlServerConnectionMock();

        cnn.Define("users");
        cnn.Column<int>("users", "Id");
        cnn.Column<string>("users", "Name");

        cnn.Define("orders");
        cnn.Column<int>("orders", "Id");
        cnn.Column<int>("orders", "UserId");
        cnn.Column<decimal>("orders", "Amount");

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

        using var cmd = new SqlServerCommandMock(cnn) { CommandText = sql };

        using var reader = cmd.ExecuteReader();
        var ids = new List<int>();
        while (reader.Read())
            ids.Add(reader.GetInt32(0));

        ids.Should().Equal(1, 3);
    }

    /// <summary>
    /// EN: Tests NotExists_ShouldFilterUsersWithoutOrders behavior.
    /// PT: Testa o comportamento de NotExists_ShouldFilterUsersWithoutOrders.
    /// </summary>
    [Fact]
    public void NotExists_ShouldFilterUsersWithoutOrders()
    {
        using var cnn = new SqlServerConnectionMock();

        cnn.Define("users");
        cnn.Column<int>("users", "Id");
        cnn.Column<string>("users", "Name");

        cnn.Define("orders");
        cnn.Column<int>("orders", "Id");
        cnn.Column<int>("orders", "UserId");
        cnn.Column<decimal>("orders", "Amount");

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

        using var cmd = new SqlServerCommandMock(cnn) { CommandText = sql };

        using var reader = cmd.ExecuteReader();
        var ids = new List<int>();
        while (reader.Read())
            ids.Add(reader.GetInt32(0));

        ids.Should().Equal(2);
    }

    /// <summary>
    /// EN: Tests Exists_WithExtraPredicate_ShouldWork behavior.
    /// PT: Testa o comportamento de Exists_WithExtraPredicate_ShouldWork.
    /// </summary>
    [Fact]
    public void Exists_WithExtraPredicate_ShouldWork()
    {
        using var cnn = new SqlServerConnectionMock();

        cnn.Define("users");
        cnn.Column<int>("users", "Id");
        cnn.Column<string>("users", "Name");

        cnn.Define("orders");
        cnn.Column<int>("orders", "Id");
        cnn.Column<int>("orders", "UserId");
        cnn.Column<decimal>("orders", "Amount");

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

        using var cmd = new SqlServerCommandMock(cnn) { CommandText = sql };

        using var reader = cmd.ExecuteReader();
        var ids = new List<int>();
        while (reader.Read())
            ids.Add(reader.GetInt32(0));

        ids.Should().Equal(1);
    }
}
