namespace DbSqlLikeMem.SqlAzure.Test;

/// <summary>
/// EN: Covers SQL Azure subquery and join scenarios over the direct mock connection.
/// PT: Cobre cenarios de subquery e join do SQL Azure sobre a conexao mock direta.
/// </summary>
public sealed class SubqueryFromAndJoinsTests(
        ITestOutputHelper helper
    ) : XUnitTestBase(helper)
{
    /// <summary>
    /// EN: Verifies FROM subqueries return the expected filtered rows.
    /// PT: Verifica se subqueries em FROM retornam as linhas filtradas esperadas.
    /// </summary>
    [Fact]
    [Trait("Category", "SubqueryFromAndJoins")]
    public void FromSubquery_ShouldReturnFilteredRows()
    {
        using var cnn = new SqlAzureConnectionMock();

        cnn.Define("users");
        cnn.Column<int>("users", "Id");
        cnn.Column<string>("users", "Name");
        cnn.Column<int>("users", "Active");

        cnn.Seed("users", null,
            [1, "Ana", 1],
            [2, "Bob", 0],
            [3, "Cid", 1]);

        using var cmd = new SqlAzureCommandMock(cnn)
        {
            CommandText = "SELECT u.Id FROM (SELECT Id FROM users WHERE Active = 1) u ORDER BY u.Id"
        };

        using var reader = cmd.ExecuteReader();
        var ids = new List<int>();
        while (reader.Read())
            ids.Add(reader.GetInt32(0));

        ids.Should().Equal(1, 3);
    }

    /// <summary>
    /// EN: Verifies joins against a subquery return the expected rows.
    /// PT: Verifica se joins com subquery retornam as linhas esperadas.
    /// </summary>
    [Fact]
    [Trait("Category", "SubqueryFromAndJoins")]
    public void JoinSubquery_ShouldJoinCorrectly()
    {
        using var cnn = new SqlAzureConnectionMock();

        cnn.Define("users");
        cnn.Column<int>("users", "Id");
        cnn.Column<string>("users", "Name");

        cnn.Define("orders");
        cnn.Column<int>("orders", "Id");
        cnn.Column<int>("orders", "UserId");
        cnn.Column<decimal>("orders", "Amount", decimalPlaces: 2);

        cnn.Seed("users", null,
            [1, "Ana"],
            [2, "Bob"],
            [3, "Cid"]);

        cnn.Seed("orders", null,
            [10, 1, 40m],
            [11, 1, 60m],
            [12, 2, 80m],
            [13, 3, 10m]);

        const string sql = @"SELECT u.Id, o.Amount
FROM users u
JOIN (SELECT UserId, Amount FROM orders WHERE Amount > 50) o ON o.UserId = u.Id
ORDER BY u.Id, o.Amount";

        using var cmd = new SqlAzureCommandMock(cnn) { CommandText = sql };

        using var reader = cmd.ExecuteReader();
        var rows = new List<(int userId, decimal amount)>();
        while (reader.Read())
            rows.Add((reader.GetInt32(0), reader.GetDecimal(1)));

        rows.Should().Equal([(1, 60m), (2, 80m)]);
    }

    /// <summary>
    /// EN: Verifies nested subqueries are parsed and executed correctly.
    /// PT: Verifica se subqueries aninhadas sao interpretadas e executadas corretamente.
    /// </summary>
    [Fact]
    [Trait("Category", "SubqueryFromAndJoins")]
    public void NestedSubquery_ShouldWork()
    {
        using var cnn = new SqlAzureConnectionMock();

        cnn.Define("users");
        cnn.Column<int>("users", "Id");
        cnn.Column<string>("users", "Name");

        cnn.Seed("users", null,
            [1, "Ana"],
            [2, "Bob"]);

        const string sql = @"SELECT t.Id
FROM (SELECT Id FROM (SELECT Id FROM users) x) t
ORDER BY t.Id";

        using var cmd = new SqlAzureCommandMock(cnn) { CommandText = sql };

        using var reader = cmd.ExecuteReader();
        var ids = new List<int>();
        while (reader.Read())
            ids.Add(reader.GetInt32(0));

        ids.Should().Equal(1, 2);
    }
}




