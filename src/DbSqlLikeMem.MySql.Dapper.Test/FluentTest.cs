namespace DbSqlLikeMem.MySql.Dapper.Test;
/// <summary>
/// Auto-generated summary.
/// </summary>
public sealed class FluentTest(
        ITestOutputHelper helper
    ) : XUnitTestBase(helper)
{
    private static MySqlConnectionMock BuildConnection()
    {
        var db = new MySqlDbMock { ThreadSafe = true };
        var cnn = new MySqlConnectionMock(db);

        // Definição fluente da tabela
        cnn.DefineTable("user")
           .Column<int>("id", pk: true, identity: true)
           .Column<string>("name")
           .Column<string>("email", nullable: true)
           .Column<DateTime>("created", nullable: false, identity: false);

        cnn.Open();
        return cnn;
    }

    /// <summary>
    /// EN: Tests InsertUpdateDeleteFluentScenario behavior.
    /// PT: Testa o comportamento de InsertUpdateDeleteFluentScenario.
    /// </summary>
    [Fact]
    [Trait("Category", "FluentTest")]
    public void InsertUpdateDeleteFluentScenario()
    {
        using var cnn = BuildConnection();

        // ---------- INSERT ----------
        var rows = cnn.Execute(
            "INSERT INTO user (name, email, created) VALUES (@name, @email, @created)",
            new { name = "Alice", email = "alice@mail.com", created = DateTime.UtcNow });

        Assert.Equal(1, rows);
        Assert.Equal(1, cnn.Metrics.Inserts);
        Assert.Single(cnn.Db.GetTable("user")); // 1 linha na tabela

        // ---------- UPDATE ----------
        rows = cnn.Execute(
            "UPDATE user SET name = @name WHERE id = @id",
            new { id = 1, name = "Alice Cooper" });

        Assert.Equal(1, rows);
        Assert.Equal(1, cnn.Metrics.Updates);
        Assert.Equal("Alice Cooper", cnn.Db.GetTable("user")[0][1]); // coluna 1 = name

        // ---------- DELETE ----------
        rows = cnn.Execute(
            "DELETE FROM user WHERE id = @id",
            new { id = 1 });

        Assert.Equal(1, rows);
        Assert.Equal(1, cnn.Metrics.Deletes);
        Assert.Empty(cnn.Db.GetTable("user")); // tabela vazia novamente

        // ---------- Métricas finais ----------
        Assert.Equal(1, cnn.Metrics.Inserts);
        Assert.Equal(1, cnn.Metrics.Updates);
        Assert.Equal(1, cnn.Metrics.Deletes);
        Assert.True(cnn.Metrics.Elapsed > TimeSpan.Zero);
    }

    /// <summary>
    /// EN: Tests TestFluent behavior.
    /// PT: Testa o comportamento de TestFluent.
    /// </summary>
    [Fact]
    [Trait("Category", "FluentTest")]
    public void TestFluent()
    {
        using var cnn = new MySqlConnectionMock();
        cnn.Open();      // abre conexão
        cnn.DefineTable("user")                               // fluent-via-connection
           .Column<int>("id", pk: true, identity: true)
           .Column<string>("name");

        cnn.Db.GetTable("user")                                      // fluent-via-table
           .Column<DateTime>("created");

        cnn.Seed("user", null,
            [null, "Alice", DateTime.UtcNow],
            [null, "Bob", DateTime.UtcNow]);

        Assert.Equal(2, cnn.GetTable("user").Count);   // 2 linhas

        var idIdx = cnn.GetTable("user").Columns["id"].Index;
        Assert.Equal(0, idIdx);     // 0
        Assert.Equal(1, cnn.GetTable("user")[0][idIdx]);             // 1 (auto-increment)
    }
}
