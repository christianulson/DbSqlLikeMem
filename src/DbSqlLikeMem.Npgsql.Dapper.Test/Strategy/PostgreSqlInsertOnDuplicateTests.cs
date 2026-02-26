namespace DbSqlLikeMem.Npgsql.Test.Strategy;

/// <summary>
/// EN: Defines the class PostgreSqlOnConflictUpsertTests.
/// PT: Define a classe PostgreSqlOnConflictUpsertTests.
/// </summary>
public sealed class PostgreSqlOnConflictUpsertTests(ITestOutputHelper helper) : XUnitTestBase(helper)
{
    /// <summary>
    /// EN: Tests Insert_OnConflict_ShouldInsert_WhenNoConflict behavior.
    /// PT: Testa o comportamento de Insert_OnConflict_ShouldInsert_WhenNoConflict.
    /// </summary>
    [Fact]
    [Trait("Category", "Strategy")]
    public void Insert_OnConflict_ShouldInsert_WhenNoConflict()
    {
        var db = new NpgsqlDbMock();
        var t = db.AddTable("users");
        t.AddColumn("Id", DbType.Int32, false);
        t.AddColumn("Name", DbType.String, false);
        t.AddPrimaryKeyIndexes("id");

        using var cnn = new NpgsqlConnectionMock(db);
        cnn.Open();

        const string sql = "INSERT INTO users (Id, Name) VALUES (1, 'A') ON CONFLICT (Id) DO UPDATE SET Name = EXCLUDED.Name";
        var affected = cnn.Execute(sql);

        Assert.Equal(1, affected);
        Assert.Single(t);
        Assert.Equal("A", (string)t[0][1]!);
    }

    /// <summary>
    /// EN: Tests Insert_OnConflict_ShouldUpdate_WhenConflict behavior.
    /// PT: Testa o comportamento de Insert_OnConflict_ShouldUpdate_WhenConflict.
    /// </summary>
    [Fact]
    [Trait("Category", "Strategy")]
    public void Insert_OnConflict_ShouldUpdate_WhenConflict()
    {
        var db = new NpgsqlDbMock();
        var t = db.AddTable("users");
        t.AddColumn("Id", DbType.Int32, false);
        t.AddColumn("Name", DbType.String, false);
        t.AddPrimaryKeyIndexes("id");

        t.Add(new Dictionary<int, object?> { [0] = 1, [1] = "OLD" });

        using var cnn = new NpgsqlConnectionMock(db);
        cnn.Open();

        const string sql = "INSERT INTO users (Id, Name) VALUES (1, 'NEW') ON CONFLICT (Id) DO UPDATE SET Name = EXCLUDED.Name";
        var affected = cnn.Execute(sql);

        // PostgreSQL reports 1 row affected for DO UPDATE
        Assert.Equal(1, affected);
        Assert.Single(t);
        Assert.Equal("NEW", (string)t[0][1]!);
    }
}
