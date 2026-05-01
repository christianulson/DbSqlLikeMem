namespace DbSqlLikeMem.Npgsql.Test.Strategy;

/// <summary>
/// EN: Covers PostgreSQL ON CONFLICT upsert scenarios against the Dapper provider.
/// PT-br: Cobre cenarios de upsert com ON CONFLICT do PostgreSQL contra o provedor Dapper.
/// </summary>
public sealed class PostgreSqlOnConflictUpsertTests(ITestOutputHelper helper) : XUnitTestBase(helper)
{
    /// <summary>
    /// EN: Verifies ON CONFLICT inserts a row when no conflict exists.
    /// PT-br: Verifica se ON CONFLICT insere uma linha quando nao existe conflito.
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
    /// EN: Verifies ON CONFLICT updates the matching row when a conflict exists.
    /// PT-br: Verifica se ON CONFLICT atualiza a linha correspondente quando ha conflito.
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

    /// <summary>
    /// EN: Verifies DO NOTHING leaves the existing row unchanged on conflict.
    /// PT-br: Verifica se DO NOTHING mantem a linha existente inalterada em caso de conflito.
    /// </summary>
    [Fact]
    [Trait("Category", "Strategy")]
    public void Insert_OnConflict_DoNothing_ShouldNotUpdate_WhenConflict()
    {
        var db = new NpgsqlDbMock();
        var t = db.AddTable("users");
        t.AddColumn("Id", DbType.Int32, false);
        t.AddColumn("Name", DbType.String, false);
        t.AddPrimaryKeyIndexes("id");
        t.Add(new Dictionary<int, object?> { [0] = 1, [1] = "OLD" });

        using var cnn = new NpgsqlConnectionMock(db);
        cnn.Open();

        const string sql = "INSERT INTO users (Id, Name) VALUES (1, 'NEW') ON CONFLICT (Id) DO NOTHING";
        var affected = cnn.Execute(sql);

        Assert.Equal(0, affected);
        Assert.Single(t);
        Assert.Equal("OLD", (string)t[0][1]!);
    }

    /// <summary>
    /// EN: Verifies a false DO UPDATE WHERE clause skips the conflict update.
    /// PT-br: Verifica se uma clausula DO UPDATE WHERE falsa ignora a atualizacao do conflito.
    /// </summary>
    [Fact]
    [Trait("Category", "Strategy")]
    public void Insert_OnConflict_DoUpdateWhereFalse_ShouldSkipUpdate_WhenConflict()
    {
        var db = new NpgsqlDbMock();
        var t = db.AddTable("users");
        t.AddColumn("Id", DbType.Int32, false);
        t.AddColumn("Name", DbType.String, false);
        t.AddPrimaryKeyIndexes("id");
        t.Add(new Dictionary<int, object?> { [0] = 1, [1] = "OLD" });

        using var cnn = new NpgsqlConnectionMock(db);
        cnn.Open();

        const string sql = "INSERT INTO users (Id, Name) VALUES (1, 'NEW') ON CONFLICT (Id) DO UPDATE SET Name = EXCLUDED.Name WHERE 1 = 0";
        var affected = cnn.Execute(sql);

        Assert.Equal(0, affected);
        Assert.Single(t);
        Assert.Equal("OLD", (string)t[0][1]!);
    }

    /// <summary>
    /// EN: Verifies a true DO UPDATE WHERE clause applies the conflict update.
    /// PT-br: Verifica se uma clausula DO UPDATE WHERE verdadeira aplica a atualizacao do conflito.
    /// </summary>
    [Fact]
    [Trait("Category", "Strategy")]
    public void Insert_OnConflict_DoUpdateWhereTrue_ShouldApplyUpdate_WhenConflict()
    {
        var db = new NpgsqlDbMock();
        var t = db.AddTable("users");
        t.AddColumn("Id", DbType.Int32, false);
        t.AddColumn("Name", DbType.String, false);
        t.AddPrimaryKeyIndexes("id");
        t.Add(new Dictionary<int, object?> { [0] = 1, [1] = "OLD" });

        using var cnn = new NpgsqlConnectionMock(db);
        cnn.Open();

        const string sql = "INSERT INTO users (Id, Name) VALUES (1, 'NEW') ON CONFLICT (Id) DO UPDATE SET Name = EXCLUDED.Name WHERE users.id = EXCLUDED.id";
        var affected = cnn.Execute(sql);

        Assert.Equal(1, affected);
        Assert.Single(t);
        Assert.Equal("NEW", (string)t[0][1]!);
    }
}
