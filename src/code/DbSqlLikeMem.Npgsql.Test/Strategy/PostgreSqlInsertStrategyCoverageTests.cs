namespace DbSqlLikeMem.Npgsql.Test.Strategy;

/// <summary>
/// EN: Covers multi-row and select-based INSERT scenarios in the Npgsql mock.
/// PT-br: Cobre cenarios de INSERT com multiplas linhas e baseado em SELECT no mock Npgsql.
/// </summary>
public sealed class PostgreSqlInsertStrategyCoverageTests(
        ITestOutputHelper helper
    ) : XUnitTestBase(helper)
{
    /// <summary>
    /// EN: Verifies that a multi-row VALUES insert adds every row.
    /// PT-br: Verifica se um INSERT VALUES com varias linhas adiciona todas as linhas.
    /// </summary>
    [Fact]
    [Trait("Category", "Strategy")]
    public void Insert_MultiRowValues_ShouldInsertAllRows()
    {
        var db = new NpgsqlDbMock();
        var t = db.AddTable("t");
        t.AddColumn("id", DbType.Int32, false, identity: false);
        t.AddColumn("name", DbType.String, false);

        using var cnn = new NpgsqlConnectionMock(db);
        using var cmd = new NpgsqlCommandMock(cnn)
        {
            CommandText = "INSERT INTO t (id, name) VALUES (1, 'A'), (2, 'B')"
        };

        var inserted = cmd.ExecuteNonQuery();

        Assert.Equal(2, inserted);
        Assert.Equal(2, t.Count);
        Assert.Equal(1, (int)t[0][0]!);
        Assert.Equal("A", (string)t[0][1]!);
        Assert.Equal(2, (int)t[1][0]!);
        Assert.Equal("B", (string)t[1][1]!);
    }

    /// <summary>
    /// EN: Verifies that omitting an identity column lets Npgsql generate the value.
    /// PT-br: Verifica se omitir a coluna identity permite ao Npgsql gerar o valor.
    /// </summary>
    [Fact]
    [Trait("Category", "Strategy")]
    public void Insert_WithIdentityColumnOmitted_ShouldAutoIncrement()
    {
        var db = new NpgsqlDbMock();
        var t = db.AddTable("t");
        t.AddColumn("id", DbType.Int32, false, identity: true);
        t.AddColumn("name", DbType.String, false);

        t.AddPrimaryKeyIndexes("id");

        using var cnn = new NpgsqlConnectionMock(db);
        using var cmd = new NpgsqlCommandMock(cnn)
        {
            CommandText = "INSERT INTO t (name) VALUES ('A'), ('B')"
        };

        var inserted = cmd.ExecuteNonQuery();

        Assert.Equal(2, inserted);
        Assert.Equal(2, t.Count);
        Assert.Equal(1, (int)t[0][0]!);
        Assert.Equal("A", (string)t[0][1]!);
        Assert.Equal(2, (int)t[1][0]!);
        Assert.Equal("B", (string)t[1][1]!);
    }

    /// <summary>
    /// EN: Verifies that INSERT ... SELECT copies rows from the source query.
    /// PT-br: Verifica se INSERT ... SELECT copia linhas da consulta de origem.
    /// </summary>
    [Fact]
    [Trait("Category", "Strategy")]
    public void InsertSelect_ShouldInsertRowsFromSelect()
    {
        var db = new NpgsqlDbMock();
        var users = db.AddTable("users");
        users.AddColumn("id", DbType.Int32, false);
        users.AddColumn("tenantid", DbType.Int32, false);
        users.Add(new Dictionary<int, object?> { [0] = 1, [1] = 10 });
        users.Add(new Dictionary<int, object?> { [0] = 2, [1] = 10 });
        users.Add(new Dictionary<int, object?> { [0] = 3, [1] = 20 });

        var t = db.AddTable("t");
        t.AddColumn("id", DbType.Int32, false);

        using var cnn = new NpgsqlConnectionMock(db);

        using var cmd = new NpgsqlCommandMock(cnn)
        {
            CommandText = "INSERT INTO t (id) SELECT id FROM users WHERE tenantid = 10"
        };

        var inserted = cmd.ExecuteNonQuery();

        Assert.Equal(2, inserted);
        Assert.Equal(2, t.Count);
        Assert.Equal([1, 2], [.. t.Select(r => (int)r[0]!)]);
    }
}
