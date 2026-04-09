namespace DbSqlLikeMem.Firebird.Test.Strategy;

/// <summary>
/// EN: Covers DELETE execution scenarios in the Firebird mock.
/// PT: Cobre cenarios de execucao de DELETE no mock Firebird.
/// </summary>
public sealed class FirebirdDeleteStrategyTests
{
    /// <summary>
    /// EN: Verifies that DELETE removes a single matching row.
    /// PT: Verifica se DELETE remove uma unica linha correspondente.
    /// </summary>
    [Fact]
    [Trait("Category", "Strategy")]
    public void ExecuteNonQuery_DELETE_remove_1_linha()
    {
        var db = new FirebirdDbMock();
        var table = db.AddTable("users");
        table.AddColumn("id", DbType.Int32, false);
        table.AddColumn("name", DbType.String, false);
        table.Add(new Dictionary<int, object?> { [0] = 1, [1] = "John" });
        table.Add(new Dictionary<int, object?> { [0] = 2, [1] = "Mary" });

        using var conn = new FirebirdConnectionMock(db);
        using var cmd = new FirebirdCommandMock(conn) { CommandText = "DELETE FROM users WHERE id = 1" };

        var affected = cmd.ExecuteNonQuery();

        Assert.Equal(1, affected);
        Assert.Single(table);
        Assert.Equal(2, table[0][0]);
    }

    /// <summary>
    /// EN: Verifies that DELETE removes every matching row.
    /// PT: Verifica se DELETE remove todas as linhas correspondentes.
    /// </summary>
    [Fact]
    [Trait("Category", "Strategy")]
    public void ExecuteNonQuery_DELETE_remove_varias_linhas()
    {
        var db = new FirebirdDbMock();
        var table = db.AddTable("users");
        table.AddColumn("id", DbType.Int32, false);
        table.AddColumn("name", DbType.String, false);
        table.Add(new Dictionary<int, object?> { [0] = 2, [1] = "A" });
        table.Add(new Dictionary<int, object?> { [0] = 2, [1] = "B" });
        table.Add(new Dictionary<int, object?> { [0] = 1, [1] = "C" });

        using var conn = new FirebirdConnectionMock(db);
        using var cmd = new FirebirdCommandMock(conn) { CommandText = "DELETE FROM users WHERE id = 2" };

        var affected = cmd.ExecuteNonQuery();

        Assert.Equal(2, affected);
        Assert.Single(table);
        Assert.Equal(1, table[0][0]);
    }

    /// <summary>
    /// EN: Verifies that DELETE returns zero when no rows match.
    /// PT: Verifica se DELETE retorna zero quando nenhuma linha corresponde.
    /// </summary>
    [Fact]
    [Trait("Category", "Strategy")]
    public void ExecuteNonQuery_DELETE_quando_nao_acha_retorna_0()
    {
        var db = new FirebirdDbMock();
        var table = db.AddTable("users");
        table.AddColumn("id", DbType.Int32, false);
        table.AddColumn("name", DbType.String, false);
        table.Add(new Dictionary<int, object?> { [0] = 1, [1] = "John" });

        using var conn = new FirebirdConnectionMock(db);
        using var cmd = new FirebirdCommandMock(conn) { CommandText = "DELETE FROM users WHERE id = 999" };

        var affected = cmd.ExecuteNonQuery();

        Assert.Equal(0, affected);
        Assert.Single(table);
    }

    /// <summary>
    /// EN: Verifies that DELETE parsing is case-insensitive.
    /// PT: Verifica se o parsing de DELETE e case-insensitive.
    /// </summary>
    [Fact]
    [Trait("Category", "Strategy")]
    public void ExecuteNonQuery_DELETE_case_insensitive()
    {
        var db = new FirebirdDbMock();
        var table = db.AddTable("users");
        table.AddColumn("id", DbType.Int32, false);
        table.AddColumn("name", DbType.String, false);
        table.Add(new Dictionary<int, object?> { [0] = 1, [1] = "John" });

        using var conn = new FirebirdConnectionMock(db);
        using var cmd = new FirebirdCommandMock(conn) { CommandText = "delete FrOm users wHeRe id = 1" };

        var affected = cmd.ExecuteNonQuery();

        Assert.Equal(1, affected);
        Assert.Empty(table);
    }
}

