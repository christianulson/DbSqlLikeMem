namespace DbSqlLikeMem.Firebird.Dapper.Test;

/// <summary>
/// EN: Covers Firebird row limit syntax through the Dapper-facing provider surface.
/// PT-br: Cobre a sintaxe de limite de linhas do Firebird pela surface do provedor exposta ao Dapper.
/// </summary>
public sealed class FirebirdRowLimitTests(ITestOutputHelper helper) : XUnitTestBase(helper)
{
    /// <summary>
    /// EN: Verifies FIRST and SKIP return the expected page of rows in Firebird.
    /// PT-br: Verifica se FIRST e SKIP retornam a pagina esperada de linhas no Firebird.
    /// </summary>
    [Fact]
    [Trait("Category", "FirebirdDapper")]
    public void SelectFirstSkip_ShouldReturnExpectedRows_Test()
    {
        var db = new FirebirdDbMock();
        var users = db.AddTable("Users");
        users.AddColumn("Id", DbType.Int32, false);
        users.AddColumn("Name", DbType.String, false);
        users.Add(new Dictionary<int, object?> { [0] = 1, [1] = "Alice" });
        users.Add(new Dictionary<int, object?> { [0] = 2, [1] = "Bob" });
        users.Add(new Dictionary<int, object?> { [0] = 3, [1] = "Carla" });
        users.Add(new Dictionary<int, object?> { [0] = 4, [1] = "Davi" });

        using var connection = new FirebirdConnectionMock(db);
        connection.Open();

        var rows = connection.Query<int>("SELECT FIRST 2 SKIP 1 Id FROM Users ORDER BY Id").ToList();

        Assert.Equal([2, 3], rows);
    }

    /// <summary>
    /// EN: Verifies ROWS TO returns the expected inclusive range of rows in Firebird.
    /// PT-br: Verifica se ROWS TO retorna a faixa inclusiva esperada de linhas no Firebird.
    /// </summary>
    [Fact]
    [Trait("Category", "FirebirdDapper")]
    public void SelectRowsRange_ShouldReturnExpectedRows_Test()
    {
        var db = new FirebirdDbMock();
        var users = db.AddTable("Users");
        users.AddColumn("Id", DbType.Int32, false);
        users.AddColumn("Name", DbType.String, false);
        users.Add(new Dictionary<int, object?> { [0] = 1, [1] = "Alice" });
        users.Add(new Dictionary<int, object?> { [0] = 2, [1] = "Bob" });
        users.Add(new Dictionary<int, object?> { [0] = 3, [1] = "Carla" });
        users.Add(new Dictionary<int, object?> { [0] = 4, [1] = "Davi" });

        using var connection = new FirebirdConnectionMock(db);
        connection.Open();

        var rows = connection.Query<int>("SELECT Id FROM Users ORDER BY Id ROWS 2 TO 4").ToList();

        Assert.Equal([2, 3, 4], rows);
    }

    /// <summary>
    /// EN: Verifies NULLS FIRST and NULLS LAST ordering return the expected Firebird row order.
    /// PT-br: Verifica se ordenacoes NULLS FIRST e NULLS LAST retornam a ordem esperada de linhas no Firebird.
    /// </summary>
    [Fact]
    [Trait("Category", "FirebirdDapper")]
    public void SelectOrderByNullsModifier_ShouldReturnExpectedRows_Test()
    {
        var db = new FirebirdDbMock();
        var users = db.AddTable("Users");
        users.AddColumn("Id", DbType.Int32, false);
        users.AddColumn("Name", DbType.String, true);
        users.Add(new Dictionary<int, object?> { [0] = 1, [1] = "Alice" });
        users.Add(new Dictionary<int, object?> { [0] = 2, [1] = null });
        users.Add(new Dictionary<int, object?> { [0] = 3, [1] = "Bob" });

        using var connection = new FirebirdConnectionMock(db);
        connection.Open();

        var nullsFirst = connection.Query<int>("SELECT Id FROM Users ORDER BY Name NULLS FIRST, Id").ToList();
        var nullsLast = connection.Query<int>("SELECT Id FROM Users ORDER BY Name NULLS LAST, Id").ToList();

        Assert.Equal([2, 1, 3], nullsFirst);
        Assert.Equal([1, 3, 2], nullsLast);
    }
}
