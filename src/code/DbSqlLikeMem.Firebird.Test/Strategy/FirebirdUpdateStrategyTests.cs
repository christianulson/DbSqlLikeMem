namespace DbSqlLikeMem.Firebird.Test.Strategy;

/// <summary>
/// EN: Covers UPDATE execution scenarios in the Firebird mock.
/// PT: Cobre cenarios de execucao de UPDATE no mock Firebird.
/// </summary>
public sealed class FirebirdUpdateStrategyTests
{
    /// <summary>
    /// EN: Verifies that UPDATE modifies an existing row.
    /// PT: Verifica se UPDATE modifica uma linha existente.
    /// </summary>
    [Fact]
    [Trait("Category", "Strategy")]
    public void UpdateTableShouldModifyExistingRow()
    {
        var db = new FirebirdDbMock();
        var table = db.AddTable("users");
        table.AddColumn("id", DbType.Int32, false);
        table.AddColumn("name", DbType.String, false);
        table.Add(new Dictionary<int, object?> { [0] = 1, [1] = "John Doe" });

        using var connection = new FirebirdConnectionMock(db);
        using var command = new FirebirdCommandMock(connection)
        {
            CommandText = "UPDATE users SET name = 'Jane Doe' WHERE id = 1"
        };

        var rowsAffected = command.ExecuteNonQuery();

        Assert.Equal(1, rowsAffected);
        Assert.Single(table);
        Assert.Equal("Jane Doe", table[0][1]);
    }

    /// <summary>
    /// EN: Verifies that UPDATE returns zero when no rows match the predicate.
    /// PT: Verifica se UPDATE retorna zero quando nenhuma linha atende ao predicado.
    /// </summary>
    [Fact]
    [Trait("Category", "Strategy")]
    public void Update_ShouldReturnZero_WhenNoRowsMatchWhere()
    {
        var db = new FirebirdDbMock();
        var table = db.AddTable("users");
        table.AddColumn("id", DbType.Int32, false);
        table.AddColumn("name", DbType.String, false);
        table.Add(new Dictionary<int, object?> { [0] = 1, [1] = "John Doe" });

        using var connection = new FirebirdConnectionMock(db);
        using var command = new FirebirdCommandMock(connection)
        {
            CommandText = "UPDATE users SET name = 'X' WHERE id = 999"
        };

        var rowsAffected = command.ExecuteNonQuery();

        Assert.Equal(0, rowsAffected);
        Assert.Single(table);
        Assert.Equal("John Doe", table[0][1]);
    }

    /// <summary>
    /// EN: Verifies that UPDATE changes every row matched by the predicate.
    /// PT: Verifica se UPDATE altera todas as linhas batidas pelo predicado.
    /// </summary>
    [Fact]
    [Trait("Category", "Strategy")]
    public void Update_ShouldUpdateMultipleRows_WhenWhereMatchesMultiple()
    {
        var db = new FirebirdDbMock();
        var table = db.AddTable("users");
        table.AddColumn("id", DbType.Int32, false);
        table.AddColumn("name", DbType.String, false);
        table.Add(new Dictionary<int, object?> { [0] = 1, [1] = "A" });
        table.Add(new Dictionary<int, object?> { [0] = 1, [1] = "B" });
        table.Add(new Dictionary<int, object?> { [0] = 2, [1] = "C" });

        using var connection = new FirebirdConnectionMock(db);
        using var command = new FirebirdCommandMock(connection)
        {
            CommandText = "UPDATE users SET name = 'Z' WHERE id = 1"
        };

        var rowsAffected = command.ExecuteNonQuery();

        Assert.Equal(2, rowsAffected);
        Assert.Equal("Z", table[0][1]);
        Assert.Equal("Z", table[1][1]);
        Assert.Equal("C", table[2][1]);
    }
}
