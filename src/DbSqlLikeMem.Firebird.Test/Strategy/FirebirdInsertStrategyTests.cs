namespace DbSqlLikeMem.Firebird.Test.Strategy;

/// <summary>
/// EN: Covers single-row INSERT execution in the Firebird mock.
/// PT: Cobre execucao de INSERT de uma linha no mock Firebird.
/// </summary>
public sealed class FirebirdInsertStrategyTests
{
    /// <summary>
    /// EN: Verifies that a single INSERT adds one row to the target table.
    /// PT: Verifica se um INSERT simples adiciona uma linha na tabela alvo.
    /// </summary>
    [Fact]
    [Trait("Category", "Strategy")]
    public void InsertIntoTableShouldAddNewRow()
    {
        var db = new FirebirdDbMock();
        var table = db.AddTable("users");
        table.AddColumn("id", DbType.Int32, false);
        table.AddColumn("name", DbType.String, false);

        using var connection = new FirebirdConnectionMock(db);
        using var command = new FirebirdCommandMock(connection)
        {
            CommandText = "INSERT INTO users (id, name) VALUES (1, 'John Doe')"
        };

        var rowsAffected = command.ExecuteNonQuery();

        Assert.Equal(1, rowsAffected);
        Assert.Single(table);
        Assert.Equal(1, table[0][0]);
        Assert.Equal("John Doe", table[0][1]);
    }
}
