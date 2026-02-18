namespace DbSqlLikeMem.MySql.Test;

/// <summary>
/// EN: Validates batch command execution behavior in <see cref="MySqlBatchMock"/>.
/// PT: Valida o comportamento de execução de comandos em lote no <see cref="MySqlBatchMock"/>.
/// </summary>
public sealed class MySqlBatchMockTests
{
    /// <summary>
    /// EN: Ensures ExecuteNonQuery executes all batch commands and returns the affected rows count.
    /// PT: Garante que o ExecuteNonQuery execute todos os comandos do lote e retorne a quantidade de linhas afetadas.
    /// </summary>
    [Fact]
    [Trait("Category", "MySqlBatchMock")]
    public void ExecuteNonQuery_ShouldExecuteAllBatchCommands()
    {
        var db = new MySqlDbMock();
        db.AddTable("Users", [
            new("Id", DbType.Int32, false),
            new("Name", DbType.String, false)
        ]);

        using var connection = new MySqlConnectionMock(db);
        connection.Open();

        using var batch = new MySqlBatchMock(connection);
        batch.BatchCommands.Add(new MySqlBatchCommandMock("INSERT INTO Users (Id, Name) VALUES (1, 'Ana')"));
        batch.BatchCommands.Add(new MySqlBatchCommandMock("INSERT INTO Users (Id, Name) VALUES (2, 'Beto')"));

        var affected = batch.ExecuteNonQuery();

        Assert.Equal(2, affected);
        Assert.Equal(2, connection.GetTable("users").Count);
    }

    /// <summary>
    /// EN: Ensures ExecuteScalar returns the first command scalar result from the batch.
    /// PT: Garante que o ExecuteScalar retorne o resultado escalar do primeiro comando do lote.
    /// </summary>
    [Fact]
    [Trait("Category", "MySqlBatchMock")]
    public void ExecuteScalar_ShouldUseFirstBatchCommandResult()
    {
        var db = new MySqlDbMock();
        db.AddTable("Users", [
            new("Id", DbType.Int32, false),
            new("Name", DbType.String, false)
        ]);

        using var connection = new MySqlConnectionMock(db);
        connection.Open();

        using (var seed = connection.CreateCommand())
        {
            seed.CommandText = "INSERT INTO Users (Id, Name) VALUES (1, 'Ana')";
            seed.ExecuteNonQuery();
        }

        using var batch = new MySqlBatchMock(connection);
        batch.BatchCommands.Add(new MySqlBatchCommandMock("SELECT Name FROM Users WHERE Id = 1"));
        batch.BatchCommands.Add(new MySqlBatchCommandMock("SELECT Id FROM Users WHERE Id = 1"));

        var result = batch.ExecuteScalar();

        Assert.Equal("Ana", result);
    }

    /// <summary>
    /// EN: Ensures ExecuteReader returns multiple result sets produced by sequential batch commands.
    /// PT: Garante que o ExecuteReader retorne múltiplos conjuntos de resultados produzidos por comandos em lote sequenciais.
    /// </summary>
    [Fact]
    [Trait("Category", "MySqlBatchMock")]
    public void ExecuteReader_ShouldReturnResultsFromMultipleBatchCommands()
    {
        var db = new MySqlDbMock();
        db.AddTable("Users", [
            new("Id", DbType.Int32, false),
            new("Name", DbType.String, false)
        ]);

        using var connection = new MySqlConnectionMock(db);
        connection.Open();

        using (var seed = connection.CreateCommand())
        {
            seed.CommandText = "INSERT INTO Users (Id, Name) VALUES (1, 'Ana')";
            seed.ExecuteNonQuery();
        }

        using var batch = new MySqlBatchMock(connection);
        batch.BatchCommands.Add(new MySqlBatchCommandMock("SELECT Name FROM Users WHERE Id = 1"));
        batch.BatchCommands.Add(new MySqlBatchCommandMock("SELECT Id FROM Users WHERE Id = 1"));

        using var reader = batch.ExecuteReader();
        Assert.True(reader.Read());
        Assert.Equal("Ana", reader.GetString(0));

        Assert.True(reader.NextResult());
        Assert.True(reader.Read());
        Assert.Equal(1, reader.GetInt32(0));
    }

    /// <summary>
    /// EN: Ensures ExecuteReader supports batches that execute non-query commands before select queries.
    /// PT: Garante que o ExecuteReader suporte lotes que executam comandos sem retorno antes de consultas select.
    /// </summary>
    [Fact]
    [Trait("Category", "MySqlBatchMock")]
    public void ExecuteReader_ShouldAllowNonQueryBeforeSelect()
    {
        var db = new MySqlDbMock();
        db.AddTable("Users", [
            new("Id", DbType.Int32, false),
            new("Name", DbType.String, false)
        ]);

        using var connection = new MySqlConnectionMock(db);
        connection.Open();

        using var batch = new MySqlBatchMock(connection);
        batch.BatchCommands.Add(new MySqlBatchCommandMock("INSERT INTO Users (Id, Name) VALUES (10, 'Caio')"));
        batch.BatchCommands.Add(new MySqlBatchCommandMock("SELECT Name FROM Users WHERE Id = 10"));

        using var reader = batch.ExecuteReader();
        Assert.True(reader.Read());
        Assert.Equal("Caio", reader.GetString(0));
    }

}
