namespace DbSqlLikeMem.MySql.Test;

/// <summary>
/// EN: Contains tests for MySQL batch mock behavior.
/// PT: Contém testes para o comportamento do simulado de lote MySQL.
/// </summary>
public sealed class MySqlBatchMockTests
{
    /// <summary>
    /// EN: Ensures non-query batch execution runs all commands and accumulates affected rows.
    /// PT: Garante que a execução não-consulta em lote rode todos os comandos e acumule as linhas afetadas.
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
    /// EN: Ensures scalar execution returns the first command result in the batch.
    /// PT: Garante que a execução escalar retorne o resultado do primeiro comando no lote.
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
    /// EN: Ensures readers can iterate through result sets produced by multiple batch commands.
    /// PT: Garante que leitores possam iterar pelos conjuntos de resultados produzidos por múltiplos comandos em lote.
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
    /// EN: Ensures batches can execute non-query commands before select commands.
    /// PT: Garante que lotes possam executar comandos não-consulta antes de comandos select.
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
