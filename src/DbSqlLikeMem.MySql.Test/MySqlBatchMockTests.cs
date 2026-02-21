namespace DbSqlLikeMem.MySql.Test;

/// <summary>
/// EN: Summary for MySqlBatchMockTests.
/// PT: Resumo para MySqlBatchMockTests.
/// </summary>
public sealed class MySqlBatchMockTests
{
    /// <summary>
    /// EN: Summary for ExecuteNonQuery_ShouldExecuteAllBatchCommands.
    /// PT: Resumo para ExecuteNonQuery_ShouldExecuteAllBatchCommands.
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
    /// EN: Summary for ExecuteScalar_ShouldUseFirstBatchCommandResult.
    /// PT: Resumo para ExecuteScalar_ShouldUseFirstBatchCommandResult.
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
    /// EN: Summary for ExecuteReader_ShouldReturnResultsFromMultipleBatchCommands.
    /// PT: Resumo para ExecuteReader_ShouldReturnResultsFromMultipleBatchCommands.
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
    /// EN: Summary for ExecuteReader_ShouldAllowNonQueryBeforeSelect.
    /// PT: Resumo para ExecuteReader_ShouldAllowNonQueryBeforeSelect.
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
