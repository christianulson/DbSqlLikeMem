namespace DbSqlLikeMem.MySql.Test;

public sealed class MySqlBatchMockTests
{
    [Fact]
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

    [Fact]
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

    [Fact]
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

    [Fact]
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
