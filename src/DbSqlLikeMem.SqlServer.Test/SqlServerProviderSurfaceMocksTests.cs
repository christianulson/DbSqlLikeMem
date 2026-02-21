namespace DbSqlLikeMem.SqlServer.Test;

/// <summary>
/// EN: Summary for SqlServerProviderSurfaceMocksTests.
/// PT: Resumo para SqlServerProviderSurfaceMocksTests.
/// </summary>
public sealed class SqlServerProviderSurfaceMocksTests
{
    /// <summary>
    /// EN: Summary for DataAdapter_ShouldKeepTypedSelectCommand.
    /// PT: Resumo para DataAdapter_ShouldKeepTypedSelectCommand.
    /// </summary>
    [Fact]
    public void DataAdapter_ShouldKeepTypedSelectCommand()
    {
        using var connection = new SqlServerConnectionMock(new SqlServerDbMock());
        var adapter = new SqlServerDataAdapterMock("SELECT 1", connection);

        Assert.NotNull(adapter.SelectCommand);
        Assert.Equal("SELECT 1", adapter.SelectCommand!.CommandText);
    }

    /// <summary>
    /// EN: Summary for DataSource_ShouldCreateSqlServerConnection.
    /// PT: Resumo para DataSource_ShouldCreateSqlServerConnection.
    /// </summary>
    [Fact]
    public void DataSource_ShouldCreateSqlServerConnection()
    {
        var source = new SqlServerDataSourceMock(new SqlServerDbMock());
#if NET8_0_OR_GREATER
        using var connection = source.CreateConnection();
#else
        using var connection = source.CreateDbConnection();
#endif
        Assert.IsType<SqlServerConnectionMock>(connection);
    }

#if NET6_0_OR_GREATER
    /// <summary>
    /// EN: Summary for Batch_ShouldExecuteAllCommands.
    /// PT: Resumo para Batch_ShouldExecuteAllCommands.
    /// </summary>
    [Fact]
    public void Batch_ShouldExecuteAllCommands()
    {
        var db = new SqlServerDbMock();
        db.AddTable("Users", [
            new("Id", DbType.Int32, false),
            new("Name", DbType.String, false)
        ]);

        using var connection = new SqlServerConnectionMock(db);
        connection.Open();

        using var batch = new SqlServerBatchMock(connection);
        batch.BatchCommands.Add(new SqlServerBatchCommandMock { CommandText = "INSERT INTO Users (Id, Name) VALUES (1, 'Ana')" });
        batch.BatchCommands.Add(new SqlServerBatchCommandMock { CommandText = "INSERT INTO Users (Id, Name) VALUES (2, 'Beto')" });

        var affected = batch.ExecuteNonQuery();

        Assert.Equal(2, affected);
        Assert.Equal(2, connection.GetTable("Users").Count);
    }

    /// <summary>
    /// EN: Summary for Batch_ExecuteScalar_ShouldUseFirstCommandResult.
    /// PT: Resumo para Batch_ExecuteScalar_ShouldUseFirstCommandResult.
    /// </summary>
    [Fact]
    public void Batch_ExecuteScalar_ShouldUseFirstCommandResult()
    {
        var db = new SqlServerDbMock();
        db.AddTable("Users", [
            new("Id", DbType.Int32, false),
            new("Name", DbType.String, false)
        ]);

        using var connection = new SqlServerConnectionMock(db);
        connection.Open();

        using (var seed = connection.CreateCommand())
        {
            seed.CommandText = "INSERT INTO Users (Id, Name) VALUES (1, 'Ana')";
            seed.ExecuteNonQuery();
        }

        using var batch = new SqlServerBatchMock(connection);
        batch.BatchCommands.Add(new SqlServerBatchCommandMock { CommandText = "SELECT Name FROM Users WHERE Id = 1" });
        batch.BatchCommands.Add(new SqlServerBatchCommandMock { CommandText = "SELECT Id FROM Users WHERE Id = 1" });

        var result = batch.ExecuteScalar();

        Assert.Equal("Ana", result);
    }

    /// <summary>
    /// EN: Summary for Batch_ExecuteReader_ShouldReturnResultsFromMultipleCommands.
    /// PT: Resumo para Batch_ExecuteReader_ShouldReturnResultsFromMultipleCommands.
    /// </summary>
    [Fact]
    public void Batch_ExecuteReader_ShouldReturnResultsFromMultipleCommands()
    {
        var db = new SqlServerDbMock();
        db.AddTable("Users", [
            new("Id", DbType.Int32, false),
            new("Name", DbType.String, false)
        ]);

        using var connection = new SqlServerConnectionMock(db);
        connection.Open();

        using (var seed = connection.CreateCommand())
        {
            seed.CommandText = "INSERT INTO Users (Id, Name) VALUES (1, 'Ana')";
            seed.ExecuteNonQuery();
        }

        using var batch = new SqlServerBatchMock(connection);
        batch.BatchCommands.Add(new SqlServerBatchCommandMock { CommandText = "SELECT Name FROM Users WHERE Id = 1" });
        batch.BatchCommands.Add(new SqlServerBatchCommandMock { CommandText = "SELECT Id FROM Users WHERE Id = 1" });

        using var reader = batch.ExecuteReader();
        Assert.True(reader.Read());
        Assert.Equal("Ana", reader.GetString(0));

        Assert.True(reader.NextResult());
        Assert.True(reader.Read());
        Assert.Equal(1, reader.GetInt32(0));
    }

    /// <summary>
    /// EN: Summary for Batch_ExecuteReader_ShouldAllowNonQueryBeforeSelect.
    /// PT: Resumo para Batch_ExecuteReader_ShouldAllowNonQueryBeforeSelect.
    /// </summary>
    [Fact]
    public void Batch_ExecuteReader_ShouldAllowNonQueryBeforeSelect()
    {
        var db = new SqlServerDbMock();
        db.AddTable("Users", [
            new("Id", DbType.Int32, false),
            new("Name", DbType.String, false)
        ]);

        using var connection = new SqlServerConnectionMock(db);
        connection.Open();

        using var batch = new SqlServerBatchMock(connection);
        batch.BatchCommands.Add(new SqlServerBatchCommandMock { CommandText = "INSERT INTO Users (Id, Name) VALUES (10, 'Caio')" });
        batch.BatchCommands.Add(new SqlServerBatchCommandMock { CommandText = "SELECT Name FROM Users WHERE Id = 10" });

        using var reader = batch.ExecuteReader();
        Assert.True(reader.Read());
        Assert.Equal("Caio", reader.GetString(0));
    }
#endif
}
