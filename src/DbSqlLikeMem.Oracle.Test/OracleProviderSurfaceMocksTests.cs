namespace DbSqlLikeMem.Oracle.Test;

/// <summary>
/// EN: Summary for OracleProviderSurfaceMocksTests.
/// PT: Resumo para OracleProviderSurfaceMocksTests.
/// </summary>
public sealed class OracleProviderSurfaceMocksTests
{
    /// <summary>
    /// EN: Summary for DataAdapter_ShouldKeepTypedSelectCommand.
    /// PT: Resumo para DataAdapter_ShouldKeepTypedSelectCommand.
    /// </summary>
    [Fact]
    public void DataAdapter_ShouldKeepTypedSelectCommand()
    {
        using var connection = new OracleConnectionMock(new OracleDbMock());
        var adapter = new OracleDataAdapterMock("SELECT 1 FROM DUAL", connection);

        Assert.NotNull(adapter.SelectCommand);
        Assert.Equal("SELECT 1 FROM DUAL", adapter.SelectCommand!.CommandText);
    }

    /// <summary>
    /// EN: Summary for DataSource_ShouldCreateOracleConnection.
    /// PT: Resumo para DataSource_ShouldCreateOracleConnection.
    /// </summary>
    [Fact]
    public void DataSource_ShouldCreateOracleConnection()
    {
        var source = new OracleDataSourceMock(new OracleDbMock());
#if NET8_0_OR_GREATER
        using var connection = source.CreateConnection();
#else
        using var connection = source.CreateDbConnection();
#endif
        Assert.IsType<OracleConnectionMock>(connection);
    }

#if NET6_0_OR_GREATER
    /// <summary>
    /// EN: Summary for Batch_ShouldExecuteAllCommands.
    /// PT: Resumo para Batch_ShouldExecuteAllCommands.
    /// </summary>
    [Fact]
    public void Batch_ShouldExecuteAllCommands()
    {
        var db = new OracleDbMock();
        db.AddTable("Users", [
            new("Id", DbType.Int32, false),
            new("Name", DbType.String, false)
        ]);

        using var connection = new OracleConnectionMock(db);
        connection.Open();

        using var batch = new OracleBatchMock(connection);
        batch.BatchCommands.Add(new OracleBatchCommandMock { CommandText = "INSERT INTO Users (Id, Name) VALUES (1, 'Ana')" });
        batch.BatchCommands.Add(new OracleBatchCommandMock { CommandText = "INSERT INTO Users (Id, Name) VALUES (2, 'Beto')" });

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
        var db = new OracleDbMock();
        db.AddTable("Users", [
            new("Id", DbType.Int32, false),
            new("Name", DbType.String, false)
        ]);

        using var connection = new OracleConnectionMock(db);
        connection.Open();

        using (var seed = connection.CreateCommand())
        {
            seed.CommandText = "INSERT INTO Users (Id, Name) VALUES (1, 'Ana')";
            seed.ExecuteNonQuery();
        }

        using var batch = new OracleBatchMock(connection);
        batch.BatchCommands.Add(new OracleBatchCommandMock { CommandText = "SELECT Name FROM Users WHERE Id = 1" });
        batch.BatchCommands.Add(new OracleBatchCommandMock { CommandText = "SELECT Id FROM Users WHERE Id = 1" });

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
        var db = new OracleDbMock();
        db.AddTable("Users", [
            new("Id", DbType.Int32, false),
            new("Name", DbType.String, false)
        ]);

        using var connection = new OracleConnectionMock(db);
        connection.Open();

        using (var seed = connection.CreateCommand())
        {
            seed.CommandText = "INSERT INTO Users (Id, Name) VALUES (1, 'Ana')";
            seed.ExecuteNonQuery();
        }

        using var batch = new OracleBatchMock(connection);
        batch.BatchCommands.Add(new OracleBatchCommandMock { CommandText = "SELECT Name FROM Users WHERE Id = 1" });
        batch.BatchCommands.Add(new OracleBatchCommandMock { CommandText = "SELECT Id FROM Users WHERE Id = 1" });

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
        var db = new OracleDbMock();
        db.AddTable("Users", [
            new("Id", DbType.Int32, false),
            new("Name", DbType.String, false)
        ]);

        using var connection = new OracleConnectionMock(db);
        connection.Open();

        using var batch = new OracleBatchMock(connection);
        batch.BatchCommands.Add(new OracleBatchCommandMock { CommandText = "INSERT INTO Users (Id, Name) VALUES (10, 'Caio')" });
        batch.BatchCommands.Add(new OracleBatchCommandMock { CommandText = "SELECT Name FROM Users WHERE Id = 10" });

        using var reader = batch.ExecuteReader();
        Assert.True(reader.Read());
        Assert.Equal("Caio", reader.GetString(0));
    }
#endif
}
