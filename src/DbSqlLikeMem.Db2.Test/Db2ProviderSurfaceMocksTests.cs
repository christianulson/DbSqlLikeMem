namespace DbSqlLikeMem.Db2.Test;

/// <summary>
/// EN: Summary for Db2ProviderSurfaceMocksTests.
/// PT: Resumo para Db2ProviderSurfaceMocksTests.
/// </summary>
public sealed class Db2ProviderSurfaceMocksTests
{
    [Fact]
    /// <summary>
    /// EN: Summary for DataAdapter_ShouldKeepTypedSelectCommand.
    /// PT: Resumo para DataAdapter_ShouldKeepTypedSelectCommand.
    /// </summary>
    public void DataAdapter_ShouldKeepTypedSelectCommand()
    {
        using var connection = new Db2ConnectionMock(new Db2DbMock());
        var adapter = new Db2DataAdapterMock("SELECT 1", connection);

        Assert.NotNull(adapter.SelectCommand);
        Assert.Equal("SELECT 1", adapter.SelectCommand!.CommandText);
    }

    [Fact]
    /// <summary>
    /// EN: Summary for DataSource_ShouldCreateDb2Connection.
    /// PT: Resumo para DataSource_ShouldCreateDb2Connection.
    /// </summary>
    public void DataSource_ShouldCreateDb2Connection()
    {
        var source = new Db2DataSourceMock(new Db2DbMock());
#if NET8_0_OR_GREATER
        using var connection = source.CreateConnection();
#else
        using var connection = source.CreateDbConnection();
#endif
        Assert.IsType<Db2ConnectionMock>(connection);
    }

#if NET6_0_OR_GREATER
    [Fact]
    /// <summary>
    /// EN: Summary for Batch_ShouldExecuteAllCommands.
    /// PT: Resumo para Batch_ShouldExecuteAllCommands.
    /// </summary>
    public void Batch_ShouldExecuteAllCommands()
    {
        var db = new Db2DbMock();
        db.AddTable("users", [
            new("id", DbType.Int32, false),
            new("name", DbType.String, false)
        ]);

        using var connection = new Db2ConnectionMock(db);
        connection.Open();

        using var batch = new Db2BatchMock(connection);
        batch.BatchCommands.Add(new Db2BatchCommandMock { CommandText = "INSERT INTO users (id, name) VALUES (1, 'Ana')" });
        batch.BatchCommands.Add(new Db2BatchCommandMock { CommandText = "INSERT INTO users (id, name) VALUES (2, 'Beto')" });

        var affected = batch.ExecuteNonQuery();

        Assert.Equal(2, affected);
        Assert.Equal(2, connection.GetTable("users").Count);
    }

    [Fact]
    /// <summary>
    /// EN: Summary for Batch_ExecuteScalar_ShouldUseFirstCommandResult.
    /// PT: Resumo para Batch_ExecuteScalar_ShouldUseFirstCommandResult.
    /// </summary>
    public void Batch_ExecuteScalar_ShouldUseFirstCommandResult()
    {
        var db = new Db2DbMock();
        db.AddTable("users", [
            new("id", DbType.Int32, false),
            new("name", DbType.String, false)
        ]);

        using var connection = new Db2ConnectionMock(db);
        connection.Open();

        using (var seed = connection.CreateCommand())
        {
            seed.CommandText = "INSERT INTO users (id, name) VALUES (1, 'Ana')";
            seed.ExecuteNonQuery();
        }

        using var batch = new Db2BatchMock(connection);
        batch.BatchCommands.Add(new Db2BatchCommandMock { CommandText = "SELECT name FROM users WHERE id = 1" });
        batch.BatchCommands.Add(new Db2BatchCommandMock { CommandText = "SELECT id FROM users WHERE id = 1" });

        var result = batch.ExecuteScalar();

        Assert.Equal("Ana", result);
    }

    [Fact]
    /// <summary>
    /// EN: Summary for Batch_ExecuteReader_ShouldReturnResultsFromMultipleCommands.
    /// PT: Resumo para Batch_ExecuteReader_ShouldReturnResultsFromMultipleCommands.
    /// </summary>
    public void Batch_ExecuteReader_ShouldReturnResultsFromMultipleCommands()
    {
        var db = new Db2DbMock();
        db.AddTable("users", [
            new("id", DbType.Int32, false),
            new("name", DbType.String, false)
        ]);

        using var connection = new Db2ConnectionMock(db);
        connection.Open();

        using (var seed = connection.CreateCommand())
        {
            seed.CommandText = "INSERT INTO users (id, name) VALUES (1, 'Ana')";
            seed.ExecuteNonQuery();
        }

        using var batch = new Db2BatchMock(connection);
        batch.BatchCommands.Add(new Db2BatchCommandMock { CommandText = "SELECT name FROM users WHERE id = 1" });
        batch.BatchCommands.Add(new Db2BatchCommandMock { CommandText = "SELECT id FROM users WHERE id = 1" });

        using var reader = batch.ExecuteReader();
        Assert.True(reader.Read());
        Assert.Equal("Ana", reader.GetString(0));

        Assert.True(reader.NextResult());
        Assert.True(reader.Read());
        Assert.Equal(1, reader.GetInt32(0));
    }

    [Fact]
    /// <summary>
    /// EN: Summary for Batch_ExecuteReader_ShouldAllowNonQueryBeforeSelect.
    /// PT: Resumo para Batch_ExecuteReader_ShouldAllowNonQueryBeforeSelect.
    /// </summary>
    public void Batch_ExecuteReader_ShouldAllowNonQueryBeforeSelect()
    {
        var db = new Db2DbMock();
        db.AddTable("users", [
            new("id", DbType.Int32, false),
            new("name", DbType.String, false)
        ]);

        using var connection = new Db2ConnectionMock(db);
        connection.Open();

        using var batch = new Db2BatchMock(connection);
        batch.BatchCommands.Add(new Db2BatchCommandMock { CommandText = "INSERT INTO users (id, name) VALUES (10, 'Caio')" });
        batch.BatchCommands.Add(new Db2BatchCommandMock { CommandText = "SELECT name FROM users WHERE id = 10" });

        using var reader = batch.ExecuteReader();
        Assert.True(reader.Read());
        Assert.Equal("Caio", reader.GetString(0));
    }
#endif
}
