namespace DbSqlLikeMem.SqlAzure.Test;

/// <summary>
/// EN: Contains tests for SQL Azure provider surface mocks.
/// PT: Contém testes para os mocks de superfície do provedor SQL Azure.
/// </summary>
public sealed class SqlAzureProviderSurfaceMocksTests
{
    /// <summary>
    /// EN: Ensures SQL Azure command uses SQL Azure parameter collection mock type.
    /// PT: Garante que o comando SQL Azure use o tipo SQL Azure de coleção de parâmetros simulada.
    /// </summary>
    [Fact]
    public void Command_ShouldExposeSqlAzureParameterCollection()
    {
        using var connection = new SqlAzureConnectionMock(new SqlAzureDbMock());
        using var command = new SqlAzureCommandMock(connection);

        command.Parameters.Should().BeOfType<SqlAzureDataParameterCollectionMock>();
    }

    /// <summary>
    /// EN: Ensures typed SelectCommand stays synchronized with base SelectCommand.
    /// PT: Garante que o SelectCommand tipado permaneça sincronizado com o SelectCommand da classe base.
    /// </summary>
    [Fact]
    public void DataAdapter_ShouldKeepTypedSelectCommand()
    {
        using var connection = new SqlAzureConnectionMock(new SqlAzureDbMock());
        var adapter = new SqlAzureDataAdapterMock("SELECT 1", connection);

        Assert.NotNull(adapter.SelectCommand);
        Assert.Equal("SELECT 1", adapter.SelectCommand!.CommandText);
    }

    /// <summary>
    /// EN: Ensures all typed command properties remain synchronized with base DbDataAdapter command slots.
    /// PT: Garante que todas as propriedades de comando tipadas permaneçam sincronizadas com os slots de comando base de DbDataAdapter.
    /// </summary>
    [Fact]
    public void DataAdapter_ShouldKeepAllTypedCommandsSynchronized()
    {
        using var connection = new SqlAzureConnectionMock(new SqlAzureDbMock());
        using var select = new SqlAzureCommandMock(connection) { CommandText = "SELECT 1" };
        using var insert = new SqlAzureCommandMock(connection) { CommandText = "INSERT INTO Users (Id) VALUES (1)" };
        using var update = new SqlAzureCommandMock(connection) { CommandText = "UPDATE Users SET Id = 2 WHERE Id = 1" };
        using var delete = new SqlAzureCommandMock(connection) { CommandText = "DELETE FROM Users WHERE Id = 2" };

        var adapter = new SqlAzureDataAdapterMock
        {
            SelectCommand = select,
            InsertCommand = insert,
            UpdateCommand = update,
            DeleteCommand = delete
        };

        Assert.Same(select, adapter.SelectCommand);
        Assert.Same(insert, adapter.InsertCommand);
        Assert.Same(update, adapter.UpdateCommand);
        Assert.Same(delete, adapter.DeleteCommand);
    }

    /// <summary>
    /// EN: Ensures data source creates SQL Azure connections bound to the same in-memory database.
    /// PT: Garante que a fonte de dados crie conexões SQL Azure ligadas ao mesmo banco em memória.
    /// </summary>
    [Fact]
    public void DataSource_ShouldCreateSqlAzureConnection()
    {
        var source = new SqlAzureDataSourceMock([]);
#if NET8_0_OR_GREATER
        using var connection = source.CreateConnection();
#else
        using var connection = source.CreateDbConnection();
#endif
        Assert.IsType<SqlAzureConnectionMock>(connection);
    }

    /// <summary>
    /// EN: Ensures SQL Azure command supports multi-statement INSERT script in ExecuteNonQuery.
    /// PT: Garante que o comando SQL Azure suporte script de INSERT multi-statement no ExecuteNonQuery.
    /// </summary>
    [Fact]
    public void Command_ExecuteNonQuery_WithMultiStatementInsertScript_ShouldInsertAllRowsAndReturnTotalAffected()
    {
        var db = new SqlAzureDbMock();
        db.AddTable("Users", [
            new("Id", DbType.Int32, false),
            new("Name", DbType.String, false),
            new("Email", DbType.String, true)
        ]);

        using var connection = new SqlAzureConnectionMock(db);
        connection.Open();

        using var command = new SqlAzureCommandMock(connection)
        {
            CommandText = """
                INSERT INTO Users (Id, Name, Email) VALUES (101, 'Ana', NULL);
                INSERT INTO Users (Id, Name, Email) VALUES (102, 'Bia', NULL);
                INSERT INTO Users (Id, Name, Email) VALUES (103, 'Caio', NULL);
                """
        };

        var rowsAffected = command.ExecuteNonQuery();

        Assert.Equal(3, rowsAffected);
        var users = connection.GetTable("Users");
        Assert.Equal(3, users.Count);
        Assert.Equal("Ana", users[0][1]);
        Assert.Equal("Bia", users[1][1]);
        Assert.Equal("Caio", users[2][1]);
    }

#if NET6_0_OR_GREATER
    /// <summary>
    /// EN: Ensures batch execution runs all commands and returns accumulated affected rows.
    /// PT: Garante que a execução em lote rode todos os comandos e retorne o total acumulado de linhas afetadas.
    /// </summary>
    [Fact]
    public void Batch_ShouldExecuteAllCommands()
    {
        var db = new SqlAzureDbMock();
        db.AddTable("Users", [
            new("Id", DbType.Int32, false),
            new("Name", DbType.String, false)
        ]);

        using var connection = new SqlAzureConnectionMock(db);
        connection.Open();

        using var batch = new SqlAzureBatchMock(connection);
        batch.BatchCommands.Add(new SqlAzureBatchCommandMock { CommandText = "INSERT INTO Users (Id, Name) VALUES (1, 'Ana')" });
        batch.BatchCommands.Add(new SqlAzureBatchCommandMock { CommandText = "INSERT INTO Users (Id, Name) VALUES (2, 'Beto')" });

        var affected = batch.ExecuteNonQuery();

        Assert.Equal(2, affected);
        Assert.Equal(2, connection.GetTable("Users").Count);
    }

    /// <summary>
    /// EN: Ensures scalar batch execution returns first command scalar result.
    /// PT: Garante que a execução escalar do lote retorne o resultado escalar do primeiro comando.
    /// </summary>
    [Fact]
    public void Batch_ExecuteScalar_ShouldUseFirstCommandResult()
    {
        var db = new SqlAzureDbMock();
        db.AddTable("Users", [
            new("Id", DbType.Int32, false),
            new("Name", DbType.String, false)
        ]);

        using var connection = new SqlAzureConnectionMock(db);
        connection.Open();

        using (var seed = connection.CreateCommand())
        {
            seed.CommandText = "INSERT INTO Users (Id, Name) VALUES (1, 'Ana')";
            seed.ExecuteNonQuery();
        }

        using var batch = new SqlAzureBatchMock(connection);
        batch.BatchCommands.Add(new SqlAzureBatchCommandMock { CommandText = "SELECT Name FROM Users WHERE Id = 1" });
        batch.BatchCommands.Add(new SqlAzureBatchCommandMock { CommandText = "SELECT Id FROM Users WHERE Id = 1" });

        var result = batch.ExecuteScalar();

        Assert.Equal("Ana", result);
    }

    /// <summary>
    /// EN: Ensures batch readers expose result sets from multiple commands.
    /// PT: Garante que leitores em lote exponham conjuntos de resultados de múltiplos comandos.
    /// </summary>
    [Fact]
    public void Batch_ExecuteReader_ShouldReturnResultsFromMultipleCommands()
    {
        var db = new SqlAzureDbMock();
        db.AddTable("Users", [
            new("Id", DbType.Int32, false),
            new("Name", DbType.String, false)
        ]);

        using var connection = new SqlAzureConnectionMock(db);
        connection.Open();

        using (var seed = connection.CreateCommand())
        {
            seed.CommandText = "INSERT INTO Users (Id, Name) VALUES (1, 'Ana')";
            seed.ExecuteNonQuery();
        }

        using var batch = new SqlAzureBatchMock(connection);
        batch.BatchCommands.Add(new SqlAzureBatchCommandMock { CommandText = "SELECT Name FROM Users WHERE Id = 1" });
        batch.BatchCommands.Add(new SqlAzureBatchCommandMock { CommandText = "SELECT Id FROM Users WHERE Id = 1" });

        using var reader = batch.ExecuteReader();
        Assert.True(reader.Read());
        Assert.Equal("Ana", reader.GetString(0));

        Assert.True(reader.NextResult());
        Assert.True(reader.Read());
        Assert.Equal(1, reader.GetInt32(0));
    }

    /// <summary>
    /// EN: Ensures non-query commands can run before select commands in the same batch.
    /// PT: Garante que comandos sem retorno possam rodar antes de select no mesmo lote.
    /// </summary>
    [Fact]
    public void Batch_ExecuteReader_ShouldAllowNonQueryBeforeSelect()
    {
        var db = new SqlAzureDbMock();
        db.AddTable("Users", [
            new("Id", DbType.Int32, false),
            new("Name", DbType.String, false)
        ]);

        using var connection = new SqlAzureConnectionMock(db);
        connection.Open();

        using var batch = new SqlAzureBatchMock(connection);
        batch.BatchCommands.Add(new SqlAzureBatchCommandMock { CommandText = "INSERT INTO Users (Id, Name) VALUES (10, 'Caio')" });
        batch.BatchCommands.Add(new SqlAzureBatchCommandMock { CommandText = "SELECT Name FROM Users WHERE Id = 10" });

        using var reader = batch.ExecuteReader();
        Assert.True(reader.Read());
        Assert.Equal("Caio", reader.GetString(0));
    }
#endif
}
