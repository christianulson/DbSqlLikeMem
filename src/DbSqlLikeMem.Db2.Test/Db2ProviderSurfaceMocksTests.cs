namespace DbSqlLikeMem.Db2.Test;

/// <summary>
/// EN: Contains tests for db2 provider surface mocks.
/// PT: Contém testes para db2 provedor surface mocks.
/// </summary>
public sealed class Db2ProviderSurfaceMocksTests
{
    /// <summary>
    /// EN: Ensures the typed SelectCommand property stays synchronized with the base SelectCommand.
    /// PT: Garante que a propriedade tipada SelectCommand permaneça sincronizada com a SelectCommand da classe base.
    /// </summary>
    [Fact]
    public void DataAdapter_ShouldKeepTypedSelectCommand()
    {
        using var connection = new Db2ConnectionMock(new Db2DbMock());
        var adapter = new Db2DataAdapterMock("SELECT 1", connection);

        Assert.NotNull(adapter.SelectCommand);
        Assert.Equal("SELECT 1", adapter.SelectCommand!.CommandText);
    }

    /// <summary>
    /// EN: Ensures the data source mock creates a provider-specific connection bound to the same in-memory database.
    /// PT: Garante que o simulado de fonte de dados crie uma conexão específica do provedor vinculada ao mesmo banco em memória.
    /// </summary>
    [Fact]
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
    /// <summary>
    /// EN: Ensures batch execution runs all commands and returns the accumulated affected rows.
    /// PT: Garante que a execução em lote rode todos os comandos e retorne o total acumulado de linhas afetadas.
    /// </summary>
    [Fact]
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

    /// <summary>
    /// EN: Ensures scalar batch execution returns the first command scalar result.
    /// PT: Garante que a execução escalar do lote retorne o resultado escalar do primeiro comando.
    /// </summary>
    [Fact]
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

    /// <summary>
    /// EN: Ensures batch readers expose result sets from multiple commands.
    /// PT: Garante que leitores de lote exponham conjuntos de resultados de múltiplos comandos.
    /// </summary>
    [Fact]
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

    /// <summary>
    /// EN: Ensures non-query commands can be executed before select commands in the same batch.
    /// PT: Garante que comandos sem retorno possam ser executados antes de comandos select no mesmo lote.
    /// </summary>
    [Fact]
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
