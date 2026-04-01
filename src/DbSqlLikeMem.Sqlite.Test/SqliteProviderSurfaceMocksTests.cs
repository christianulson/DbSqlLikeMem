using FluentAssertions;

namespace DbSqlLikeMem.Sqlite.Test;

/// <summary>
/// EN: Contains tests for sqlite provider surface mocks.
/// PT: Contém testes para sqlite provedor surface mocks.
/// </summary>
public sealed class SqliteProviderSurfaceMocksTests(
        ITestOutputHelper helper
    ) : XUnitTestBase(helper)
{
    /// <summary>
    /// EN: Ensures the typed SelectCommand property stays synchronized with the base SelectCommand.
    /// PT: Garante que a propriedade tipada SelectCommand permaneça sincronizada com a SelectCommand da classe base.
    /// </summary>
    [Fact]
    public void DataAdapter_ShouldKeepTypedSelectCommand()
    {
        using var connection = new SqliteConnectionMock(new SqliteDbMock());
        var adapter = new SqliteDataAdapterMock("SELECT 1", connection);

        adapter.SelectCommand.Should().NotBeNull();
        adapter.SelectCommand!.CommandText.Should().Be("SELECT 1");
    }

    /// <summary>
    /// EN: Ensures the data source mock creates a provider-specific connection bound to the same in-memory database.
    /// PT: Garante que o simulado de fonte de dados crie uma conexão específica do provedor vinculada ao mesmo banco em memória.
    /// </summary>
    [Fact]
    public void DataSource_ShouldCreateSqliteConnection()
    {
        var source = new SqliteDataSourceMock([]);
#if NET8_0_OR_GREATER
        using var connection = source.CreateConnection();
#else
        using var connection = source.CreateDbConnection();
#endif
        connection.Should().BeOfType<SqliteConnectionMock>();
    }

#if NET8_0_OR_GREATER
    /// <summary>
    /// EN: Ensures batch execution runs all commands and returns the accumulated affected rows.
    /// PT: Garante que a execução em lote rode todos os comandos e retorne o total acumulado de linhas afetadas.
    /// </summary>
    [Fact]
    public void Batch_ShouldExecuteAllCommands()
    {
        var db = new SqliteDbMock();
        db.AddTable("users", [
            new("id", DbType.Int32, false),
            new("name", DbType.String, false)
        ]);

        using var connection = new SqliteConnectionMock(db);
        connection.Open();

        using var batch = new SqliteBatchMock(connection);
        batch.BatchCommands.Add(new SqliteBatchCommandMock { CommandText = "INSERT INTO users (id, name) VALUES (1, 'Ana')" });
        batch.BatchCommands.Add(new SqliteBatchCommandMock { CommandText = "INSERT INTO users (id, name) VALUES (2, 'Beto')" });

        var affected = batch.ExecuteNonQuery();

        affected.Should().Be(2);
        connection.GetTable("users").Count.Should().Be(2);
    }

    /// <summary>
    /// EN: Ensures scalar batch execution returns the first command scalar result.
    /// PT: Garante que a execução escalar do lote retorne o resultado escalar do primeiro comando.
    /// </summary>
    [Fact]
    public void Batch_ExecuteScalar_ShouldUseFirstCommandResult()
    {
        var db = new SqliteDbMock();
        db.AddTable("users", [
            new("id", DbType.Int32, false),
            new("name", DbType.String, false)
        ]);

        using var connection = new SqliteConnectionMock(db);
        connection.Open();

        using (var seed = connection.CreateCommand())
        {
            seed.CommandText = "INSERT INTO users (id, name) VALUES (1, 'Ana')";
            seed.ExecuteNonQuery();
        }

        using var batch = new SqliteBatchMock(connection);
        batch.BatchCommands.Add(new SqliteBatchCommandMock { CommandText = "SELECT name FROM users WHERE id = 1" });
        batch.BatchCommands.Add(new SqliteBatchCommandMock { CommandText = "SELECT id FROM users WHERE id = 1" });

        var result = batch.ExecuteScalar();

        result.Should().Be("Ana");
    }

    /// <summary>
    /// EN: Ensures batch readers expose result sets from multiple commands.
    /// PT: Garante que leitores de lote exponham conjuntos de resultados de múltiplos comandos.
    /// </summary>
    [Fact]
    public void Batch_ExecuteReader_ShouldReturnResultsFromMultipleCommands()
    {
        var db = new SqliteDbMock();
        db.AddTable("users", [
            new("id", DbType.Int32, false),
            new("name", DbType.String, false)
        ]);

        using var connection = new SqliteConnectionMock(db);
        connection.Open();

        using (var seed = connection.CreateCommand())
        {
            seed.CommandText = "INSERT INTO users (id, name) VALUES (1, 'Ana')";
            seed.ExecuteNonQuery();
        }

        using var batch = new SqliteBatchMock(connection);
        batch.BatchCommands.Add(new SqliteBatchCommandMock { CommandText = "SELECT name FROM users WHERE id = 1" });
        batch.BatchCommands.Add(new SqliteBatchCommandMock { CommandText = "SELECT id FROM users WHERE id = 1" });

        using var reader = batch.ExecuteReader();
        reader.Read().Should().BeTrue();
        reader.GetString(0).Should().Be("Ana");

        reader.NextResult().Should().BeTrue();
        reader.Read().Should().BeTrue();
        reader.GetInt32(0).Should().Be(1);
    }

    /// <summary>
    /// EN: Ensures non-query commands can be executed before select commands in the same batch.
    /// PT: Garante que comandos sem retorno possam ser executados antes de comandos select no mesmo lote.
    /// </summary>
    [Fact]
    public void Batch_ExecuteReader_ShouldAllowNonQueryBeforeSelect()
    {
        var db = new SqliteDbMock();
        db.AddTable("users", [
            new("id", DbType.Int32, false),
            new("name", DbType.String, false)
        ]);

        using var connection = new SqliteConnectionMock(db);
        connection.Open();

        using var batch = new SqliteBatchMock(connection);
        batch.BatchCommands.Add(new SqliteBatchCommandMock { CommandText = "INSERT INTO users (id, name) VALUES (10, 'Caio')" });
        batch.BatchCommands.Add(new SqliteBatchCommandMock { CommandText = "SELECT name FROM users WHERE id = 10" });

        using var reader = batch.ExecuteReader();
        reader.Read().Should().BeTrue();
        reader.GetString(0).Should().Be("Caio");
    }
#endif
}
