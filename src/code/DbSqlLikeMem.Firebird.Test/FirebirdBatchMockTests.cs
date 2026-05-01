namespace DbSqlLikeMem.Firebird.Test;

#if NET8_0_OR_GREATER

/// <summary>
/// EN: Contains tests for Firebird batch mock behavior.
/// PT-br: Contem testes para o comportamento do simulado de lote Firebird.
/// </summary>
public sealed class FirebirdBatchMockTests(
        ITestOutputHelper helper
    ) : XUnitTestBase(helper)
{
    /// <summary>
    /// EN: Ensures non-query batch execution runs all commands and accumulates affected rows.
    /// PT-br: Garante que a execução não-consulta em lote execute todos os comandos e acumule as linhas afetadas.
    /// </summary>
    [Fact]
    [Trait("Category", "FirebirdBatchMock")]
    public void ExecuteNonQuery_ShouldExecuteAllBatchCommands()
    {
        var db = new FirebirdDbMock();
        db.AddTable("Users", [
            new("Id", DbType.Int32, false),
            new("Name", DbType.String, false)
        ]);

        using var connection = new FirebirdConnectionMock(db);
        connection.Open();

        using var batch = new FirebirdBatchMock(connection);
        batch.BatchCommands.Add(new FirebirdBatchCommandMock { CommandText = "INSERT INTO Users (Id, Name) VALUES (1, 'Ana')" });
        batch.BatchCommands.Add(new FirebirdBatchCommandMock { CommandText = "INSERT INTO Users (Id, Name) VALUES (2, 'Beto')" });

        var affected = batch.ExecuteNonQuery();

        affected.Should().Be(2);
        Assert.Collection(connection.GetTable("users"),
            _ => { },
            _ => { });
    }

    /// <summary>
    /// EN: Ensures scalar execution returns the first command result in the batch.
    /// PT-br: Garante que a execução escalar retorne o resultado do primeiro comando no lote.
    /// </summary>
    [Fact]
    [Trait("Category", "FirebirdBatchMock")]
    public void ExecuteScalar_ShouldUseFirstBatchCommandResult()
    {
        var db = new FirebirdDbMock();
        db.AddTable("Users", [
            new("Id", DbType.Int32, false),
            new("Name", DbType.String, false)
        ]);

        using var connection = new FirebirdConnectionMock(db);
        connection.Open();

        using (var seed = connection.CreateCommand())
        {
            seed.CommandText = "INSERT INTO Users (Id, Name) VALUES (1, 'Ana')";
            seed.ExecuteNonQuery();
        }

        using var batch = new FirebirdBatchMock(connection);
        batch.BatchCommands.Add(new FirebirdBatchCommandMock { CommandText = "SELECT Name FROM Users WHERE Id = 1" });
        batch.BatchCommands.Add(new FirebirdBatchCommandMock { CommandText = "SELECT Id FROM Users WHERE Id = 1" });

        var result = batch.ExecuteScalar();

        result.Should().Be("Ana");
    }

    /// <summary>
    /// EN: Ensures readers can iterate through result sets produced by multiple batch commands.
    /// PT-br: Garante que leitores possam iterar pelos conjuntos de resultados produzidos por múltiplos comandos em lote.
    /// </summary>
    [Fact]
    [Trait("Category", "FirebirdBatchMock")]
    public void ExecuteReader_ShouldReturnResultsFromMultipleBatchCommands()
    {
        var db = new FirebirdDbMock();
        db.AddTable("Users", [
            new("Id", DbType.Int32, false),
            new("Name", DbType.String, false)
        ]);

        using var connection = new FirebirdConnectionMock(db);
        connection.Open();

        using (var seed = connection.CreateCommand())
        {
            seed.CommandText = "INSERT INTO Users (Id, Name) VALUES (1, 'Ana')";
            seed.ExecuteNonQuery();
        }

        using var batch = new FirebirdBatchMock(connection);
        batch.BatchCommands.Add(new FirebirdBatchCommandMock { CommandText = "SELECT Name FROM Users WHERE Id = 1" });
        batch.BatchCommands.Add(new FirebirdBatchCommandMock { CommandText = "SELECT Id FROM Users WHERE Id = 1" });

        using var reader = batch.ExecuteReader();
        reader.Read().Should().BeTrue();
        reader.GetString(0).Should().Be("Ana");

        reader.NextResult().Should().BeTrue();
        reader.Read().Should().BeTrue();
        reader.GetInt32(0).Should().Be(1);
    }

    /// <summary>
    /// EN: Ensures batches can execute non-query commands before select commands.
    /// PT-br: Garante que lotes possam executar comandos não-consulta antes de comandos select.
    /// </summary>
    [Fact]
    [Trait("Category", "FirebirdBatchMock")]
    public void ExecuteReader_ShouldAllowNonQueryBeforeSelect()
    {
        var db = new FirebirdDbMock();
        db.AddTable("Users", [
            new("Id", DbType.Int32, false),
            new("Name", DbType.String, false)
        ]);

        using var connection = new FirebirdConnectionMock(db);
        connection.Open();

        using var batch = new FirebirdBatchMock(connection);
        batch.BatchCommands.Add(new FirebirdBatchCommandMock { CommandText = "INSERT INTO Users (Id, Name) VALUES (10, 'Caio')" });
        batch.BatchCommands.Add(new FirebirdBatchCommandMock { CommandText = "SELECT Name FROM Users WHERE Id = 10" });

        using var reader = batch.ExecuteReader();
        reader.Read().Should().BeTrue();
        reader.GetString(0).Should().Be("Caio");
    }
}

#endif
