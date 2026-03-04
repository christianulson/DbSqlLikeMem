namespace DbSqlLikeMem.SqlAzure.Test;

/// <summary>
/// EN: Covers SQL Azure dialect behaviors inherited from SQL Server semantics.
/// PT: Cobre comportamentos de dialeto do SQL Azure herdados da semântica do SQL Server.
/// </summary>
public sealed class SqlAzureDialectBehaviorTests
{
    private static SqlAzureConnectionMock CreateOpenConnection()
    {
        var db = new SqlAzureDbMock();
        db.AddTable("Users", [
            new("Id", DbType.Int32, false),
            new("Name", DbType.String, false),
            new("Email", DbType.String, true)
        ]);

        var connection = new SqlAzureConnectionMock(db);
        connection.Open();
        return connection;
    }

    /// <summary>
    /// EN: Ensures SQL Server-style table hints remain accepted through SQL Azure provider mocks.
    /// PT: Garante que table hints no estilo SQL Server continuem aceitos pelos mocks do provedor SQL Azure.
    /// </summary>
    [Fact]
    public void Select_WithSqlServerTableHints_ShouldExecute()
    {
        using var connection = CreateOpenConnection();
        using var seed = new SqlAzureCommandMock(connection)
        {
            CommandText = "INSERT INTO Users (Id, Name, Email) VALUES (10, 'Hint User', 'hint@example.com')"
        };
        seed.ExecuteNonQuery();

        using var command = new SqlAzureCommandMock(connection)
        {
            CommandText = "SELECT Name FROM Users WITH (NOLOCK, INDEX([IX_Users_Name])) WHERE Id = 10"
        };

        var name = command.ExecuteScalar();
        Assert.Equal("Hint User", name);
    }

    /// <summary>
    /// EN: Ensures TOP and @@ROWCOUNT semantics behave consistently for SQL Azure compatibility tests.
    /// PT: Garante que a semântica de TOP e @@ROWCOUNT se comporte de forma consistente para testes de compatibilidade SQL Azure.
    /// </summary>
    [Fact]
    public void Select_TopThenRowCount_ShouldReturnLastSelectCount()
    {
        using var connection = CreateOpenConnection();
        using (var seed = new SqlAzureCommandMock(connection))
        {
            seed.CommandText = """
                INSERT INTO Users (Id, Name, Email) VALUES (101, 'Ana', NULL);
                INSERT INTO Users (Id, Name, Email) VALUES (102, 'Bia', NULL);
                INSERT INTO Users (Id, Name, Email) VALUES (103, 'Caio', NULL);
                """;
            seed.ExecuteNonQuery();
        }

        using var command = new SqlAzureCommandMock(connection)
        {
            CommandText = "SELECT TOP 1 Name FROM Users ORDER BY Id; SELECT @@ROWCOUNT;"
        };

        using var reader = command.ExecuteReader();
        Assert.True(reader.Read());
        Assert.Equal("Ana", reader.GetString(0));
        Assert.False(reader.Read());
        Assert.True(reader.NextResult());
        Assert.True(reader.Read());
        Assert.Equal(1L, Convert.ToInt64(reader.GetValue(0), CultureInfo.InvariantCulture));
    }

    /// <summary>
    /// EN: Ensures INSERT OUTPUT returns inserted projection through SQL Azure mocks.
    /// PT: Garante que INSERT OUTPUT retorne a projeção inserida nos mocks SQL Azure.
    /// </summary>
    [Fact]
    public void ExecuteReader_InsertOutput_ShouldReturnInsertedProjection()
    {
        using var connection = CreateOpenConnection();
        using var command = new SqlAzureCommandMock(connection)
        {
            CommandText = "INSERT INTO Users (Id, Name, Email) OUTPUT inserted.Id, inserted.Name AS user_name VALUES (701, 'Output Insert', 'insert@test.local')"
        };

        using var reader = command.ExecuteReader();
        Assert.True(reader.Read());
        Assert.Equal(701, reader.GetInt32(reader.GetOrdinal("Id")));
        Assert.Equal("Output Insert", reader.GetString(reader.GetOrdinal("user_name")));
        Assert.False(reader.Read());
    }

    /// <summary>
    /// EN: Ensures UPDATE OUTPUT exposes deleted/inserted values for SQL Azure compatibility.
    /// PT: Garante que UPDATE OUTPUT exponha valores deleted/inserted para compatibilidade SQL Azure.
    /// </summary>
    [Fact]
    public void ExecuteReader_UpdateOutput_ShouldReturnDeletedAndInsertedValues()
    {
        using var connection = CreateOpenConnection();
        using (var setup = new SqlAzureCommandMock(connection)
        {
            CommandText = "INSERT INTO Users (Id, Name, Email) VALUES (702, 'Before Update', 'before@test.local')"
        })
        {
            setup.ExecuteNonQuery();
        }

        using var command = new SqlAzureCommandMock(connection)
        {
            CommandText = "UPDATE Users SET Name = 'After Update' OUTPUT deleted.Name AS old_name, inserted.Name AS new_name WHERE Id = 702"
        };

        using var reader = command.ExecuteReader();
        Assert.True(reader.Read());
        Assert.Equal("Before Update", reader.GetString(reader.GetOrdinal("old_name")));
        Assert.Equal("After Update", reader.GetString(reader.GetOrdinal("new_name")));
        Assert.False(reader.Read());
    }

    /// <summary>
    /// EN: Ensures DELETE OUTPUT returns deleted snapshot through SQL Azure mocks.
    /// PT: Garante que DELETE OUTPUT retorne o snapshot excluído nos mocks SQL Azure.
    /// </summary>
    [Fact]
    public void ExecuteReader_DeleteOutput_ShouldReturnDeletedSnapshot()
    {
        using var connection = CreateOpenConnection();
        using (var setup = new SqlAzureCommandMock(connection)
        {
            CommandText = "INSERT INTO Users (Id, Name, Email) VALUES (703, 'To Delete', 'delete@test.local')"
        })
        {
            setup.ExecuteNonQuery();
        }

        using var command = new SqlAzureCommandMock(connection)
        {
            CommandText = "DELETE FROM Users OUTPUT deleted.Id, deleted.Name WHERE Id = 703"
        };

        using var reader = command.ExecuteReader();
        Assert.True(reader.Read());
        Assert.Equal(703, reader.GetInt32(reader.GetOrdinal("Id")));
        Assert.Equal("To Delete", reader.GetString(reader.GetOrdinal("Name")));
        Assert.False(reader.Read());
        Assert.DoesNotContain(connection.GetTable("Users"), row => Convert.ToInt32(row[0], CultureInfo.InvariantCulture) == 703);
    }

    /// <summary>
    /// EN: Ensures ROWCOUNT function tracks the last select row count for SQL Azure compatibility.
    /// PT: Garante que a função ROWCOUNT acompanhe a contagem da última consulta para compatibilidade SQL Azure.
    /// </summary>
    [Fact]
    public void Select_RowCountFunction_ShouldReturnLastSelectRowCount()
    {
        using var connection = CreateOpenConnection();
        using (var seed = new SqlAzureCommandMock(connection))
        {
            seed.CommandText = """
                INSERT INTO Users (Id, Name, Email) VALUES (131, 'RowCount A', NULL);
                INSERT INTO Users (Id, Name, Email) VALUES (132, 'RowCount B', NULL);
                """;
            seed.ExecuteNonQuery();
        }

        using var command = new SqlAzureCommandMock(connection)
        {
            CommandText = "SELECT TOP 1 Name FROM Users ORDER BY Id; SELECT ROWCOUNT();"
        };

        using var reader = command.ExecuteReader();
        Assert.True(reader.Read());
        Assert.Equal("RowCount A", reader.GetString(0));
        Assert.True(reader.NextResult());
        Assert.True(reader.Read());
        Assert.Equal(1L, Convert.ToInt64(reader.GetValue(0), CultureInfo.InvariantCulture));
    }

    /// <summary>
    /// EN: Ensures FOUND_ROWS function remains aligned with SQL Azure row-count compatibility behavior.
    /// PT: Garante que a função FOUND_ROWS permaneça alinhada ao comportamento de compatibilidade de contagem de linhas do SQL Azure.
    /// </summary>
    [Fact]
    public void Select_FoundRows_ShouldReturnLastSelectRowCount()
    {
        using var connection = CreateOpenConnection();
        using (var seed = new SqlAzureCommandMock(connection))
        {
            seed.CommandText = """
                INSERT INTO Users (Id, Name, Email) VALUES (141, 'Found A', NULL);
                INSERT INTO Users (Id, Name, Email) VALUES (142, 'Found B', NULL);
                """;
            seed.ExecuteNonQuery();
        }

        using var command = new SqlAzureCommandMock(connection)
        {
            CommandText = "SELECT TOP 1 Name FROM Users ORDER BY Id; SELECT FOUND_ROWS();"
        };

        using var reader = command.ExecuteReader();
        Assert.True(reader.Read());
        Assert.Equal("Found A", reader.GetString(0));
        Assert.True(reader.NextResult());
        Assert.True(reader.Read());
        Assert.Equal(1L, Convert.ToInt64(reader.GetValue(0), CultureInfo.InvariantCulture));
    }
}
