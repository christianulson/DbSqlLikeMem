namespace DbSqlLikeMem.SqlServer.Test;

/// <summary>
/// EN: Defines the class SqlServerMockTests.
/// PT: Define a classe SqlServerMockTests.
/// </summary>
public sealed class SqlServerMockTests
    : XUnitTestBase
{
    private readonly SqlServerConnectionMock _connection;

    /// <summary>
    /// EN: Tests SqlServerMockTests behavior.
    /// PT: Testa o comportamento de SqlServerMockTests.
    /// </summary>
    public SqlServerMockTests(
        ITestOutputHelper helper
        ) : base(helper)
    {
        var db = new SqlServerDbMock();
        db.AddTable("Users", [
            new("Id", DbType.Int32, false),
            new("Name", DbType.String, false) ,
            new ("Email", DbType.String, true)
        ]);
        db.AddTable("Orders", [
            new("OrderId",  DbType.Int32, false),
            new("UserId",  DbType.Int32, false),
            new("Amount",  DbType.Decimal, false, decimalPlaces: 2)
        ]);
        _connection = new SqlServerConnectionMock(db);
        _connection.Open();
    }

    /// <summary>
    /// EN: Tests TestInsert behavior.
    /// PT: Testa o comportamento de TestInsert.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlServerMock")]
    public void TestInsert()
    {
        using var command = new SqlServerCommandMock(_connection)
        {
            CommandText = "INSERT INTO Users (Id, Name, Email) VALUES (1, 'John Doe', 'john@example.com')"
        };
        var rowsAffected = command.ExecuteNonQuery();
        Assert.Equal(1, rowsAffected);
        Assert.Equal("John Doe",_connection.GetTable("Users")[0][1]);
    }

    /// <summary>
    /// EN: Tests TestUpdate behavior.
    /// PT: Testa o comportamento de TestUpdate.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlServerMock")]
    public void TestUpdate()
    {
        using var command = new SqlServerCommandMock(_connection)
        {
            CommandText = "INSERT INTO Users (Id, Name, Email) VALUES (1, 'John Doe', 'john@example.com')"
        };
        command.ExecuteNonQuery();

        command.CommandText = "UPDATE Users SET Name = 'Jane Doe' WHERE Id = 1";
        var rowsAffected = command.ExecuteNonQuery();
        Assert.Equal(1, rowsAffected);
        Assert.Equal("Jane Doe",_connection.GetTable("Users")[0][1]);
    }

    /// <summary>
    /// EN: Tests TestDelete behavior.
    /// PT: Testa o comportamento de TestDelete.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlServerMock")]
    public void TestDelete()
    {
        using var command = new SqlServerCommandMock(_connection)
        {
            CommandText = "INSERT INTO Users (Id, Name, Email) VALUES (1, 'John Doe', 'john@example.com')"
        };
        command.ExecuteNonQuery();

        command.CommandText = "DELETE FROM Users WHERE Id = 1";
        var rowsAffected = command.ExecuteNonQuery();
        Assert.Equal(1, rowsAffected);
        Assert.Empty(_connection.GetTable("Users"));
    }

    /// <summary>
    /// EN: Tests TestTransactionCommit behavior.
    /// PT: Testa o comportamento de TestTransactionCommit.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlServerMock")]
    public void TestTransactionCommit()
    {
        using (var transaction = _connection.BeginTransaction())
        {
            using var command = new SqlServerCommandMock(_connection, (SqlServerTransactionMock)transaction)
            {
                CommandText = "INSERT INTO Users (Id, Name, Email) VALUES (1, 'John Doe', 'john@example.com')"
            };
            command.ExecuteNonQuery();
            transaction.Commit();
        }

        using var queryCommand = new SqlServerCommandMock(_connection)
        {
            CommandText = "SELECT * FROM Users"
        };
        using var reader = queryCommand.ExecuteReader();
        var users = new List<Dictionary<int, object>>();
        while (reader.Read())
        {
            var user = new Dictionary<int, object>();
            for (int i = 0; i < reader.FieldCount; i++)
            {
                user[i] = reader.GetValue(i);
            }
            users.Add(user);
        }
        Assert.Single(users);
    }

    /// <summary>
    /// EN: Tests TestTransactionCommitInsertUpdate behavior.
    /// PT: Testa o comportamento de TestTransactionCommitInsertUpdate.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlServerMock")]
    public void TestTransactionCommitInsertUpdate()
    {
        using var cmd = _connection.CreateCommand();
        cmd.CommandText = "INSERT INTO users (id, name) VALUES (1, 'Alice')";
        cmd.ExecuteNonQuery();

        _connection.BeginTransaction();
        cmd.CommandText = "UPDATE users SET name = 'Bob' WHERE id = 1";
        cmd.ExecuteNonQuery();
        _connection.CommitTransaction();

        cmd.CommandText = "SELECT name FROM users WHERE id = 1";
        var name = (string?)cmd.ExecuteScalar();

        Assert.Equal("Bob", name);
    }

    /// <summary>
    /// EN: Tests TestTransactionRollback behavior.
    /// PT: Testa o comportamento de TestTransactionRollback.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlServerMock")]
    public void TestTransactionRollback()
    {
        using (var transaction = _connection.BeginTransaction())
        {
            using var command = new SqlServerCommandMock(_connection, (SqlServerTransactionMock)transaction)
            {
                CommandText = "INSERT INTO Users (Id, Name, Email) VALUES (1, 'John Doe', 'john@example.com')"
            };
            command.ExecuteNonQuery();
            transaction.Rollback();
        }

        using var queryCommand = new SqlServerCommandMock(_connection)
        {
            CommandText = "SELECT * FROM Users"
        };
        using var reader = queryCommand.ExecuteReader();
        var users = new List<Dictionary<int, object>>();
        while (reader.Read())
        {
            var user = new Dictionary<int, object>();
            for (int i = 0; i < reader.FieldCount; i++)
            {
                user[i] = reader.GetValue(i);
            }
            users.Add(user);
        }
        Assert.Empty(users);
    }


    /// <summary>
    /// EN: Ensures SELECT with SQL Server table hints executes correctly.
    /// PT: Garante que SELECT com hints de tabela do SQL Server execute corretamente.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlServerMock")]
    public void TestSelect_WithSqlServerTableHints_ShouldExecute()
    {
        using var command = new SqlServerCommandMock(_connection)
        {
            CommandText = "INSERT INTO Users (Id, Name, Email) VALUES (10, 'Hint User', 'hint@example.com')"
        };
        command.ExecuteNonQuery();

        command.CommandText = "SELECT Name FROM Users WITH (NOLOCK, INDEX([IX_Users_Name])) WHERE Id = 10";
        var name = command.ExecuteScalar();

        Assert.Equal("Hint User", name);
    }

    /// <summary>
    /// EN: Disposes test resources.
    /// PT: Descarta os recursos do teste.
    /// </summary>
    /// <param name="disposing">EN: True to dispose managed resources. PT: True para descartar recursos gerenciados.</param>
    protected override void Dispose(bool disposing)
    {
        _connection.Dispose();
        base.Dispose(disposing);
    }

    /// <summary>
    /// EN: Tests TestSelect_FoundRows_ShouldReturnLastSelectRowCount behavior.
    /// PT: Testa o comportamento de TestSelect_FoundRows_ShouldReturnLastSelectRowCount.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlServerMock")]
    public void TestSelect_FoundRows_ShouldReturnLastSelectRowCount()
    {
        using var command = new SqlServerCommandMock(_connection);
        command.CommandText = """
            INSERT INTO Users (Id, Name, Email) VALUES (101, 'Ana', NULL);
            INSERT INTO Users (Id, Name, Email) VALUES (102, 'Bia', NULL);
            INSERT INTO Users (Id, Name, Email) VALUES (103, 'Caio', NULL);
            """;
        command.ExecuteNonQuery();

        command.CommandText = "SELECT Name FROM Users ORDER BY Id OFFSET 0 ROWS FETCH NEXT 1 ROWS ONLY; SELECT FOUND_ROWS();";
        using var reader = command.ExecuteReader();

        Assert.True(reader.Read());
        Assert.Equal("Ana", reader.GetString(0));
        Assert.True(reader.NextResult());
        Assert.True(reader.Read());
        Assert.Equal(1L, Convert.ToInt64(reader.GetValue(0), CultureInfo.InvariantCulture));
    }


    /// <summary>
    /// EN: Tests TestSelect_RowCountFunction_ShouldReturnLastSelectRowCount behavior.
    /// PT: Testa o comportamento de TestSelect_RowCountFunction_ShouldReturnLastSelectRowCount.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlServerMock")]
    public void TestSelect_RowCountFunction_ShouldReturnLastSelectRowCount()
    {
        using var command = new SqlServerCommandMock(_connection);
        command.CommandText = """
            INSERT INTO Users (Id, Name, Email) VALUES (131, 'RowCount A', NULL);
            INSERT INTO Users (Id, Name, Email) VALUES (132, 'RowCount B', NULL);
            """;
        command.ExecuteNonQuery();

        command.CommandText = "SELECT Name FROM Users ORDER BY Id OFFSET 0 ROWS FETCH NEXT 1 ROWS ONLY; SELECT ROWCOUNT();";
        using var reader = command.ExecuteReader();

        Assert.True(reader.Read());
        Assert.True(reader.NextResult());
        Assert.True(reader.Read());
        Assert.Equal(1L, Convert.ToInt64(reader.GetValue(0), CultureInfo.InvariantCulture));
    }


    /// <summary>
    /// EN: Tests TestSelect_SystemRowCountVariable_ShouldReturnLastSelectRowCount behavior.
    /// PT: Testa o comportamento de TestSelect_SystemRowCountVariable_ShouldReturnLastSelectRowCount.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlServerMock")]
    public void TestSelect_SystemRowCountVariable_ShouldReturnLastSelectRowCount()
    {
        using var command = new SqlServerCommandMock(_connection);
        command.CommandText = """
            INSERT INTO Users (Id, Name, Email) VALUES (141, 'SysRowCount A', NULL);
            INSERT INTO Users (Id, Name, Email) VALUES (142, 'SysRowCount B', NULL);
            """;
        command.ExecuteNonQuery();

        command.CommandText = "SELECT Name FROM Users ORDER BY Id OFFSET 0 ROWS FETCH NEXT 1 ROWS ONLY; SELECT @@ROWCOUNT;";
        using var reader = command.ExecuteReader();

        Assert.True(reader.Read());
        Assert.True(reader.NextResult());
        Assert.True(reader.Read());
        Assert.Equal(1L, Convert.ToInt64(reader.GetValue(0), CultureInfo.InvariantCulture));
    }


    /// <summary>
    /// EN: Tests TestUpdate_SystemRowCountVariable_ShouldReturnAffectedRows behavior.
    /// PT: Testa o comportamento de TestUpdate_SystemRowCountVariable_ShouldReturnAffectedRows.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlServerMock")]
    public void TestUpdate_SystemRowCountVariable_ShouldReturnAffectedRows()
    {
        using var command = new SqlServerCommandMock(_connection)
        {
            CommandText = "INSERT INTO Users (Id, Name, Email) VALUES (120, 'Row Count User', NULL)"
        };
        command.ExecuteNonQuery();

        command.CommandText = "UPDATE Users SET Name = 'Updated User' WHERE Id = 120; SELECT @@ROWCOUNT;";
        using var reader = command.ExecuteReader();

        Assert.True(reader.Read());
        Assert.Equal(1L, Convert.ToInt64(reader.GetValue(0), CultureInfo.InvariantCulture));
    }


    /// <summary>
    /// EN: Tests TestCreateView_SystemRowCountVariable_ShouldReturnZero behavior.
    /// PT: Testa o comportamento de TestCreateView_SystemRowCountVariable_ShouldReturnZero.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlServerMock")]
    public void TestCreateView_SystemRowCountVariable_ShouldReturnZero()
    {
        using var command = new SqlServerCommandMock(_connection)
        {
            CommandText = "CREATE VIEW vw_users_rowcount AS SELECT Id FROM Users; SELECT @@ROWCOUNT;"
        };

        using var reader = command.ExecuteReader();

        Assert.True(reader.Read());
        Assert.Equal(0L, Convert.ToInt64(reader.GetValue(0), CultureInfo.InvariantCulture));
    }


    /// <summary>
    /// EN: Tests TestBeginTransaction_SystemRowCountVariable_ShouldReturnZero behavior.
    /// PT: Testa o comportamento de TestBeginTransaction_SystemRowCountVariable_ShouldReturnZero.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlServerMock")]
    public void TestBeginTransaction_SystemRowCountVariable_ShouldReturnZero()
    {
        using var command = new SqlServerCommandMock(_connection)
        {
            CommandText = "BEGIN TRANSACTION"
        };
        command.ExecuteNonQuery();

        command.CommandText = "SELECT @@ROWCOUNT";
        Assert.Equal(0L, Convert.ToInt64(command.ExecuteScalar(), CultureInfo.InvariantCulture));
    }

    /// <summary>
    /// EN: Tests TestBatch_BeginTransactionThenRowCount_ShouldReturnZero behavior.
    /// PT: Testa o comportamento de TestBatch_BeginTransactionThenRowCount_ShouldReturnZero.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlServerMock")]
    public void TestBatch_BeginTransactionThenRowCount_ShouldReturnZero()
    {
        using var command = new SqlServerCommandMock(_connection)
        {
            CommandText = "BEGIN TRANSACTION; SELECT @@ROWCOUNT;"
        };

        using var reader = command.ExecuteReader();

        Assert.True(reader.Read());
        Assert.Equal(0L, Convert.ToInt64(reader.GetValue(0), CultureInfo.InvariantCulture));
    }

    /// <summary>
    /// EN: Tests TestBatch_BeginSavepointThenRowCount_ShouldReturnZero behavior.
    /// PT: Testa o comportamento de TestBatch_BeginSavepointThenRowCount_ShouldReturnZero.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlServerMock")]
    public void TestBatch_BeginSavepointThenRowCount_ShouldReturnZero()
    {
        using var command = new SqlServerCommandMock(_connection)
        {
            CommandText = "BEGIN TRANSACTION; SAVEPOINT sp1; SELECT @@ROWCOUNT;"
        };

        using var reader = command.ExecuteReader();

        Assert.True(reader.Read());
        Assert.Equal(0L, Convert.ToInt64(reader.GetValue(0), CultureInfo.InvariantCulture));
    }

    /// <summary>
    /// EN: Tests TestBatch_CallThenRowCount_ShouldReturnZero behavior.
    /// PT: Testa o comportamento de TestBatch_CallThenRowCount_ShouldReturnZero.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlServerMock")]
    public void TestBatch_CallThenRowCount_ShouldReturnZero()
    {
        _connection.AddProdecure("sp_ping", new ProcedureDef([], [], [], null));

        using var command = new SqlServerCommandMock(_connection)
        {
            CommandText = "CALL sp_ping(); SELECT @@ROWCOUNT;"
        };

        using var reader = command.ExecuteReader();

        Assert.True(reader.Read());
        Assert.Equal(0L, Convert.ToInt64(reader.GetValue(0), CultureInfo.InvariantCulture));
    }

    /// <summary>
    /// EN: Tests TestBatch_UpdateCommitThenRowCount_ShouldReturnZeroAfterCommit behavior.
    /// PT: Testa o comportamento de TestBatch_UpdateCommitThenRowCount_ShouldReturnZeroAfterCommit.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlServerMock")]
    public void TestBatch_UpdateCommitThenRowCount_ShouldReturnZeroAfterCommit()
    {
        using var command = new SqlServerCommandMock(_connection)
        {
            CommandText = "UPDATE Users SET Name = 'After Commit' WHERE Id = 1; COMMIT; SELECT @@ROWCOUNT;"
        };

        using var reader = command.ExecuteReader();

        Assert.True(reader.Read());
        Assert.Equal(0L, Convert.ToInt64(reader.GetValue(0), CultureInfo.InvariantCulture));
    }


    /// <summary>
    /// EN: Tests TestBatch_RollbackToSavepointThenRowCount_ShouldReturnZero behavior.
    /// PT: Testa o comportamento de TestBatch_RollbackToSavepointThenRowCount_ShouldReturnZero.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlServerMock")]
    public void TestBatch_RollbackToSavepointThenRowCount_ShouldReturnZero()
    {
        using var command = new SqlServerCommandMock(_connection)
        {
            CommandText = "BEGIN TRANSACTION; SAVEPOINT sp1; UPDATE Users SET Name = 'Tmp' WHERE Id = 1; ROLLBACK TO SAVEPOINT sp1; SELECT @@ROWCOUNT;"
        };

        using var reader = command.ExecuteReader();

        Assert.True(reader.Read());
        Assert.Equal(0L, Convert.ToInt64(reader.GetValue(0), CultureInfo.InvariantCulture));
    }

    /// <summary>
    /// EN: Tests TestBatch_ReleaseSavepointThenRowCount_ShouldReturnZero behavior.
    /// PT: Testa o comportamento de TestBatch_ReleaseSavepointThenRowCount_ShouldReturnZero.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlServerMock")]
    public void TestBatch_ReleaseSavepointThenRowCount_ShouldReturnZero()
    {
        using var command = new SqlServerCommandMock(_connection)
        {
            CommandText = "BEGIN TRANSACTION; SAVEPOINT sp1; RELEASE SAVEPOINT sp1; SELECT @@ROWCOUNT;"
        };

        using var reader = command.ExecuteReader();

        Assert.True(reader.Read());
        Assert.Equal(0L, Convert.ToInt64(reader.GetValue(0), CultureInfo.InvariantCulture));
    }


    /// <summary>
    /// EN: Tests TestBatch_SelectThenUpdateThenRowCount_ShouldReflectLastDml behavior.
    /// PT: Testa o comportamento de TestBatch_SelectThenUpdateThenRowCount_ShouldReflectLastDml.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlServerMock")]
    public void TestBatch_SelectThenUpdateThenRowCount_ShouldReflectLastDml()
    {
        using var seed = new SqlServerCommandMock(_connection)
        {
            CommandText = "INSERT INTO Users (Id, Name, Email) VALUES (151, 'Before Batch Update', NULL)"
        };
        seed.ExecuteNonQuery();

        using var command = new SqlServerCommandMock(_connection)
        {
            CommandText = "SELECT Name FROM Users ORDER BY Id OFFSET 0 ROWS FETCH NEXT 1 ROWS ONLY; UPDATE Users SET Name = 'Mixed Batch User' WHERE Id = 151; SELECT @@ROWCOUNT;"
        };

        using var reader = command.ExecuteReader();

        Assert.True(reader.Read());
        Assert.True(reader.NextResult());
        Assert.True(reader.Read());
        Assert.Equal(1L, Convert.ToInt64(reader.GetValue(0), CultureInfo.InvariantCulture));
    }


    /// <summary>
    /// EN: Tests TestBatch_CallUpdateCommitThenRowCount_ShouldReturnZeroAfterCommit behavior.
    /// PT: Testa o comportamento de TestBatch_CallUpdateCommitThenRowCount_ShouldReturnZeroAfterCommit.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlServerMock")]
    public void TestBatch_CallUpdateCommitThenRowCount_ShouldReturnZeroAfterCommit()
    {
        _connection.AddProdecure("sp_ping", new ProcedureDef([], [], [], null));

        using var command = new SqlServerCommandMock(_connection)
        {
            CommandText = "CALL sp_ping(); UPDATE Users SET Name = 'Call Dml User' WHERE Id = 1; COMMIT; SELECT @@ROWCOUNT;"
        };

        using var reader = command.ExecuteReader();

        Assert.True(reader.Read());
        Assert.Equal(0L, Convert.ToInt64(reader.GetValue(0), CultureInfo.InvariantCulture));
    }


    /// <summary>
    /// EN: Tests TestBatch_UpdateThenSelectThenRowCount_ShouldReflectLastSelect behavior.
    /// PT: Testa o comportamento de TestBatch_UpdateThenSelectThenRowCount_ShouldReflectLastSelect.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlServerMock")]
    public void TestBatch_UpdateThenSelectThenRowCount_ShouldReflectLastSelect()
    {
        using var seedFirst = new SqlServerCommandMock(_connection)
        {
            CommandText = "INSERT INTO Users (Id, Name, Email) VALUES (161, 'Before Last Select A', NULL)"
        };
        seedFirst.ExecuteNonQuery();
        using var seedSecond = new SqlServerCommandMock(_connection)
        {
            CommandText = "INSERT INTO Users (Id, Name, Email) VALUES (162, 'Before Last Select B', NULL)"
        };
        seedSecond.ExecuteNonQuery();

        using var command = new SqlServerCommandMock(_connection)
        {
            CommandText = "UPDATE Users SET Name = 'Last Select User' WHERE Id = 161; SELECT Name FROM Users ORDER BY Id OFFSET 0 ROWS FETCH NEXT 2 ROWS ONLY; SELECT @@ROWCOUNT;"
        };

        using var reader = command.ExecuteReader();

        var rows = 0;
        while (reader.Read()) rows++;
        Assert.Equal(2, rows);

        Assert.True(reader.NextResult());
        Assert.True(reader.Read());
        Assert.Equal(2L, Convert.ToInt64(reader.GetValue(0), CultureInfo.InvariantCulture));
    }

    /// <summary>
    /// EN: Tests ExecuteReader_InsertOutput_ShouldReturnInsertedProjection behavior.
    /// PT: Testa o comportamento de ExecuteReader_InsertOutput_ShouldReturnInsertedProjection.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlServerMock")]
    public void ExecuteReader_InsertOutput_ShouldReturnInsertedProjection()
    {
        using var command = new SqlServerCommandMock(_connection)
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
    /// EN: Tests ExecuteReader_UpdateOutput_ShouldReturnDeletedAndInsertedValues behavior.
    /// PT: Testa o comportamento de ExecuteReader_UpdateOutput_ShouldReturnDeletedAndInsertedValues.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlServerMock")]
    public void ExecuteReader_UpdateOutput_ShouldReturnDeletedAndInsertedValues()
    {
        using var setup = new SqlServerCommandMock(_connection)
        {
            CommandText = "INSERT INTO Users (Id, Name, Email) VALUES (702, 'Before Update', 'before@test.local')"
        };
        setup.ExecuteNonQuery();

        using var command = new SqlServerCommandMock(_connection)
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
    /// EN: Tests ExecuteReader_DeleteOutput_ShouldReturnDeletedSnapshot behavior.
    /// PT: Testa o comportamento de ExecuteReader_DeleteOutput_ShouldReturnDeletedSnapshot.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlServerMock")]
    public void ExecuteReader_DeleteOutput_ShouldReturnDeletedSnapshot()
    {
        using var setup = new SqlServerCommandMock(_connection)
        {
            CommandText = "INSERT INTO Users (Id, Name, Email) VALUES (703, 'To Delete', 'delete@test.local')"
        };
        setup.ExecuteNonQuery();

        using var command = new SqlServerCommandMock(_connection)
        {
            CommandText = "DELETE FROM Users OUTPUT deleted.Id, deleted.Name WHERE Id = 703"
        };

        using var reader = command.ExecuteReader();

        Assert.True(reader.Read());
        Assert.Equal(703, reader.GetInt32(reader.GetOrdinal("Id")));
        Assert.Equal("To Delete", reader.GetString(reader.GetOrdinal("Name")));
        Assert.False(reader.Read());
        Assert.DoesNotContain(_connection.GetTable("Users"), r => Convert.ToInt32(r[0], CultureInfo.InvariantCulture) == 703);
    }

}

