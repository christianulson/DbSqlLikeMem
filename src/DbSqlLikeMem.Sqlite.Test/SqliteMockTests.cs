namespace DbSqlLikeMem.Sqlite.Test;

/// <summary>
/// EN: Defines the class SqliteMockTests.
/// PT: Define a classe SqliteMockTests.
/// </summary>
public sealed class SqliteMockTests
    : XUnitTestBase
{
    private readonly SqliteConnectionMock _connection;

    /// <summary>
    /// EN: Tests SqliteMockTests behavior.
    /// PT: Testa o comportamento de SqliteMockTests.
    /// </summary>
    public SqliteMockTests(
        ITestOutputHelper helper
        ) : base(helper)
    {
        var db = new SqliteDbMock();
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

        _connection = new SqliteConnectionMock(db);
        _connection.Open();
    }

    /// <summary>
    /// EN: Tests TestInsert behavior.
    /// PT: Testa o comportamento de TestInsert.
    /// </summary>
    [Fact]
    [Trait("Category", "SqliteMock")]
    public void TestInsert()
    {
        using var command = new SqliteCommandMock(_connection)
        {
            CommandText = "INSERT INTO Users (Id, Name, Email) VALUES (1, 'John Doe', 'john@example.com')"
        };
        var rowsAffected = command.ExecuteNonQuery();
        Assert.Equal(1, rowsAffected);
        Assert.Equal("John Doe", _connection.GetTable("users")[0][1]);
    }

    /// <summary>
    /// EN: Tests TestUpdate behavior.
    /// PT: Testa o comportamento de TestUpdate.
    /// </summary>
    [Fact]
    [Trait("Category", "SqliteMock")]
    public void TestUpdate()
    {
        using var command = new SqliteCommandMock(_connection)
        {
            CommandText = "INSERT INTO Users (Id, Name, Email) VALUES (1, 'John Doe', 'john@example.com')"
        };
        command.ExecuteNonQuery();

        command.CommandText = "UPDATE Users SET Name = 'Jane Doe' WHERE Id = 1";
        var rowsAffected = command.ExecuteNonQuery();
        Assert.Equal(1, rowsAffected);
        Assert.Equal("Jane Doe", _connection.GetTable("users")[0][1]);
    }

    /// <summary>
    /// EN: Tests TestDelete behavior.
    /// PT: Testa o comportamento de TestDelete.
    /// </summary>
    [Fact]
    [Trait("Category", "SqliteMock")]
    public void TestDelete()
    {
        using var command = new SqliteCommandMock(_connection)
        {
            CommandText = "INSERT INTO Users (Id, Name, Email) VALUES (1, 'John Doe', 'john@example.com')"
        };
        command.ExecuteNonQuery();

        command.CommandText = "DELETE FROM Users WHERE Id = 1";
        var rowsAffected = command.ExecuteNonQuery();
        Assert.Equal(1, rowsAffected);
        Assert.Empty(_connection.GetTable("users"));
    }

    /// <summary>
    /// EN: Tests TestTransactionCommit behavior.
    /// PT: Testa o comportamento de TestTransactionCommit.
    /// </summary>
    [Fact]
    [Trait("Category", "SqliteMock")]
    public void TestTransactionCommit()
    {
        using (var transaction = _connection.BeginTransaction())
        {
            using var command = new SqliteCommandMock(_connection, (SqliteTransactionMock)transaction)
            {
                CommandText = "INSERT INTO Users (Id, Name, Email) VALUES (1, 'John Doe', 'john@example.com')"
            };
            command.ExecuteNonQuery();
            transaction.Commit();
        }

        using var queryCommand = new SqliteCommandMock(_connection)
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
    [Trait("Category", "SqliteMock")]
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
    [Trait("Category", "SqliteMock")]
    public void TestTransactionRollback()
    {
        using (var transaction = _connection.BeginTransaction())
        {
            using var command = new SqliteCommandMock(_connection, (SqliteTransactionMock)transaction)
            {
                CommandText = "INSERT INTO Users (Id, Name, Email) VALUES (1, 'John Doe', 'john@example.com')"
            };
            command.ExecuteNonQuery();
            transaction.Rollback();
        }

        using var queryCommand = new SqliteCommandMock(_connection)
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
    /// EN: Verifies FOUND_ROWS returns the row count from the last SELECT in the same batch.
    /// PT: Verifica que FOUND_ROWS retorna a contagem de linhas do último SELECT no mesmo batch.
    /// </summary>
    [Fact]
    [Trait("Category", "SqliteMock")]
    public void TestSelect_FoundRows_ShouldReturnLastSelectRowCount()
    {
        using var command = new SqliteCommandMock(_connection);
        command.CommandText = """
            INSERT INTO Users (Id, Name, Email) VALUES (101, 'Ana', NULL);
            INSERT INTO Users (Id, Name, Email) VALUES (102, 'Bia', NULL);
            INSERT INTO Users (Id, Name, Email) VALUES (103, 'Caio', NULL);
            """;
        command.ExecuteNonQuery();

        command.CommandText = "SELECT Name FROM Users ORDER BY Id LIMIT 1; SELECT FOUND_ROWS();";
        using var reader = command.ExecuteReader();

        Assert.True(reader.Read());
        Assert.Equal("Ana", reader.GetString(0));
        Assert.True(reader.NextResult());
        Assert.True(reader.Read());
        Assert.Equal(1L, Convert.ToInt64(reader.GetValue(0), CultureInfo.InvariantCulture));
    }


    /// <summary>
    /// EN: Verifies CHANGES returns affected rows for the last UPDATE statement.
    /// PT: Verifica que CHANGES retorna as linhas afetadas pelo último UPDATE.
    /// </summary>
    [Fact]
    [Trait("Category", "SqliteMock")]
    public void TestUpdate_ChangesFunction_ShouldReturnAffectedRows()
    {
        using var command = new SqliteCommandMock(_connection)
        {
            CommandText = "INSERT INTO Users (Id, Name, Email) VALUES (150, 'Changes User', NULL)"
        };
        command.ExecuteNonQuery();

        command.CommandText = "UPDATE Users SET Name = 'Updated User' WHERE Id = 150; SELECT CHANGES();";
        using var reader = command.ExecuteReader();

        Assert.True(reader.Read());
        Assert.Equal(1L, Convert.ToInt64(reader.GetValue(0)));
    }


    /// <summary>
    /// EN: Verifies CHANGES returns zero immediately after beginning a transaction.
    /// PT: Verifica que CHANGES retorna zero imediatamente após iniciar uma transação.
    /// </summary>
    [Fact]
    [Trait("Category", "SqliteMock")]
    public void TestBeginTransaction_ChangesFunction_ShouldReturnZero()
    {
        using var command = new SqliteCommandMock(_connection)
        {
            CommandText = "BEGIN TRANSACTION"
        };
        command.ExecuteNonQuery();

        command.CommandText = "SELECT CHANGES();";
        Assert.Equal(0L, Convert.ToInt64(command.ExecuteScalar()));
    }



    /// <summary>
    /// EN: Verifies a BEGIN TRANSACTION followed by CHANGES returns zero in batch execution.
    /// PT: Verifica que BEGIN TRANSACTION seguido de CHANGES retorna zero em execução em batch.
    /// </summary>
    [Fact]
    [Trait("Category", "SqliteMock")]
    public void TestBatch_BeginTransactionThenChanges_ShouldReturnZero()
    {
        using var command = new SqliteCommandMock(_connection)
        {
            CommandText = "BEGIN TRANSACTION; SELECT CHANGES();"
        };

        using var reader = command.ExecuteReader();

        Assert.True(reader.Read());
        Assert.Equal(0L, Convert.ToInt64(reader.GetValue(0)));
    }

    /// <summary>
    /// EN: Verifies CALL followed by CHANGES returns zero when no DML affected rows.
    /// PT: Verifica que CALL seguido de CHANGES retorna zero quando nenhum DML afetou linhas.
    /// </summary>
    [Fact]
    [Trait("Category", "SqliteMock")]
    public void TestBatch_CallThenChanges_ShouldReturnZero()
    {
        _connection.AddProdecure("sp_ping", new ProcedureDef([], [], [], null));

        using var command = new SqliteCommandMock(_connection)
        {
            CommandText = "CALL sp_ping(); SELECT CHANGES();"
        };

        using var reader = command.ExecuteReader();

        Assert.True(reader.Read());
        Assert.Equal(0L, Convert.ToInt64(reader.GetValue(0)));
    }

    /// <summary>
    /// EN: Verifies CHANGES returns zero after COMMIT in a batch that previously updated rows.
    /// PT: Verifica que CHANGES retorna zero após COMMIT em um batch que atualizou linhas anteriormente.
    /// </summary>
    [Fact]
    [Trait("Category", "SqliteMock")]
    public void TestBatch_UpdateCommitThenChanges_ShouldReturnZeroAfterCommit()
    {
        using var command = new SqliteCommandMock(_connection)
        {
            CommandText = "UPDATE Users SET Name = 'After Commit' WHERE Id = 1; COMMIT; SELECT CHANGES();"
        };

        using var reader = command.ExecuteReader();

        Assert.True(reader.Read());
        Assert.Equal(0L, Convert.ToInt64(reader.GetValue(0)));
    }


    /// <summary>
    /// EN: Verifies CHANGES returns zero after rolling back to a savepoint in batch execution.
    /// PT: Verifica que CHANGES retorna zero após rollback para savepoint em execução em batch.
    /// </summary>
    [Fact]
    [Trait("Category", "SqliteMock")]
    public void TestBatch_RollbackToSavepointThenChanges_ShouldReturnZero()
    {
        using var command = new SqliteCommandMock(_connection)
        {
            CommandText = "BEGIN TRANSACTION; SAVEPOINT sp1; UPDATE Users SET Name = 'Tmp' WHERE Id = 1; ROLLBACK TO SAVEPOINT sp1; SELECT CHANGES();"
        };

        using var reader = command.ExecuteReader();

        Assert.True(reader.Read());
        Assert.Equal(0L, Convert.ToInt64(reader.GetValue(0)));
    }

    /// <summary>
    /// EN: Verifies CHANGES returns zero after releasing a savepoint in batch execution.
    /// PT: Verifica que CHANGES retorna zero após liberar um savepoint em execução em batch.
    /// </summary>
    [Fact]
    [Trait("Category", "SqliteMock")]
    public void TestBatch_ReleaseSavepointThenChanges_ShouldReturnZero()
    {
        using var command = new SqliteCommandMock(_connection)
        {
            CommandText = "BEGIN TRANSACTION; SAVEPOINT sp1; RELEASE SAVEPOINT sp1; SELECT CHANGES();"
        };

        using var reader = command.ExecuteReader();

        Assert.True(reader.Read());
        Assert.Equal(0L, Convert.ToInt64(reader.GetValue(0)));
    }


    /// <summary>
    /// EN: Tests TestBatch_SelectThenUpdateThenChanges_ShouldReflectLastDml behavior.
    /// PT: Testa o comportamento de TestBatch_SelectThenUpdateThenChanges_ShouldReflectLastDml.
    /// </summary>
    [Fact]
    [Trait("Category", "SqliteMock")]
    public void TestBatch_SelectThenUpdateThenChanges_ShouldReflectLastDml()
    {
        using var seed = new SqliteCommandMock(_connection)
        {
            CommandText = "INSERT INTO Users (Id, Name, Email) VALUES (1, 'Seed User', NULL)"
        };
        seed.ExecuteNonQuery();

        using var command = new SqliteCommandMock(_connection)
        {
            CommandText = "SELECT Name FROM Users ORDER BY Id LIMIT 1; UPDATE Users SET Name = 'Mixed Batch User' WHERE Id = 1; SELECT CHANGES();"
        };

        using var reader = command.ExecuteReader();

        Assert.True(reader.Read());
        Assert.True(reader.NextResult());
        Assert.True(reader.Read());
        Assert.Equal(1L, Convert.ToInt64(reader.GetValue(0)));
    }


    /// <summary>
    /// EN: Tests TestBatch_CallUpdateCommitThenChanges_ShouldReturnZeroAfterCommit behavior.
    /// PT: Testa o comportamento de TestBatch_CallUpdateCommitThenChanges_ShouldReturnZeroAfterCommit.
    /// </summary>
    [Fact]
    [Trait("Category", "SqliteMock")]
    public void TestBatch_CallUpdateCommitThenChanges_ShouldReturnZeroAfterCommit()
    {
        _connection.AddProdecure("sp_ping", new ProcedureDef([], [], [], null));

        using var command = new SqliteCommandMock(_connection)
        {
            CommandText = "CALL sp_ping(); UPDATE Users SET Name = 'Call Dml User' WHERE Id = 1; COMMIT; SELECT CHANGES();"
        };

        using var reader = command.ExecuteReader();

        Assert.True(reader.Read());
        Assert.Equal(0L, Convert.ToInt64(reader.GetValue(0)));
    }


    /// <summary>
    /// EN: Tests TestBatch_UpdateThenSelectThenChanges_ShouldReflectLastSelect behavior.
    /// PT: Testa o comportamento de TestBatch_UpdateThenSelectThenChanges_ShouldReflectLastSelect.
    /// </summary>
    [Fact]
    [Trait("Category", "SqliteMock")]
    public void TestBatch_UpdateThenSelectThenChanges_ShouldReflectLastSelect()
    {
        using var seed = new SqliteCommandMock(_connection)
        {
            CommandText = "INSERT INTO Users (Id, Name, Email) VALUES (1, 'Seed User 1', NULL)"
        };
        seed.ExecuteNonQuery();
        seed.CommandText = "INSERT INTO Users (Id, Name, Email) VALUES (2, 'Seed User 2', NULL)";
        seed.ExecuteNonQuery();

        using var command = new SqliteCommandMock(_connection)
        {
            CommandText = "UPDATE Users SET Name = 'Last Select User' WHERE Id = 1; SELECT Name FROM Users ORDER BY Id LIMIT 2; SELECT CHANGES();"
        };

        using var reader = command.ExecuteReader();

        var rows = 0;
        while (reader.Read()) rows++;
        Assert.Equal(2, rows);

        Assert.True(reader.NextResult());
        Assert.True(reader.Read());
        Assert.Equal(2L, Convert.ToInt64(reader.GetValue(0)));
    }

    /// <summary>
    /// EN: Tests ExecuteReader_InsertReturning_ShouldReturnInsertedRows behavior.
    /// PT: Testa o comportamento de ExecuteReader_InsertReturning_ShouldReturnInsertedRows.
    /// </summary>
    [Fact]
    [Trait("Category", "SqliteMock")]
    public void ExecuteReader_InsertReturning_ShouldReturnInsertedRows()
    {
        using var command = new SqliteCommandMock(_connection)
        {
            CommandText = "INSERT INTO Users (Id, Name, Email) VALUES (601, 'Returning Insert', 'insert@test.local') RETURNING Id, Name AS user_name"
        };

        using var reader = command.ExecuteReader();

        Assert.True(reader.Read());
        Assert.Equal(601, reader.GetInt32(reader.GetOrdinal("Id")));
        Assert.Equal("Returning Insert", reader.GetString(reader.GetOrdinal("user_name")));
        Assert.False(reader.Read());
    }

    /// <summary>
    /// EN: Tests ExecuteReader_UpdateReturning_ShouldReturnUpdatedProjection behavior.
    /// PT: Testa o comportamento de ExecuteReader_UpdateReturning_ShouldReturnUpdatedProjection.
    /// </summary>
    [Fact]
    [Trait("Category", "SqliteMock")]
    public void ExecuteReader_UpdateReturning_ShouldReturnUpdatedProjection()
    {
        using var setup = new SqliteCommandMock(_connection)
        {
            CommandText = "INSERT INTO Users (Id, Name, Email) VALUES (602, 'Before Update', 'before@test.local')"
        };
        setup.ExecuteNonQuery();

        using var command = new SqliteCommandMock(_connection)
        {
            CommandText = "UPDATE Users SET Name = 'After Update' WHERE Id = 602 RETURNING Id, Name"
        };

        using var reader = command.ExecuteReader();

        Assert.True(reader.Read());
        Assert.Equal(602, reader.GetInt32(reader.GetOrdinal("Id")));
        Assert.Equal("After Update", reader.GetString(reader.GetOrdinal("Name")));
        Assert.False(reader.Read());
    }

    /// <summary>
    /// EN: Tests ExecuteReader_DeleteReturning_ShouldReturnDeletedRowSnapshot behavior.
    /// PT: Testa o comportamento de ExecuteReader_DeleteReturning_ShouldReturnDeletedRowSnapshot.
    /// </summary>
    [Fact]
    [Trait("Category", "SqliteMock")]
    public void ExecuteReader_DeleteReturning_ShouldReturnDeletedRowSnapshot()
    {
        using var setup = new SqliteCommandMock(_connection)
        {
            CommandText = "INSERT INTO Users (Id, Name, Email) VALUES (603, 'To Delete', 'delete@test.local')"
        };
        setup.ExecuteNonQuery();

        using var command = new SqliteCommandMock(_connection)
        {
            CommandText = "DELETE FROM Users WHERE Id = 603 RETURNING Id, Name"
        };

        using var reader = command.ExecuteReader();

        Assert.True(reader.Read());
        Assert.Equal(603, reader.GetInt32(reader.GetOrdinal("Id")));
        Assert.Equal("To Delete", reader.GetString(reader.GetOrdinal("Name")));
        Assert.False(reader.Read());
        Assert.DoesNotContain(_connection.GetTable("users"), r => Convert.ToInt32(r[0]) == 603);
    }

    /// <summary>
    /// EN: Tests ExecuteReader_InsertSelectReturning_ShouldReturnAllInsertedRows behavior.
    /// PT: Testa o comportamento de ExecuteReader_InsertSelectReturning_ShouldReturnAllInsertedRows.
    /// </summary>
    [Fact]
    [Trait("Category", "SqliteMock")]
    public void ExecuteReader_InsertSelectReturning_ShouldReturnAllInsertedRows()
    {
        using var seed = new SqliteCommandMock(_connection)
        {
            CommandText = "INSERT INTO Users (Id, Name, Email) VALUES (611, 'Seed A', 'seed-a@test.local')"
        };
        seed.ExecuteNonQuery();
        seed.CommandText = "INSERT INTO Users (Id, Name, Email) VALUES (612, 'Seed B', 'seed-b@test.local')";
        seed.ExecuteNonQuery();

        using var command = new SqliteCommandMock(_connection)
        {
            CommandText = """
                INSERT INTO Users (Id, Name, Email)
                SELECT Id + 1000, Name, Email
                FROM Users
                WHERE Id IN (611, 612)
                RETURNING Id
                """
        };

        using var reader = command.ExecuteReader();

        Assert.True(reader.Read());
        Assert.Equal(1611, reader.GetInt32(0));
        Assert.True(reader.Read());
        Assert.Equal(1612, reader.GetInt32(0));
        Assert.False(reader.Read());
    }

    /// <summary>
    /// EN: Tests ExecuteReader_UpdateReturningQualifiedWildcard_ShouldReturnAllColumns behavior.
    /// PT: Testa o comportamento de ExecuteReader_UpdateReturningQualifiedWildcard_ShouldReturnAllColumns.
    /// </summary>
    [Fact]
    [Trait("Category", "SqliteMock")]
    public void ExecuteReader_UpdateReturningQualifiedWildcard_ShouldReturnAllColumns()
    {
        using var setup = new SqliteCommandMock(_connection)
        {
            CommandText = "INSERT INTO Users (Id, Name, Email) VALUES (613, 'Before', 'before613@test.local')"
        };
        setup.ExecuteNonQuery();

        using var command = new SqliteCommandMock(_connection)
        {
            CommandText = "UPDATE Users SET Name = 'After' WHERE Id = 613 RETURNING users.*"
        };

        using var reader = command.ExecuteReader();

        Assert.True(reader.Read());
        Assert.Equal(613, reader.GetInt32(reader.GetOrdinal("Id")));
        Assert.Equal("After", reader.GetString(reader.GetOrdinal("Name")));
        Assert.Equal("before613@test.local", reader.GetString(reader.GetOrdinal("Email")));
        Assert.False(reader.Read());
    }

}
