namespace DbSqlLikeMem.Npgsql.Test;

/// <summary>
/// EN: Defines the class PostgreSqlMockTests.
/// PT: Define a classe PostgreSqlMockTests.
/// </summary>
public sealed class PostgreSqlMockTests
    : XUnitTestBase
{
    private readonly NpgsqlConnectionMock _connection;

    /// <summary>
    /// EN: Tests PostgreSqlMockTests behavior.
    /// PT: Testa o comportamento de PostgreSqlMockTests.
    /// </summary>
    public PostgreSqlMockTests(
        ITestOutputHelper helper
        ) : base(helper)
    {
        var db = new NpgsqlDbMock();
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
        _connection = new NpgsqlConnectionMock(db);
        _connection.Open();
    }

    /// <summary>
    /// EN: Tests TestInsert behavior.
    /// PT: Testa o comportamento de TestInsert.
    /// </summary>
    [Fact]
    [Trait("Category", "PostgreSqlMock")]
    public void TestInsert()
    {
        using var command = new NpgsqlCommandMock(_connection)
        {
            CommandText = "INSERT INTO Users (Id, Name, Email) VALUES (1, 'John Doe', 'john@example.com')"
        };
        var rowsAffected = command.ExecuteNonQuery();
        Assert.Equal(1, rowsAffected);
        Assert.Equal("John Doe", _connection.GetTable("Users")[0][1]);
    }

    /// <summary>
    /// EN: Tests TestUpdate behavior.
    /// PT: Testa o comportamento de TestUpdate.
    /// </summary>
    [Fact]
    [Trait("Category", "PostgreSqlMock")]
    public void TestUpdate()
    {
        using var command = new NpgsqlCommandMock(_connection)
        {
            CommandText = "INSERT INTO Users (Id, Name, Email) VALUES (1, 'John Doe', 'john@example.com')"
        };
        command.ExecuteNonQuery();

        command.CommandText = "UPDATE Users SET Name = 'Jane Doe' WHERE Id = 1";
        var rowsAffected = command.ExecuteNonQuery();
        Assert.Equal(1, rowsAffected);
        Assert.Equal("Jane Doe", _connection.GetTable("Users")[0][1]);
    }

    /// <summary>
    /// EN: Tests TestDelete behavior.
    /// PT: Testa o comportamento de TestDelete.
    /// </summary>
    [Fact]
    [Trait("Category", "PostgreSqlMock")]
    public void TestDelete()
    {
        using var command = new NpgsqlCommandMock(_connection)
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
    [Trait("Category", "PostgreSqlMock")]
    public void TestTransactionCommit()
    {
        using (var transaction = _connection.BeginTransaction())
        {
            using var command = new NpgsqlCommandMock(_connection, (NpgsqlTransactionMock)transaction)
            {
                CommandText = "INSERT INTO Users (Id, Name, Email) VALUES (1, 'John Doe', 'john@example.com')"
            };
            command.ExecuteNonQuery();
            transaction.Commit();
        }

        using var queryCommand = new NpgsqlCommandMock(_connection)
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
    [Trait("Category", "PostgreSqlMock")]
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
    [Trait("Category", "PostgreSqlMock")]
    public void TestTransactionRollback()
    {
        using (var transaction = _connection.BeginTransaction())
        {
            using var command = new NpgsqlCommandMock(_connection, (NpgsqlTransactionMock)transaction)
            {
                CommandText = "INSERT INTO Users (Id, Name, Email) VALUES (1, 'John Doe', 'john@example.com')"
            };
            command.ExecuteNonQuery();
            transaction.Rollback();
        }

        using var queryCommand = new NpgsqlCommandMock(_connection)
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

    [Fact]
    [Trait("Category", "PostgreSqlMock")]
    public void TestSelect_FoundRows_ShouldReturnLastSelectRowCount()
    {
        using var command = new NpgsqlCommandMock(_connection);
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
        Assert.Equal(1L, Convert.ToInt64(reader.GetValue(0)));
    }


    [Fact]
    [Trait("Category", "PostgreSqlMock")]
    public void TestSelect_RowCountFunction_ShouldReturnLastSelectRowCount()
    {
        using var command = new NpgsqlCommandMock(_connection);
        command.CommandText = "SELECT Name FROM Users ORDER BY Id LIMIT 1; SELECT ROW_COUNT();";
        using var reader = command.ExecuteReader();

        Assert.True(reader.Read());
        Assert.True(reader.NextResult());
        Assert.True(reader.Read());
        Assert.Equal(1L, Convert.ToInt64(reader.GetValue(0)));
    }

    [Fact]
    [Trait("Category", "PostgreSqlMock")]
    public void TestSelect_SqlCalcFoundRows_ShouldThrow_NotSupported()
    {
        using var command = new NpgsqlCommandMock(_connection)
        {
            CommandText = "SELECT SQL_CALC_FOUND_ROWS Name FROM Users LIMIT 1"
        };

        Assert.Throws<NotSupportedException>(() => command.ExecuteReader());
    }


    [Fact]
    [Trait("Category", "PostgreSqlMock")]
    public void TestUpdate_RowCountFunction_ShouldReturnAffectedRows()
    {
        using var command = new NpgsqlCommandMock(_connection)
        {
            CommandText = "INSERT INTO Users (Id, Name, Email) VALUES (120, 'Row Count User', NULL)"
        };
        command.ExecuteNonQuery();

        command.CommandText = "UPDATE Users SET Name = 'Updated User' WHERE Id = 120; SELECT ROW_COUNT();";
        using var reader = command.ExecuteReader();

        Assert.True(reader.Read());
        Assert.Equal(1L, Convert.ToInt64(reader.GetValue(0)));
    }



    [Fact]
    [Trait("Category", "PostgreSqlMock")]
    public void TestBatch_BeginTransactionThenRowCount_ShouldReturnZero()
    {
        using var command = new NpgsqlCommandMock(_connection)
        {
            CommandText = "BEGIN TRANSACTION; SELECT ROW_COUNT();"
        };

        using var reader = command.ExecuteReader();

        Assert.True(reader.Read());
        Assert.Equal(0L, Convert.ToInt64(reader.GetValue(0)));
    }

    [Fact]
    [Trait("Category", "PostgreSqlMock")]
    public void TestBatch_CallThenRowCount_ShouldReturnZero()
    {
        _connection.AddProdecure("sp_ping", new ProcedureDef([], [], [], null));

        using var command = new NpgsqlCommandMock(_connection)
        {
            CommandText = "CALL sp_ping(); SELECT ROW_COUNT();"
        };

        using var reader = command.ExecuteReader();

        Assert.True(reader.Read());
        Assert.Equal(0L, Convert.ToInt64(reader.GetValue(0)));
    }

    [Fact]
    [Trait("Category", "PostgreSqlMock")]
    public void TestBatch_UpdateCommitThenRowCount_ShouldReturnZeroAfterCommit()
    {
        using var command = new NpgsqlCommandMock(_connection)
        {
            CommandText = "UPDATE Users SET Name = 'After Commit' WHERE Id = 1; COMMIT; SELECT ROW_COUNT();"
        };

        using var reader = command.ExecuteReader();

        Assert.True(reader.Read());
        Assert.Equal(0L, Convert.ToInt64(reader.GetValue(0)));
    }


    [Fact]
    [Trait("Category", "PostgreSqlMock")]
    public void TestBatch_RollbackToSavepointThenRowCount_ShouldReturnZero()
    {
        using var command = new NpgsqlCommandMock(_connection)
        {
            CommandText = "BEGIN TRANSACTION; SAVEPOINT sp1; UPDATE Users SET Name = 'Tmp' WHERE Id = 1; ROLLBACK TO SAVEPOINT sp1; SELECT ROW_COUNT();"
        };

        using var reader = command.ExecuteReader();

        Assert.True(reader.Read());
        Assert.Equal(0L, Convert.ToInt64(reader.GetValue(0)));
    }

    [Fact]
    [Trait("Category", "PostgreSqlMock")]
    public void TestBatch_ReleaseSavepointThenRowCount_ShouldReturnZero()
    {
        using var command = new NpgsqlCommandMock(_connection)
        {
            CommandText = "BEGIN TRANSACTION; SAVEPOINT sp1; RELEASE SAVEPOINT sp1; SELECT ROW_COUNT();"
        };

        using var reader = command.ExecuteReader();

        Assert.True(reader.Read());
        Assert.Equal(0L, Convert.ToInt64(reader.GetValue(0)));
    }


    [Fact]
    [Trait("Category", "PostgreSqlMock")]
    public void TestBatch_SelectThenUpdateThenRowCount_ShouldReflectLastDml()
    {
        using var command = new NpgsqlCommandMock(_connection)
        {
            CommandText = "SELECT Name FROM Users ORDER BY Id LIMIT 1; UPDATE Users SET Name = 'Mixed Batch User' WHERE Id = 1; SELECT ROW_COUNT();"
        };

        using var reader = command.ExecuteReader();

        Assert.True(reader.Read());
        Assert.True(reader.NextResult());
        Assert.True(reader.Read());
        Assert.Equal(1L, Convert.ToInt64(reader.GetValue(0)));
    }


    [Fact]
    [Trait("Category", "PostgreSqlMock")]
    public void TestBatch_CallUpdateCommitThenRowCount_ShouldReturnZeroAfterCommit()
    {
        _connection.AddProdecure("sp_ping", new ProcedureDef([], [], [], null));

        using var command = new NpgsqlCommandMock(_connection)
        {
            CommandText = "CALL sp_ping(); UPDATE Users SET Name = 'Call Dml User' WHERE Id = 1; COMMIT; SELECT ROW_COUNT();"
        };

        using var reader = command.ExecuteReader();

        Assert.True(reader.Read());
        Assert.Equal(0L, Convert.ToInt64(reader.GetValue(0)));
    }


    [Fact]
    [Trait("Category", "PostgreSqlMock")]
    public void TestBatch_UpdateThenSelectThenRowCount_ShouldReflectLastSelect()
    {
        using var command = new NpgsqlCommandMock(_connection)
        {
            CommandText = "UPDATE Users SET Name = 'Last Select User' WHERE Id = 1; SELECT Name FROM Users ORDER BY Id LIMIT 2; SELECT ROW_COUNT();"
        };

        using var reader = command.ExecuteReader();

        var rows = 0;
        while (reader.Read()) rows++;
        Assert.Equal(2, rows);

        Assert.True(reader.NextResult());
        Assert.True(reader.Read());
        Assert.Equal(2L, Convert.ToInt64(reader.GetValue(0)));
    }

    [Fact]
    [Trait("Category", "PostgreSqlMock")]
    public void ExecuteReader_InsertReturning_ShouldReturnInsertedRows()
    {
        using var command = new NpgsqlCommandMock(_connection)
        {
            CommandText = "INSERT INTO Users (Id, Name, Email) VALUES (501, 'Returning Insert', 'insert@test.local') RETURNING Id, Name AS user_name"
        };

        using var reader = command.ExecuteReader();

        Assert.True(reader.Read());
        Assert.Equal(501, reader.GetInt32(reader.GetOrdinal("Id")));
        Assert.Equal("Returning Insert", reader.GetString(reader.GetOrdinal("user_name")));
        Assert.False(reader.Read());
    }

    [Fact]
    [Trait("Category", "PostgreSqlMock")]
    public void ExecuteReader_UpdateReturning_ShouldReturnUpdatedProjection()
    {
        using var setup = new NpgsqlCommandMock(_connection)
        {
            CommandText = "INSERT INTO Users (Id, Name, Email) VALUES (502, 'Before Update', 'before@test.local')"
        };
        setup.ExecuteNonQuery();

        using var command = new NpgsqlCommandMock(_connection)
        {
            CommandText = "UPDATE Users SET Name = 'After Update' WHERE Id = 502 RETURNING Id, Name"
        };

        using var reader = command.ExecuteReader();

        Assert.True(reader.Read());
        Assert.Equal(502, reader.GetInt32(reader.GetOrdinal("Id")));
        Assert.Equal("After Update", reader.GetString(reader.GetOrdinal("Name")));
        Assert.False(reader.Read());
    }

    [Fact]
    [Trait("Category", "PostgreSqlMock")]
    public void ExecuteReader_DeleteReturning_ShouldReturnDeletedRowSnapshot()
    {
        using var setup = new NpgsqlCommandMock(_connection)
        {
            CommandText = "INSERT INTO Users (Id, Name, Email) VALUES (503, 'To Delete', 'delete@test.local')"
        };
        setup.ExecuteNonQuery();

        using var command = new NpgsqlCommandMock(_connection)
        {
            CommandText = "DELETE FROM Users WHERE Id = 503 RETURNING Id, Name"
        };

        using var reader = command.ExecuteReader();

        Assert.True(reader.Read());
        Assert.Equal(503, reader.GetInt32(reader.GetOrdinal("Id")));
        Assert.Equal("To Delete", reader.GetString(reader.GetOrdinal("Name")));
        Assert.False(reader.Read());
        Assert.Empty(_connection.GetTable("Users").Where(r => Convert.ToInt32(r[0]) == 503));
    }

}
