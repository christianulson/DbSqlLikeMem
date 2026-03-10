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
    /// EN: Tests ExecuteNonQuery with multi-statement INSERT script behavior.
    /// PT: Testa o comportamento de ExecuteNonQuery com script de INSERT multi-statement.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlServerMock")]
    public void ExecuteNonQuery_MultiStatementInsertScript_ShouldInsertAllRowsAndReturnTotalAffected()
    {
        using var command = new SqlServerCommandMock(_connection)
        {
            CommandText = """
                INSERT INTO Users (Id, Name, Email) VALUES (101, 'Ana', NULL);
                INSERT INTO Users (Id, Name, Email) VALUES (102, 'Bia', NULL);
                INSERT INTO Users (Id, Name, Email) VALUES (103, 'Caio', NULL);
                """
        };

        var rowsAffected = command.ExecuteNonQuery();

        Assert.Equal(3, rowsAffected);
        var users = _connection.GetTable("Users");
        Assert.Equal(3, users.Count);
        Assert.Equal("Ana", users[0][1]);
        Assert.Equal("Bia", users[1][1]);
        Assert.Equal("Caio", users[2][1]);
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
    /// EN: Verifies SQL Server rejects FOUND_ROWS because the provider exposes ROWCOUNT and @@ROWCOUNT for row-count inspection.
    /// PT: Verifica que o SQL Server rejeita FOUND_ROWS porque o provider expoe ROWCOUNT e @@ROWCOUNT para inspecao de contagem de linhas.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlServerMock")]
    public void TestSelect_FoundRows_ShouldThrowNotSupportedException()
    {
        using var command = new SqlServerCommandMock(_connection);
        command.CommandText = """
            INSERT INTO Users (Id, Name, Email) VALUES (101, 'Ana', NULL);
            INSERT INTO Users (Id, Name, Email) VALUES (102, 'Bia', NULL);
            INSERT INTO Users (Id, Name, Email) VALUES (103, 'Caio', NULL);
            """;
        command.ExecuteNonQuery();

        command.CommandText = "SELECT Name FROM Users ORDER BY Id OFFSET 0 ROWS FETCH NEXT 1 ROWS ONLY; SELECT FOUND_ROWS();";
        var ex = Assert.Throws<NotSupportedException>(() => command.ExecuteReader());

        Assert.Contains("FOUND_ROWS", ex.Message, StringComparison.OrdinalIgnoreCase);
    }


    /// <summary>
    /// EN: Tests TestSelect_RowCountFunction_ShouldReturnLastSelectRowCount behavior.
    /// PT: Testa o comportamento de TestSelect_RowCountFunction_ShouldReturnLastSelectRowCount.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlServerMock")]
    public void TestSelect_RowCountFunction_ShouldReturnLastSelectRowCount()
    {
        using var seedFirst = new SqlServerCommandMock(_connection)
        {
            CommandText = "INSERT INTO Users (Id, Name, Email) VALUES (131, 'RowCount A', NULL)"
        };
        seedFirst.ExecuteNonQuery();
        using var seedSecond = new SqlServerCommandMock(_connection)
        {
            CommandText = "INSERT INTO Users (Id, Name, Email) VALUES (132, 'RowCount B', NULL)"
        };
        seedSecond.ExecuteNonQuery();

        using var command = new SqlServerCommandMock(_connection);
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
        using var seedFirst = new SqlServerCommandMock(_connection)
        {
            CommandText = "INSERT INTO Users (Id, Name, Email) VALUES (141, 'SysRowCount A', NULL)"
        };
        seedFirst.ExecuteNonQuery();
        using var seedSecond = new SqlServerCommandMock(_connection)
        {
            CommandText = "INSERT INTO Users (Id, Name, Email) VALUES (142, 'SysRowCount B', NULL)"
        };
        seedSecond.ExecuteNonQuery();

        using var command = new SqlServerCommandMock(_connection);
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

    /// <summary>
    /// EN: Ensures CROSS APPLY executes correlated derived subqueries and keeps only rows with matching right-side results.
    /// PT: Garante que CROSS APPLY execute subqueries derivadas correlacionadas e mantenha apenas linhas com resultado correspondente no lado direito.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlServerMock")]
    public void ExecuteReader_CrossApply_WithCorrelatedDerivedSubquery_ShouldReturnOnlyMatchingRows()
    {
        using (var seed = new SqlServerCommandMock(_connection))
        {
            seed.CommandText = """
                INSERT INTO Users (Id, Name, Email) VALUES (801, 'Ana', NULL);
                INSERT INTO Users (Id, Name, Email) VALUES (802, 'Bia', NULL);
                INSERT INTO Users (Id, Name, Email) VALUES (803, 'Caio', NULL);
                INSERT INTO Orders (OrderId, UserId, Amount) VALUES (9001, 801, 10.50);
                INSERT INTO Orders (OrderId, UserId, Amount) VALUES (9002, 801, 19.75);
                INSERT INTO Orders (OrderId, UserId, Amount) VALUES (9003, 802, 7.00);
                """;
            seed.ExecuteNonQuery();
        }

        using var command = new SqlServerCommandMock(_connection)
        {
            CommandText = """
                SELECT u.Id AS UserId, latest.OrderId AS LatestOrderId, latest.Amount AS LatestAmount
                FROM Users u
                CROSS APPLY (
                    SELECT TOP 1 o.OrderId, o.Amount
                    FROM Orders o
                    WHERE o.UserId = u.Id
                    ORDER BY o.OrderId DESC
                ) latest
                ORDER BY u.Id
                """
        };

        using var reader = command.ExecuteReader();

        Assert.True(reader.Read());
        Assert.Equal(801, reader.GetInt32(reader.GetOrdinal("UserId")));
        Assert.Equal(9002, reader.GetInt32(reader.GetOrdinal("LatestOrderId")));
        Assert.Equal(19.75m, reader.GetDecimal(reader.GetOrdinal("LatestAmount")));

        Assert.True(reader.Read());
        Assert.Equal(802, reader.GetInt32(reader.GetOrdinal("UserId")));
        Assert.Equal(9003, reader.GetInt32(reader.GetOrdinal("LatestOrderId")));
        Assert.Equal(7.00m, reader.GetDecimal(reader.GetOrdinal("LatestAmount")));

        Assert.False(reader.Read());
    }

    /// <summary>
    /// EN: Ensures OUTER APPLY executes correlated derived subqueries and preserves left rows when the right-side result is empty.
    /// PT: Garante que OUTER APPLY execute subqueries derivadas correlacionadas e preserve linhas da esquerda quando o resultado do lado direito estiver vazio.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlServerMock")]
    public void ExecuteReader_OuterApply_WithCorrelatedDerivedSubquery_ShouldPreserveLeftRowsWithoutMatches()
    {
        using (var seed = new SqlServerCommandMock(_connection))
        {
            seed.CommandText = """
                INSERT INTO Users (Id, Name, Email) VALUES (811, 'Ana', NULL);
                INSERT INTO Users (Id, Name, Email) VALUES (812, 'Bia', NULL);
                INSERT INTO Orders (OrderId, UserId, Amount) VALUES (9101, 811, 3.25);
                """;
            seed.ExecuteNonQuery();
        }

        using var command = new SqlServerCommandMock(_connection)
        {
            CommandText = """
                SELECT u.Id AS UserId, latest.OrderId AS LatestOrderId
                FROM Users u
                OUTER APPLY (
                    SELECT TOP 1 o.OrderId
                    FROM Orders o
                    WHERE o.UserId = u.Id
                    ORDER BY o.OrderId DESC
                ) latest
                ORDER BY u.Id
                """
        };

        using var reader = command.ExecuteReader();

        Assert.True(reader.Read());
        Assert.Equal(811, reader.GetInt32(reader.GetOrdinal("UserId")));
        Assert.Equal(9101, reader.GetInt32(reader.GetOrdinal("LatestOrderId")));

        Assert.True(reader.Read());
        Assert.Equal(812, reader.GetInt32(reader.GetOrdinal("UserId")));
        Assert.True(reader.IsDBNull(reader.GetOrdinal("LatestOrderId")));

        Assert.False(reader.Read());
    }

    /// <summary>
    /// EN: Ensures CROSS APPLY OPENJSON expands correlated JSON arrays into rows on the shared SQL Server runtime path.
    /// PT: Garante que CROSS APPLY OPENJSON expanda arrays JSON correlacionados em linhas no caminho compartilhado de runtime do SQL Server.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlServerMock")]
    public void ExecuteReader_CrossApply_OpenJson_ShouldExpandJsonRows()
    {
        using (var seed = new SqlServerCommandMock(_connection))
        {
            seed.CommandText = """
                INSERT INTO Users (Id, Name, Email) VALUES (821, 'Ana', '["red","blue"]');
                INSERT INTO Users (Id, Name, Email) VALUES (822, 'Bia', '[]');
                INSERT INTO Users (Id, Name, Email) VALUES (823, 'Caio', NULL);
                """;
            seed.ExecuteNonQuery();
        }

        using var command = new SqlServerCommandMock(_connection)
        {
            CommandText = """
                SELECT u.Id AS UserId, tags.[key] AS TagIndex, tags.[value] AS TagValue, tags.[type] AS TagType
                FROM Users u
                CROSS APPLY OPENJSON(u.Email) tags
                ORDER BY u.Id, tags.[key]
                """
        };

        using var reader = command.ExecuteReader();

        Assert.True(reader.Read());
        Assert.Equal(821, reader.GetInt32(reader.GetOrdinal("UserId")));
        Assert.Equal("0", reader.GetString(reader.GetOrdinal("TagIndex")));
        Assert.Equal("red", reader.GetString(reader.GetOrdinal("TagValue")));
        Assert.Equal(1, reader.GetInt32(reader.GetOrdinal("TagType")));

        Assert.True(reader.Read());
        Assert.Equal(821, reader.GetInt32(reader.GetOrdinal("UserId")));
        Assert.Equal("1", reader.GetString(reader.GetOrdinal("TagIndex")));
        Assert.Equal("blue", reader.GetString(reader.GetOrdinal("TagValue")));
        Assert.Equal(1, reader.GetInt32(reader.GetOrdinal("TagType")));

        Assert.False(reader.Read());
    }

    /// <summary>
    /// EN: Ensures OPENJSON WITH explicit schema projects typed columns and JSON fragments on the shared SQL Server runtime path.
    /// PT: Garante que OPENJSON WITH com schema explicito projete colunas tipadas e fragmentos JSON no caminho compartilhado de runtime do SQL Server.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlServerMock")]
    public void ExecuteReader_CrossApply_OpenJsonWithSchema_ShouldProjectTypedColumns()
    {
        using (var seed = new SqlServerCommandMock(_connection))
        {
            seed.CommandText = """
                INSERT INTO Users (Id, Name, Email) VALUES (851, 'Ana', '[{"Name":"red","Qty":2,"Payload":{"kind":"primary"}},{"Name":"blue","Qty":5,"Payload":[1,2]}]');
                """;
            seed.ExecuteNonQuery();
        }

        using var command = new SqlServerCommandMock(_connection)
        {
            CommandText = """
                SELECT data.Name AS ColorName, data.Qty AS ColorQty, data.PayloadJson AS PayloadJson, data.RawJson AS RawJson
                FROM Users u
                CROSS APPLY OPENJSON(u.Email) WITH (
                    Name NVARCHAR(20) '$.Name',
                    Qty INT '$.Qty',
                    PayloadJson NVARCHAR(MAX) '$.Payload' AS JSON,
                    RawJson NVARCHAR(MAX) '$' AS JSON
                ) data
                WHERE u.Id = 851
                ORDER BY data.Qty
                """
        };

        using var reader = command.ExecuteReader();

        Assert.True(reader.Read());
        Assert.Equal("red", reader.GetString(reader.GetOrdinal("ColorName")));
        Assert.Equal(2, reader.GetInt32(reader.GetOrdinal("ColorQty")));
        Assert.Equal("""{"kind":"primary"}""", reader.GetString(reader.GetOrdinal("PayloadJson")));
        Assert.Equal("""{"Name":"red","Qty":2,"Payload":{"kind":"primary"}}""", reader.GetString(reader.GetOrdinal("RawJson")));

        Assert.True(reader.Read());
        Assert.Equal("blue", reader.GetString(reader.GetOrdinal("ColorName")));
        Assert.Equal(5, reader.GetInt32(reader.GetOrdinal("ColorQty")));
        Assert.Equal("[1,2]", reader.GetString(reader.GetOrdinal("PayloadJson")));
        Assert.Equal("""{"Name":"blue","Qty":5,"Payload":[1,2]}""", reader.GetString(reader.GetOrdinal("RawJson")));

        Assert.False(reader.Read());
    }

    /// <summary>
    /// EN: Ensures OPENJSON supports quoted-key paths and array indexes in the shared SQL Server runtime path.
    /// PT: Garante que OPENJSON suporte paths com chave entre aspas e indices de array no caminho compartilhado de runtime do SQL Server.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlServerMock")]
    public void ExecuteReader_CrossApply_OpenJsonWithQuotedKeyAndIndexPath_ShouldProjectValue()
    {
        using (var seed = new SqlServerCommandMock(_connection))
        {
            seed.CommandText = """
                INSERT INTO Users (Id, Name, Email) VALUES (861, 'Ana', '{"items":[{"Name.With.Dot":"red"},{"Name.With.Dot":"blue"}]}');
                """;
            seed.ExecuteNonQuery();
        }

        using var command = new SqlServerCommandMock(_connection)
        {
            CommandText = """
                SELECT data.Color AS ColorName
                FROM Users u
                CROSS APPLY OPENJSON(u.Email, 'lax $.items[1]') WITH (
                    Color NVARCHAR(20) '$."Name.With.Dot"'
                ) data
                WHERE u.Id = 861
                """
        };

        using var reader = command.ExecuteReader();
        Assert.True(reader.Read());
        Assert.Equal("blue", reader.GetString(reader.GetOrdinal("ColorName")));
        Assert.False(reader.Read());
    }

    /// <summary>
    /// EN: Ensures OPENJSON strict column paths raise an actionable error when the requested property is missing.
    /// PT: Garante que paths strict em colunas do OPENJSON gerem erro acionavel quando a propriedade solicitada estiver ausente.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlServerMock")]
    public void ExecuteReader_CrossApply_OpenJsonWithStrictColumnPath_ShouldThrowWhenMissing()
    {
        using (var seed = new SqlServerCommandMock(_connection))
        {
            seed.CommandText = """
                INSERT INTO Users (Id, Name, Email) VALUES (862, 'Ana', '[{"Name":"red"}]');
                """;
            seed.ExecuteNonQuery();
        }

        using var command = new SqlServerCommandMock(_connection)
        {
            CommandText = """
                SELECT data.Qty
                FROM Users u
                CROSS APPLY OPENJSON(u.Email) WITH (
                    Qty INT 'strict $.Qty'
                ) data
                WHERE u.Id = 862
                """
        };

        var ex = Assert.Throws<InvalidOperationException>(() => command.ExecuteReader());
        Assert.Contains("strict", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("Qty", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures UNPIVOT expands selected source columns into rows and skips NULL values on the shared SQL Server runtime path.
    /// PT: Garante que UNPIVOT expanda colunas selecionadas da fonte em linhas e ignore valores NULL no caminho compartilhado de runtime do SQL Server.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlServerMock")]
    public void ExecuteReader_WithUnpivot_ShouldExpandRowsAndSkipNulls()
    {
        using (var seed = new SqlServerCommandMock(_connection))
        {
            seed.CommandText = """
                INSERT INTO Users (Id, Name, Email) VALUES (871, 'Ana', 'ana@example.com');
                INSERT INTO Users (Id, Name, Email) VALUES (872, 'Bia', NULL);
                """;
            seed.ExecuteNonQuery();
        }

        using var command = new SqlServerCommandMock(_connection)
        {
            CommandText = """
                SELECT up.Id AS UserId, up.FieldName AS FieldName, up.FieldValue AS FieldValue
                FROM (SELECT Id, Name, Email FROM Users WHERE Id IN (871, 872)) src
                UNPIVOT (FieldValue FOR FieldName IN (Name, Email)) up
                ORDER BY up.Id, up.FieldName
                """
        };

        using var reader = command.ExecuteReader();

        Assert.True(reader.Read());
        Assert.Equal(871, reader.GetInt32(reader.GetOrdinal("UserId")));
        Assert.Equal("Email", reader.GetString(reader.GetOrdinal("FieldName")));
        Assert.Equal("ana@example.com", reader.GetString(reader.GetOrdinal("FieldValue")));

        Assert.True(reader.Read());
        Assert.Equal(871, reader.GetInt32(reader.GetOrdinal("UserId")));
        Assert.Equal("Name", reader.GetString(reader.GetOrdinal("FieldName")));
        Assert.Equal("Ana", reader.GetString(reader.GetOrdinal("FieldValue")));

        Assert.True(reader.Read());
        Assert.Equal(872, reader.GetInt32(reader.GetOrdinal("UserId")));
        Assert.Equal("Name", reader.GetString(reader.GetOrdinal("FieldName")));
        Assert.Equal("Bia", reader.GetString(reader.GetOrdinal("FieldValue")));

        Assert.False(reader.Read());
    }

    /// <summary>
    /// EN: Ensures FOR JSON PATH serializes the final SQL Server rowset with nested aliases and ROOT wrapper.
    /// PT: Garante que FOR JSON PATH serialize o rowset final do SQL Server com aliases aninhados e wrapper ROOT.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlServerMock")]
    public void ExecuteScalar_ForJsonPath_ShouldSerializeNestedJson()
    {
        using (var seed = new SqlServerCommandMock(_connection))
        {
            seed.CommandText = """
                INSERT INTO Users (Id, Name, Email) VALUES (881, 'Ana', 'ana@example.com');
                INSERT INTO Users (Id, Name, Email) VALUES (882, 'Bia', NULL);
                """;
            seed.ExecuteNonQuery();
        }

        using var command = new SqlServerCommandMock(_connection)
        {
            CommandText = """
                SELECT u.Id AS [User.Id], u.Name AS [User.Name], u.Email AS [User.Email]
                FROM Users u
                WHERE u.Id IN (881, 882)
                ORDER BY u.Id
                FOR JSON PATH, ROOT('users')
                """
        };

        var json = Assert.IsType<string>(command.ExecuteScalar());
        using var document = System.Text.Json.JsonDocument.Parse(json);
        var root = document.RootElement;

        Assert.True(root.TryGetProperty("users", out var users));
        Assert.Equal(System.Text.Json.JsonValueKind.Array, users.ValueKind);
        Assert.Equal(2, users.GetArrayLength());

        var firstUser = users[0].GetProperty("User");
        Assert.Equal(881, firstUser.GetProperty("Id").GetInt32());
        Assert.Equal("Ana", firstUser.GetProperty("Name").GetString());
        Assert.Equal("ana@example.com", firstUser.GetProperty("Email").GetString());

        var secondUser = users[1].GetProperty("User");
        Assert.Equal(882, secondUser.GetProperty("Id").GetInt32());
        Assert.Equal("Bia", secondUser.GetProperty("Name").GetString());
        Assert.False(secondUser.TryGetProperty("Email", out _));
    }

    /// <summary>
    /// EN: Ensures FOR JSON AUTO groups joined SQL Server rows into nested arrays by non-root source alias.
    /// PT: Garante que FOR JSON AUTO agrupe linhas com join no SQL Server em arrays aninhados pelo alias da fonte não raiz.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlServerMock")]
    public void ExecuteScalar_ForJsonAuto_ShouldNestJoinedRows()
    {
        using (var seed = new SqlServerCommandMock(_connection))
        {
            seed.CommandText = """
                INSERT INTO Users (Id, Name, Email) VALUES (891, 'Ana', NULL);
                INSERT INTO Users (Id, Name, Email) VALUES (892, 'Bia', NULL);
                INSERT INTO Orders (OrderId, UserId, Amount) VALUES (9901, 891, 10.50);
                INSERT INTO Orders (OrderId, UserId, Amount) VALUES (9902, 891, 19.75);
                INSERT INTO Orders (OrderId, UserId, Amount) VALUES (9903, 892, 7.00);
                """;
            seed.ExecuteNonQuery();
        }

        using var command = new SqlServerCommandMock(_connection)
        {
            CommandText = """
                SELECT u.Id, u.Name, o.OrderId, o.Amount
                FROM Users u
                JOIN Orders o ON o.UserId = u.Id
                WHERE u.Id IN (891, 892)
                ORDER BY u.Id, o.OrderId
                FOR JSON AUTO
                """
        };

        var json = Assert.IsType<string>(command.ExecuteScalar());
        using var document = System.Text.Json.JsonDocument.Parse(json);
        var array = document.RootElement;

        Assert.Equal(System.Text.Json.JsonValueKind.Array, array.ValueKind);
        Assert.Equal(2, array.GetArrayLength());

        var first = array[0];
        Assert.Equal(891, first.GetProperty("Id").GetInt32());
        Assert.Equal("Ana", first.GetProperty("Name").GetString());
        var firstOrders = first.GetProperty("o");
        Assert.Equal(2, firstOrders.GetArrayLength());
        Assert.Equal(9901, firstOrders[0].GetProperty("OrderId").GetInt32());
        Assert.Equal(9902, firstOrders[1].GetProperty("OrderId").GetInt32());

        var second = array[1];
        Assert.Equal(892, second.GetProperty("Id").GetInt32());
        Assert.Equal("Bia", second.GetProperty("Name").GetString());
        var secondOrders = second.GetProperty("o");
        Assert.Single(secondOrders.EnumerateArray());
        Assert.Equal(9903, secondOrders[0].GetProperty("OrderId").GetInt32());
    }

    /// <summary>
    /// EN: Ensures OUTER APPLY STRING_SPLIT preserves left rows and materializes split values for correlated text sources.
    /// PT: Garante que OUTER APPLY STRING_SPLIT preserve linhas da esquerda e materialize valores divididos para fontes de texto correlacionadas.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlServerMock")]
    public void ExecuteReader_OuterApply_StringSplit_ShouldPreserveLeftRowsWithoutTokens()
    {
        using (var seed = new SqlServerCommandMock(_connection))
        {
            seed.CommandText = """
                INSERT INTO Users (Id, Name, Email) VALUES (831, 'Ana', 'red,blue');
                INSERT INTO Users (Id, Name, Email) VALUES (832, 'Bia', NULL);
                """;
            seed.ExecuteNonQuery();
        }

        using var command = new SqlServerCommandMock(_connection)
        {
            CommandText = """
                SELECT u.Id AS UserId, part.value AS Token
                FROM Users u
                OUTER APPLY STRING_SPLIT(u.Email, ',') part
                ORDER BY u.Id, part.value
                """
        };

        using var reader = command.ExecuteReader();

        Assert.True(reader.Read());
        Assert.Equal(831, reader.GetInt32(reader.GetOrdinal("UserId")));
        Assert.Equal("blue", reader.GetString(reader.GetOrdinal("Token")));

        Assert.True(reader.Read());
        Assert.Equal(831, reader.GetInt32(reader.GetOrdinal("UserId")));
        Assert.Equal("red", reader.GetString(reader.GetOrdinal("Token")));

        Assert.True(reader.Read());
        Assert.Equal(832, reader.GetInt32(reader.GetOrdinal("UserId")));
        Assert.True(reader.IsDBNull(reader.GetOrdinal("Token")));

        Assert.False(reader.Read());
    }

    /// <summary>
    /// EN: Ensures STRING_SPLIT with enable_ordinal returns a one-based ordinal column on SQL Server 2022 semantics.
    /// PT: Garante que STRING_SPLIT com enable_ordinal retorne uma coluna ordinal baseada em um na semantica do SQL Server 2022.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlServerMock")]
    public void ExecuteReader_CrossApply_StringSplitWithOrdinal_ShouldReturnOrdinalColumn()
    {
        using (var seed = new SqlServerCommandMock(_connection))
        {
            seed.CommandText = """
                INSERT INTO Users (Id, Name, Email) VALUES (841, 'Ana', 'red,blue,green');
                """;
            seed.ExecuteNonQuery();
        }

        using var command = new SqlServerCommandMock(_connection)
        {
            CommandText = """
                SELECT part.value AS Token, part.ordinal AS TokenOrdinal
                FROM Users u
                CROSS APPLY STRING_SPLIT(u.Email, ',', 1) part
                WHERE u.Id = 841
                ORDER BY part.ordinal
                """
        };

        using var reader = command.ExecuteReader();

        Assert.True(reader.Read());
        Assert.Equal("red", reader.GetString(reader.GetOrdinal("Token")));
        Assert.Equal(1L, reader.GetInt64(reader.GetOrdinal("TokenOrdinal")));

        Assert.True(reader.Read());
        Assert.Equal("blue", reader.GetString(reader.GetOrdinal("Token")));
        Assert.Equal(2L, reader.GetInt64(reader.GetOrdinal("TokenOrdinal")));

        Assert.True(reader.Read());
        Assert.Equal("green", reader.GetString(reader.GetOrdinal("Token")));
        Assert.Equal(3L, reader.GetInt64(reader.GetOrdinal("TokenOrdinal")));

        Assert.False(reader.Read());
    }

}
