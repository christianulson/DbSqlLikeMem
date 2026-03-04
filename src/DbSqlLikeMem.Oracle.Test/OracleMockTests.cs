namespace DbSqlLikeMem.Oracle.Test;

/// <summary>
/// EN: Defines the class OracleMockTests.
/// PT: Define a classe OracleMockTests.
/// </summary>
public sealed class OracleMockTests
    : XUnitTestBase
{
    private readonly OracleConnectionMock _connection;

    /// <summary>
    /// EN: Initializes a new instance of OracleMockTests.
    /// PT: Inicializa uma nova instância de OracleMockTests.
    /// </summary>
    public OracleMockTests(
        ITestOutputHelper helper
        ) : base(helper)
    {
        var db = new OracleDbMock();
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

        _connection = new OracleConnectionMock(db);
        _connection.Open();
    }

    /// <summary>
    /// EN: Tests TestInsert behavior.
    /// PT: Testa o comportamento de TestInsert.
    /// </summary>
    [Fact]
    [Trait("Category", "OracleMock")]
    public void TestInsert()
    {
        using var command = new OracleCommandMock(_connection)
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
    [Trait("Category", "OracleMock")]
    public void TestUpdate()
    {
        using var command = new OracleCommandMock(_connection)
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
    [Trait("Category", "OracleMock")]
    public void TestDelete()
    {
        using var command = new OracleCommandMock(_connection)
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
    [Trait("Category", "OracleMock")]
    public void TestTransactionCommit()
    {
        using (var transaction = _connection.BeginTransaction())
        {
            using var command = new OracleCommandMock(_connection, (OracleTransactionMock)transaction)
            {
                CommandText = "INSERT INTO Users (Id, Name, Email) VALUES (1, 'John Doe', 'john@example.com')"
            };
            command.ExecuteNonQuery();
            transaction.Commit();
        }

        using var queryCommand = new OracleCommandMock(_connection)
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
    [Trait("Category", "OracleMock")]
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
    [Trait("Category", "OracleMock")]
    public void TestTransactionRollback()
    {
        using (var transaction = _connection.BeginTransaction())
        {
            using var command = new OracleCommandMock(_connection, (OracleTransactionMock)transaction)
            {
                CommandText = "INSERT INTO Users (Id, Name, Email) VALUES (1, 'John Doe', 'john@example.com')"
            };
            command.ExecuteNonQuery();
            transaction.Rollback();
        }

        using var queryCommand = new OracleCommandMock(_connection)
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
    /// EN: Tests TemporalFunctions_ShouldWorkInSelectAndWhere behavior.
    /// PT: Testa o comportamento de TemporalFunctions_ShouldWorkInSelectAndWhere.
    /// </summary>
    [Fact]
    [Trait("Category", "OracleMock")]
    public void TemporalFunctions_ShouldWorkInSelectAndWhere()
    {
        using var seed = new OracleCommandMock(_connection)
        {
            CommandText = "INSERT INTO Users (Id, Name, Email) VALUES (10, 'Ana', 'ana@x.com')"
        };
        seed.ExecuteNonQuery();

        using var command = new OracleCommandMock(_connection)
        {
            CommandText = "SELECT SYSDATE, SYSTEMDATE, CURRENT_DATE, CURRENT_TIMESTAMP FROM Users WHERE SYSDATE IS NOT NULL AND Id = 10"
        };

        using var reader = command.ExecuteReader();
        Assert.True(reader.Read());
        Assert.IsType<DateTime>(reader.GetValue(0));
        Assert.IsType<DateTime>(reader.GetValue(1));
        Assert.IsType<DateTime>(reader.GetValue(2));
        Assert.IsType<DateTime>(reader.GetValue(3));
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
    [Trait("Category", "OracleMock")]
    public void TestSelect_FoundRows_ShouldReturnLastSelectRowCount()
    {
        using var command = new OracleCommandMock(_connection);
        command.CommandText = """
            INSERT INTO Users (Id, Name, Email) VALUES (101, 'Ana', NULL);
            INSERT INTO Users (Id, Name, Email) VALUES (102, 'Bia', NULL);
            INSERT INTO Users (Id, Name, Email) VALUES (103, 'Caio', NULL);
            """;
        command.ExecuteNonQuery();

        command.CommandText = "SELECT Name FROM Users ORDER BY Id FETCH FIRST 1 ROWS ONLY; SELECT FOUND_ROWS();";
        using var reader = command.ExecuteReader();

        Assert.True(reader.Read());
        Assert.Equal("Ana", reader.GetString(0));
        Assert.True(reader.NextResult());
        Assert.True(reader.Read());
        Assert.Equal(1L, Convert.ToInt64(reader.GetValue(0)));
    }


    /// <summary>
    /// EN: Tests TestSelect_RowCountFunction_ShouldReturnLastSelectRowCount behavior.
    /// PT: Testa o comportamento de TestSelect_RowCountFunction_ShouldReturnLastSelectRowCount.
    /// </summary>
    [Fact]
    [Trait("Category", "OracleMock")]
    public void TestSelect_RowCountFunction_ShouldReturnLastSelectRowCount()
    {
        using var seed = new OracleCommandMock(_connection)
        {
            CommandText = "INSERT INTO Users (Id, Name, Email) VALUES (1, 'Seed User', NULL)"
        };
        seed.ExecuteNonQuery();

        using var command = new OracleCommandMock(_connection);
        command.CommandText = "SELECT Name FROM Users ORDER BY Id FETCH FIRST 1 ROWS ONLY; SELECT ROW_COUNT();";
        using var reader = command.ExecuteReader();

        Assert.True(reader.Read());
        Assert.True(reader.NextResult());
        Assert.True(reader.Read());
        Assert.Equal(1L, Convert.ToInt64(reader.GetValue(0)));
    }



    /// <summary>
    /// EN: Tests TestBatch_BeginTransactionThenRowCount_ShouldReturnZero behavior.
    /// PT: Testa o comportamento de TestBatch_BeginTransactionThenRowCount_ShouldReturnZero.
    /// </summary>
    [Fact]
    [Trait("Category", "OracleMock")]
    public void TestBatch_BeginTransactionThenRowCount_ShouldReturnZero()
    {
        using var command = new OracleCommandMock(_connection)
        {
            CommandText = "BEGIN TRANSACTION; SELECT ROW_COUNT();"
        };

        using var reader = command.ExecuteReader();

        Assert.True(reader.Read());
        Assert.Equal(0L, Convert.ToInt64(reader.GetValue(0)));
    }

    /// <summary>
    /// EN: Tests TestBatch_CallThenRowCount_ShouldReturnZero behavior.
    /// PT: Testa o comportamento de TestBatch_CallThenRowCount_ShouldReturnZero.
    /// </summary>
    [Fact]
    [Trait("Category", "OracleMock")]
    public void TestBatch_CallThenRowCount_ShouldReturnZero()
    {
        _connection.AddProdecure("sp_ping", new ProcedureDef([], [], [], null));

        using var command = new OracleCommandMock(_connection)
        {
            CommandText = "CALL sp_ping(); SELECT ROW_COUNT();"
        };

        using var reader = command.ExecuteReader();

        Assert.True(reader.Read());
        Assert.Equal(0L, Convert.ToInt64(reader.GetValue(0)));
    }

    /// <summary>
    /// EN: Tests TestBatch_UpdateCommitThenRowCount_ShouldReturnZeroAfterCommit behavior.
    /// PT: Testa o comportamento de TestBatch_UpdateCommitThenRowCount_ShouldReturnZeroAfterCommit.
    /// </summary>
    [Fact]
    [Trait("Category", "OracleMock")]
    public void TestBatch_UpdateCommitThenRowCount_ShouldReturnZeroAfterCommit()
    {
        using var command = new OracleCommandMock(_connection)
        {
            CommandText = "UPDATE Users SET Name = 'After Commit' WHERE Id = 1; COMMIT; SELECT ROW_COUNT();"
        };

        using var reader = command.ExecuteReader();

        Assert.True(reader.Read());
        Assert.Equal(0L, Convert.ToInt64(reader.GetValue(0)));
    }


    /// <summary>
    /// EN: Tests TestBatch_RollbackToSavepointThenRowCount_ShouldReturnZero behavior.
    /// PT: Testa o comportamento de TestBatch_RollbackToSavepointThenRowCount_ShouldReturnZero.
    /// </summary>
    [Fact]
    [Trait("Category", "OracleMock")]
    public void TestBatch_RollbackToSavepointThenRowCount_ShouldReturnZero()
    {
        using var command = new OracleCommandMock(_connection)
        {
            CommandText = "BEGIN TRANSACTION; SAVEPOINT sp1; UPDATE Users SET Name = 'Tmp' WHERE Id = 1; ROLLBACK TO SAVEPOINT sp1; SELECT ROW_COUNT();"
        };

        using var reader = command.ExecuteReader();

        Assert.True(reader.Read());
        Assert.Equal(0L, Convert.ToInt64(reader.GetValue(0)));
    }

    /// <summary>
    /// EN: Tests TestBatch_ReleaseSavepointThenRowCount_ShouldReturnZero behavior.
    /// PT: Testa o comportamento de TestBatch_ReleaseSavepointThenRowCount_ShouldReturnZero.
    /// </summary>
    [Fact]
    [Trait("Category", "OracleMock")]
    public void TestBatch_ReleaseSavepointThenRowCount_ShouldReturnZero()
    {
        using var command = new OracleCommandMock(_connection)
        {
            CommandText = "BEGIN TRANSACTION; SAVEPOINT sp1; RELEASE SAVEPOINT sp1; SELECT ROW_COUNT();"
        };

        using var reader = command.ExecuteReader();

        Assert.True(reader.Read());
        Assert.Equal(0L, Convert.ToInt64(reader.GetValue(0)));
    }


    /// <summary>
    /// EN: Tests TestBatch_SelectThenUpdateThenRowCount_ShouldReflectLastDml behavior.
    /// PT: Testa o comportamento de TestBatch_SelectThenUpdateThenRowCount_ShouldReflectLastDml.
    /// </summary>
    [Fact]
    [Trait("Category", "OracleMock")]
    public void TestBatch_SelectThenUpdateThenRowCount_ShouldReflectLastDml()
    {
        using var seed = new OracleCommandMock(_connection)
        {
            CommandText = "INSERT INTO Users (Id, Name, Email) VALUES (1, 'Seed User', NULL)"
        };
        seed.ExecuteNonQuery();

        using var command = new OracleCommandMock(_connection)
        {
            CommandText = "SELECT Name FROM Users ORDER BY Id FETCH FIRST 1 ROWS ONLY; UPDATE Users SET Name = 'Mixed Batch User' WHERE Id = 1; SELECT ROW_COUNT();"
        };

        using var reader = command.ExecuteReader();

        Assert.True(reader.Read());
        Assert.True(reader.NextResult());
        Assert.True(reader.Read());
        Assert.Equal(1L, Convert.ToInt64(reader.GetValue(0)));
    }


    /// <summary>
    /// EN: Tests TestBatch_CallUpdateCommitThenRowCount_ShouldReturnZeroAfterCommit behavior.
    /// PT: Testa o comportamento de TestBatch_CallUpdateCommitThenRowCount_ShouldReturnZeroAfterCommit.
    /// </summary>
    [Fact]
    [Trait("Category", "OracleMock")]
    public void TestBatch_CallUpdateCommitThenRowCount_ShouldReturnZeroAfterCommit()
    {
        _connection.AddProdecure("sp_ping", new ProcedureDef([], [], [], null));

        using var command = new OracleCommandMock(_connection)
        {
            CommandText = "CALL sp_ping(); UPDATE Users SET Name = 'Call Dml User' WHERE Id = 1; COMMIT; SELECT ROW_COUNT();"
        };

        using var reader = command.ExecuteReader();

        Assert.True(reader.Read());
        Assert.Equal(0L, Convert.ToInt64(reader.GetValue(0)));
    }


    /// <summary>
    /// EN: Tests TestBatch_UpdateThenSelectThenRowCount_ShouldReflectLastSelect behavior.
    /// PT: Testa o comportamento de TestBatch_UpdateThenSelectThenRowCount_ShouldReflectLastSelect.
    /// </summary>
    [Fact]
    [Trait("Category", "OracleMock")]
    public void TestBatch_UpdateThenSelectThenRowCount_ShouldReflectLastSelect()
    {
        using var seed = new OracleCommandMock(_connection)
        {
            CommandText = "INSERT INTO Users (Id, Name, Email) VALUES (1, 'Seed User 1', NULL)"
        };
        seed.ExecuteNonQuery();
        seed.CommandText = "INSERT INTO Users (Id, Name, Email) VALUES (2, 'Seed User 2', NULL)";
        seed.ExecuteNonQuery();

        using var command = new OracleCommandMock(_connection)
        {
            CommandText = "UPDATE Users SET Name = 'Last Select User' WHERE Id = 1; SELECT Name FROM Users ORDER BY Id FETCH FIRST 2 ROWS ONLY; SELECT ROW_COUNT();"
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
    /// EN: Tests ExecuteNonQuery_InsertReturningInto_ShouldPopulateOutputParameter behavior.
    /// PT: Testa o comportamento de ExecuteNonQuery_InsertReturningInto_ShouldPopulateOutputParameter.
    /// </summary>
    [Fact]
    [Trait("Category", "OracleMock")]
    public void ExecuteNonQuery_InsertReturningInto_ShouldPopulateOutputParameter()
    {
        using var command = new OracleCommandMock(_connection)
        {
            CommandText = "INSERT INTO Users (Id, Name, Email) VALUES (801, 'Returning Into', 'insert@test.local') RETURNING Id INTO :out_id"
        };

        var outParam = new OracleParameter(":out_id", OracleDbType.Int32)
        {
            Direction = ParameterDirection.Output
        };
        command.Parameters.Add(outParam);

        var affected = command.ExecuteNonQuery();

        Assert.Equal(1, affected);
        Assert.Equal(801, Convert.ToInt32(outParam.Value, CultureInfo.InvariantCulture));
    }

    /// <summary>
    /// EN: Ensures Oracle RETURNING INTO parsing ignores keyword-like text inside string literals.
    /// PT: Garante que o parsing de RETURNING INTO no Oracle ignore texto semelhante a palavra-chave dentro de literais.
    /// </summary>
    [Fact]
    [Trait("Category", "OracleMock")]
    public void ExecuteNonQuery_InsertReturningInto_WithKeywordTextInsideLiteral_ShouldPopulateOutputParameter()
    {
        using var command = new OracleCommandMock(_connection)
        {
            CommandText = "INSERT INTO Users (Id, Name, Email) VALUES (804, 'msg RETURNING INTO literal', 'insert@test.local') RETURNING Id INTO :out_id"
        };

        var outParam = new OracleParameter(":out_id", OracleDbType.Int32)
        {
            Direction = ParameterDirection.Output
        };
        command.Parameters.Add(outParam);

        var affected = command.ExecuteNonQuery();

        Assert.Equal(1, affected);
        Assert.Equal(804, Convert.ToInt32(outParam.Value, CultureInfo.InvariantCulture));
    }

    /// <summary>
    /// EN: Tests ExecuteNonQuery_UpdateReturningInto_ShouldPopulateOutputParameter behavior.
    /// PT: Testa o comportamento de ExecuteNonQuery_UpdateReturningInto_ShouldPopulateOutputParameter.
    /// </summary>
    [Fact]
    [Trait("Category", "OracleMock")]
    public void ExecuteNonQuery_UpdateReturningInto_ShouldPopulateOutputParameter()
    {
        using var setup = new OracleCommandMock(_connection)
        {
            CommandText = "INSERT INTO Users (Id, Name, Email) VALUES (802, 'Before Update', 'before@test.local')"
        };
        setup.ExecuteNonQuery();

        using var command = new OracleCommandMock(_connection)
        {
            CommandText = "UPDATE Users SET Name = 'After Update' WHERE Id = 802 RETURNING Name INTO :out_name"
        };

        var outParam = new OracleParameter(":out_name", OracleDbType.Varchar2)
        {
            Direction = ParameterDirection.Output
        };
        command.Parameters.Add(outParam);

        var affected = command.ExecuteNonQuery();

        Assert.Equal(1, affected);
        Assert.Equal("After Update", Convert.ToString(outParam.Value, CultureInfo.InvariantCulture));
    }

    /// <summary>
    /// EN: Tests ExecuteNonQuery_DeleteReturningInto_ShouldPopulateOutputParameter behavior.
    /// PT: Testa o comportamento de ExecuteNonQuery_DeleteReturningInto_ShouldPopulateOutputParameter.
    /// </summary>
    [Fact]
    [Trait("Category", "OracleMock")]
    public void ExecuteNonQuery_DeleteReturningInto_ShouldPopulateOutputParameter()
    {
        using var setup = new OracleCommandMock(_connection)
        {
            CommandText = "INSERT INTO Users (Id, Name, Email) VALUES (803, 'To Delete', 'delete@test.local')"
        };
        setup.ExecuteNonQuery();

        using var command = new OracleCommandMock(_connection)
        {
            CommandText = "DELETE FROM Users WHERE Id = 803 RETURNING Name INTO :out_name"
        };

        var outParam = new OracleParameter(":out_name", OracleDbType.Varchar2)
        {
            Direction = ParameterDirection.Output
        };
        command.Parameters.Add(outParam);

        var affected = command.ExecuteNonQuery();

        Assert.Equal(1, affected);
        Assert.Equal("To Delete", Convert.ToString(outParam.Value, CultureInfo.InvariantCulture));
        Assert.DoesNotContain(_connection.GetTable("Users"), r => Convert.ToInt32(r[0], CultureInfo.InvariantCulture) == 803);
    }

}

