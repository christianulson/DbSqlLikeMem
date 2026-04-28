namespace DbSqlLikeMem.Oracle.Test;

/// <summary>
/// EN: Covers Oracle mock CRUD, transactions, batch, temporal, and RETURNING INTO behavior.
/// PT: Cobre comportamento de CRUD, transacoes, batch, temporal e RETURNING INTO do mock Oracle.
/// </summary>
public sealed class OracleMockTests
    : XUnitTestBase
{
    private readonly OracleConnectionMock _connection;

    /// <summary>
    /// EN: Creates the Oracle mock database used by the test suite.
    /// PT: Cria o banco mock Oracle usado pela suite de testes.
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
    /// EN: Verifies INSERT adds a row to the Oracle mock.
    /// PT: Verifica se INSERT adiciona uma linha no mock Oracle.
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
        Assert.Equal("John Doe", _connection.GetTable("Users")[0][1]);
    }

    /// <summary>
    /// EN: Verifies sequence defaults assign the next Oracle value when the column is omitted.
    /// PT: Verifica se defaults de sequence atribuem o proximo valor do Oracle quando a coluna e omitida.
    /// </summary>
    [Fact]
    [Trait("Category", "OracleMock")]
    public void SequenceDefault_ShouldPopulateMissingColumnValue()
    {
        var db = new OracleDbMock();
        db.CreateSchema("APP");
        var sequence = db.AddSequence("SEQ_USERS", startValue: 10, incrementBy: 5, schemaName: "APP");
        db.AddTable(
            "Users",
            [
                new("Id", DbType.Int32, false, defaultValue: sequence),
                new("Name", DbType.String, false, size: 50)
            ],
            schemaName: "APP");

        using var connection = new OracleConnectionMock(db, "APP");
        connection.Open();

        using (var command = new OracleCommandMock(connection))
        {
            command.CommandText = "INSERT INTO Users (Name) VALUES ('Ana')";
            Assert.Equal(1, command.ExecuteNonQuery());

            command.CommandText = "INSERT INTO Users (Name) VALUES ('Bia')";
            Assert.Equal(1, command.ExecuteNonQuery());
        }

        var users = connection.GetTable("Users", "APP");
        Assert.Equal(10L, Convert.ToInt64(users[0][0], CultureInfo.InvariantCulture));
        Assert.Equal(15L, Convert.ToInt64(users[1][0], CultureInfo.InvariantCulture));
    }

    /// <summary>
    /// EN: Verifies ExecuteNonQuery applies each INSERT in a multi-statement script and returns the total affected rows.
    /// PT: Verifica se ExecuteNonQuery aplica cada INSERT em um script com multiplas instrucoes e retorna o total de linhas afetadas.
    /// </summary>
    [Fact]
    [Trait("Category", "OracleMock")]
    public void ExecuteNonQuery_MultiStatementInsertScript_ShouldInsertAllRowsAndReturnTotalAffected()
    {
        using var command = new OracleCommandMock(_connection)
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
    /// EN: Verifies UPDATE modifies an existing row in the Oracle mock.
    /// PT: Verifica se UPDATE modifica uma linha existente no mock Oracle.
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
        Assert.Equal("Jane Doe", _connection.GetTable("Users")[0][1]);
    }

    /// <summary>
    /// EN: Verifies DELETE removes an existing row in the Oracle mock.
    /// PT: Verifica se DELETE remove uma linha existente no mock Oracle.
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
    /// EN: Verifies committed transactions persist their changes.
    /// PT: Verifica se transacoes confirmadas persistem suas alteracoes.
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
    /// EN: Verifies insert and update changes survive a committed transaction.
    /// PT: Verifica se alteracoes de insert e update sobrevivem a uma transacao confirmada.
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
    /// EN: Verifies rolled back transactions discard their changes.
    /// PT: Verifica se transacoes revertidas descartam suas alteracoes.
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
    /// EN: Verifies temporal functions can be used in SELECT and WHERE clauses.
    /// PT: Verifica se funcoes temporais podem ser usadas em clausulas SELECT e WHERE.
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
    /// EN: Verifies Oracle rejects FOUND_ROWS because the provider exposes ROW_COUNT for row-count inspection.
    /// PT: Verifica que o Oracle rejeita FOUND_ROWS porque o provider expoe ROW_COUNT para inspecao de contagem de linhas.
    /// </summary>
    [Fact]
    [Trait("Category", "OracleMock")]
    public void TestSelect_FoundRows_ShouldThrowNotSupportedException()
    {
        using var command = new OracleCommandMock(_connection);
        command.CommandText = """
            INSERT INTO Users (Id, Name, Email) VALUES (101, 'Ana', NULL);
            INSERT INTO Users (Id, Name, Email) VALUES (102, 'Bia', NULL);
            INSERT INTO Users (Id, Name, Email) VALUES (103, 'Caio', NULL);
            """;
        command.ExecuteNonQuery();

        command.CommandText = "SELECT Name FROM Users ORDER BY Id FETCH FIRST 1 ROWS ONLY; SELECT FOUND_ROWS();";
        var ex = Assert.Throws<NotSupportedException>(command.ExecuteReader);

        Assert.Contains("FOUND_ROWS", ex.Message, StringComparison.OrdinalIgnoreCase);
    }


    /// <summary>
    /// EN: Verifies ROW_COUNT returns the row count from the last SELECT statement.
    /// PT: Verifica se ROW_COUNT retorna a contagem de linhas do ultimo SELECT.
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
    /// EN: Verifies ROW_COUNT returns zero after BEGIN TRANSACTION in a batch.
    /// PT: Verifica se ROW_COUNT retorna zero apos BEGIN TRANSACTION em um batch.
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
    /// EN: Verifies ROW_COUNT returns zero after CALL in a batch.
    /// PT: Verifica se ROW_COUNT retorna zero apos CALL em um batch.
    /// </summary>
    [Fact]
    [Trait("Category", "OracleMock")]
    public void TestBatch_CallThenRowCount_ShouldReturnZero()
    {
        _connection.AddProdecure(new ProcedureDef("sp_ping", [], [], [], null));

        using var command = new OracleCommandMock(_connection)
        {
            CommandText = "CALL sp_ping(); SELECT ROW_COUNT();"
        };

        using var reader = command.ExecuteReader();

        Assert.True(reader.Read());
        Assert.Equal(0L, Convert.ToInt64(reader.GetValue(0)));
    }

    /// <summary>
    /// EN: Verifies ROW_COUNT returns zero after UPDATE followed by COMMIT.
    /// PT: Verifica se ROW_COUNT retorna zero apos UPDATE seguido de COMMIT.
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
    /// EN: Verifies ROW_COUNT returns zero after ROLLBACK TO SAVEPOINT.
    /// PT: Verifica se ROW_COUNT retorna zero apos ROLLBACK TO SAVEPOINT.
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
    /// EN: Verifies RELEASE SAVEPOINT emits the standardized not-supported error in Oracle batches.
    /// PT: Verifica se RELEASE SAVEPOINT emite o erro padronizado de nao suportado em lotes Oracle.
    /// </summary>
    [Fact]
    [Trait("Category", "OracleMock")]
    public void TestBatch_ReleaseSavepointThenRowCount_ShouldThrowNotSupported()
    {
        using var command = new OracleCommandMock(_connection)
        {
            CommandText = "BEGIN TRANSACTION; SAVEPOINT sp1; RELEASE SAVEPOINT sp1; SELECT ROW_COUNT();"
        };

        var ex = Assert.Throws<NotSupportedException>(() => command.ExecuteReader());
        Assert.Contains("SQL não suportado para dialeto", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("RELEASE SAVEPOINT", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("oracle", ex.Message, StringComparison.OrdinalIgnoreCase);
    }


    /// <summary>
    /// EN: Verifies ROW_COUNT reflects the last DML statement in a mixed batch.
    /// PT: Verifica se ROW_COUNT reflete a ultima instrucao DML em um batch misto.
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
    /// EN: Verifies ROW_COUNT returns zero after CALL, UPDATE, and COMMIT in a batch.
    /// PT: Verifica se ROW_COUNT retorna zero apos CALL, UPDATE e COMMIT em um batch.
    /// </summary>
    [Fact]
    [Trait("Category", "OracleMock")]
    public void TestBatch_CallUpdateCommitThenRowCount_ShouldReturnZeroAfterCommit()
    {
        _connection.AddProdecure(new ProcedureDef("sp_ping", [], [], [], null));

        using var command = new OracleCommandMock(_connection)
        {
            CommandText = "CALL sp_ping(); UPDATE Users SET Name = 'Call Dml User' WHERE Id = 1; COMMIT; SELECT ROW_COUNT();"
        };

        using var reader = command.ExecuteReader();

        Assert.True(reader.Read());
        Assert.Equal(0L, Convert.ToInt64(reader.GetValue(0)));
    }


    /// <summary>
    /// EN: Verifies ROW_COUNT reflects the last SELECT result set in a mixed batch.
    /// PT: Verifica se ROW_COUNT reflete o ultimo conjunto de resultados SELECT em um batch misto.
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
    /// EN: Verifies INSERT ... RETURNING INTO populates the output parameter.
    /// PT: Verifica se INSERT ... RETURNING INTO preenche o parametro de saida.
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
    /// EN: Verifies RETURNING INTO parsing ignores keyword-like text inside string literals.
    /// PT: Verifica se o parsing de RETURNING INTO ignora texto semelhante a palavra-chave dentro de literais.
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
    /// EN: Verifies UPDATE ... RETURNING INTO populates the output parameter.
    /// PT: Verifica se UPDATE ... RETURNING INTO preenche o parametro de saida.
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
    /// EN: Verifies DELETE ... RETURNING INTO populates the output parameter.
    /// PT: Verifica se DELETE ... RETURNING INTO preenche o parametro de saida.
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


