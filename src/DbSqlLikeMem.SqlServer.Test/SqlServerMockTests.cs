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
    /// EN: Ensures common scalar functions return expected SQL Server values.
    /// PT: Garante que funcoes escalares comuns retornem valores esperados no SQL Server.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlServerMock")]
    public void TestSelect_CommonScalarFunctions_ShouldReturnExpectedValues()
    {
        using var command = new SqlServerCommandMock(_connection)
        {
            CommandText = "SELECT APP_NAME()"
        };
        Assert.Equal("DbSqlLikeMem", command.ExecuteScalar());

        command.CommandText = "SELECT CHARINDEX('bar', 'foobar')";
        Assert.Equal(4, Convert.ToInt32(command.ExecuteScalar(), CultureInfo.InvariantCulture));

        command.CommandText = "SELECT ABS(-10)";
        Assert.Equal(10, Convert.ToInt32(command.ExecuteScalar(), CultureInfo.InvariantCulture));

        command.CommandText = "SELECT CEILING(1.2)";
        Assert.Equal(2, Convert.ToInt32(command.ExecuteScalar(), CultureInfo.InvariantCulture));

        command.CommandText = "SELECT ATN2(0, 1)";
        Assert.Equal(0d, Convert.ToDouble(command.ExecuteScalar(), CultureInfo.InvariantCulture));

        command.CommandText = "SELECT COS(0)";
        Assert.Equal(1d, Convert.ToDouble(command.ExecuteScalar(), CultureInfo.InvariantCulture));

        command.CommandText = "SELECT COT(1)";
        var cotValue = Convert.ToDouble(command.ExecuteScalar(), CultureInfo.InvariantCulture);
        Assert.True(cotValue > 0);

        command.CommandText = "SELECT ASCII('A')";
        Assert.Equal(65, Convert.ToInt32(command.ExecuteScalar(), CultureInfo.InvariantCulture));
    }

    /// <summary>
    /// EN: Ensures SQL Server date and metadata helpers return expected values.
    /// PT: Garante que helpers de data e metadados do SQL Server retornem valores esperados.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlServerMock")]
    public void TestSelect_DateHelpers_ShouldReturnExpectedValues()
    {
        using var command = new SqlServerCommandMock(_connection)
        {
            CommandText = "SELECT CURRENT_USER"
        };
        Assert.Equal("dbo", command.ExecuteScalar());

        command.CommandText = "SELECT DATALENGTH('AB')";
        Assert.Equal(4, Convert.ToInt32(command.ExecuteScalar(), CultureInfo.InvariantCulture));

        command.CommandText = "SELECT DATENAME(month, '2020-02-10')";
        Assert.Equal("February", command.ExecuteScalar());

        command.CommandText = "SELECT DATEPART(month, '2020-02-10')";
        Assert.Equal(2, Convert.ToInt32(command.ExecuteScalar(), CultureInfo.InvariantCulture));

        command.CommandText = "SELECT DATEDIFF(day, '2020-01-01', '2020-01-03')";
        Assert.Equal(2, Convert.ToInt32(command.ExecuteScalar(), CultureInfo.InvariantCulture));

        command.CommandText = "SELECT DEGREES(PI())";
        Assert.Equal(180d, Convert.ToDouble(command.ExecuteScalar(), CultureInfo.InvariantCulture));
    }

    /// <summary>
    /// EN: Ensures SQL Server date part constructors return expected values.
    /// PT: Garante que construtores de data do SQL Server retornem valores esperados.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlServerMock")]
    public void TestSelect_DatePartConstructors_ShouldReturnExpectedValues()
    {
        using var command = new SqlServerCommandMock(_connection)
        {
            CommandText = "SELECT DATEFROMPARTS(2020, 2, 29)"
        };
        Assert.Equal(new DateTime(2020, 2, 29), Convert.ToDateTime(command.ExecuteScalar(), CultureInfo.InvariantCulture));

        command.CommandText = "SELECT DATETIMEFROMPARTS(2020, 2, 29, 10, 11, 12)";
        Assert.Equal(new DateTime(2020, 2, 29, 10, 11, 12), Convert.ToDateTime(command.ExecuteScalar(), CultureInfo.InvariantCulture));

        command.CommandText = "SELECT DATETIME2FROMPARTS(2020, 2, 29, 10, 11, 12, 1234567)";
        Assert.Equal(new DateTime(2020, 2, 29, 10, 11, 12).AddTicks(1234567 * 10L), Convert.ToDateTime(command.ExecuteScalar(), CultureInfo.InvariantCulture));

        command.CommandText = "SELECT DATETIMEOFFSETFROMPARTS(2020, 2, 29, 10, 11, 12, 1234567, 60)";
        var offset = (DateTimeOffset)command.ExecuteScalar()!;
        Assert.Equal(new DateTimeOffset(new DateTime(2020, 2, 29, 10, 11, 12).AddTicks(1234567 * 10L), TimeSpan.FromMinutes(60)), offset);

        command.CommandText = "SELECT DATEDIFF_BIG(day, '2020-01-01', '2020-01-03')";
        Assert.Equal(2L, Convert.ToInt64(command.ExecuteScalar(), CultureInfo.InvariantCulture));

        command.CommandText = "SELECT EOMONTH('2020-02-15')";
        Assert.Equal(new DateTime(2020, 2, 29), Convert.ToDateTime(command.ExecuteScalar(), CultureInfo.InvariantCulture));
    }

    /// <summary>
    /// EN: Ensures SQL Server math and error helpers return expected values.
    /// PT: Garante que helpers matematicos e de erro do SQL Server retornem valores esperados.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlServerMock")]
    public void TestSelect_MathAndErrorFunctions_ShouldReturnExpectedValues()
    {
        using var command = new SqlServerCommandMock(_connection)
        {
            CommandText = "SELECT EXP(1)"
        };
        var exp = Convert.ToDouble(command.ExecuteScalar(), CultureInfo.InvariantCulture);
        Assert.True(exp > 2.7 && exp < 2.8);

        command.CommandText = "SELECT FLOOR(1.9)";
        Assert.Equal(1, Convert.ToInt32(command.ExecuteScalar(), CultureInfo.InvariantCulture));

        command.CommandText = "SELECT GETUTCDATE()";
        Assert.IsType<DateTime>(command.ExecuteScalar());

        command.CommandText = "SELECT ERROR_LINE()";
        Assert.Equal(0, Convert.ToInt32(command.ExecuteScalar(), CultureInfo.InvariantCulture));

        command.CommandText = "SELECT ERROR_MESSAGE()";
        Assert.Equal(string.Empty, command.ExecuteScalar());

        command.CommandText = "SELECT ERROR_NUMBER()";
        Assert.Equal(0, Convert.ToInt32(command.ExecuteScalar(), CultureInfo.InvariantCulture));

        command.CommandText = "SELECT DIFFERENCE('Robert', 'Rupert')";
        Assert.Equal(4, Convert.ToInt32(command.ExecuteScalar(), CultureInfo.InvariantCulture));
    }

    /// <summary>
    /// EN: Ensures common SQL Server metadata and validation helpers return expected values.
    /// PT: Garante que helpers de metadados e validacao do SQL Server retornem valores esperados.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlServerMock")]
    public void TestSelect_MetadataHelpers_ShouldReturnExpectedValues()
    {
        using var command = new SqlServerCommandMock(_connection)
        {
            CommandText = "SELECT GETANSINULL()"
        };
        Assert.Equal(1, Convert.ToInt32(command.ExecuteScalar(), CultureInfo.InvariantCulture));

        command.CommandText = "SELECT GROUPING(1)";
        Assert.Equal(0, Convert.ToInt32(command.ExecuteScalar(), CultureInfo.InvariantCulture));

        command.CommandText = "SELECT GROUPING_ID(1, 2)";
        Assert.Equal(0, Convert.ToInt32(command.ExecuteScalar(), CultureInfo.InvariantCulture));

        command.CommandText = "SELECT HOST_ID()";
        Assert.Equal(1, Convert.ToInt32(command.ExecuteScalar(), CultureInfo.InvariantCulture));

        command.CommandText = "SELECT HOST_NAME()";
        Assert.Equal("localhost", command.ExecuteScalar());

        command.CommandText = "SELECT ISDATE('2020-01-01')";
        Assert.Equal(1, Convert.ToInt32(command.ExecuteScalar(), CultureInfo.InvariantCulture));

        command.CommandText = "SELECT ISDATE('invalid')";
        Assert.Equal(0, Convert.ToInt32(command.ExecuteScalar(), CultureInfo.InvariantCulture));

        command.CommandText = "SELECT ISJSON('{\"a\":1}')";
        Assert.Equal(1, Convert.ToInt32(command.ExecuteScalar(), CultureInfo.InvariantCulture));

        command.CommandText = "SELECT ISJSON('invalid')";
        Assert.Equal(0, Convert.ToInt32(command.ExecuteScalar(), CultureInfo.InvariantCulture));

        command.CommandText = "SELECT ISNUMERIC('10.5')";
        Assert.Equal(1, Convert.ToInt32(command.ExecuteScalar(), CultureInfo.InvariantCulture));

        command.CommandText = "SELECT ISNUMERIC('abc')";
        Assert.Equal(0, Convert.ToInt32(command.ExecuteScalar(), CultureInfo.InvariantCulture));
    }

    /// <summary>
    /// EN: Ensures common string and math functions return expected SQL Server values.
    /// PT: Garante que funcoes comuns de string e matematica retornem valores esperados no SQL Server.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlServerMock")]
    public void TestSelect_StringAndLogFunctions_ShouldReturnExpectedValues()
    {
        using var command = new SqlServerCommandMock(_connection)
        {
            CommandText = "SELECT LEN('abcd')"
        };
        Assert.Equal(4, Convert.ToInt32(command.ExecuteScalar(), CultureInfo.InvariantCulture));

        command.CommandText = "SELECT LTRIM('  abc')";
        Assert.Equal("abc", command.ExecuteScalar());

        command.CommandText = "SELECT LOG(10)";
        var log = Convert.ToDouble(command.ExecuteScalar(), CultureInfo.InvariantCulture);
        Assert.True(log > 2.3 && log < 2.4);

        command.CommandText = "SELECT LOG10(1000)";
        Assert.Equal(3d, Convert.ToDouble(command.ExecuteScalar(), CultureInfo.InvariantCulture));
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
    /// EN: Ensures PIVOT and UNPIVOT preserve source column metadata for copied columns on the shared SQL Server runtime path.
    /// PT: Garante que PIVOT e UNPIVOT preservem o metadata das colunas de origem para colunas copiadas no caminho compartilhado de runtime do SQL Server.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlServerMock")]
    public void ExecuteReader_WithPivotAndUnpivotCopiedColumns_ShouldExposeSourceFieldTypes()
    {
        using var pivotCommand = new SqlServerCommandMock(_connection)
        {
            CommandText = """
                SELECT p.Category, p.T10
                FROM (
                    SELECT CAST('A' AS NVARCHAR(10)) AS Category, 10 AS TenantId, CAST(2.0 AS FLOAT) AS Amount
                    UNION ALL SELECT CAST('A' AS NVARCHAR(10)), 10, CAST(4.0 AS FLOAT)
                ) src
                PIVOT (
                    MAX(Amount) FOR TenantId IN (10 AS T10)
                ) p
                """
        };

        using var pivotReader = pivotCommand.ExecuteReader();
        Assert.Equal(typeof(string), pivotReader.GetFieldType(pivotReader.GetOrdinal("Category")));
        Assert.Equal("String", pivotReader.GetDataTypeName(pivotReader.GetOrdinal("Category")));

        using var unpivotCommand = new SqlServerCommandMock(_connection)
        {
            CommandText = """
                SELECT up.Id, up.FieldName, up.FieldValue
                FROM (
                    SELECT 1 AS Id, CAST('Ana' AS NVARCHAR(50)) AS Name, CAST('ana@example.com' AS NVARCHAR(100)) AS Email
                ) src
                UNPIVOT (FieldValue FOR FieldName IN (Name, Email)) up
                """
        };

        using var unpivotReader = unpivotCommand.ExecuteReader();
        Assert.Equal(typeof(int), unpivotReader.GetFieldType(unpivotReader.GetOrdinal("Id")));
        Assert.Equal("Int32", unpivotReader.GetDataTypeName(unpivotReader.GetOrdinal("Id")));
        Assert.Equal(typeof(string), unpivotReader.GetFieldType(unpivotReader.GetOrdinal("FieldName")));
        Assert.Equal("String", unpivotReader.GetDataTypeName(unpivotReader.GetOrdinal("FieldName")));
        Assert.Equal(typeof(string), unpivotReader.GetFieldType(unpivotReader.GetOrdinal("FieldValue")));
        Assert.Equal("String", unpivotReader.GetDataTypeName(unpivotReader.GetOrdinal("FieldValue")));

        using var mixedUnpivotCommand = new SqlServerCommandMock(_connection)
        {
            CommandText = """
                SELECT up.Id, up.FieldValue
                FROM (
                    SELECT 1 AS Id, CAST('Ana' AS NVARCHAR(50)) AS Name, CAST(42 AS INT) AS Score
                ) src
                UNPIVOT (FieldValue FOR FieldName IN (Name, Score)) up
                """
        };

        using var mixedUnpivotReader = mixedUnpivotCommand.ExecuteReader();
        Assert.Equal(typeof(object), mixedUnpivotReader.GetFieldType(mixedUnpivotReader.GetOrdinal("FieldValue")));
        Assert.Equal("Object", mixedUnpivotReader.GetDataTypeName(mixedUnpivotReader.GetOrdinal("FieldValue")));

        using var schemaCommand = new SqlServerCommandMock(_connection)
        {
            CommandText = """
                SELECT up.Id, up.FieldName, up.FieldValue
                FROM (
                    SELECT 1 AS Id, CAST('Ana' AS NVARCHAR(50)) AS Name, CAST('ana@example.com' AS NVARCHAR(100)) AS Email
                ) src
                UNPIVOT (FieldValue FOR FieldName IN (Name, Email)) up
                """
        };

        using var schemaReader = schemaCommand.ExecuteReader();
        var schema = schemaReader.GetSchemaTable();
        Assert.NotNull(schema);
        Assert.False(schema!.Columns["FieldValue"]!.AllowDBNull);
        Assert.False(schema.Columns["FieldName"]!.AllowDBNull);
    }

    /// <summary>
    /// EN: Ensures PIVOT computes STDEV, STDEVP, VAR, and VARP through the shared SQL Server runtime path.
    /// PT: Garante que PIVOT calcule STDEV, STDEVP, VAR e VARP pelo caminho compartilhado de runtime do SQL Server.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlServerMock")]
    public void ExecuteReader_WithPivotVarianceAggregates_ShouldReturnExpectedNumbers()
    {
        static string BuildPivotSql(string aggregateName)
            => $"""
                SELECT p.T10, p.T20
                FROM (
                    SELECT 10 AS TenantId, CAST(2.0 AS FLOAT) AS Amount
                    UNION ALL SELECT 10, CAST(4.0 AS FLOAT)
                    UNION ALL SELECT 20, CAST(1.0 AS FLOAT)
                    UNION ALL SELECT 20, CAST(5.0 AS FLOAT)
                ) src
                PIVOT (
                    {aggregateName}(Amount) FOR TenantId IN (10 AS T10, 20 AS T20)
                ) p
                """;

        static void AssertPivotAggregate(
            SqlServerConnectionMock connection,
            string aggregateName,
            double expectedTenant10,
            double expectedTenant20)
        {
            using var command = new SqlServerCommandMock(connection)
            {
                CommandText = BuildPivotSql(aggregateName)
            };

            using var reader = command.ExecuteReader();
            Assert.True(reader.Read());
            Assert.Equal(expectedTenant10, reader.GetDouble(reader.GetOrdinal("T10")), 10);
            Assert.Equal(expectedTenant20, reader.GetDouble(reader.GetOrdinal("T20")), 10);
            Assert.False(reader.Read());
        }

        AssertPivotAggregate(_connection, "STDEV", Math.Sqrt(2d), Math.Sqrt(8d));
        AssertPivotAggregate(_connection, "STDEVP", 1d, 2d);
        AssertPivotAggregate(_connection, "VAR", 2d, 8d);
        AssertPivotAggregate(_connection, "VARP", 1d, 4d);
    }

    /// <summary>
    /// EN: Ensures PIVOT computes COUNT_BIG with bigint-shaped results on the shared SQL Server runtime path.
    /// PT: Garante que PIVOT calcule COUNT_BIG com resultado no shape bigint no caminho compartilhado de runtime do SQL Server.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlServerMock")]
    public void ExecuteReader_WithPivotCountBig_ShouldReturnInt64Counts()
    {
        using var command = new SqlServerCommandMock(_connection)
        {
            CommandText = """
                SELECT p.T10, p.T20
                FROM (
                    SELECT 10 AS TenantId, CAST(2.0 AS FLOAT) AS Amount
                    UNION ALL SELECT 10, CAST(NULL AS FLOAT)
                    UNION ALL SELECT 10, CAST(4.0 AS FLOAT)
                    UNION ALL SELECT 20, CAST(1.0 AS FLOAT)
                    UNION ALL SELECT 20, CAST(5.0 AS FLOAT)
                ) src
                PIVOT (
                    COUNT_BIG(Amount) FOR TenantId IN (10 AS T10, 20 AS T20)
                ) p
                """
        };

        using var reader = command.ExecuteReader();
        Assert.True(reader.Read());
        Assert.Equal(2L, reader.GetInt64(reader.GetOrdinal("T10")));
        Assert.Equal(2L, reader.GetInt64(reader.GetOrdinal("T20")));
        Assert.False(reader.Read());
    }

    /// <summary>
    /// EN: Ensures PIVOT exposes aggregate column metadata aligned with COUNT_BIG and statistical return types on the shared SQL Server runtime path.
    /// PT: Garante que PIVOT exponha metadados de coluna alinhados aos tipos de retorno de COUNT_BIG e agregadores estatisticos no caminho compartilhado de runtime do SQL Server.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlServerMock")]
    public void ExecuteReader_WithPivotAggregateMetadata_ShouldExposeExpectedFieldTypes()
    {
        using var countBigCommand = new SqlServerCommandMock(_connection)
        {
            CommandText = """
                SELECT p.T10
                FROM (
                    SELECT 10 AS TenantId, CAST(2.0 AS FLOAT) AS Amount
                    UNION ALL SELECT 10, CAST(4.0 AS FLOAT)
                ) src
                PIVOT (
                    COUNT_BIG(Amount) FOR TenantId IN (10 AS T10)
                ) p
                """
        };

        using var countBigReader = countBigCommand.ExecuteReader();
        Assert.Equal(typeof(long), countBigReader.GetFieldType(countBigReader.GetOrdinal("T10")));
        Assert.Equal("Int64", countBigReader.GetDataTypeName(countBigReader.GetOrdinal("T10")));
        var countBigSchema = countBigReader.GetSchemaTable();
        Assert.NotNull(countBigSchema);
        Assert.True(countBigSchema!.Columns["T10"]!.AllowDBNull);

        using var countCommand = new SqlServerCommandMock(_connection)
        {
            CommandText = """
                SELECT p.T10
                FROM (
                    SELECT 10 AS TenantId, CAST(2.0 AS FLOAT) AS Amount
                    UNION ALL SELECT 10, CAST(4.0 AS FLOAT)
                ) src
                PIVOT (
                    COUNT(Amount) FOR TenantId IN (10 AS T10)
                ) p
                """
        };

        using var countReader = countCommand.ExecuteReader();
        Assert.Equal(typeof(int), countReader.GetFieldType(countReader.GetOrdinal("T10")));
        Assert.Equal("Int32", countReader.GetDataTypeName(countReader.GetOrdinal("T10")));
        var countSchema = countReader.GetSchemaTable();
        Assert.NotNull(countSchema);
        Assert.True(countSchema!.Columns["T10"]!.AllowDBNull);

        using var countStarCommand = new SqlServerCommandMock(_connection)
        {
            CommandText = """
                SELECT p.T10
                FROM (
                    SELECT 10 AS TenantId, CAST(2.0 AS FLOAT) AS Amount
                    UNION ALL SELECT 10, CAST(NULL AS FLOAT)
                ) src
                PIVOT (
                    COUNT(*) FOR TenantId IN (10 AS T10)
                ) p
                """
        };

        using var countStarReader = countStarCommand.ExecuteReader();
        Assert.Equal(typeof(int), countStarReader.GetFieldType(countStarReader.GetOrdinal("T10")));
        Assert.Equal("Int32", countStarReader.GetDataTypeName(countStarReader.GetOrdinal("T10")));
        var countStarSchema = countStarReader.GetSchemaTable();
        Assert.NotNull(countStarSchema);
        Assert.True(countStarSchema!.Columns["T10"]!.AllowDBNull);

        using var stdevCommand = new SqlServerCommandMock(_connection)
        {
            CommandText = """
                SELECT p.T10
                FROM (
                    SELECT 10 AS TenantId, CAST(2.0 AS FLOAT) AS Amount
                    UNION ALL SELECT 10, CAST(4.0 AS FLOAT)
                ) src
                PIVOT (
                    STDEV(Amount) FOR TenantId IN (10 AS T10)
                ) p
                """
        };

        using var stdevReader = stdevCommand.ExecuteReader();
        Assert.Equal(typeof(double), stdevReader.GetFieldType(stdevReader.GetOrdinal("T10")));
        Assert.Equal("Double", stdevReader.GetDataTypeName(stdevReader.GetOrdinal("T10")));

        using var maxCommand = new SqlServerCommandMock(_connection)
        {
            CommandText = """
                SELECT p.T10
                FROM (
                    SELECT 10 AS TenantId, CAST(2.0 AS FLOAT) AS Amount
                    UNION ALL SELECT 10, CAST(4.0 AS FLOAT)
                ) src
                PIVOT (
                    MAX(Amount) FOR TenantId IN (10 AS T10)
                ) p
                """
        };

        using var maxReader = maxCommand.ExecuteReader();
        Assert.Equal(typeof(double), maxReader.GetFieldType(maxReader.GetOrdinal("T10")));
        Assert.Equal("Double", maxReader.GetDataTypeName(maxReader.GetOrdinal("T10")));

        using var sumCommand = new SqlServerCommandMock(_connection)
        {
            CommandText = """
                SELECT p.T10
                FROM (
                    SELECT 10 AS TenantId, CAST(2.0 AS FLOAT) AS Amount
                    UNION ALL SELECT 10, CAST(4.0 AS FLOAT)
                ) src
                PIVOT (
                    SUM(Amount) FOR TenantId IN (10 AS T10)
                ) p
                """
        };

        using var sumReader = sumCommand.ExecuteReader();
        Assert.Equal(typeof(double), sumReader.GetFieldType(sumReader.GetOrdinal("T10")));
        Assert.Equal("Double", sumReader.GetDataTypeName(sumReader.GetOrdinal("T10")));

        using var sumSmallIntCommand = new SqlServerCommandMock(_connection)
        {
            CommandText = """
                SELECT p.T10
                FROM (
                    SELECT 10 AS TenantId, CAST(2 AS SMALLINT) AS Amount
                    UNION ALL SELECT 10, CAST(4 AS SMALLINT)
                ) src
                PIVOT (
                    SUM(Amount) FOR TenantId IN (10 AS T10)
                ) p
                """
        };

        using var sumSmallIntReader = sumSmallIntCommand.ExecuteReader();
        Assert.Equal(typeof(int), sumSmallIntReader.GetFieldType(sumSmallIntReader.GetOrdinal("T10")));
        Assert.Equal("Int32", sumSmallIntReader.GetDataTypeName(sumSmallIntReader.GetOrdinal("T10")));
        Assert.True(sumSmallIntReader.Read());
        Assert.Equal(6, sumSmallIntReader.GetInt32(sumSmallIntReader.GetOrdinal("T10")));
        Assert.IsType<int>(sumSmallIntReader.GetValue(sumSmallIntReader.GetOrdinal("T10")));
        Assert.False(sumSmallIntReader.Read());

        using var sumTinyIntCommand = new SqlServerCommandMock(_connection)
        {
            CommandText = """
                SELECT p.T10
                FROM (
                    SELECT 10 AS TenantId, CAST(2 AS TINYINT) AS Amount
                    UNION ALL SELECT 10, CAST(4 AS TINYINT)
                ) src
                PIVOT (
                    SUM(Amount) FOR TenantId IN (10 AS T10)
                ) p
                """
        };

        using var sumTinyIntReader = sumTinyIntCommand.ExecuteReader();
        Assert.Equal(typeof(int), sumTinyIntReader.GetFieldType(sumTinyIntReader.GetOrdinal("T10")));
        Assert.Equal("Int32", sumTinyIntReader.GetDataTypeName(sumTinyIntReader.GetOrdinal("T10")));
        Assert.True(sumTinyIntReader.Read());
        Assert.Equal(6, sumTinyIntReader.GetInt32(sumTinyIntReader.GetOrdinal("T10")));
        Assert.IsType<int>(sumTinyIntReader.GetValue(sumTinyIntReader.GetOrdinal("T10")));
        Assert.False(sumTinyIntReader.Read());

        using var avgCommand = new SqlServerCommandMock(_connection)
        {
            CommandText = """
                SELECT p.T10
                FROM (
                    SELECT 10 AS TenantId, CAST(2.0 AS FLOAT) AS Amount
                    UNION ALL SELECT 10, CAST(4.0 AS FLOAT)
                ) src
                PIVOT (
                    AVG(Amount) FOR TenantId IN (10 AS T10)
                ) p
                """
        };

        using var avgReader = avgCommand.ExecuteReader();
        Assert.Equal(typeof(double), avgReader.GetFieldType(avgReader.GetOrdinal("T10")));
        Assert.Equal("Double", avgReader.GetDataTypeName(avgReader.GetOrdinal("T10")));
        var avgSchema = avgReader.GetSchemaTable();
        Assert.NotNull(avgSchema);
        Assert.True(avgSchema!.Columns["T10"]!.AllowDBNull);

        using var avgSmallIntCommand = new SqlServerCommandMock(_connection)
        {
            CommandText = """
                SELECT p.T10
                FROM (
                    SELECT 10 AS TenantId, CAST(2 AS SMALLINT) AS Amount
                    UNION ALL SELECT 10, CAST(4 AS SMALLINT)
                ) src
                PIVOT (
                    AVG(Amount) FOR TenantId IN (10 AS T10)
                ) p
                """
        };

        using var avgSmallIntReader = avgSmallIntCommand.ExecuteReader();
        Assert.Equal(typeof(int), avgSmallIntReader.GetFieldType(avgSmallIntReader.GetOrdinal("T10")));
        Assert.Equal("Int32", avgSmallIntReader.GetDataTypeName(avgSmallIntReader.GetOrdinal("T10")));
        Assert.True(avgSmallIntReader.Read());
        Assert.Equal(3, avgSmallIntReader.GetInt32(avgSmallIntReader.GetOrdinal("T10")));
        Assert.IsType<int>(avgSmallIntReader.GetValue(avgSmallIntReader.GetOrdinal("T10")));
        Assert.False(avgSmallIntReader.Read());

        using var avgTinyIntCommand = new SqlServerCommandMock(_connection)
        {
            CommandText = """
                SELECT p.T10
                FROM (
                    SELECT 10 AS TenantId, CAST(2 AS TINYINT) AS Amount
                    UNION ALL SELECT 10, CAST(4 AS TINYINT)
                ) src
                PIVOT (
                    AVG(Amount) FOR TenantId IN (10 AS T10)
                ) p
                """
        };

        using var avgTinyIntReader = avgTinyIntCommand.ExecuteReader();
        Assert.Equal(typeof(int), avgTinyIntReader.GetFieldType(avgTinyIntReader.GetOrdinal("T10")));
        Assert.Equal("Int32", avgTinyIntReader.GetDataTypeName(avgTinyIntReader.GetOrdinal("T10")));
        Assert.True(avgTinyIntReader.Read());
        Assert.Equal(3, avgTinyIntReader.GetInt32(avgTinyIntReader.GetOrdinal("T10")));
        Assert.IsType<int>(avgTinyIntReader.GetValue(avgTinyIntReader.GetOrdinal("T10")));
        Assert.False(avgTinyIntReader.Read());

        using var avgTinyIntFractionCommand = new SqlServerCommandMock(_connection)
        {
            CommandText = """
                SELECT p.T10
                FROM (
                    SELECT 10 AS TenantId, CAST(1 AS TINYINT) AS Amount
                    UNION ALL SELECT 10, CAST(2 AS TINYINT)
                ) src
                PIVOT (
                    AVG(Amount) FOR TenantId IN (10 AS T10)
                ) p
                """
        };

        using var avgTinyIntFractionReader = avgTinyIntFractionCommand.ExecuteReader();
        Assert.True(avgTinyIntFractionReader.Read());
        Assert.Equal(1, avgTinyIntFractionReader.GetInt32(avgTinyIntFractionReader.GetOrdinal("T10")));
        Assert.IsType<int>(avgTinyIntFractionReader.GetValue(avgTinyIntFractionReader.GetOrdinal("T10")));
        Assert.False(avgTinyIntFractionReader.Read());

        using var avgIntCommand = new SqlServerCommandMock(_connection)
        {
            CommandText = """
                SELECT p.T10
                FROM (
                    SELECT 10 AS TenantId, CAST(2 AS INT) AS Amount
                    UNION ALL SELECT 10, CAST(4 AS INT)
                ) src
                PIVOT (
                    AVG(Amount) FOR TenantId IN (10 AS T10)
                ) p
                """
        };

        using var avgIntReader = avgIntCommand.ExecuteReader();
        Assert.Equal(typeof(int), avgIntReader.GetFieldType(avgIntReader.GetOrdinal("T10")));
        Assert.Equal("Int32", avgIntReader.GetDataTypeName(avgIntReader.GetOrdinal("T10")));
        Assert.True(avgIntReader.Read());
        Assert.Equal(3, avgIntReader.GetInt32(avgIntReader.GetOrdinal("T10")));
        Assert.IsType<int>(avgIntReader.GetValue(avgIntReader.GetOrdinal("T10")));
        Assert.False(avgIntReader.Read());

        using var avgIntFractionCommand = new SqlServerCommandMock(_connection)
        {
            CommandText = """
                SELECT p.T10
                FROM (
                    SELECT 10 AS TenantId, CAST(1 AS INT) AS Amount
                    UNION ALL SELECT 10, CAST(2 AS INT)
                ) src
                PIVOT (
                    AVG(Amount) FOR TenantId IN (10 AS T10)
                ) p
                """
        };

        using var avgIntFractionReader = avgIntFractionCommand.ExecuteReader();
        Assert.True(avgIntFractionReader.Read());
        Assert.Equal(1, avgIntFractionReader.GetInt32(avgIntFractionReader.GetOrdinal("T10")));
        Assert.IsType<int>(avgIntFractionReader.GetValue(avgIntFractionReader.GetOrdinal("T10")));
        Assert.False(avgIntFractionReader.Read());

        using var avgIntNegativeFractionCommand = new SqlServerCommandMock(_connection)
        {
            CommandText = """
                SELECT p.T10
                FROM (
                    SELECT 10 AS TenantId, CAST(-1 AS INT) AS Amount
                    UNION ALL SELECT 10, CAST(-2 AS INT)
                ) src
                PIVOT (
                    AVG(Amount) FOR TenantId IN (10 AS T10)
                ) p
                """
        };

        using var avgIntNegativeFractionReader = avgIntNegativeFractionCommand.ExecuteReader();
        Assert.True(avgIntNegativeFractionReader.Read());
        Assert.Equal(-1, avgIntNegativeFractionReader.GetInt32(avgIntNegativeFractionReader.GetOrdinal("T10")));
        Assert.IsType<int>(avgIntNegativeFractionReader.GetValue(avgIntNegativeFractionReader.GetOrdinal("T10")));
        Assert.False(avgIntNegativeFractionReader.Read());

        using var avgBigIntCommand = new SqlServerCommandMock(_connection)
        {
            CommandText = """
                SELECT p.T10
                FROM (
                    SELECT 10 AS TenantId, CAST(2 AS BIGINT) AS Amount
                    UNION ALL SELECT 10, CAST(4 AS BIGINT)
                ) src
                PIVOT (
                    AVG(Amount) FOR TenantId IN (10 AS T10)
                ) p
                """
        };

        using var avgBigIntReader = avgBigIntCommand.ExecuteReader();
        Assert.Equal(typeof(long), avgBigIntReader.GetFieldType(avgBigIntReader.GetOrdinal("T10")));
        Assert.Equal("Int64", avgBigIntReader.GetDataTypeName(avgBigIntReader.GetOrdinal("T10")));
        Assert.True(avgBigIntReader.Read());
        Assert.Equal(3L, avgBigIntReader.GetInt64(avgBigIntReader.GetOrdinal("T10")));
        Assert.IsType<long>(avgBigIntReader.GetValue(avgBigIntReader.GetOrdinal("T10")));
        Assert.False(avgBigIntReader.Read());

        using var avgBigIntFractionCommand = new SqlServerCommandMock(_connection)
        {
            CommandText = """
                SELECT p.T10
                FROM (
                    SELECT 10 AS TenantId, CAST(1 AS BIGINT) AS Amount
                    UNION ALL SELECT 10, CAST(2 AS BIGINT)
                ) src
                PIVOT (
                    AVG(Amount) FOR TenantId IN (10 AS T10)
                ) p
                """
        };

        using var avgBigIntFractionReader = avgBigIntFractionCommand.ExecuteReader();
        Assert.True(avgBigIntFractionReader.Read());
        Assert.Equal(1L, avgBigIntFractionReader.GetInt64(avgBigIntFractionReader.GetOrdinal("T10")));
        Assert.IsType<long>(avgBigIntFractionReader.GetValue(avgBigIntFractionReader.GetOrdinal("T10")));
        Assert.False(avgBigIntFractionReader.Read());

        using var avgBigIntNegativeFractionCommand = new SqlServerCommandMock(_connection)
        {
            CommandText = """
                SELECT p.T10
                FROM (
                    SELECT 10 AS TenantId, CAST(-1 AS BIGINT) AS Amount
                    UNION ALL SELECT 10, CAST(-2 AS BIGINT)
                ) src
                PIVOT (
                    AVG(Amount) FOR TenantId IN (10 AS T10)
                ) p
                """
        };

        using var avgBigIntNegativeFractionReader = avgBigIntNegativeFractionCommand.ExecuteReader();
        Assert.True(avgBigIntNegativeFractionReader.Read());
        Assert.Equal(-1L, avgBigIntNegativeFractionReader.GetInt64(avgBigIntNegativeFractionReader.GetOrdinal("T10")));
        Assert.IsType<long>(avgBigIntNegativeFractionReader.GetValue(avgBigIntNegativeFractionReader.GetOrdinal("T10")));
        Assert.False(avgBigIntNegativeFractionReader.Read());
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
    /// EN: Ensures FOR JSON PATH rejects conflicting nested alias order instead of silently merging incompatible object paths.
    /// PT: Garante que FOR JSON PATH rejeite ordem conflitante de aliases aninhados em vez de mesclar silenciosamente caminhos de objeto incompativeis.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlServerMock")]
    public void ExecuteScalar_ForJsonPath_WithConflictingNestedAliasOrder_ShouldThrow()
    {
        using var command = new SqlServerCommandMock(_connection)
        {
            CommandText = """
                SELECT
                    1 AS [Movement.Something.LocationName],
                    2 AS [Movement.Transporter.Id],
                    3 AS [Movement.Something.Destination]
                FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
                """
        };

        var ex = Assert.Throws<InvalidOperationException>(() => command.ExecuteScalar());
        Assert.Contains("FOR JSON PATH", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("Movement.Something.Destination", ex.Message, StringComparison.OrdinalIgnoreCase);
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
    /// EN: Ensures FOR JSON AUTO does not materialize nested arrays for LEFT JOIN rows without child matches, even with INCLUDE_NULL_VALUES.
    /// PT: Garante que FOR JSON AUTO não materialize arrays aninhados para linhas de LEFT JOIN sem correspondência filha, mesmo com INCLUDE_NULL_VALUES.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlServerMock")]
    public void ExecuteScalar_ForJsonAuto_LeftJoinWithoutChild_ShouldSkipNestedAlias()
    {
        using (var seed = new SqlServerCommandMock(_connection))
        {
            seed.CommandText = """
                INSERT INTO Users (Id, Name, Email) VALUES (895, 'Ana', NULL);
                INSERT INTO Users (Id, Name, Email) VALUES (896, 'Bia', NULL);
                INSERT INTO Orders (OrderId, UserId, Amount) VALUES (9904, 895, 10.50);
                """;
            seed.ExecuteNonQuery();
        }

        using var command = new SqlServerCommandMock(_connection)
        {
            CommandText = """
                SELECT u.Id, u.Name, o.OrderId, o.Amount
                FROM Users u
                LEFT JOIN Orders o ON o.UserId = u.Id
                WHERE u.Id IN (895, 896)
                ORDER BY u.Id, o.OrderId
                FOR JSON AUTO, INCLUDE_NULL_VALUES
                """
        };

        var json = Assert.IsType<string>(command.ExecuteScalar());
        using var document = System.Text.Json.JsonDocument.Parse(json);
        var array = document.RootElement;

        Assert.Equal(2, array.GetArrayLength());

        var first = array[0];
        Assert.True(first.TryGetProperty("o", out var firstOrders));
        Assert.Single(firstOrders.EnumerateArray());
        Assert.Equal(9904, firstOrders[0].GetProperty("OrderId").GetInt32());

        var second = array[1];
        Assert.Equal(896, second.GetProperty("Id").GetInt32());
        Assert.False(second.TryGetProperty("o", out _));
    }

    /// <summary>
    /// EN: Ensures FOR JSON PATH embeds OPENJSON AS JSON fragments as nested JSON instead of escaped text.
    /// PT: Garante que FOR JSON PATH incorpore fragmentos de OPENJSON AS JSON como JSON aninhado em vez de texto escapado.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlServerMock")]
    public void ExecuteScalar_ForJsonPath_WithOpenJsonAsJson_ShouldEmbedJsonFragment()
    {
        using (var seed = new SqlServerCommandMock(_connection))
        {
            seed.CommandText = """
                INSERT INTO Users (Id, Name, Email) VALUES (893, 'Ana', '{"profile":{"active":true,"roles":["admin","ops"]}}');
                """;
            seed.ExecuteNonQuery();
        }

        using var command = new SqlServerCommandMock(_connection)
        {
            CommandText = """
                SELECT profile.Profile AS [User.Profile]
                FROM Users u
                CROSS APPLY OPENJSON(u.Email) WITH (
                    Profile nvarchar(max) '$.profile' AS JSON
                ) profile
                WHERE u.Id = 893
                FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
                """
        };

        var json = Assert.IsType<string>(command.ExecuteScalar());
        using var document = System.Text.Json.JsonDocument.Parse(json);
        var profile = document.RootElement.GetProperty("User").GetProperty("Profile");

        Assert.Equal(System.Text.Json.JsonValueKind.Object, profile.ValueKind);
        Assert.True(profile.GetProperty("active").GetBoolean());
        Assert.Equal(2, profile.GetProperty("roles").GetArrayLength());
        Assert.Equal("admin", profile.GetProperty("roles")[0].GetString());
    }

    /// <summary>
    /// EN: Ensures FOR JSON PATH preserves JSON_QUERY fragments as nested JSON instead of escaped strings.
    /// PT: Garante que FOR JSON PATH preserve fragmentos de JSON_QUERY como JSON aninhado em vez de strings escapadas.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlServerMock")]
    public void ExecuteScalar_ForJsonPath_WithJsonQuery_ShouldEmbedJsonFragment()
    {
        using (var seed = new SqlServerCommandMock(_connection))
        {
            seed.CommandText = """
                INSERT INTO Users (Id, Name, Email) VALUES (894, 'Ana', '{"profile":{"active":true,"roles":["admin","ops"]}}');
                """;
            seed.ExecuteNonQuery();
        }

        using var command = new SqlServerCommandMock(_connection)
        {
            CommandText = """
                SELECT JSON_QUERY(u.Email, '$.profile') AS [User.Profile]
                FROM Users u
                WHERE u.Id = 894
                FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
                """
        };

        var json = Assert.IsType<string>(command.ExecuteScalar());
        using var document = System.Text.Json.JsonDocument.Parse(json);
        var profile = document.RootElement.GetProperty("User").GetProperty("Profile");

        Assert.Equal(System.Text.Json.JsonValueKind.Object, profile.ValueKind);
        Assert.True(profile.GetProperty("active").GetBoolean());
        Assert.Equal("ops", profile.GetProperty("roles")[1].GetString());
    }

    /// <summary>
    /// EN: Ensures JSON_QUERY without an explicit path preserves a root JSON object as a raw fragment on the shared SQL Server runtime path.
    /// PT: Garante que JSON_QUERY sem path explicito preserve um objeto JSON de raiz como fragmento bruto no caminho compartilhado de runtime do SQL Server.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlServerMock")]
    public void ExecuteScalar_JsonQuery_WithoutPath_ShouldReturnRootJsonFragment()
    {
        using (var seed = new SqlServerCommandMock(_connection))
        {
            seed.CommandText = """
                INSERT INTO Users (Id, Name, Email) VALUES (895, 'Bia', '{"profile":{"active":true},"roles":["admin","ops"]}');
                """;
            seed.ExecuteNonQuery();
        }

        using var scalarCommand = new SqlServerCommandMock(_connection)
        {
            CommandText = """
                SELECT JSON_QUERY(u.Email)
                FROM Users u
                WHERE u.Id = 895
                """
        };

        var scalarJson = Assert.IsType<string>(scalarCommand.ExecuteScalar());
        using (var scalarDocument = System.Text.Json.JsonDocument.Parse(scalarJson))
        {
            Assert.Equal(System.Text.Json.JsonValueKind.Object, scalarDocument.RootElement.ValueKind);
            Assert.True(scalarDocument.RootElement.GetProperty("profile").GetProperty("active").GetBoolean());
        }

        using var forJsonCommand = new SqlServerCommandMock(_connection)
        {
            CommandText = """
                SELECT JSON_QUERY(u.Email) AS [User.Payload]
                FROM Users u
                WHERE u.Id = 895
                FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
                """
        };

        var embeddedJson = Assert.IsType<string>(forJsonCommand.ExecuteScalar());
        using var embeddedDocument = System.Text.Json.JsonDocument.Parse(embeddedJson);
        var payload = embeddedDocument.RootElement.GetProperty("User").GetProperty("Payload");

        Assert.Equal(System.Text.Json.JsonValueKind.Object, payload.ValueKind);
        Assert.Equal("ops", payload.GetProperty("roles")[1].GetString());
    }

    /// <summary>
    /// EN: Ensures schema-qualified STRING_SPLIT works through CROSS APPLY on the shared SQL Server runtime path.
    /// PT: Garante que STRING_SPLIT qualificado por schema funcione via CROSS APPLY no caminho compartilhado de runtime do SQL Server.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlServerMock")]
    public void ExecuteReader_CrossApply_SchemaQualifiedStringSplit_ShouldReturnTokens()
    {
        using (var seed = new SqlServerCommandMock(_connection))
        {
            seed.CommandText = "INSERT INTO Users (Id, Name, Email) VALUES (897, 'Ana', 'red,blue')";
            seed.ExecuteNonQuery();
        }

        using var command = new SqlServerCommandMock(_connection)
        {
            CommandText = """
                SELECT part.value AS Token
                FROM Users u
                CROSS APPLY dbo.STRING_SPLIT(u.Email, ',') part
                WHERE u.Id = 897
                ORDER BY part.value
                """
        };

        using var reader = command.ExecuteReader();
        Assert.True(reader.Read());
        Assert.Equal("blue", reader.GetString(reader.GetOrdinal("Token")));
        Assert.True(reader.Read());
        Assert.Equal("red", reader.GetString(reader.GetOrdinal("Token")));
        Assert.False(reader.Read());
    }

    /// <summary>
    /// EN: Ensures schema-qualified OPENJSON WITH explicit schema works through CROSS APPLY on the shared SQL Server runtime path.
    /// PT: Garante que OPENJSON qualificado por schema com WITH explicito funcione via CROSS APPLY no caminho compartilhado de runtime do SQL Server.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlServerMock")]
    public void ExecuteReader_CrossApply_SchemaQualifiedOpenJsonWithSchema_ShouldProjectTypedColumns()
    {
        using (var seed = new SqlServerCommandMock(_connection))
        {
            seed.CommandText = """
                INSERT INTO Users (Id, Name, Email) VALUES (898, 'Ana', '[{"Name":"red","Payload":{"kind":"primary"}}]');
                """;
            seed.ExecuteNonQuery();
        }

        using var command = new SqlServerCommandMock(_connection)
        {
            CommandText = """
                SELECT data.Name AS ColorName, data.PayloadJson AS PayloadJson
                FROM Users u
                CROSS APPLY dbo.OPENJSON(u.Email) WITH (
                    Name NVARCHAR(20) '$.Name',
                    PayloadJson NVARCHAR(MAX) '$.Payload' AS JSON
                ) data
                WHERE u.Id = 898
                """
        };

        using var reader = command.ExecuteReader();
        Assert.True(reader.Read());
        Assert.Equal("red", reader.GetString(reader.GetOrdinal("ColorName")));
        Assert.Equal("""{"kind":"primary"}""", reader.GetString(reader.GetOrdinal("PayloadJson")));
        Assert.False(reader.Read());
    }

    /// <summary>
    /// EN: Ensures schema-qualified STRING_SPLIT with enable_ordinal returns the ordinal column on the shared SQL Server runtime path.
    /// PT: Garante que STRING_SPLIT qualificado por schema com enable_ordinal retorne a coluna ordinal no caminho compartilhado de runtime do SQL Server.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlServerMock")]
    public void ExecuteReader_CrossApply_SchemaQualifiedStringSplitWithOrdinal_ShouldReturnOrdinalColumn()
    {
        using (var seed = new SqlServerCommandMock(_connection))
        {
            seed.CommandText = "INSERT INTO Users (Id, Name, Email) VALUES (899, 'Ana', 'red,blue,green')";
            seed.ExecuteNonQuery();
        }

        using var command = new SqlServerCommandMock(_connection)
        {
            CommandText = """
                SELECT part.value AS Token, part.ordinal AS TokenOrdinal
                FROM Users u
                CROSS APPLY dbo.STRING_SPLIT(u.Email, ',', 1) part
                WHERE u.Id = 899
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

    /// <summary>
    /// EN: Ensures schema-qualified STRING_SPLIT enable_ordinal accepts numeric text that coerces exactly to 1 on the shared SQL Server runtime path.
    /// PT: Garante que STRING_SPLIT qualificado por schema com enable_ordinal aceite texto numerico que coerce exatamente para 1 no caminho compartilhado de runtime do SQL Server.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlServerMock")]
    public void ExecuteReader_CrossApply_SchemaQualifiedStringSplitWithOrdinalNumericTextFlag_ShouldReturnOrdinalColumn()
    {
        using (var seed = new SqlServerCommandMock(_connection))
        {
            seed.CommandText = "INSERT INTO Users (Id, Name, Email) VALUES (846, 'Ana', 'red,blue')";
            seed.ExecuteNonQuery();
        }

        using var command = new SqlServerCommandMock(_connection)
        {
            CommandText = """
                SELECT part.value AS Token, part.ordinal AS TokenOrdinal
                FROM Users u
                CROSS APPLY dbo.STRING_SPLIT(u.Email, ',', '1.0') part
                WHERE u.Id = 846
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
        Assert.False(reader.Read());
    }

    /// <summary>
    /// EN: Ensures schema-qualified STRING_SPLIT enable_ordinal rejects invalid numeric text outside the 0 or 1 subset on the shared SQL Server runtime path.
    /// PT: Garante que STRING_SPLIT qualificado por schema com enable_ordinal rejeite texto numerico invalido fora do subset 0 ou 1 no caminho compartilhado de runtime do SQL Server.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlServerMock")]
    public void ExecuteReader_CrossApply_SchemaQualifiedStringSplitWithOrdinalInvalidNumericTextFlag_ShouldThrow()
    {
        using (var seed = new SqlServerCommandMock(_connection))
        {
            seed.CommandText = "INSERT INTO Users (Id, Name, Email) VALUES (847, 'Ana', 'red,blue')";
            seed.ExecuteNonQuery();
        }

        using var command = new SqlServerCommandMock(_connection)
        {
            CommandText = """
                SELECT part.value
                FROM Users u
                CROSS APPLY dbo.STRING_SPLIT(u.Email, ',', '2.0') part
                WHERE u.Id = 847
                """
        };

        var ex = Assert.Throws<InvalidOperationException>(() => command.ExecuteReader());
        Assert.Contains("enable_ordinal", ex.Message, StringComparison.OrdinalIgnoreCase);
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

    /// <summary>
    /// EN: Ensures STRING_SPLIT enable_ordinal accepts decimal values that coerce exactly to 0 or 1 on the shared SQL Server runtime path.
    /// PT: Garante que STRING_SPLIT enable_ordinal aceite valores decimais que coercem exatamente para 0 ou 1 no caminho compartilhado de runtime do SQL Server.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlServerMock")]
    public void ExecuteReader_CrossApply_StringSplitWithOrdinalDecimalFlag_ShouldReturnOrdinalColumn()
    {
        using (var seed = new SqlServerCommandMock(_connection))
        {
            seed.CommandText = """
                INSERT INTO Users (Id, Name, Email) VALUES (842, 'Ana', 'red,blue');
                """;
            seed.ExecuteNonQuery();
        }

        using var command = new SqlServerCommandMock(_connection)
        {
            CommandText = """
                SELECT part.value AS Token, part.ordinal AS TokenOrdinal
                FROM Users u
                CROSS APPLY STRING_SPLIT(u.Email, ',', CAST(1 AS DECIMAL(10,2))) part
                WHERE u.Id = 842
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
        Assert.False(reader.Read());
    }

    /// <summary>
    /// EN: Ensures STRING_SPLIT enable_ordinal accepts numeric text that coerces exactly to 0 or 1 on the shared SQL Server runtime path.
    /// PT: Garante que STRING_SPLIT enable_ordinal aceite texto numerico que coerce exatamente para 0 ou 1 no caminho compartilhado de runtime do SQL Server.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlServerMock")]
    public void ExecuteReader_CrossApply_StringSplitWithOrdinalNumericTextFlag_ShouldReturnOrdinalColumn()
    {
        using (var seed = new SqlServerCommandMock(_connection))
        {
            seed.CommandText = """
                INSERT INTO Users (Id, Name, Email) VALUES (843, 'Ana', 'red,blue');
                """;
            seed.ExecuteNonQuery();
        }

        using var command = new SqlServerCommandMock(_connection)
        {
            CommandText = """
                SELECT part.value AS Token, part.ordinal AS TokenOrdinal
                FROM Users u
                CROSS APPLY STRING_SPLIT(u.Email, ',', '1.0') part
                WHERE u.Id = 843
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
        Assert.False(reader.Read());
    }

    /// <summary>
    /// EN: Ensures STRING_SPLIT enable_ordinal accepts numeric text that coerces exactly to 0 and suppresses the ordinal column on the shared SQL Server runtime path.
    /// PT: Garante que STRING_SPLIT enable_ordinal aceite texto numerico que coerce exatamente para 0 e suprima a coluna ordinal no caminho compartilhado de runtime do SQL Server.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlServerMock")]
    public void ExecuteReader_CrossApply_StringSplitWithOrdinalNumericTextZeroFlag_ShouldSuppressOrdinalColumn()
    {
        using (var seed = new SqlServerCommandMock(_connection))
        {
            seed.CommandText = """
                INSERT INTO Users (Id, Name, Email) VALUES (844, 'Ana', 'red,blue');
                """;
            seed.ExecuteNonQuery();
        }

        using var command = new SqlServerCommandMock(_connection)
        {
            CommandText = """
                SELECT part.value AS Token
                FROM Users u
                CROSS APPLY STRING_SPLIT(u.Email, ',', '0.0') part
                WHERE u.Id = 844
                ORDER BY part.value
                """
        };

        using var reader = command.ExecuteReader();
        Assert.Equal(1, reader.FieldCount);
        Assert.Equal("Token", reader.GetName(0));
        Assert.True(reader.Read());
        Assert.Equal("blue", reader.GetString(reader.GetOrdinal("Token")));
        Assert.True(reader.Read());
        Assert.Equal("red", reader.GetString(reader.GetOrdinal("Token")));
        Assert.False(reader.Read());
    }

    /// <summary>
    /// EN: Ensures STRING_SPLIT enable_ordinal rejects invalid numeric text values outside the 0 or 1 subset on the shared SQL Server runtime path.
    /// PT: Garante que STRING_SPLIT enable_ordinal rejeite valores textuais numericos invalidos fora do subset 0 ou 1 no caminho compartilhado de runtime do SQL Server.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlServerMock")]
    public void ExecuteReader_CrossApply_StringSplitWithOrdinalInvalidNumericTextFlag_ShouldThrow()
    {
        using (var seed = new SqlServerCommandMock(_connection))
        {
            seed.CommandText = """
                INSERT INTO Users (Id, Name, Email) VALUES (845, 'Ana', 'red,blue');
                """;
            seed.ExecuteNonQuery();
        }

        using var command = new SqlServerCommandMock(_connection)
        {
            CommandText = """
                SELECT part.value
                FROM Users u
                CROSS APPLY STRING_SPLIT(u.Email, ',', '2.0') part
                WHERE u.Id = 845
                """
        };

        var ex = Assert.Throws<InvalidOperationException>(() => command.ExecuteReader());
        Assert.Contains("enable_ordinal", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

}
