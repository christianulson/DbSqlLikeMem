namespace DbSqlLikeMem.MySql.Test;

/// <summary>
/// EN: Defines the class MySqlMockTests.
/// PT: Define a classe MySqlMockTests.
/// </summary>
public sealed class MySqlMockTests
    : XUnitTestBase
{
    private readonly MySqlConnectionMock _connection;

    /// <summary>
    /// EN: Tests MySqlMockTests behavior.
    /// PT: Testa o comportamento de MySqlMockTests.
    /// </summary>
    public MySqlMockTests(
        ITestOutputHelper helper
        ) : base(helper)
    {
        var db = new MySqlDbMock();
        db.AddTable("Users", [
            new("Id", DbType.Int32, false),
            new("Name", DbType.String, false) ,
            new ("Email", DbType.String, true)
        ]);
        db.AddTable("Orders", [
            new("OrderId",  DbType.Int32, false),
            new("UserId",  DbType.Int32, false),
            new("Amount",  DbType.Decimal, false, decimalPlaces : 2)
        ]);

        _connection = new MySqlConnectionMock(db);
        _connection.Open();
    }

    /// <summary>
    /// EN: Tests TestInsert behavior.
    /// PT: Testa o comportamento de TestInsert.
    /// </summary>
    [Fact]
    [Trait("Category", "MySqlMock")]
    public void TestInsert()
    {
        using var command = new MySqlCommandMock(_connection)
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
    [Trait("Category", "MySqlMock")]
    public void TestUpdate()
    {
        using var command = new MySqlCommandMock(_connection)
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
    [Trait("Category", "MySqlMock")]
    public void TestDelete()
    {
        using var command = new MySqlCommandMock(_connection)
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
    /// EN: Tests creating a table with an inline primary key and inserting data into it.
    /// PT: Testa a criação de uma tabela com chave primária inline e a inserção de dados nela.
    /// </summary>
    [Fact]
    [Trait("Category", "MySqlMock")]
    public void CreateTable_WithInlinePrimaryKey_ShouldCreateColumnAndAllowInsert()
    {
        using var cmd = _connection.CreateCommand();
        cmd.CommandText = "CREATE TABLE users_nh (id INT PRIMARY KEY, name VARCHAR(100))";
        cmd.ExecuteNonQuery();

        cmd.CommandText = "INSERT INTO users_nh (id, name) VALUES (1, 'Alice')";
        var rows = cmd.ExecuteNonQuery();

        Assert.Equal(1, rows);
        Assert.Equal("Alice", _connection.GetTable("users_nh")[0][1]);
    }

    /// <summary>
    /// EN: Tests TestTransactionCommit behavior.
    /// PT: Testa o comportamento de TestTransactionCommit.
    /// </summary>
    [Fact]
    [Trait("Category", "MySqlMock")]
    public void TestTransactionCommit()
    {
        using (var transaction = _connection.BeginTransaction())
        {
            using var command = new MySqlCommandMock(_connection, (MySqlTransactionMock)transaction)
            {
                CommandText = "INSERT INTO Users (Id, Name, Email) VALUES (1, 'John Doe', 'john@example.com')"
            };
            command.ExecuteNonQuery();
            transaction.Commit();
        }

        using var queryCommand = new MySqlCommandMock(_connection)
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
    [Trait("Category", "MySqlMock")]
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
    [Trait("Category", "MySqlMock")]
    public void TestTransactionRollback()
    {
        using (var transaction = _connection.BeginTransaction())
        {
            using var command = new MySqlCommandMock(_connection, (MySqlTransactionMock)transaction)
            {
                CommandText = "INSERT INTO Users (Id, Name, Email) VALUES (1, 'John Doe', 'john@example.com')"
            };
            command.ExecuteNonQuery();
            transaction.Rollback();
        }

        using var queryCommand = new MySqlCommandMock(_connection)
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
    /// EN: Ensures SELECT with MySQL index hints executes correctly.
    /// PT: Garante que SELECT com hints de índice do MySQL execute corretamente.
    /// </summary>
    [Fact]
    [Trait("Category", "MySqlMock")]
    public void TestSelect_WithMySqlIndexHint_ShouldExecute()
    {
        using var command = new MySqlCommandMock(_connection)
        {
            CommandText = "INSERT INTO Users (Id, Name, Email) VALUES (10, 'Hint User', 'hint@example.com')"
        };
        command.ExecuteNonQuery();

        command.CommandText = "SELECT Name FROM Users USE INDEX (idx_users_name) WHERE Id = 10";
        var name = command.ExecuteScalar();

        Assert.Equal("Hint User", name);
    }


    /// <summary>
    /// EN: Ensures common scalar function evaluation paths execute with MySQL semantics.
    /// PT: Garante que caminhos comuns de avaliação de funções escalares executem com semântica MySQL.
    /// </summary>
    [Fact]
    [Trait("Category", "MySqlMock")]
    public void TestSelect_ScalarFunctions_ShouldReturnExpectedValues()
    {
        using var command = new MySqlCommandMock(_connection)
        {
            CommandText = "INSERT INTO Users (Id, Name, Email) VALUES (20, 'Maria Clara', NULL)"
        };
        command.ExecuteNonQuery();

        command.CommandText = "SELECT COALESCE(Email, 'none') FROM Users WHERE Id = 20";
        Assert.Equal("none", command.ExecuteScalar());

        command.CommandText = "SELECT IFNULL(Email, 'fallback') FROM Users WHERE Id = 20";
        Assert.Equal("fallback", command.ExecuteScalar());

        command.CommandText = "SELECT IIF(Email IS NULL, 1, 0) FROM Users WHERE Id = 20";
        Assert.Equal(1, Convert.ToInt32(command.ExecuteScalar(), CultureInfo.InvariantCulture));
    }

    /// <summary>
    /// EN: Ensures string functions exercise parser/executor branches in DbSqlLikeMem core.
    /// PT: Garante que funções de string exercitem ramificações do parser/executor no núcleo DbSqlLikeMem.
    /// </summary>
    [Fact]
    [Trait("Category", "MySqlMock")]
    public void TestSelect_StringFunctions_ShouldReturnExpectedValues()
    {
        using var command = new MySqlCommandMock(_connection)
        {
            CommandText = "INSERT INTO Users (Id, Name, Email) VALUES (21, 'Joao Pedro', 'jp@example.com')"
        };
        command.ExecuteNonQuery();

        command.CommandText = "SELECT SUBSTRING(Name, 6, 5) FROM Users WHERE Id = 21";
        Assert.Equal("Pedro", command.ExecuteScalar());

        command.CommandText = "SELECT LENGTH(Name) FROM Users WHERE Id = 21";
        Assert.Equal(10, Convert.ToInt32(command.ExecuteScalar(), CultureInfo.InvariantCulture));

        command.CommandText = "SELECT REPLACE(Name, ' ', '-') FROM Users WHERE Id = 21";
        Assert.Equal("Joao-Pedro", command.ExecuteScalar());
    }

    /// <summary>
    /// EN: Ensures FIND_IN_SET is evaluated and keeps one-based indexing behavior.
    /// PT: Garante que FIND_IN_SET seja avaliada e mantenha o comportamento de índice iniciado em um.
    /// </summary>
    [Fact]
    [Trait("Category", "MySqlMock")]
    public void TestSelect_FindInSet_ShouldReturnOneBasedPosition()
    {
        using var command = new MySqlCommandMock(_connection)
        {
            CommandText = "SELECT FIND_IN_SET('b', 'a,b,c')"
        };

        Assert.Equal(2, command.ExecuteScalar());
    }


    /// <summary>
    /// EN: Ensures text normalization helpers (LOWER/UPPER/TRIM/CHAR_LENGTH) execute correctly.
    /// PT: Garante que funções de normalização de texto (LOWER/UPPER/TRIM/CHAR_LENGTH) executem corretamente.
    /// </summary>
    [Fact]
    [Trait("Category", "MySqlMock")]
    public void TestSelect_TextNormalizationFunctions_ShouldReturnExpectedValues()
    {
        using var command = new MySqlCommandMock(_connection)
        {
            CommandText = "INSERT INTO Users (Id, Name, Email) VALUES (22, '  MiXeD  ', 'text@example.com')"
        };
        command.ExecuteNonQuery();

        command.CommandText = "SELECT LOWER(Name) FROM Users WHERE Id = 22";
        Assert.Equal("  mixed  ", command.ExecuteScalar());

        command.CommandText = "SELECT UPPER(Name) FROM Users WHERE Id = 22";
        Assert.Equal("  MIXED  ", command.ExecuteScalar());

        command.CommandText = "SELECT TRIM(Name) FROM Users WHERE Id = 22";
        Assert.Equal("MiXeD", command.ExecuteScalar());

        command.CommandText = "SELECT CHAR_LENGTH(TRIM(Name)) FROM Users WHERE Id = 22";
        Assert.Equal(5, Convert.ToInt32(command.ExecuteScalar(), CultureInfo.InvariantCulture));
    }

    /// <summary>
    /// EN: Ensures TRY_CAST follows MySQL mock behavior and does not throw on non-convertible values.
    /// PT: Garante que TRY_CAST siga o comportamento do mock MySQL e não lance exceção em valores não conversíveis.
    /// </summary>
    [Fact]
    [Trait("Category", "MySqlMock")]
    public void TestSelect_TryCast_ShouldReturnNullWhenConversionFails()
    {
        using var command = new MySqlCommandMock(_connection)
        {
            CommandText = "SELECT TRY_CAST('abc' AS SIGNED)"
        };

        Assert.Null(command.ExecuteScalar());

        command.CommandText = "SELECT TRY_CAST('42' AS SIGNED)";
        Assert.Equal(42, Convert.ToInt32(command.ExecuteScalar(), CultureInfo.InvariantCulture));
    }

    /// <summary>
    /// EN: Ensures backtick alias split logic is preserved through parser and execution.
    /// PT: Garante que a lógica de alias com crase seja preservada no parser e na execução.
    /// </summary>
    [Fact]
    [Trait("Category", "MySqlMock")]
    public void TestSelect_BacktickAliasWithoutAs_ShouldExecute()
    {
        using var command = new MySqlCommandMock(_connection)
        {
            CommandText = "INSERT INTO Users (Id, Name, Email) VALUES (23, 'Alias User', 'alias@example.com')"
        };
        command.ExecuteNonQuery();

        command.CommandText = "SELECT Name `User Name` FROM Users WHERE Id = 23";
        using var reader = command.ExecuteReader();

        Assert.True(reader.Read());
        Assert.Equal("Alias User", reader.GetString(0));
        Assert.Equal("User Name", reader.GetName(0));
    }


    /// <summary>
    /// EN: Ensures INSERT without an explicit column list maps values in table column order.
    /// PT: Garante que INSERT sem lista explícita de colunas mapeie valores na ordem das colunas da tabela.
    /// </summary>
    [Fact]
    [Trait("Category", "MySqlMock")]
    public void TestInsert_WithoutColumnList_ShouldMapByColumnOrdinal()
    {
        using var command = new MySqlCommandMock(_connection)
        {
            CommandText = "INSERT INTO Users VALUES (30, 'NoCols', 'nocols@example.com')"
        };

        var affected = command.ExecuteNonQuery();

        Assert.Equal(1, affected);
        var row = _connection.GetTable("users").Single(_ => Convert.ToInt32(_[0], CultureInfo.InvariantCulture) == 30);
        Assert.Equal("NoCols", row[1]);
        Assert.Equal("nocols@example.com", row[2]);
    }

    /// <summary>
    /// EN: Ensures window slot computation path executes for ROW_NUMBER over ordered rows.
    /// PT: Garante que o caminho de cálculo de janela execute para ROW_NUMBER sobre linhas ordenadas.
    /// </summary>
    [Fact]
    [Trait("Category", "MySqlMock")]
    public void TestSelect_RowNumberWindowFunction_ShouldReturnSequentialRanks()
    {
        using var seed = new MySqlCommandMock(_connection)
        {
            CommandText = "INSERT INTO Users (Id, Name, Email) VALUES (101, 'A', 'a@x.com'), (102, 'B', 'b@x.com'), (103, 'C', 'c@x.com')"
        };
        seed.ExecuteNonQuery();

        using var query = new MySqlCommandMock(_connection)
        {
            CommandText = "SELECT Id, ROW_NUMBER() OVER (ORDER BY Id) AS rn FROM Users WHERE Id >= 101 ORDER BY Id"
        };

        using var reader = query.ExecuteReader();
        Assert.True(reader.Read());
        Assert.Equal(101, reader.GetInt32(0));
        Assert.Equal(1L, reader.GetInt64(1));

        Assert.True(reader.Read());
        Assert.Equal(102, reader.GetInt32(0));
        Assert.Equal(2L, reader.GetInt64(1));

        Assert.True(reader.Read());
        Assert.Equal(103, reader.GetInt32(0));
        Assert.Equal(3L, reader.GetInt64(1));
    }


    /// <summary>
    /// EN: Ensures INSERT INTO ... SELECT executes end-to-end and copies projected rows.
    /// PT: Garante que INSERT INTO ... SELECT execute de ponta a ponta e copie as linhas projetadas.
    /// </summary>
    [Fact]
    [Trait("Category", "MySqlMock")]
    public void TestInsertSelect_ShouldCopyRowsFromSourceQuery()
    {
        using var setup = _connection.CreateCommand();
        setup.CommandText = "CREATE TABLE users_archive (Id INT, Name VARCHAR(100), Email VARCHAR(200))";
        setup.ExecuteNonQuery();

        setup.CommandText = "INSERT INTO Users (Id, Name, Email) VALUES (201, 'Copy A', 'a@copy.com'), (202, 'Copy B', 'b@copy.com')";
        setup.ExecuteNonQuery();

        setup.CommandText = "INSERT INTO users_archive (Id, Name, Email) SELECT Id, Name, Email FROM Users WHERE Id >= 201";
        var affected = setup.ExecuteNonQuery();

        Assert.Equal(2, affected);
        var target = _connection.GetTable("users_archive");
        Assert.Equal(2, target.Count);
        Assert.Equal("Copy A", target[0][1]);
        Assert.Equal("Copy B", target[1][1]);
    }

    /// <summary>
    /// EN: Ensures date-like scalar function paths execute in function evaluator.
    /// PT: Garante que caminhos de funções escalares relacionadas a data executem no avaliador de funções.
    /// </summary>
    [Fact]
    [Trait("Category", "MySqlMock")]
    public void TestSelect_DateFunctions_ShouldReturnExpectedDateParts()
    {
        using var command = new MySqlCommandMock(_connection)
        {
            CommandText = "SELECT DATE('2024-05-06 12:34:56'), DATETIME('2024-05-06 12:34:56', '+1 day')"
        };

        using var reader = command.ExecuteReader();
        Assert.True(reader.Read());
        Assert.Equal(new DateTime(2024, 5, 6), (DateTime)reader.GetValue(0));
        Assert.Equal(new DateTime(2024, 5, 7, 12, 34, 56), (DateTime)reader.GetValue(1));
    }

    /// <summary>
    /// EN: Disposes test resources.
    /// PT: Descarta os recursos do teste.
    /// </summary>
    /// <param name="disposing">EN: True to dispose managed resources. PT: True para descartar recursos gerenciados.</param>


    /// <summary>
    /// EN: Ensures DbMock implements IReadOnlyDictionary indexer for existing schemas.
    /// PT: Garante que DbMock implemente o indexador de IReadOnlyDictionary para schemas existentes.
    /// </summary>
    [Fact]
    [Trait("Category", "MySqlMock")]
    public void IReadOnlySchemaDictionary_Indexer_ShouldReturnSchema()
    {
        var db = new MySqlDbMock();
        var readOnly = (IReadOnlyDictionary<string, ISchemaMock>)db;

        var schema = readOnly["DefaultSchema"];

        Assert.NotNull(schema);
        Assert.Equal("DefaultSchema", schema.Name);
    }

    /// <summary>
    /// EN: Ensures DbMock IReadOnlyDictionary indexer throws for missing schema names.
    /// PT: Garante que o indexador IReadOnlyDictionary de DbMock lance erro para schema inexistente.
    /// </summary>
    [Fact]
    [Trait("Category", "MySqlMock")]
    public void IReadOnlySchemaDictionary_Indexer_ShouldThrowForMissingSchema()
    {
        var db = new MySqlDbMock();
        var readOnly = (IReadOnlyDictionary<string, ISchemaMock>)db;

        Assert.Throws<KeyNotFoundException>(() => _ = readOnly["schema_that_does_not_exist"]);
    }

    protected override void Dispose(bool disposing)
    {
        _connection.Dispose();
        base.Dispose(disposing);
    }
}
