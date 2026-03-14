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
    /// EN: Tests ExecuteNonQuery with multi-statement INSERT script behavior.
    /// PT: Testa o comportamento de ExecuteNonQuery com script de INSERT multi-statement.
    /// </summary>
    [Fact]
    [Trait("Category", "MySqlMock")]
    public void ExecuteNonQuery_MultiStatementInsertScript_ShouldInsertAllRowsAndReturnTotalAffected()
    {
        using var command = new MySqlCommandMock(_connection)
        {
            CommandText = """
                INSERT INTO Users (Id, Name, Email) VALUES (101, 'Ana', NULL);
                INSERT INTO Users (Id, Name, Email) VALUES (102, 'Bia', NULL);
                INSERT INTO Users (Id, Name, Email) VALUES (103, 'Caio', NULL);
                """
        };

        var rowsAffected = command.ExecuteNonQuery();

        Assert.Equal(3, rowsAffected);
        var users = _connection.GetTable("users");
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
    /// EN: Verifies CREATE INDEX and DROP INDEX ON table mutate table index metadata through ExecuteNonQuery.
    /// PT: Verifica se CREATE INDEX e DROP INDEX ON table alteram a metadata de indices da tabela via ExecuteNonQuery.
    /// </summary>
    [Fact]
    [Trait("Category", "MySqlMock")]
    public void ExecuteNonQuery_CreateAndDropIndex_ShouldMutateTableIndexes()
    {
        using var command = new MySqlCommandMock(_connection)
        {
            CommandText = "CREATE INDEX IX_Users_Name ON Users (Name)"
        };

        command.ExecuteNonQuery();
        _connection.GetTable("users").Indexes.ContainsKey("ix_users_name").Should().BeTrue();

        command.CommandText = "DROP INDEX IX_Users_Name ON Users";
        command.ExecuteNonQuery();

        _connection.GetTable("users").Indexes.ContainsKey("ix_users_name").Should().BeFalse();
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
    /// EN: Ensures common numeric functions return expected results in MySQL.
    /// PT: Garante que funcoes numericas comuns retornem resultados esperados no MySQL.
    /// </summary>
    [Fact]
    [Trait("Category", "MySqlMock")]
    public void TestSelect_NumericFunctions_ShouldReturnExpectedValues()
    {
        using var command = new MySqlCommandMock(_connection)
        {
            CommandText = "SELECT ABS(-10)"
        };
        Assert.Equal(10, Convert.ToInt32(command.ExecuteScalar(), CultureInfo.InvariantCulture));

        command.CommandText = "SELECT ACOS(1)";
        Assert.Equal(0d, Convert.ToDouble(command.ExecuteScalar(), CultureInfo.InvariantCulture));

        command.CommandText = "SELECT ASIN(0)";
        Assert.Equal(0d, Convert.ToDouble(command.ExecuteScalar(), CultureInfo.InvariantCulture));

        command.CommandText = "SELECT ATAN(0)";
        Assert.Equal(0d, Convert.ToDouble(command.ExecuteScalar(), CultureInfo.InvariantCulture));

        command.CommandText = "SELECT ATAN2(0, 1)";
        Assert.Equal(0d, Convert.ToDouble(command.ExecuteScalar(), CultureInfo.InvariantCulture));

        command.CommandText = "SELECT CEIL(1.2)";
        Assert.Equal(2, Convert.ToInt32(command.ExecuteScalar(), CultureInfo.InvariantCulture));

        command.CommandText = "SELECT CEILING(1.1)";
        Assert.Equal(2, Convert.ToInt32(command.ExecuteScalar(), CultureInfo.InvariantCulture));

        command.CommandText = "SELECT BIN(12)";
        Assert.Equal("1100", command.ExecuteScalar());

        command.CommandText = "SELECT BIT_COUNT(7)";
        Assert.Equal(3, Convert.ToInt32(command.ExecuteScalar(), CultureInfo.InvariantCulture));

        command.CommandText = "SELECT BIT_LENGTH('abc')";
        Assert.Equal(24, Convert.ToInt32(command.ExecuteScalar(), CultureInfo.InvariantCulture));

        command.CommandText = "SELECT ASCII('A')";
        Assert.Equal(65, Convert.ToInt32(command.ExecuteScalar(), CultureInfo.InvariantCulture));
    }

    /// <summary>
    /// EN: Ensures ADDDATE and ADDTIME behave for literal arguments.
    /// PT: Garante que ADDDATE e ADDTIME se comportem com argumentos literais.
    /// </summary>
    [Fact]
    [Trait("Category", "MySqlMock")]
    public void TestSelect_DateArithmeticFunctions_ShouldReturnExpectedValues()
    {
        using var command = new MySqlCommandMock(_connection)
        {
            CommandText = "SELECT ADDDATE('2020-01-01', 1)"
        };
        Assert.Equal(new DateTime(2020, 1, 2), Convert.ToDateTime(command.ExecuteScalar(), CultureInfo.InvariantCulture));

        command.CommandText = "SELECT ADDDATE('2020-01-01', INTERVAL 2 DAY)";
        Assert.Equal(new DateTime(2020, 1, 3), Convert.ToDateTime(command.ExecuteScalar(), CultureInfo.InvariantCulture));

        command.CommandText = "SELECT ADDTIME('2020-01-01 10:00:00', '02:30:00')";
        Assert.Equal(new DateTime(2020, 1, 1, 12, 30, 0), Convert.ToDateTime(command.ExecuteScalar(), CultureInfo.InvariantCulture));
    }

    /// <summary>
    /// EN: Ensures ANY_VALUE and BIT_ aggregates return expected values.
    /// PT: Garante que agregados ANY_VALUE e BIT_ retornem valores esperados.
    /// </summary>
    [Fact]
    [Trait("Category", "MySqlMock")]
    public void TestSelect_AggregateBitwiseFunctions_ShouldReturnExpectedValues()
    {
        using var command = new MySqlCommandMock(_connection)
        {
            CommandText = "INSERT INTO Users (Id, Name, Email) VALUES (1, 'Ana', NULL)"
        };
        command.ExecuteNonQuery();

        command.CommandText = "INSERT INTO Users (Id, Name, Email) VALUES (2, 'Ana', NULL)";
        command.ExecuteNonQuery();

        command.CommandText = "SELECT ANY_VALUE(Name) FROM Users";
        Assert.Equal("Ana", command.ExecuteScalar());

        command.CommandText = "SELECT BIT_AND(Id) FROM Users";
        Assert.Equal(0, Convert.ToInt32(command.ExecuteScalar(), CultureInfo.InvariantCulture));

        command.CommandText = "SELECT BIT_OR(Id) FROM Users";
        Assert.Equal(3, Convert.ToInt32(command.ExecuteScalar(), CultureInfo.InvariantCulture));

        command.CommandText = "SELECT BIT_XOR(Id) FROM Users";
        Assert.Equal(3, Convert.ToInt32(command.ExecuteScalar(), CultureInfo.InvariantCulture));
    }

    /// <summary>
    /// EN: Ensures IPv4/IPv6 predicate helpers report expected results.
    /// PT: Garante que validacoes de IPv4/IPv6 retornem resultados esperados.
    /// </summary>
    [Fact]
    [Trait("Category", "MySqlMock")]
    public void TestSelect_IpFunctions_ShouldReturnExpectedValues()
    {
        using var command = new MySqlCommandMock(_connection)
        {
            CommandText = "SELECT IS_IPV4('192.168.0.1')"
        };
        Assert.Equal(1, Convert.ToInt32(command.ExecuteScalar(), CultureInfo.InvariantCulture));

        command.CommandText = "SELECT IS_IPV4('::1')";
        Assert.Equal(0, Convert.ToInt32(command.ExecuteScalar(), CultureInfo.InvariantCulture));

        command.CommandText = "SELECT IS_IPV6('::1')";
        Assert.Equal(1, Convert.ToInt32(command.ExecuteScalar(), CultureInfo.InvariantCulture));

        command.CommandText = "SELECT IS_IPV6('192.168.0.1')";
        Assert.Equal(0, Convert.ToInt32(command.ExecuteScalar(), CultureInfo.InvariantCulture));

        command.CommandText = "SELECT IS_IPV4_COMPAT('::192.168.0.1')";
        Assert.Equal(1, Convert.ToInt32(command.ExecuteScalar(), CultureInfo.InvariantCulture));

        command.CommandText = "SELECT IS_IPV4_MAPPED('::ffff:192.168.0.1')";
        Assert.Equal(1, Convert.ToInt32(command.ExecuteScalar(), CultureInfo.InvariantCulture));

        command.CommandText = "SELECT IS_IPV4_MAPPED('::192.168.0.1')";
        Assert.Equal(0, Convert.ToInt32(command.ExecuteScalar(), CultureInfo.InvariantCulture));
    }

    /// <summary>
    /// EN: Ensures UUID and JSON helper functions return expected results.
    /// PT: Garante que funcoes auxiliares de UUID e JSON retornem resultados esperados.
    /// </summary>
    [Fact]
    [Trait("Category", "MySqlMock")]
    public void TestSelect_UuidAndJsonFunctions_ShouldReturnExpectedValues()
    {
        using var command = new MySqlCommandMock(_connection)
        {
            CommandText = "SELECT IS_UUID('550e8400-e29b-41d4-a716-446655440000')"
        };
        Assert.Equal(1, Convert.ToInt32(command.ExecuteScalar(), CultureInfo.InvariantCulture));

        command.CommandText = "SELECT IS_UUID('invalid')";
        Assert.Equal(0, Convert.ToInt32(command.ExecuteScalar(), CultureInfo.InvariantCulture));

        command.CommandText = "SELECT JSON_ARRAY(1, 'a', NULL)";
        Assert.Equal("[1,\"a\",null]", command.ExecuteScalar());

        command.CommandText = "SELECT JSON_DEPTH('{\"a\": [1, 2]}')";
        Assert.Equal(3, Convert.ToInt32(command.ExecuteScalar(), CultureInfo.InvariantCulture));

        command.CommandText = "INSERT INTO Users (Id, Name, Email) VALUES (1, 'Ana', NULL)";
        command.ExecuteNonQuery();

        command.CommandText = "INSERT INTO Users (Id, Name, Email) VALUES (2, 'Bob', NULL)";
        command.ExecuteNonQuery();

        command.CommandText = "SELECT JSON_ARRAYAGG(Name) FROM Users";
        Assert.Equal("[\"Ana\",\"Bob\"]", command.ExecuteScalar());
    }

    /// <summary>
    /// EN: Ensures JSON utility helpers return expected results for common inputs.
    /// PT: Garante que utilitarios JSON retornem resultados esperados para entradas comuns.
    /// </summary>
    [Fact]
    [Trait("Category", "MySqlMock")]
    public void TestSelect_JsonUtilityFunctions_ShouldReturnExpectedValues()
    {
        using var command = new MySqlCommandMock(_connection)
        {
            CommandText = "SELECT JSON_VALID('{\"a\":1}')"
        };
        Assert.Equal(1, Convert.ToInt32(command.ExecuteScalar(), CultureInfo.InvariantCulture));

        command.CommandText = "SELECT JSON_VALID('invalid')";
        Assert.Equal(0, Convert.ToInt32(command.ExecuteScalar(), CultureInfo.InvariantCulture));

        command.CommandText = "SELECT JSON_TYPE('{\"a\":1}')";
        Assert.Equal("OBJECT", command.ExecuteScalar());

        command.CommandText = "SELECT JSON_TYPE('[1,2]')";
        Assert.Equal("ARRAY", command.ExecuteScalar());

        command.CommandText = "SELECT JSON_LENGTH('[1,2,3]')";
        Assert.Equal(3, Convert.ToInt32(command.ExecuteScalar(), CultureInfo.InvariantCulture));

        command.CommandText = "SELECT JSON_LENGTH('{\"a\":1,\"b\":2}')";
        Assert.Equal(2, Convert.ToInt32(command.ExecuteScalar(), CultureInfo.InvariantCulture));

        command.CommandText = "SELECT JSON_OBJECT('a', 1, 'b', 'x')";
        Assert.Equal("{\"a\":1,\"b\":\"x\"}", command.ExecuteScalar());

        command.CommandText = "SELECT JSON_QUOTE('text')";
        Assert.Equal("\"text\"", command.ExecuteScalar());

        command.CommandText = "SELECT JSON_PRETTY('{\"a\":1}')";
        Assert.Equal("{\n  \"a\": 1\n}", command.ExecuteScalar());

        command.CommandText = "SELECT JSON_KEYS('{\"a\":1,\"b\":2}')";
        Assert.Equal("[\"a\",\"b\"]", command.ExecuteScalar());

        command.CommandText = "SELECT JSON_KEYS('{\"a\":{\"b\":1}}', '$.a')";
        Assert.Equal("[\"b\"]", command.ExecuteScalar());

        command.CommandText = "SELECT JSON_SET('{\"a\":1}', '$.b', 2)";
        Assert.Equal("{\"a\":1,\"b\":2}", command.ExecuteScalar());

        command.CommandText = "SELECT JSON_SET('{\"a\":1}', '$.a', 3)";
        Assert.Equal("{\"a\":3}", command.ExecuteScalar());

        command.CommandText = "SELECT JSON_SET('{\"a\":1}', '$.c.d', 4)";
        Assert.Equal("{\"a\":1,\"c\":{\"d\":4}}", command.ExecuteScalar());

        command.CommandText = "SELECT JSON_REMOVE('{\"a\":1,\"b\":2}', '$.a')";
        Assert.Equal("{\"b\":2}", command.ExecuteScalar());

        command.CommandText = "SELECT JSON_REMOVE('{\"a\":{\"b\":1},\"c\":2}', '$.a.b')";
        Assert.Equal("{\"a\":{},\"c\":2}", command.ExecuteScalar());

        command.CommandText = "SELECT JSON_CONTAINS('{\"a\":1,\"b\":2}', '1', '$.a')";
        Assert.Equal(1, Convert.ToInt32(command.ExecuteScalar(), CultureInfo.InvariantCulture));

        command.CommandText = "SELECT JSON_CONTAINS('{\"a\":1,\"b\":2}', '3', '$.a')";
        Assert.Equal(0, Convert.ToInt32(command.ExecuteScalar(), CultureInfo.InvariantCulture));

        command.CommandText = "SELECT JSON_CONTAINS_PATH('{\"a\":1,\"b\":2}', 'one', '$.a', '$.c')";
        Assert.Equal(1, Convert.ToInt32(command.ExecuteScalar(), CultureInfo.InvariantCulture));

        command.CommandText = "SELECT JSON_CONTAINS_PATH('{\"a\":1,\"b\":2}', 'all', '$.a', '$.c')";
        Assert.Equal(0, Convert.ToInt32(command.ExecuteScalar(), CultureInfo.InvariantCulture));

        command.CommandText = "SELECT JSON_SEARCH('{\"a\":\"foo\",\"b\":\"bar\"}', 'one', 'ba')";
        Assert.Equal("$.b", command.ExecuteScalar());

        command.CommandText = "SELECT JSON_SEARCH('[\"foo\",\"bar\"]', 'all', 'o')";
        Assert.Equal("[\"$[0]\"]", command.ExecuteScalar());

        command.CommandText = "SELECT JSON_INSERT('{\"a\":1}', '$.b', 2)";
        Assert.Equal("{\"a\":1,\"b\":2}", command.ExecuteScalar());

        command.CommandText = "SELECT JSON_INSERT('{\"a\":1}', '$.a', 3)";
        Assert.Equal("{\"a\":1}", command.ExecuteScalar());

        command.CommandText = "SELECT JSON_REPLACE('{\"a\":1}', '$.a', 5)";
        Assert.Equal("{\"a\":5}", command.ExecuteScalar());

        command.CommandText = "SELECT JSON_REPLACE('{\"a\":1}', '$.b', 5)";
        Assert.Equal("{\"a\":1}", command.ExecuteScalar());
    }

    /// <summary>
    /// EN: Ensures LAST_DAY, LAST_INSERT_ID, and LEAST return expected values.
    /// PT: Garante que LAST_DAY, LAST_INSERT_ID e LEAST retornem valores esperados.
    /// </summary>
    [Fact]
    [Trait("Category", "MySqlMock")]
    public void TestSelect_CommonUtilityFunctions_ShouldReturnExpectedValues()
    {
        using var command = new MySqlCommandMock(_connection)
        {
            CommandText = "SELECT LAST_DAY('2020-02-10')"
        };
        Assert.Equal(new DateTime(2020, 2, 29), Convert.ToDateTime(command.ExecuteScalar(), CultureInfo.InvariantCulture));

        command.CommandText = "SELECT LAST_INSERT_ID()";
        Assert.Equal(0, Convert.ToInt32(command.ExecuteScalar(), CultureInfo.InvariantCulture));

        command.CommandText = "SELECT LAST_INSERT_ID(15)";
        Assert.Equal(15, Convert.ToInt32(command.ExecuteScalar(), CultureInfo.InvariantCulture));

        command.CommandText = "SELECT LAST_INSERT_ID()";
        Assert.Equal(15, Convert.ToInt32(command.ExecuteScalar(), CultureInfo.InvariantCulture));

        command.CommandText = "SELECT LEAST(5, 2, 9)";
        Assert.Equal(2, Convert.ToInt32(command.ExecuteScalar(), CultureInfo.InvariantCulture));
    }

    /// <summary>
    /// EN: Ensures LOCATE, LOG and LPAD return expected values.
    /// PT: Garante que LOCATE, LOG e LPAD retornem valores esperados.
    /// </summary>
    [Fact]
    [Trait("Category", "MySqlMock")]
    public void TestSelect_MathAndLocateFunctions_ShouldReturnExpectedValues()
    {
        using var command = new MySqlCommandMock(_connection)
        {
            CommandText = "SELECT LOCATE('bar', 'foobar')"
        };
        Assert.Equal(4, Convert.ToInt32(command.ExecuteScalar(), CultureInfo.InvariantCulture));

        command.CommandText = "SELECT LOCATE('bar', 'foobar', 5)";
        Assert.Equal(0, Convert.ToInt32(command.ExecuteScalar(), CultureInfo.InvariantCulture));

        command.CommandText = "SELECT LN(1)";
        Assert.Equal(0d, Convert.ToDouble(command.ExecuteScalar(), CultureInfo.InvariantCulture));

        command.CommandText = "SELECT LOG(10)";
        Assert.True(Convert.ToDouble(command.ExecuteScalar(), CultureInfo.InvariantCulture) > 2.0);

        command.CommandText = "SELECT LOG(10, 100)";
        Assert.Equal(2d, Convert.ToDouble(command.ExecuteScalar(), CultureInfo.InvariantCulture));

        command.CommandText = "SELECT LOG10(1000)";
        Assert.Equal(3d, Convert.ToDouble(command.ExecuteScalar(), CultureInfo.InvariantCulture));

        command.CommandText = "SELECT LOG2(8)";
        Assert.Equal(3d, Convert.ToDouble(command.ExecuteScalar(), CultureInfo.InvariantCulture));

        command.CommandText = "SELECT LPAD('abc', 5, '0')";
        Assert.Equal("00abc", command.ExecuteScalar());
    }

    /// <summary>
    /// EN: Ensures MAKEDATE, MAKETIME, MICROSECOND, and MD5 return expected values.
    /// PT: Garante que MAKEDATE, MAKETIME, MICROSECOND e MD5 retornem valores esperados.
    /// </summary>
    [Fact]
    [Trait("Category", "MySqlMock")]
    public void TestSelect_DateTimeAndHashFunctions_ShouldReturnExpectedValues()
    {
        using var command = new MySqlCommandMock(_connection)
        {
            CommandText = "SELECT MAKEDATE(2020, 60)"
        };
        Assert.Equal(new DateTime(2020, 2, 29), Convert.ToDateTime(command.ExecuteScalar(), CultureInfo.InvariantCulture));

        command.CommandText = "SELECT MAKETIME(12, 30, 15)";
        Assert.Equal(new TimeSpan(12, 30, 15), (TimeSpan)command.ExecuteScalar()!);

        command.CommandText = "SELECT MICROSECOND('2020-01-01 10:00:00.123456')";
        Assert.Equal(123456, Convert.ToInt32(command.ExecuteScalar(), CultureInfo.InvariantCulture));

        command.CommandText = "SELECT MD5('abc')";
        Assert.Equal("900150983cd24fb0d6963f7d28e17f72", command.ExecuteScalar());
    }

    /// <summary>
    /// EN: Ensures MID, MOD, and MONTHNAME return expected values.
    /// PT: Garante que MID, MOD e MONTHNAME retornem valores esperados.
    /// </summary>
    [Fact]
    [Trait("Category", "MySqlMock")]
    public void TestSelect_TextMathAndMonthFunctions_ShouldReturnExpectedValues()
    {
        using var command = new MySqlCommandMock(_connection)
        {
            CommandText = "SELECT MID('abcdef', 2, 3)"
        };
        Assert.Equal("bcd", command.ExecuteScalar());

        command.CommandText = "SELECT MOD(10, 3)";
        Assert.Equal(1m, Convert.ToDecimal(command.ExecuteScalar(), CultureInfo.InvariantCulture));

        command.CommandText = "SELECT MONTHNAME('2020-03-15')";
        Assert.Equal("March", command.ExecuteScalar());
    }

    /// <summary>
    /// EN: Ensures OCT, OCTET_LENGTH, and NAME_CONST return expected values.
    /// PT: Garante que OCT, OCTET_LENGTH e NAME_CONST retornem valores esperados.
    /// </summary>
    [Fact]
    [Trait("Category", "MySqlMock")]
    public void TestSelect_OctFunctions_ShouldReturnExpectedValues()
    {
        using var command = new MySqlCommandMock(_connection)
        {
            CommandText = "SELECT OCT(8)"
        };
        Assert.Equal("10", command.ExecuteScalar());

        command.CommandText = "SELECT OCTET_LENGTH('á')";
        Assert.Equal(2, Convert.ToInt32(command.ExecuteScalar(), CultureInfo.InvariantCulture));

        command.CommandText = "SELECT NAME_CONST('a', 10)";
        Assert.Equal(10, Convert.ToInt32(command.ExecuteScalar(), CultureInfo.InvariantCulture));
    }

    /// <summary>
    /// EN: Ensures ORD, POSITION, PI, POWER, and PERIOD_* functions return expected values.
    /// PT: Garante que ORD, POSITION, PI, POWER e PERIOD_* retornem valores esperados.
    /// </summary>
    [Fact]
    [Trait("Category", "MySqlMock")]
    public void TestSelect_PeriodAndPowerFunctions_ShouldReturnExpectedValues()
    {
        using var command = new MySqlCommandMock(_connection)
        {
            CommandText = "SELECT ORD('A')"
        };
        Assert.Equal(65, Convert.ToInt32(command.ExecuteScalar(), CultureInfo.InvariantCulture));

        command.CommandText = "SELECT POSITION('bar', 'foobar')";
        Assert.Equal(4, Convert.ToInt32(command.ExecuteScalar(), CultureInfo.InvariantCulture));

        command.CommandText = "SELECT PI()";
        Assert.Equal(Math.PI, Convert.ToDouble(command.ExecuteScalar(), CultureInfo.InvariantCulture));

        command.CommandText = "SELECT POWER(2, 3)";
        Assert.Equal(8d, Convert.ToDouble(command.ExecuteScalar(), CultureInfo.InvariantCulture));

        command.CommandText = "SELECT POW(2, 3)";
        Assert.Equal(8d, Convert.ToDouble(command.ExecuteScalar(), CultureInfo.InvariantCulture));

        command.CommandText = "SELECT PERIOD_ADD(202001, 2)";
        Assert.Equal(202003, Convert.ToInt32(command.ExecuteScalar(), CultureInfo.InvariantCulture));

        command.CommandText = "SELECT PERIOD_DIFF(202003, 202001)";
        Assert.Equal(2, Convert.ToInt32(command.ExecuteScalar(), CultureInfo.InvariantCulture));
    }

    /// <summary>
    /// EN: Ensures common date, math, and string helpers return expected values.
    /// PT: Garante que helpers comuns de data, matematica e string retornem valores esperados.
    /// </summary>
    [Fact]
    [Trait("Category", "MySqlMock")]
    public void TestSelect_CommonStringMathFunctions_ShouldReturnExpectedValues()
    {
        using var command = new MySqlCommandMock(_connection)
        {
            CommandText = "SELECT QUARTER('2020-05-01')"
        };
        Assert.Equal(2, Convert.ToInt32(command.ExecuteScalar(), CultureInfo.InvariantCulture));

        command.CommandText = "SELECT QUOTE(\"O'Reilly\")";
        Assert.Equal("'O\\'Reilly'", command.ExecuteScalar());

        command.CommandText = "SELECT RADIANS(180)";
        Assert.Equal(Math.PI, Convert.ToDouble(command.ExecuteScalar(), CultureInfo.InvariantCulture));

        command.CommandText = "SELECT RAND(1)";
        var seeded1 = Convert.ToDouble(command.ExecuteScalar(), CultureInfo.InvariantCulture);
        command.CommandText = "SELECT RAND(1)";
        var seeded2 = Convert.ToDouble(command.ExecuteScalar(), CultureInfo.InvariantCulture);
        Assert.Equal(seeded1, seeded2);

        command.CommandText = "SELECT REPEAT('ab', 3)";
        Assert.Equal("ababab", command.ExecuteScalar());

        command.CommandText = "SELECT REVERSE('abc')";
        Assert.Equal("cba", command.ExecuteScalar());

        command.CommandText = "SELECT RIGHT('abcdef', 2)";
        Assert.Equal("ef", command.ExecuteScalar());
    }

    /// <summary>
    /// EN: Ensures ROUND, RPAD, SEC_TO_TIME, SHA*, SIN, and SOUNDEX return expected values.
    /// PT: Garante que ROUND, RPAD, SEC_TO_TIME, SHA*, SIN e SOUNDEX retornem valores esperados.
    /// </summary>
    [Fact]
    [Trait("Category", "MySqlMock")]
    public void TestSelect_StringHashAndTimeFunctions_ShouldReturnExpectedValues()
    {
        using var command = new MySqlCommandMock(_connection)
        {
            CommandText = "SELECT ROUND(1.234, 2)"
        };
        Assert.Equal(1.23m, Convert.ToDecimal(command.ExecuteScalar(), CultureInfo.InvariantCulture));

        command.CommandText = "SELECT RPAD('abc', 5, '0')";
        Assert.Equal("abc00", command.ExecuteScalar());

        command.CommandText = "SELECT SEC_TO_TIME(3661)";
        Assert.Equal(new TimeSpan(1, 1, 1), (TimeSpan)command.ExecuteScalar()!);

        command.CommandText = "SELECT SHA('abc')";
        Assert.Equal("a9993e364706816aba3e25717850c26c9cd0d89d", command.ExecuteScalar());

        command.CommandText = "SELECT SHA1('abc')";
        Assert.Equal("a9993e364706816aba3e25717850c26c9cd0d89d", command.ExecuteScalar());

        command.CommandText = "SELECT SHA2('abc', 256)";
        Assert.Equal("ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad", command.ExecuteScalar());

        command.CommandText = "SELECT SIN(0)";
        Assert.Equal(0d, Convert.ToDouble(command.ExecuteScalar(), CultureInfo.InvariantCulture));

        command.CommandText = "SELECT SOUNDEX('Robert')";
        Assert.Equal("R163", command.ExecuteScalar());
    }

    /// <summary>
    /// EN: Ensures SPACE and SQRT return expected values.
    /// PT: Garante que SPACE e SQRT retornem valores esperados.
    /// </summary>
    [Fact]
    [Trait("Category", "MySqlMock")]
    public void TestSelect_SpaceAndSqrtFunctions_ShouldReturnExpectedValues()
    {
        using var command = new MySqlCommandMock(_connection)
        {
            CommandText = "SELECT SPACE(3)"
        };
        Assert.Equal("   ", command.ExecuteScalar());

        command.CommandText = "SELECT SQRT(9)";
        Assert.Equal(3d, Convert.ToDouble(command.ExecuteScalar(), CultureInfo.InvariantCulture));
    }

    /// <summary>
    /// EN: Ensures common time and string helpers return expected values.
    /// PT: Garante que helpers comuns de tempo e string retornem valores esperados.
    /// </summary>
    [Fact]
    [Trait("Category", "MySqlMock")]
    public void TestSelect_TimeAndSubstringFunctions_ShouldReturnExpectedValues()
    {
        using var command = new MySqlCommandMock(_connection)
        {
            CommandText = "SELECT SUBDATE('2020-01-10', 2)"
        };
        Assert.Equal(new DateTime(2020, 1, 8), Convert.ToDateTime(command.ExecuteScalar(), CultureInfo.InvariantCulture));

        command.CommandText = "SELECT SUBTIME('10:00:00', '01:30:00')";
        Assert.Equal(new TimeSpan(8, 30, 0), (TimeSpan)command.ExecuteScalar()!);

        command.CommandText = "SELECT SUBSTRING_INDEX('a,b,c', ',', 2)";
        Assert.Equal("a,b", command.ExecuteScalar());

        command.CommandText = "SELECT SUBSTRING_INDEX('a,b,c', ',', -1)";
        Assert.Equal("c", command.ExecuteScalar());

        command.CommandText = "SELECT TIME_FORMAT('10:05:06', '%H:%i:%s')";
        Assert.Equal("10:05:06", command.ExecuteScalar());

        command.CommandText = "SELECT TIME_TO_SEC('01:02:03')";
        Assert.Equal(3723, Convert.ToInt32(command.ExecuteScalar(), CultureInfo.InvariantCulture));

        command.CommandText = "SELECT TIMEDIFF('10:10:10', '09:00:00')";
        Assert.Equal(new TimeSpan(1, 10, 10), (TimeSpan)command.ExecuteScalar()!);

        command.CommandText = "SELECT TAN(0)";
        Assert.Equal(0d, Convert.ToDouble(command.ExecuteScalar(), CultureInfo.InvariantCulture));

        command.CommandText = "SELECT SYSTEM_USER()";
        Assert.Equal("root@localhost", command.ExecuteScalar());
    }

    /// <summary>
    /// EN: Ensures common date conversion helpers return expected values.
    /// PT: Garante que helpers comuns de conversao de data retornem valores esperados.
    /// </summary>
    [Fact]
    [Trait("Category", "MySqlMock")]
    public void TestSelect_DateConversionFunctions_ShouldReturnExpectedValues()
    {
        using var command = new MySqlCommandMock(_connection)
        {
            CommandText = "SELECT TIMESTAMPDIFF(DAY, '2020-01-01', '2020-01-03')"
        };
        Assert.Equal(2, Convert.ToInt32(command.ExecuteScalar(), CultureInfo.InvariantCulture));

        command.CommandText = "SELECT TO_DAYS('2020-01-01')";
        var days = Convert.ToInt32(command.ExecuteScalar(), CultureInfo.InvariantCulture);
        Assert.True(days > 0);

        command.CommandText = "SELECT TO_SECONDS('2020-01-01 00:00:01')";
        var seconds = Convert.ToInt64(command.ExecuteScalar(), CultureInfo.InvariantCulture);
        Assert.True(seconds > 0);

        command.CommandText = "SELECT TRUNCATE(12.3456, 2)";
        Assert.Equal(12.34m, Convert.ToDecimal(command.ExecuteScalar(), CultureInfo.InvariantCulture));

        command.CommandText = "SELECT UNIX_TIMESTAMP('1970-01-01 00:00:00')";
        Assert.Equal(0L, Convert.ToInt64(command.ExecuteScalar(), CultureInfo.InvariantCulture));

        command.CommandText = "SELECT USER()";
        Assert.Equal("root@localhost", command.ExecuteScalar());

        command.CommandText = "SELECT UTC_DATE()";
        Assert.IsType<DateTime>(command.ExecuteScalar());

        command.CommandText = "SELECT UTC_TIME()";
        Assert.IsType<TimeSpan>(command.ExecuteScalar());
    }

    /// <summary>
    /// EN: Ensures UTC_TIMESTAMP, UUID_SHORT, and week helpers return expected values.
    /// PT: Garante que UTC_TIMESTAMP, UUID_SHORT e helpers de semana retornem valores esperados.
    /// </summary>
    [Fact]
    [Trait("Category", "MySqlMock")]
    public void TestSelect_UtcAndWeekFunctions_ShouldReturnExpectedValues()
    {
        using var command = new MySqlCommandMock(_connection)
        {
            CommandText = "SELECT UTC_TIMESTAMP()"
        };
        Assert.IsType<DateTime>(command.ExecuteScalar());

        command.CommandText = "SELECT UUID_SHORT()";
        var first = Convert.ToInt64(command.ExecuteScalar(), CultureInfo.InvariantCulture);
        command.CommandText = "SELECT UUID_SHORT()";
        var second = Convert.ToInt64(command.ExecuteScalar(), CultureInfo.InvariantCulture);
        Assert.True(second > first);

        command.CommandText = "SELECT WEEKDAY('2020-01-01')";
        Assert.Equal(2, Convert.ToInt32(command.ExecuteScalar(), CultureInfo.InvariantCulture));

        command.CommandText = "SELECT WEEK('2020-01-01')";
        var week = Convert.ToInt32(command.ExecuteScalar(), CultureInfo.InvariantCulture);
        Assert.True(week >= 0);

        command.CommandText = "SELECT WEEKOFYEAR('2020-01-01')";
        Assert.Equal(1, Convert.ToInt32(command.ExecuteScalar(), CultureInfo.InvariantCulture));

        command.CommandText = "SELECT YEARWEEK('2020-01-01')";
        Assert.Equal(202001, Convert.ToInt32(command.ExecuteScalar(), CultureInfo.InvariantCulture));
    }

    /// <summary>
    /// EN: Ensures variance aggregates return expected values.
    /// PT: Garante que agregados de variancia retornem valores esperados.
    /// </summary>
    [Fact]
    [Trait("Category", "MySqlMock")]
    public void TestSelect_VarianceFunctions_ShouldReturnExpectedValues()
    {
        using var command = new MySqlCommandMock(_connection)
        {
            CommandText = "INSERT INTO Orders (OrderId, UserId, Amount) VALUES (10, 1, 10)"
        };
        command.ExecuteNonQuery();
        command.CommandText = "INSERT INTO Orders (OrderId, UserId, Amount) VALUES (11, 1, 20)";
        command.ExecuteNonQuery();
        command.CommandText = "INSERT INTO Orders (OrderId, UserId, Amount) VALUES (12, 1, 30)";
        command.ExecuteNonQuery();

        command.CommandText = "SELECT VAR_POP(Amount) FROM Orders";
        Assert.Equal(66.66666666666667d, Convert.ToDouble(command.ExecuteScalar(), CultureInfo.InvariantCulture));

        command.CommandText = "SELECT VAR_SAMP(Amount) FROM Orders";
        Assert.Equal(100d, Convert.ToDouble(command.ExecuteScalar(), CultureInfo.InvariantCulture));

        command.CommandText = "SELECT VARIANCE(Amount) FROM Orders";
        Assert.Equal(66.66666666666667d, Convert.ToDouble(command.ExecuteScalar(), CultureInfo.InvariantCulture));
    }

    /// <summary>
    /// EN: Ensures TRY_CAST follows MySQL mock behavior and returns DBNull on non-convertible values in ExecuteScalar.
    /// PT: Garante que TRY_CAST siga o comportamento do mock MySQL e retorne DBNull no ExecuteScalar para valores não conversíveis.
    /// </summary>
    [Fact]
    [Trait("Category", "MySqlMock")]
    public void TestSelect_TryCast_ShouldReturnDbNullWhenConversionFails()
    {
        using var command = new MySqlCommandMock(_connection)
        {
            CommandText = "SELECT TRY_CAST('abc' AS SIGNED)"
        };

        Assert.Equal(DBNull.Value, command.ExecuteScalar());

        command.CommandText = "SELECT TRY_CAST('42' AS SIGNED)";
        Assert.Equal(42, Convert.ToInt32(command.ExecuteScalar(), CultureInfo.InvariantCulture));
    }

    /// <summary>
    /// EN: Ensures CAST to JSON accepts JSON parameter payloads and keeps JSON_EXTRACT usable.
    /// PT: Garante que CAST para JSON aceite payload JSON em parâmetro e mantenha JSON_EXTRACT funcional.
    /// </summary>
    [Fact]
    [Trait("Category", "MySqlMock")]
    public void TestSelect_CastParameterAsJson_ShouldAllowJsonExtract()
    {
        using var command = new MySqlCommandMock(_connection)
        {
            CommandText = "SELECT JSON_EXTRACT(CAST(@ParamsJson AS JSON), '$.a')"
        };

        command.Parameters.Add(new MySqlParameter("@ParamsJson", new { a = 123 }));

        Assert.Equal(123L, Convert.ToInt64(command.ExecuteScalar(), CultureInfo.InvariantCulture));
    }

    /// <summary>
    /// EN: Ensures INSERT/UPSERT with CAST(@param AS JSON) stores JSON payload instead of raw CAST SQL text.
    /// PT: Garante que INSERT/UPSERT com CAST(@param AS JSON) persista payload JSON, e não texto bruto CAST SQL.
    /// </summary>
    [Fact]
    [Trait("Category", "MySqlMock")]
    public void TestInsertOnDuplicate_WithCastParameterAsJson_ShouldPersistJsonPayload()
    {
        using var setup = new MySqlCommandMock(_connection)
        {
            CommandText = """
                CREATE TABLE kernel_decisions (
                    decision_id VARCHAR(64) PRIMARY KEY,
                    domain VARCHAR(64),
                    ts DATETIME,
                    severity INT,
                    summary TEXT,
                    action_type VARCHAR(64),
                    params_json JSON,
                    risk_score DECIMAL(10,4),
                    confidence DECIMAL(10,4),
                    status VARCHAR(32),
                    evidence_fact_ids JSON,
                    evidence_signal_ids JSON,
                    hypothesis_ids JSON
                );
                """
        };
        setup.ExecuteNonQuery();

        using var cmd = new MySqlCommandMock(_connection)
        {
            CommandText = """
                INSERT INTO kernel_decisions
                (decision_id, domain, ts, severity, summary, action_type, params_json, risk_score, confidence, status,
                 evidence_fact_ids, evidence_signal_ids, hypothesis_ids)
                VALUES
                (@DecisionId, @Domain, @Ts, @Severity, @Summary, @ActionType, CAST(@ParamsJson AS JSON), @RiskScore, @Confidence, @Status,
                 CAST(@EvidenceFactIds AS JSON), CAST(@EvidenceSignalIds AS JSON), CAST(@HypothesisIds AS JSON))
                ON DUPLICATE KEY UPDATE
                severity=VALUES(severity),
                summary=VALUES(summary),
                action_type=VALUES(action_type),
                params_json=VALUES(params_json),
                risk_score=VALUES(risk_score),
                confidence=VALUES(confidence),
                status=VALUES(status),
                evidence_fact_ids=VALUES(evidence_fact_ids),
                evidence_signal_ids=VALUES(evidence_signal_ids),
                hypothesis_ids=VALUES(hypothesis_ids);
                """
        };

        cmd.Parameters.Add(new MySqlParameter("@DecisionId", "d-1"));
        cmd.Parameters.Add(new MySqlParameter("@Domain", "kernel"));
        cmd.Parameters.Add(new MySqlParameter("@Ts", DateTime.Parse("2026-03-05T10:00:00Z", CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind)));
        cmd.Parameters.Add(new MySqlParameter("@Severity", 2));
        cmd.Parameters.Add(new MySqlParameter("@Summary", "initial"));
        cmd.Parameters.Add(new MySqlParameter("@ActionType", "notify"));
        cmd.Parameters.Add(new MySqlParameter("@ParamsJson", """{"k":"v1"}"""));
        cmd.Parameters.Add(new MySqlParameter("@RiskScore", 0.5m));
        cmd.Parameters.Add(new MySqlParameter("@Confidence", 0.9m));
        cmd.Parameters.Add(new MySqlParameter("@Status", "open"));
        cmd.Parameters.Add(new MySqlParameter("@EvidenceFactIds", "[1,2]"));
        cmd.Parameters.Add(new MySqlParameter("@EvidenceSignalIds", "[10,20]"));
        cmd.Parameters.Add(new MySqlParameter("@HypothesisIds", "[100]"));

        var affectedInsert = cmd.ExecuteNonQuery();
        Assert.True(affectedInsert > 0);

        cmd.Parameters.Clear();
        cmd.Parameters.Add(new MySqlParameter("@DecisionId", "d-1"));
        cmd.Parameters.Add(new MySqlParameter("@Domain", "kernel"));
        cmd.Parameters.Add(new MySqlParameter("@Ts", DateTime.Parse("2026-03-05T10:00:00Z", CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind)));
        cmd.Parameters.Add(new MySqlParameter("@Severity", 3));
        cmd.Parameters.Add(new MySqlParameter("@Summary", "updated"));
        cmd.Parameters.Add(new MySqlParameter("@ActionType", "notify"));
        cmd.Parameters.Add(new MySqlParameter("@ParamsJson", """{"k":"v2"}"""));
        cmd.Parameters.Add(new MySqlParameter("@RiskScore", 0.6m));
        cmd.Parameters.Add(new MySqlParameter("@Confidence", 0.95m));
        cmd.Parameters.Add(new MySqlParameter("@Status", "open"));
        cmd.Parameters.Add(new MySqlParameter("@EvidenceFactIds", "[1,2,3]"));
        cmd.Parameters.Add(new MySqlParameter("@EvidenceSignalIds", "[10,20,30]"));
        cmd.Parameters.Add(new MySqlParameter("@HypothesisIds", "[100,200]"));

        var affectedUpdate = cmd.ExecuteNonQuery();
        Assert.True(affectedUpdate > 0);

        using var query = new MySqlCommandMock(_connection)
        {
            CommandText = "SELECT summary, params_json, evidence_fact_ids, evidence_signal_ids, hypothesis_ids FROM kernel_decisions WHERE decision_id = @DecisionId"
        };
        query.Parameters.Add(new MySqlParameter("@DecisionId", "d-1"));

        using var reader = query.ExecuteReader();
        Assert.True(reader.Read());

        Assert.Equal("updated", Convert.ToString(reader.GetValue(0), CultureInfo.InvariantCulture));

        var paramsJson = Convert.ToString(reader.GetValue(1), CultureInfo.InvariantCulture) ?? string.Empty;
        var evidenceFactIds = Convert.ToString(reader.GetValue(2), CultureInfo.InvariantCulture) ?? string.Empty;
        var evidenceSignalIds = Convert.ToString(reader.GetValue(3), CultureInfo.InvariantCulture) ?? string.Empty;
        var hypothesisIds = Convert.ToString(reader.GetValue(4), CultureInfo.InvariantCulture) ?? string.Empty;

        Assert.DoesNotContain("CAST(", paramsJson, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("CAST(", evidenceFactIds, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("CAST(", evidenceSignalIds, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("CAST(", hypothesisIds, StringComparison.OrdinalIgnoreCase);

        Assert.Contains("\"k\":\"v2\"", paramsJson, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("1", evidenceFactIds, StringComparison.OrdinalIgnoreCase);
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
    /// EN: Tests TestSelect_TemporalFunctions_ShouldWorkInSelectAndWhere behavior.
    /// PT: Testa o comportamento de TestSelect_TemporalFunctions_ShouldWorkInSelectAndWhere.
    /// </summary>
    [Fact]
    [Trait("Category", "MySqlMock")]
    public void TestSelect_TemporalFunctions_ShouldWorkInSelectAndWhere()
    {
        using var seed = new MySqlCommandMock(_connection)
        {
            CommandText = "INSERT INTO Users (Id, Name, Email) VALUES (900, 'clock', 'clock@x.com')"
        };
        seed.ExecuteNonQuery();

        using var command = new MySqlCommandMock(_connection)
        {
            CommandText = "SELECT NOW(), CURRENT_DATE, CURRENT_TIME, CURRENT_TIMESTAMP, SYSTEMDATE FROM Users WHERE NOW() IS NOT NULL LIMIT 1"
        };

        using var reader = command.ExecuteReader();
        Assert.True(reader.Read());
        Assert.IsType<DateTime>(reader.GetValue(0));
        Assert.IsType<DateTime>(reader.GetValue(1));
        Assert.IsType<TimeSpan>(reader.GetValue(2));
        Assert.IsType<DateTime>(reader.GetValue(3));
        Assert.IsType<DateTime>(reader.GetValue(4));
    }

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
        Assert.Equal("DefaultSchema", schema.SchemaName);
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
    /// EN: Tests TestSelect_SqlCalcFoundRows_ShouldExposeCountInFoundRows behavior.
    /// PT: Testa o comportamento de TestSelect_SqlCalcFoundRows_ShouldExposeCountInFoundRows.
    /// </summary>
    [Fact]
    [Trait("Category", "MySqlMock")]
    public void TestSelect_SqlCalcFoundRows_ShouldExposeCountInFoundRows()
    {
        using var command = new MySqlCommandMock(_connection);
        command.CommandText = "INSERT INTO Users (Id, Name, Email) VALUES (101, 'Ana', NULL)";
        command.ExecuteNonQuery();
        command.CommandText = "INSERT INTO Users (Id, Name, Email) VALUES (102, 'Bia', NULL)";
        command.ExecuteNonQuery();
        command.CommandText = "INSERT INTO Users (Id, Name, Email) VALUES (103, 'Caio', NULL)";
        command.ExecuteNonQuery();

        command.CommandText = "SELECT SQL_CALC_FOUND_ROWS Name FROM Users ORDER BY Id LIMIT 1; SELECT FOUND_ROWS();";
        using var reader = command.ExecuteReader();

        Assert.True(reader.Read());
        Assert.Equal("Ana", reader.GetString(0));
        Assert.True(reader.NextResult());
        Assert.True(reader.Read());
        Assert.Equal(3L, Convert.ToInt64(reader.GetValue(0), CultureInfo.InvariantCulture));
    }


    /// <summary>
    /// EN: Tests TestSelect_FoundRows_WithArgument_ShouldThrow behavior.
    /// PT: Testa o comportamento de TestSelect_FoundRows_WithArgument_ShouldThrow.
    /// </summary>
    [Fact]
    [Trait("Category", "MySqlMock")]
    public void TestSelect_FoundRows_WithArgument_ShouldThrow()
    {
        using var command = new MySqlCommandMock(_connection)
        {
            CommandText = "SELECT FOUND_ROWS(1)"
        };

        Assert.Throws<InvalidOperationException>(() => command.ExecuteScalar());
    }


    /// <summary>
    /// EN: Tests TestInsert_FoundRows_ShouldReturnAffectedRows behavior.
    /// PT: Testa o comportamento de TestInsert_FoundRows_ShouldReturnAffectedRows.
    /// </summary>
    [Fact]
    [Trait("Category", "MySqlMock")]
    public void TestInsert_FoundRows_ShouldReturnAffectedRows()
    {
        using var command = new MySqlCommandMock(_connection)
        {
            CommandText = "INSERT INTO Users (Id, Name, Email) VALUES (130, 'Rows User', NULL); SELECT FOUND_ROWS();"
        };

        using var reader = command.ExecuteReader();

        Assert.True(reader.Read());
        Assert.Equal(1L, Convert.ToInt64(reader.GetValue(0), CultureInfo.InvariantCulture));
    }


    /// <summary>
    /// EN: Tests TestUpdate_RowCountFunction_ShouldReturnAffectedRows behavior.
    /// PT: Testa o comportamento de TestUpdate_RowCountFunction_ShouldReturnAffectedRows.
    /// </summary>
    [Fact]
    [Trait("Category", "MySqlMock")]
    public void TestUpdate_RowCountFunction_ShouldReturnAffectedRows()
    {
        using var command = new MySqlCommandMock(_connection)
        {
            CommandText = "INSERT INTO Users (Id, Name, Email) VALUES (140, 'Row Count User', NULL)"
        };
        command.ExecuteNonQuery();

        command.CommandText = "UPDATE Users SET Name = 'Updated User' WHERE Id = 140; SELECT ROW_COUNT();";
        using var reader = command.ExecuteReader();

        Assert.True(reader.Read());
        Assert.Equal(1L, Convert.ToInt64(reader.GetValue(0), CultureInfo.InvariantCulture));
    }


    /// <summary>
    /// EN: Tests TestBeginTransaction_FoundRows_ShouldReturnZero behavior.
    /// PT: Testa o comportamento de TestBeginTransaction_FoundRows_ShouldReturnZero.
    /// </summary>
    [Fact]
    [Trait("Category", "MySqlMock")]
    public void TestBeginTransaction_FoundRows_ShouldReturnZero()
    {
        using var command = new MySqlCommandMock(_connection)
        {
            CommandText = "BEGIN TRANSACTION"
        };
        command.ExecuteNonQuery();

        command.CommandText = "SELECT FOUND_ROWS();";
        Assert.Equal(0L, Convert.ToInt64(command.ExecuteScalar(), CultureInfo.InvariantCulture));
    }

    /// <summary>
    /// EN: Tests TestBatch_BeginTransactionThenFoundRows_ShouldReturnZero behavior.
    /// PT: Testa o comportamento de TestBatch_BeginTransactionThenFoundRows_ShouldReturnZero.
    /// </summary>
    [Fact]
    [Trait("Category", "MySqlMock")]
    public void TestBatch_BeginTransactionThenFoundRows_ShouldReturnZero()
    {
        using var command = new MySqlCommandMock(_connection)
        {
            CommandText = "BEGIN TRANSACTION; SELECT FOUND_ROWS();"
        };

        using var reader = command.ExecuteReader();

        Assert.True(reader.Read());
        Assert.Equal(0L, Convert.ToInt64(reader.GetValue(0), CultureInfo.InvariantCulture));
    }

    /// <summary>
    /// EN: Tests TestBatch_BeginSavepointThenFoundRows_ShouldReturnZero behavior.
    /// PT: Testa o comportamento de TestBatch_BeginSavepointThenFoundRows_ShouldReturnZero.
    /// </summary>
    [Fact]
    [Trait("Category", "MySqlMock")]
    public void TestBatch_BeginSavepointThenFoundRows_ShouldReturnZero()
    {
        using var command = new MySqlCommandMock(_connection)
        {
            CommandText = "BEGIN TRANSACTION; SAVEPOINT sp1; SELECT FOUND_ROWS();"
        };

        using var reader = command.ExecuteReader();

        Assert.True(reader.Read());
        Assert.Equal(0L, Convert.ToInt64(reader.GetValue(0), CultureInfo.InvariantCulture));
    }

    /// <summary>
    /// EN: Tests TestBatch_CallThenFoundRows_ShouldReturnZero behavior.
    /// PT: Testa o comportamento de TestBatch_CallThenFoundRows_ShouldReturnZero.
    /// </summary>
    [Fact]
    [Trait("Category", "MySqlMock")]
    public void TestBatch_CallThenFoundRows_ShouldReturnZero()
    {
        _connection.AddProdecure("sp_ping", new ProcedureDef([], [], [], null));

        using var command = new MySqlCommandMock(_connection)
        {
            CommandText = "CALL sp_ping(); SELECT FOUND_ROWS();"
        };

        using var reader = command.ExecuteReader();

        Assert.True(reader.Read());
        Assert.Equal(0L, Convert.ToInt64(reader.GetValue(0), CultureInfo.InvariantCulture));
    }

    /// <summary>
    /// EN: Tests TestBatch_UpdateCommitThenFoundRows_ShouldReturnZeroAfterCommit behavior.
    /// PT: Testa o comportamento de TestBatch_UpdateCommitThenFoundRows_ShouldReturnZeroAfterCommit.
    /// </summary>
    [Fact]
    [Trait("Category", "MySqlMock")]
    public void TestBatch_UpdateCommitThenFoundRows_ShouldReturnZeroAfterCommit()
    {
        using var command = new MySqlCommandMock(_connection)
        {
            CommandText = "UPDATE Users SET Name = 'After Commit' WHERE Id = 1; COMMIT; SELECT FOUND_ROWS();"
        };

        using var reader = command.ExecuteReader();

        Assert.True(reader.Read());
        Assert.Equal(0L, Convert.ToInt64(reader.GetValue(0), CultureInfo.InvariantCulture));
    }


    /// <summary>
    /// EN: Tests TestBatch_RollbackToSavepointThenFoundRows_ShouldReturnZero behavior.
    /// PT: Testa o comportamento de TestBatch_RollbackToSavepointThenFoundRows_ShouldReturnZero.
    /// </summary>
    [Fact]
    [Trait("Category", "MySqlMock")]
    public void TestBatch_RollbackToSavepointThenFoundRows_ShouldReturnZero()
    {
        using var command = new MySqlCommandMock(_connection)
        {
            CommandText = "BEGIN TRANSACTION; SAVEPOINT sp1; UPDATE Users SET Name = 'Tmp' WHERE Id = 1; ROLLBACK TO SAVEPOINT sp1; SELECT FOUND_ROWS();"
        };

        using var reader = command.ExecuteReader();

        Assert.True(reader.Read());
        Assert.Equal(0L, Convert.ToInt64(reader.GetValue(0), CultureInfo.InvariantCulture));
    }

    /// <summary>
    /// EN: Tests TestBatch_ReleaseSavepointThenFoundRows_ShouldReturnZero behavior.
    /// PT: Testa o comportamento de TestBatch_ReleaseSavepointThenFoundRows_ShouldReturnZero.
    /// </summary>
    [Fact]
    [Trait("Category", "MySqlMock")]
    public void TestBatch_ReleaseSavepointThenFoundRows_ShouldReturnZero()
    {
        using var command = new MySqlCommandMock(_connection)
        {
            CommandText = "BEGIN TRANSACTION; SAVEPOINT sp1; RELEASE SAVEPOINT sp1; SELECT FOUND_ROWS();"
        };

        using var reader = command.ExecuteReader();

        Assert.True(reader.Read());
        Assert.Equal(0L, Convert.ToInt64(reader.GetValue(0), CultureInfo.InvariantCulture));
    }


    /// <summary>
    /// EN: Tests TestBatch_SelectThenUpdateThenFoundRows_ShouldReflectLastDml behavior.
    /// PT: Testa o comportamento de TestBatch_SelectThenUpdateThenFoundRows_ShouldReflectLastDml.
    /// </summary>
    [Fact]
    [Trait("Category", "MySqlMock")]
    public void TestBatch_SelectThenUpdateThenFoundRows_ShouldReflectLastDml()
    {
        using var seed = new MySqlCommandMock(_connection)
        {
            CommandText = "INSERT INTO Users (Id, Name, Email) VALUES (1, 'Seed User', NULL)"
        };
        seed.ExecuteNonQuery();

        using var command = new MySqlCommandMock(_connection)
        {
            CommandText = "SELECT Name FROM Users ORDER BY Id LIMIT 1; UPDATE Users SET Name = 'Mixed Batch User' WHERE Id = 1; SELECT FOUND_ROWS();"
        };

        using var reader = command.ExecuteReader();

        Assert.True(reader.Read());
        Assert.True(reader.NextResult());
        Assert.True(reader.Read());
        Assert.Equal(1L, Convert.ToInt64(reader.GetValue(0), CultureInfo.InvariantCulture));
    }


    /// <summary>
    /// EN: Tests TestBatch_CallUpdateCommitThenFoundRows_ShouldReturnZeroAfterCommit behavior.
    /// PT: Testa o comportamento de TestBatch_CallUpdateCommitThenFoundRows_ShouldReturnZeroAfterCommit.
    /// </summary>
    [Fact]
    [Trait("Category", "MySqlMock")]
    public void TestBatch_CallUpdateCommitThenFoundRows_ShouldReturnZeroAfterCommit()
    {
        _connection.AddProdecure("sp_ping", new ProcedureDef([], [], [], null));

        using var command = new MySqlCommandMock(_connection)
        {
            CommandText = "CALL sp_ping(); UPDATE Users SET Name = 'Call Dml User' WHERE Id = 1; COMMIT; SELECT FOUND_ROWS();"
        };

        using var reader = command.ExecuteReader();

        Assert.True(reader.Read());
        Assert.Equal(0L, Convert.ToInt64(reader.GetValue(0), CultureInfo.InvariantCulture));
    }


    /// <summary>
    /// EN: Tests TestBatch_UpdateThenSelectThenFoundRows_ShouldReflectLastSelect behavior.
    /// PT: Testa o comportamento de TestBatch_UpdateThenSelectThenFoundRows_ShouldReflectLastSelect.
    /// </summary>
    [Fact]
    [Trait("Category", "MySqlMock")]
    public void TestBatch_UpdateThenSelectThenFoundRows_ShouldReflectLastSelect()
    {
        using var seed = new MySqlCommandMock(_connection)
        {
            CommandText = "INSERT INTO Users (Id, Name, Email) VALUES (1, 'Seed User 1', NULL)"
        };
        seed.ExecuteNonQuery();
        seed.CommandText = "INSERT INTO Users (Id, Name, Email) VALUES (2, 'Seed User 2', NULL)";
        seed.ExecuteNonQuery();

        using var command = new MySqlCommandMock(_connection)
        {
            CommandText = "UPDATE Users SET Name = 'Last Select User' WHERE Id = 1; SELECT Name FROM Users ORDER BY Id LIMIT 2; SELECT FOUND_ROWS();"
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
    /// EN: Ensures boolean-mode MATCH ... AGAINST filtering honors required and prohibited terms.
    /// PT: Garante que o filtro MATCH ... AGAINST em modo boolean respeite termos obrigatórios e proibidos.
    /// </summary>
    [Fact]
    [Trait("Category", "MySqlMock")]
    public void Select_WithMatchAgainstBooleanModeWhere_ShouldRespectRequiredAndProhibitedTerms()
    {
        using var seed = new MySqlCommandMock(_connection)
        {
            CommandText = """
                INSERT INTO Users (Id, Name, Email) VALUES (1, 'John Doe', 'john@example.com');
                INSERT INTO Users (Id, Name, Email) VALUES (2, 'Maria Silva', 'maria@example.com');
                INSERT INTO Users (Id, Name, Email) VALUES (3, 'John Maria', 'john-maria@example.com');
                """
        };
        seed.ExecuteNonQuery();

        using var cmd = new MySqlCommandMock(_connection)
        {
            CommandText = "SELECT COUNT(*) FROM Users WHERE MATCH(Name, Email) AGAINST ('+john -maria' IN BOOLEAN MODE) > 0"
        };

        var count = Convert.ToInt64(cmd.ExecuteScalar(), CultureInfo.InvariantCulture);
        Assert.Equal(1L, count);
    }

    /// <summary>
    /// EN: Ensures ORDER BY MATCH ... AGAINST score prioritizes most relevant row in mock scoring.
    /// PT: Garante que ORDER BY com score de MATCH ... AGAINST priorize a linha mais relevante no scoring do mock.
    /// </summary>
    [Fact]
    [Trait("Category", "MySqlMock")]
    public void Select_OrderByMatchAgainstScoreDesc_ShouldPrioritizeMoreRelevantRow()
    {
        using var seed = new MySqlCommandMock(_connection)
        {
            CommandText = """
                INSERT INTO Users (Id, Name, Email) VALUES (1, 'John Doe', 'john@example.com');
                INSERT INTO Users (Id, Name, Email) VALUES (2, 'John', 'john@domain.test');
                INSERT INTO Users (Id, Name, Email) VALUES (3, 'Mary', 'mary@domain.test');
                """
        };
        seed.ExecuteNonQuery();

        using var cmd = new MySqlCommandMock(_connection)
        {
            CommandText = "SELECT Id FROM Users ORDER BY MATCH(Name, Email) AGAINST ('john doe' IN BOOLEAN MODE) DESC, Id ASC LIMIT 1"
        };

        var topId = Convert.ToInt32(cmd.ExecuteScalar(), CultureInfo.InvariantCulture);
        Assert.Equal(1, topId);
    }

    /// <summary>
    /// EN: Ensures semantic chunk lexical candidate query with MATCH ... AGAINST and parameterized LIMIT executes end-to-end.
    /// PT: Garante que a query de candidatos léxicos com MATCH ... AGAINST e LIMIT parametrizado execute ponta a ponta.
    /// </summary>
    [Fact]
    [Trait("Category", "MySqlMock")]
    public void Select_SemanticChunksLexicalCandidates_WithMatchAgainstAndParameterizedLimit_ShouldWork()
    {
        using var setup = new MySqlCommandMock(_connection)
        {
            CommandText = """
                CREATE TABLE semantic_chunks (
                    chunk_id INT PRIMARY KEY,
                    doc_id INT,
                    chunk_index INT,
                    text VARCHAR(255),
                    embedding_json TEXT,
                    created_at DATETIME
                );
                INSERT INTO semantic_chunks (chunk_id, doc_id, chunk_index, text, embedding_json, created_at) VALUES
                    (1, 100, 0, 'john doe article', '{"v":[0.1,0.2]}', '2026-01-01 10:00:00'),
                    (2, 100, 1, 'john article', '{"v":[0.2,0.3]}', '2026-01-01 10:01:00'),
                    (3, 101, 0, 'maria note', '{"v":[0.4,0.5]}', '2026-01-01 10:02:00');
                """
        };
        setup.ExecuteNonQuery();

        using var cmd = new MySqlCommandMock(_connection)
        {
            CommandText = """
                SELECT chunk_id, doc_id, chunk_index, text, embedding_json, created_at,
                       MATCH(text) AGAINST (@QueryText IN NATURAL LANGUAGE MODE) AS lexical_score,
                       'lexical' AS candidate_source
                FROM semantic_chunks
                WHERE MATCH(text) AGAINST (@QueryText IN NATURAL LANGUAGE MODE)
                ORDER BY lexical_score DESC
                LIMIT @CandidateLimit;
                """
        };
        cmd.Parameters.Add(new MySqlParameter("@QueryText", "john doe"));
        cmd.Parameters.Add(new MySqlParameter("@CandidateLimit", 2));

        using var reader = cmd.ExecuteReader();
        var rows = new List<(int ChunkId, int LexicalScore, string CandidateSource)>();
        while (reader.Read())
        {
            rows.Add((
                Convert.ToInt32(reader.GetValue(0), CultureInfo.InvariantCulture),
                Convert.ToInt32(reader.GetValue(6), CultureInfo.InvariantCulture),
                Convert.ToString(reader.GetValue(7), CultureInfo.InvariantCulture) ?? string.Empty));
        }

        Assert.Equal(2, rows.Count);
        Assert.Equal(1, rows[0].ChunkId);
        Assert.Equal(2, rows[0].LexicalScore);
        Assert.Equal(2, rows[1].ChunkId);
        Assert.Equal(1, rows[1].LexicalScore);
        Assert.All(rows, row => Assert.Equal("lexical", row.CandidateSource));
    }

}
