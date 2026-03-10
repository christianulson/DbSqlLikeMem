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
    /// EN: Verifies automatic dialect mode executes SQL Server TOP syntax on the shared runtime pipeline.
    /// PT: Verifica se o modo automatico de dialeto executa sintaxe TOP do SQL Server no pipeline compartilhado de runtime.
    /// </summary>
    [Fact]
    [Trait("Category", "SqliteMock")]
    public void ExecuteReader_WithAutoSqlDialect_ShouldAcceptTopSyntax()
    {
        _connection.UseAutoSqlDialect = true;

        using (var seed = new SqliteCommandMock(_connection))
        {
            seed.CommandText = """
                INSERT INTO Users (Id, Name, Email) VALUES (1, 'Ana', NULL);
                INSERT INTO Users (Id, Name, Email) VALUES (2, 'Bia', NULL);
                """;
            seed.ExecuteNonQuery();
        }

        using var command = new SqliteCommandMock(_connection)
        {
            CommandText = "SELECT TOP 1 Name FROM Users ORDER BY Id"
        };

        using var reader = command.ExecuteReader();
        Assert.True(reader.Read());
        Assert.Equal("Ana", reader.GetString(0));
    }

    /// <summary>
    /// EN: Verifies automatic dialect mode executes ANSI FETCH FIRST syntax on the shared runtime pipeline.
    /// PT: Verifica se o modo automatico de dialeto executa sintaxe ANSI FETCH FIRST no pipeline compartilhado de runtime.
    /// </summary>
    [Fact]
    [Trait("Category", "SqliteMock")]
    public void ExecuteReader_WithAutoSqlDialect_ShouldAcceptFetchFirstSyntax()
    {
        _connection.UseAutoSqlDialect = true;

        using (var seed = new SqliteCommandMock(_connection))
        {
            seed.CommandText = """
                INSERT INTO Users (Id, Name, Email) VALUES (1, 'Ana', NULL);
                INSERT INTO Users (Id, Name, Email) VALUES (2, 'Bia', NULL);
                """;
            seed.ExecuteNonQuery();
        }

        using var command = new SqliteCommandMock(_connection)
        {
            CommandText = "SELECT Name FROM Users ORDER BY Id FETCH FIRST 1 ROWS ONLY"
        };

        using var reader = command.ExecuteReader();
        Assert.True(reader.Read());
        Assert.Equal("Ana", reader.GetString(0));
    }

    /// <summary>
    /// EN: Verifies automatic dialect mode executes INSERT SELECT with TOP through the shared non-query pipeline.
    /// PT: Verifica se o modo automatico de dialeto executa INSERT SELECT com TOP pelo pipeline compartilhado de non-query.
    /// </summary>
    [Fact]
    [Trait("Category", "SqliteMock")]
    public void ExecuteNonQuery_WithAutoSqlDialect_ShouldAcceptInsertSelectTopSyntax()
    {
        _connection.UseAutoSqlDialect = true;

        using (var seed = new SqliteCommandMock(_connection))
        {
            seed.CommandText = """
                INSERT INTO Users (Id, Name, Email) VALUES (1, 'Ana', NULL);
                INSERT INTO Users (Id, Name, Email) VALUES (2, 'Bia', NULL);
                """;
            seed.ExecuteNonQuery();
        }

        using var command = new SqliteCommandMock(_connection)
        {
            CommandText = """
                INSERT INTO Users (Id, Name, Email)
                SELECT TOP 1 10, Name, Email
                FROM Users
                ORDER BY Id
                """
        };

        var rowsAffected = command.ExecuteNonQuery();

        Assert.Equal(1, rowsAffected);
        Assert.Equal(3, _connection.GetTable("users").Count);
        Assert.Equal("Ana", _connection.GetTable("users")[2][1]);
    }

    /// <summary>
    /// EN: Verifies automatic dialect mode supports equivalent pagination syntaxes inside the same reader batch.
    /// PT: Verifica se o modo automatico de dialeto suporta sintaxes equivalentes de paginacao no mesmo batch de leitura.
    /// </summary>
    [Fact]
    [Trait("Category", "SqliteMock")]
    public void ExecuteReaderBatch_WithAutoSqlDialect_ShouldAcceptEquivalentPaginationSyntaxes()
    {
        _connection.UseAutoSqlDialect = true;

        using (var seed = new SqliteCommandMock(_connection))
        {
            seed.CommandText = """
                INSERT INTO Users (Id, Name, Email) VALUES (1, 'Ana', NULL);
                INSERT INTO Users (Id, Name, Email) VALUES (2, 'Bia', NULL);
                INSERT INTO Users (Id, Name, Email) VALUES (3, 'Caio', NULL);
                """;
            seed.ExecuteNonQuery();
        }

        using var command = new SqliteCommandMock(_connection)
        {
            CommandText = """
                SELECT TOP 1 Name FROM Users ORDER BY Id;
                SELECT Name FROM Users ORDER BY Id FETCH FIRST 1 ROWS ONLY;
                SELECT Name FROM Users ORDER BY Id LIMIT 1;
                """
        };

        using var reader = command.ExecuteReader();
        Assert.True(reader.Read());
        Assert.Equal("Ana", reader.GetString(0));
        Assert.True(reader.NextResult());
        Assert.True(reader.Read());
        Assert.Equal("Ana", reader.GetString(0));
        Assert.True(reader.NextResult());
        Assert.True(reader.Read());
        Assert.Equal("Ana", reader.GetString(0));
    }

    /// <summary>
    /// EN: Verifies equivalent pagination syntaxes return the same runtime result when automatic dialect mode is enabled.
    /// PT: Verifica se sintaxes equivalentes de paginacao retornam o mesmo resultado em runtime quando o modo automatico de dialeto esta habilitado.
    /// </summary>
    [Fact]
    [Trait("Category", "SqliteMock")]
    public void ExecuteReader_WithAutoSqlDialect_ShouldReturnSameResult_ForEquivalentPaginationSyntaxes()
    {
        _connection.UseAutoSqlDialect = true;

        using (var seed = new SqliteCommandMock(_connection))
        {
            seed.CommandText = """
                INSERT INTO Users (Id, Name, Email) VALUES (1, 'Ana', NULL);
                INSERT INTO Users (Id, Name, Email) VALUES (2, 'Bia', NULL);
                INSERT INTO Users (Id, Name, Email) VALUES (3, 'Caio', NULL);
                """;
            seed.ExecuteNonQuery();
        }

        static List<string> ReadSingleColumn(SqliteConnectionMock connection, string sql)
        {
            using var command = new SqliteCommandMock(connection)
            {
                CommandText = sql
            };
            using var reader = command.ExecuteReader();
            var values = new List<string>();
            while (reader.Read())
                values.Add(reader.GetString(0));
            return values;
        }

        var top = ReadSingleColumn(_connection, "SELECT TOP 2 Name FROM Users ORDER BY Id");
        var limit = ReadSingleColumn(_connection, "SELECT Name FROM Users ORDER BY Id LIMIT 2");
        var fetch = ReadSingleColumn(_connection, "SELECT Name FROM Users ORDER BY Id FETCH FIRST 2 ROWS ONLY");
        var rownum = ReadSingleColumn(_connection, "SELECT Name FROM Users WHERE ROWNUM <= 2 ORDER BY Id");

        Assert.Equal(new[] { "Ana", "Bia" }, top);
        Assert.Equal(top, limit);
        Assert.Equal(top, fetch);
        Assert.Equal(top, rownum);
    }

    /// <summary>
    /// EN: Verifies automatic dialect mode executes shared sequence DDL and expression families through the SQLite runtime pipeline.
    /// PT: Verifica se o modo automatico de dialeto executa DDL compartilhado de sequence e suas familias de expressoes pelo pipeline de runtime do SQLite.
    /// </summary>
    [Fact]
    [Trait("Category", "SqliteMock")]
    public void ExecuteScalar_WithAutoSqlDialect_ShouldAcceptSharedSequenceSyntaxFamilies()
    {
        _connection.UseAutoSqlDialect = true;

        using var command = new SqliteCommandMock(_connection)
        {
            CommandText = "CREATE SEQUENCE seq_users START WITH 10 INCREMENT BY 2"
        };
        Assert.Equal(0, command.ExecuteNonQuery());

        command.CommandText = "SELECT NEXT VALUE FOR seq_users";
        Assert.Equal(10L, Convert.ToInt64(command.ExecuteScalar()));

        command.CommandText = "SELECT seq_users.NEXTVAL";
        Assert.Equal(12L, Convert.ToInt64(command.ExecuteScalar()));

        command.CommandText = "SELECT CURRVAL('seq_users')";
        Assert.Equal(12L, Convert.ToInt64(command.ExecuteScalar()));

        command.CommandText = "SELECT LASTVAL()";
        Assert.Equal(12L, Convert.ToInt64(command.ExecuteScalar()));
    }

    /// <summary>
    /// EN: Verifies automatic dialect mode executes sequence expressions inside shared DML paths.
    /// PT: Verifica se o modo automatico de dialeto executa expressoes de sequence dentro de caminhos compartilhados de DML.
    /// </summary>
    [Fact]
    [Trait("Category", "SqliteMock")]
    public void ExecuteNonQuery_WithAutoSqlDialect_ShouldAcceptSequenceExpressionsInsideInsert()
    {
        _connection.UseAutoSqlDialect = true;

        using var command = new SqliteCommandMock(_connection)
        {
            CommandText = """
                CREATE SEQUENCE seq_users START WITH 20 INCREMENT BY 5;
                INSERT INTO Users (Id, Name, Email) VALUES (NEXT VALUE FOR seq_users, 'Ana', NULL);
                INSERT INTO Users (Id, Name, Email) VALUES (NEXTVAL('seq_users'), 'Bia', NULL);
                INSERT INTO Users (Id, Name, Email) VALUES (seq_users.NEXTVAL, 'Caio', NULL);
                """
        };

        Assert.Equal(3, command.ExecuteNonQuery());

        var users = _connection.GetTable("users");
        Assert.Equal(3, users.Count);
        Assert.Equal(20, Convert.ToInt32(users[0][0]));
        Assert.Equal(25, Convert.ToInt32(users[1][0]));
        Assert.Equal(30, Convert.ToInt32(users[2][0]));
    }

    /// <summary>
    /// EN: Verifies automatic dialect mode preserves session-scoped sequence state and shared DROP SEQUENCE behavior.
    /// PT: Verifica se o modo automatico de dialeto preserva o estado de sequence por sessao e o comportamento compartilhado de DROP SEQUENCE.
    /// </summary>
    [Fact]
    [Trait("Category", "SqliteMock")]
    public void ExecuteScalar_WithAutoSqlDialect_ShouldAcceptPreviousValueForAndDropSequence()
    {
        _connection.UseAutoSqlDialect = true;

        using var command = new SqliteCommandMock(_connection)
        {
            CommandText = "CREATE SEQUENCE seq_runtime START WITH 7 INCREMENT BY 3"
        };
        Assert.Equal(0, command.ExecuteNonQuery());

        command.CommandText = "SELECT NEXT VALUE FOR seq_runtime";
        Assert.Equal(7L, Convert.ToInt64(command.ExecuteScalar()));

        command.CommandText = "SELECT PREVIOUS VALUE FOR seq_runtime";
        Assert.Equal(7L, Convert.ToInt64(command.ExecuteScalar()));

        command.CommandText = "DROP SEQUENCE IF EXISTS seq_runtime";
        Assert.Equal(0, command.ExecuteNonQuery());
        Assert.False(_connection.TryGetSequence("seq_runtime", out _));
    }

    /// <summary>
    /// EN: Verifies automatic dialect mode executes shared JSON arrow operators through the SQLite runtime pipeline.
    /// PT: Verifica se o modo automatico de dialeto executa operadores JSON compartilhados pelo pipeline de runtime do SQLite.
    /// </summary>
    [Fact]
    [Trait("Category", "SqliteMock")]
    public void ExecuteScalar_WithAutoSqlDialect_ShouldAcceptJsonArrowOperators()
    {
        _connection.UseAutoSqlDialect = true;

        using var command = new SqliteCommandMock(_connection)
        {
            CommandText = "SELECT '{\"tenant\":\"acme\",\"region\":\"us\"}'->>'$.tenant'"
        };

        Assert.Equal("acme", Convert.ToString(command.ExecuteScalar()));

        command.CommandText = "SELECT '{\"tenant\":\"acme\",\"region\":\"us\"}'->>'$.region'";
        Assert.Equal("us", Convert.ToString(command.ExecuteScalar()));
    }

    /// <summary>
    /// EN: Verifies automatic dialect mode executes shared JSON_EXTRACT and JSON_VALUE functions through the SQLite runtime pipeline.
    /// PT: Verifica se o modo automatico de dialeto executa funcoes compartilhadas JSON_EXTRACT e JSON_VALUE pelo pipeline de runtime do SQLite.
    /// </summary>
    [Fact]
    [Trait("Category", "SqliteMock")]
    public void ExecuteScalar_WithAutoSqlDialect_ShouldAcceptSharedJsonFunctions()
    {
        _connection.UseAutoSqlDialect = true;

        using var command = new SqliteCommandMock(_connection)
        {
            CommandText = "SELECT JSON_EXTRACT('{\"tenant\":\"acme\",\"region\":\"us\"}', '$.tenant')"
        };

        Assert.Equal("acme", Convert.ToString(command.ExecuteScalar()));

        command.CommandText = "SELECT JSON_VALUE('{\"tenant\":\"acme\",\"region\":\"us\"}', '$.region')";
        Assert.Equal("us", Convert.ToString(command.ExecuteScalar()));

        command.CommandText = "SELECT JSON_VALUE('{\"tenantId\":42}', '$.tenantId' RETURNING NUMBER)";
        Assert.Equal(42m, Convert.ToDecimal(command.ExecuteScalar()));
    }

    /// <summary>
    /// EN: Verifies automatic dialect mode executes shared temporal aliases through the SQLite runtime pipeline.
    /// PT: Verifica se o modo automatico de dialeto executa aliases temporais compartilhados pelo pipeline de runtime do SQLite.
    /// </summary>
    [Fact]
    [Trait("Category", "SqliteMock")]
    public void ExecuteScalar_WithAutoSqlDialect_ShouldAcceptSharedTemporalAliases()
    {
        _connection.UseAutoSqlDialect = true;

        using var command = new SqliteCommandMock(_connection)
        {
            CommandText = "SELECT NOW()"
        };

        Assert.IsType<DateTime>(command.ExecuteScalar());

        command.CommandText = "SELECT GETDATE()";
        Assert.IsType<DateTime>(command.ExecuteScalar());

        command.CommandText = "SELECT CURRENT_DATE";
        Assert.IsType<DateTime>(command.ExecuteScalar());

        command.CommandText = "SELECT SYSTEMDATE";
        Assert.IsType<DateTime>(command.ExecuteScalar());
    }

    /// <summary>
    /// EN: Verifies automatic dialect mode executes shared date-add function families through the SQLite runtime pipeline.
    /// PT: Verifica se o modo automatico de dialeto executa familias compartilhadas de funcoes de adicao temporal pelo pipeline de runtime do SQLite.
    /// </summary>
    [Fact]
    [Trait("Category", "SqliteMock")]
    public void ExecuteScalar_WithAutoSqlDialect_ShouldAcceptSharedDateAddFamilies()
    {
        _connection.UseAutoSqlDialect = true;

        using var command = new SqliteCommandMock(_connection)
        {
            CommandText = "SELECT DATE_ADD('2024-01-10', INTERVAL 2 DAY)"
        };

        Assert.Equal(new DateTime(2024, 1, 12), Assert.IsType<DateTime>(command.ExecuteScalar()));

        command.CommandText = "SELECT DATEADD(DAY, 2, '2024-01-10')";
        Assert.Equal(new DateTime(2024, 1, 12), Assert.IsType<DateTime>(command.ExecuteScalar()));

        command.CommandText = "SELECT TIMESTAMPADD(DAY, 2, '2024-01-10')";
        Assert.Equal(new DateTime(2024, 1, 12), Assert.IsType<DateTime>(command.ExecuteScalar()));
    }

    /// <summary>
    /// EN: Verifies automatic dialect mode executes shared string-aggregate families through the SQLite runtime pipeline.
    /// PT: Verifica se o modo automatico de dialeto executa familias compartilhadas de agregacao textual pelo pipeline de runtime do SQLite.
    /// </summary>
    [Fact]
    [Trait("Category", "SqliteMock")]
    public void ExecuteScalar_WithAutoSqlDialect_ShouldAcceptSharedStringAggregateFamilies()
    {
        _connection.UseAutoSqlDialect = true;

        using (var seed = new SqliteCommandMock(_connection))
        {
            seed.CommandText = """
                INSERT INTO Users (Id, Name, Email) VALUES (1, 'Ana', NULL);
                INSERT INTO Users (Id, Name, Email) VALUES (2, 'Bia', NULL);
                INSERT INTO Users (Id, Name, Email) VALUES (3, 'Caio', NULL);
                """;
            seed.ExecuteNonQuery();
        }

        using var command = new SqliteCommandMock(_connection)
        {
            CommandText = "SELECT GROUP_CONCAT(Name, '|') FROM Users"
        };

        Assert.Equal("Ana|Bia|Caio", Convert.ToString(command.ExecuteScalar()));

        command.CommandText = "SELECT STRING_AGG(Name, '|') FROM Users";
        Assert.Equal("Ana|Bia|Caio", Convert.ToString(command.ExecuteScalar()));

        command.CommandText = "SELECT LISTAGG(Name, '|') WITHIN GROUP (ORDER BY Name DESC) FROM Users";
        Assert.Equal("Caio|Bia|Ana", Convert.ToString(command.ExecuteScalar()));
    }

    /// <summary>
    /// EN: Verifies automatic dialect mode executes shared rowcount helpers through the SQLite runtime pipeline.
    /// PT: Verifica se o modo automatico de dialeto executa helpers compartilhados de rowcount pelo pipeline de runtime do SQLite.
    /// </summary>
    [Fact]
    [Trait("Category", "SqliteMock")]
    public void ExecuteScalar_WithAutoSqlDialect_ShouldAcceptSharedRowCountHelpers()
    {
        _connection.UseAutoSqlDialect = true;

        using var command = new SqliteCommandMock(_connection)
        {
            CommandText = "UPDATE Users SET Name = 'Updated User' WHERE Id = 999"
        };
        command.ExecuteNonQuery();

        command.CommandText = "SELECT CHANGES()";
        Assert.Equal(0L, Convert.ToInt64(command.ExecuteScalar()));

        command.CommandText = "SELECT ROW_COUNT()";
        Assert.Equal(0L, Convert.ToInt64(command.ExecuteScalar()));

        command.CommandText = "SELECT FOUND_ROWS()";
        Assert.Equal(0L, Convert.ToInt64(command.ExecuteScalar()));

        command.CommandText = "SELECT ROWCOUNT()";
        Assert.Equal(0L, Convert.ToInt64(command.ExecuteScalar()));

        command.CommandText = "SELECT @@ROWCOUNT";
        Assert.Equal(0L, Convert.ToInt64(command.ExecuteScalar()));
    }

    /// <summary>
    /// EN: Verifies automatic dialect mode executes SQL_CALC_FOUND_ROWS with FOUND_ROWS through the shared runtime pipeline.
    /// PT: Verifica se o modo automatico de dialeto executa SQL_CALC_FOUND_ROWS com FOUND_ROWS pelo pipeline compartilhado de runtime.
    /// </summary>
    [Fact]
    [Trait("Category", "SqliteMock")]
    public void ExecuteReader_WithAutoSqlDialect_ShouldAcceptSqlCalcFoundRowsModifier()
    {
        _connection.UseAutoSqlDialect = true;

        using (var seed = new SqliteCommandMock(_connection))
        {
            seed.CommandText = """
                INSERT INTO Users (Id, Name, Email) VALUES (1, 'Ana', NULL);
                INSERT INTO Users (Id, Name, Email) VALUES (2, 'Bia', NULL);
                INSERT INTO Users (Id, Name, Email) VALUES (3, 'Caio', NULL);
                """;
            seed.ExecuteNonQuery();
        }

        using var command = new SqliteCommandMock(_connection)
        {
            CommandText = "SELECT SQL_CALC_FOUND_ROWS Name FROM Users ORDER BY Id LIMIT 1; SELECT FOUND_ROWS();"
        };

        using var reader = command.ExecuteReader();
        Assert.True(reader.Read());
        Assert.Equal("Ana", reader.GetString(0));
        Assert.True(reader.NextResult());
        Assert.True(reader.Read());
        Assert.Equal(3L, Convert.ToInt64(reader.GetValue(0)));
    }

    /// <summary>
    /// EN: Verifies automatic dialect mode executes null-safe equality through the shared runtime pipeline.
    /// PT: Verifica se o modo automatico de dialeto executa igualdade null-safe pelo pipeline compartilhado de runtime.
    /// </summary>
    [Fact]
    [Trait("Category", "SqliteMock")]
    public void ExecuteScalar_WithAutoSqlDialect_ShouldAcceptNullSafeEquality()
    {
        _connection.UseAutoSqlDialect = true;

        using var command = new SqliteCommandMock(_connection)
        {
            CommandText = "SELECT NULL <=> NULL"
        };

        Assert.True(Convert.ToBoolean(command.ExecuteScalar()));

        command.CommandText = "SELECT NULL <=> 1";
        Assert.False(Convert.ToBoolean(command.ExecuteScalar()));
    }

    /// <summary>
    /// EN: Verifies automatic dialect mode executes ILIKE through the shared runtime pipeline.
    /// PT: Verifica se o modo automatico de dialeto executa ILIKE pelo pipeline compartilhado de runtime.
    /// </summary>
    [Fact]
    [Trait("Category", "SqliteMock")]
    public void ExecuteScalar_WithAutoSqlDialect_ShouldAcceptIlike()
    {
        _connection.UseAutoSqlDialect = true;

        using var command = new SqliteCommandMock(_connection)
        {
            CommandText = "SELECT 'John' ILIKE 'jo%'"
        };

        Assert.True(Convert.ToBoolean(command.ExecuteScalar()));

        command.CommandText = "SELECT 'John' ILIKE 'ma%'";
        Assert.False(Convert.ToBoolean(command.ExecuteScalar()));
    }

    /// <summary>
    /// EN: Verifies automatic dialect mode executes MATCH ... AGAINST through the shared runtime pipeline.
    /// PT: Verifica se o modo automatico de dialeto executa MATCH ... AGAINST pelo pipeline compartilhado de runtime.
    /// </summary>
    [Fact]
    [Trait("Category", "SqliteMock")]
    public void ExecuteScalar_WithAutoSqlDialect_ShouldAcceptMatchAgainst()
    {
        _connection.UseAutoSqlDialect = true;

        using var command = new SqliteCommandMock(_connection)
        {
            CommandText = "SELECT MATCH('john doe', 'john') AGAINST ('john' IN BOOLEAN MODE)"
        };

        Assert.Equal(1, Convert.ToInt32(command.ExecuteScalar()));

        command.CommandText = "SELECT MATCH('john doe', 'john') AGAINST ('+maria -john' IN BOOLEAN MODE)";
        Assert.Equal(0, Convert.ToInt32(command.ExecuteScalar()));
    }

    /// <summary>
    /// EN: Verifies automatic dialect mode executes shared conditional and null-substitute helpers.
    /// PT: Verifica se o modo automatico de dialeto executa helpers compartilhados condicionais e de substituicao de nulos.
    /// </summary>
    [Fact]
    [Trait("Category", "SqliteMock")]
    public void ExecuteScalar_WithAutoSqlDialect_ShouldAcceptConditionalAndNullSubstituteHelpers()
    {
        _connection.UseAutoSqlDialect = true;

        using var command = new SqliteCommandMock(_connection)
        {
            CommandText = "SELECT IF(1 = 1, 'yes', 'no')"
        };

        Assert.Equal("yes", Convert.ToString(command.ExecuteScalar()));

        command.CommandText = "SELECT IIF(1 = 0, 'yes', 'no')";
        Assert.Equal("no", Convert.ToString(command.ExecuteScalar()));

        command.CommandText = "SELECT IFNULL(NULL, 'fallback')";
        Assert.Equal("fallback", Convert.ToString(command.ExecuteScalar()));

        command.CommandText = "SELECT ISNULL(NULL, 'fallback')";
        Assert.Equal("fallback", Convert.ToString(command.ExecuteScalar()));

        command.CommandText = "SELECT NVL(NULL, 'fallback')";
        Assert.Equal("fallback", Convert.ToString(command.ExecuteScalar()));

        command.CommandText = "SELECT COALESCE(NULL, 'fallback')";
        Assert.Equal("fallback", Convert.ToString(command.ExecuteScalar()));

        command.CommandText = "SELECT NULLIF('same', 'same')";
        Assert.Equal(DBNull.Value, command.ExecuteScalar());
    }

    /// <summary>
    /// EN: Verifies automatic dialect mode executes the shared OPENJSON scalar subset.
    /// PT: Verifica se o modo automatico de dialeto executa o subset escalar compartilhado de OPENJSON.
    /// </summary>
    [Fact]
    [Trait("Category", "SqliteMock")]
    public void ExecuteScalar_WithAutoSqlDialect_ShouldAcceptOpenJson()
    {
        _connection.UseAutoSqlDialect = true;

        using var command = new SqliteCommandMock(_connection)
        {
            CommandText = """SELECT OPENJSON('{"tenant":"acme","region":"sa"}')"""
        };

        Assert.Equal("""{"tenant":"acme","region":"sa"}""", Convert.ToString(command.ExecuteScalar()));

        command.CommandText = "SELECT OPENJSON(NULL)";
        Assert.Equal(DBNull.Value, command.ExecuteScalar());
    }

    /// <summary>
    /// EN: Verifies automatic dialect mode executes shared window functions through the shared runtime pipeline.
    /// PT: Verifica se o modo automatico de dialeto executa funcoes de janela compartilhadas pelo pipeline compartilhado de runtime.
    /// </summary>
    [Fact]
    [Trait("Category", "SqliteMock")]
    public void ExecuteReader_WithAutoSqlDialect_ShouldAcceptWindowFunctions()
    {
        _connection.UseAutoSqlDialect = true;

        using (var seed = new SqliteCommandMock(_connection))
        {
            seed.CommandText = """
                INSERT INTO Orders (OrderId, UserId, Amount) VALUES (1, 10, 10.00);
                INSERT INTO Orders (OrderId, UserId, Amount) VALUES (2, 10, 15.00);
                INSERT INTO Orders (OrderId, UserId, Amount) VALUES (3, 20, 8.00);
                """;
            seed.ExecuteNonQuery();
        }

        using var command = new SqliteCommandMock(_connection)
        {
            CommandText = """
                SELECT
                    OrderId,
                    ROW_NUMBER() OVER (ORDER BY OrderId) AS rn,
                    LAG(OrderId, 1, 0) OVER (ORDER BY OrderId) AS prev_id
                FROM Orders
                ORDER BY OrderId
                """
        };

        using var reader = command.ExecuteReader();

        Assert.True(reader.Read());
        Assert.Equal(1, reader.GetInt32(0));
        Assert.Equal(1L, Convert.ToInt64(reader.GetValue(1)));
        Assert.Equal(0L, Convert.ToInt64(reader.GetValue(2)));

        Assert.True(reader.Read());
        Assert.Equal(2, reader.GetInt32(0));
        Assert.Equal(2L, Convert.ToInt64(reader.GetValue(1)));
        Assert.Equal(1, Convert.ToInt32(reader.GetValue(2)));

        Assert.True(reader.Read());
        Assert.Equal(3, reader.GetInt32(0));
        Assert.Equal(3L, Convert.ToInt64(reader.GetValue(1)));
        Assert.Equal(2, Convert.ToInt32(reader.GetValue(2)));

        Assert.False(reader.Read());
    }

    /// <summary>
    /// EN: Verifies automatic dialect mode executes the shared PIVOT subset through the shared runtime pipeline.
    /// PT: Verifica se o modo automatico de dialeto executa o subset compartilhado de PIVOT pelo pipeline compartilhado de runtime.
    /// </summary>
    [Fact]
    [Trait("Category", "SqliteMock")]
    public void ExecuteReader_WithAutoSqlDialect_ShouldAcceptPivot()
    {
        _connection.UseAutoSqlDialect = true;

        using (var seed = new SqliteCommandMock(_connection))
        {
            seed.CommandText = """
                INSERT INTO Orders (OrderId, UserId, Amount) VALUES (1, 10, 10.00);
                INSERT INTO Orders (OrderId, UserId, Amount) VALUES (2, 10, 15.00);
                INSERT INTO Orders (OrderId, UserId, Amount) VALUES (3, 20, 8.00);
                """;
            seed.ExecuteNonQuery();
        }

        using var command = new SqliteCommandMock(_connection)
        {
            CommandText = """
                SELECT u10, u20
                FROM (SELECT UserId, OrderId FROM Orders) src
                PIVOT (COUNT(OrderId) FOR UserId IN (10 AS u10, 20 AS u20)) p
                """
        };

        using var reader = command.ExecuteReader();

        Assert.True(reader.Read());
        Assert.Equal(2, Convert.ToInt32(reader.GetValue(0)));
        Assert.Equal(1, Convert.ToInt32(reader.GetValue(1)));
        Assert.False(reader.Read());
    }

    /// <summary>
    /// EN: Verifies automatic dialect mode executes shared WITH/CTE syntax through the shared runtime pipeline.
    /// PT: Verifica se o modo automatico de dialeto executa sintaxe compartilhada de WITH/CTE pelo pipeline compartilhado de runtime.
    /// </summary>
    [Fact]
    [Trait("Category", "SqliteMock")]
    public void ExecuteReader_WithAutoSqlDialect_ShouldAcceptWithCte()
    {
        _connection.UseAutoSqlDialect = true;

        using (var seed = new SqliteCommandMock(_connection))
        {
            seed.CommandText = """
                INSERT INTO Users (Id, Name, Email) VALUES (1, 'Ana', NULL);
                INSERT INTO Users (Id, Name, Email) VALUES (2, 'Bia', NULL);
                INSERT INTO Users (Id, Name, Email) VALUES (3, 'Caio', NULL);
                """;
            seed.ExecuteNonQuery();
        }

        using var command = new SqliteCommandMock(_connection)
        {
            CommandText = """
                WITH active_users AS (
                    SELECT Id, Name
                    FROM Users
                    WHERE Id <= 2
                )
                SELECT Name
                FROM active_users
                ORDER BY Id
                """
        };

        using var reader = command.ExecuteReader();
        Assert.True(reader.Read());
        Assert.Equal("Ana", reader.GetString(0));
        Assert.True(reader.Read());
        Assert.Equal("Bia", reader.GetString(0));
        Assert.False(reader.Read());
    }

    /// <summary>
    /// EN: Verifies automatic dialect mode executes shared DML RETURNING syntax through the SQLite runtime pipeline.
    /// PT: Verifica se o modo automatico de dialeto executa sintaxe compartilhada de RETURNING em DML pelo pipeline de runtime do SQLite.
    /// </summary>
    [Fact]
    [Trait("Category", "SqliteMock")]
    public void ExecuteReader_WithAutoSqlDialect_ShouldAcceptReturning()
    {
        _connection.UseAutoSqlDialect = true;

        using var command = new SqliteCommandMock(_connection)
        {
            CommandText = "INSERT INTO Users (Id, Name, Email) VALUES (701, 'Returning Auto', NULL) RETURNING Id, Name AS user_name"
        };

        using (var reader = command.ExecuteReader())
        {
            Assert.True(reader.Read());
            Assert.Equal(701, reader.GetInt32(0));
            Assert.Equal("Returning Auto", reader.GetString(1));
            Assert.False(reader.Read());
        }

        command.CommandText = "UPDATE Users SET Name = 'Returning Updated' WHERE Id = 701 RETURNING Id, Name";
        using (var reader = command.ExecuteReader())
        {
            Assert.True(reader.Read());
            Assert.Equal(701, reader.GetInt32(0));
            Assert.Equal("Returning Updated", reader.GetString(1));
            Assert.False(reader.Read());
        }

        command.CommandText = "DELETE FROM Users WHERE Id = 701 RETURNING Id";
        using (var reader = command.ExecuteReader())
        {
            Assert.True(reader.Read());
            Assert.Equal(701, reader.GetInt32(0));
            Assert.False(reader.Read());
        }
    }

    /// <summary>
    /// EN: Verifies automatic dialect mode executes shared ORDER BY NULLS FIRST/LAST semantics through the shared runtime pipeline.
    /// PT: Verifica se o modo automatico de dialeto executa a semantica compartilhada de ORDER BY NULLS FIRST/LAST pelo pipeline compartilhado de runtime.
    /// </summary>
    [Fact]
    [Trait("Category", "SqliteMock")]
    public void ExecuteReader_WithAutoSqlDialect_ShouldAcceptOrderByNulls()
    {
        _connection.UseAutoSqlDialect = true;

        using (var seed = new SqliteCommandMock(_connection))
        {
            seed.CommandText = """
                INSERT INTO Users (Id, Name, Email) VALUES (1, 'Ana', NULL);
                INSERT INTO Users (Id, Name, Email) VALUES (2, 'Bia', 'bia@test.local');
                INSERT INTO Users (Id, Name, Email) VALUES (3, 'Caio', NULL);
                """;
            seed.ExecuteNonQuery();
        }

        using var command = new SqliteCommandMock(_connection)
        {
            CommandText = "SELECT Name FROM Users ORDER BY Email NULLS FIRST, Id"
        };

        using (var reader = command.ExecuteReader())
        {
            Assert.True(reader.Read());
            Assert.Equal("Ana", reader.GetString(0));
            Assert.True(reader.Read());
            Assert.Equal("Caio", reader.GetString(0));
            Assert.True(reader.Read());
            Assert.Equal("Bia", reader.GetString(0));
            Assert.False(reader.Read());
        }

        command.CommandText = "SELECT Name FROM Users ORDER BY Email NULLS LAST, Id";
        using (var reader = command.ExecuteReader())
        {
            Assert.True(reader.Read());
            Assert.Equal("Bia", reader.GetString(0));
            Assert.True(reader.Read());
            Assert.Equal("Ana", reader.GetString(0));
            Assert.True(reader.Read());
            Assert.Equal("Caio", reader.GetString(0));
            Assert.False(reader.Read());
        }
    }

    /// <summary>
    /// EN: Tests ExecuteNonQuery with multi-statement INSERT script behavior.
    /// PT: Testa o comportamento de ExecuteNonQuery com script de INSERT multi-statement.
    /// </summary>
    [Fact]
    [Trait("Category", "SqliteMock")]
    public void ExecuteNonQuery_MultiStatementInsertScript_ShouldInsertAllRowsAndReturnTotalAffected()
    {
        using var command = new SqliteCommandMock(_connection)
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
    /// EN: Verifies SQLite rejects FOUND_ROWS because the provider exposes CHANGES for row-count inspection.
    /// PT: Verifica que o SQLite rejeita FOUND_ROWS porque o provider expoe CHANGES para inspecao de contagem de linhas.
    /// </summary>
    [Fact]
    [Trait("Category", "SqliteMock")]
    public void TestSelect_FoundRows_ShouldThrowNotSupportedException()
    {
        using var command = new SqliteCommandMock(_connection);
        command.CommandText = """
            INSERT INTO Users (Id, Name, Email) VALUES (101, 'Ana', NULL);
            INSERT INTO Users (Id, Name, Email) VALUES (102, 'Bia', NULL);
            INSERT INTO Users (Id, Name, Email) VALUES (103, 'Caio', NULL);
            """;
        command.ExecuteNonQuery();

        command.CommandText = "SELECT Name FROM Users ORDER BY Id LIMIT 1; SELECT FOUND_ROWS();";
        var ex = Assert.Throws<NotSupportedException>(() => command.ExecuteReader());

        Assert.Contains("FOUND_ROWS", ex.Message, StringComparison.OrdinalIgnoreCase);
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
