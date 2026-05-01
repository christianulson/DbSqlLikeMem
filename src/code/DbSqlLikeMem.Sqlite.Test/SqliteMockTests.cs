namespace DbSqlLikeMem.Sqlite.Test;

/// <summary>
/// EN: Covers SQLite mock command and transaction scenarios.
/// PT-br: Cobre cenarios de comandos e transacoes do mock SQLite.
/// </summary>
public sealed class SqliteMockTests
    : XUnitTestBase
{
    private readonly SqliteConnectionMock _connection;

    /// <summary>
    /// EN: Creates the SQLite mock test fixture with seeded tables.
    /// PT-br: Cria a fixture de teste do mock SQLite com tabelas semeadas.
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
    /// EN: Verifies INSERT statements write rows into the SQLite mock.
    /// PT-br: Verifica se comandos INSERT gravam linhas no mock SQLite.
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
        rowsAffected.Should().Be(1);
        _connection.GetTable("users")[0][1].Should().Be("John Doe");
    }

    /// <summary>
    /// EN: Verifies a prepared SQLite command can be executed repeatedly with updated parameter values.
    /// PT-br: Verifica se um comando SQLite preparado pode ser executado repetidamente com valores de parametro atualizados.
    /// </summary>
    [Fact]
    [Trait("Category", "SqliteMock")]
    public void Prepare_ShouldExecuteRepeatedPreparedInsertWithUpdatedParameters()
    {
        using var command = new SqliteCommandMock(_connection)
        {
            CommandText = "INSERT INTO Users (Id, Name, Email) VALUES (@id, @name, NULL)"
        };

        var id = new SqliteParameter("@id", 1);
        var name = new SqliteParameter("@name", "Ana");
        command.Parameters.Add(id);
        command.Parameters.Add(name);

        command.Prepare();

        var firstRowsAffected = command.ExecuteNonQuery();
        id.Value = 2;
        name.Value = "Bia";
        var secondRowsAffected = command.ExecuteNonQuery();

        firstRowsAffected.Should().Be(1);
        secondRowsAffected.Should().Be(1);
        _connection.GetTable("users").Count.Should().Be(2);
        _connection.GetTable("users")[0][1].Should().Be("Ana");
        _connection.GetTable("users")[1][1].Should().Be("Bia");
    }

    /// <summary>
    /// EN: Verifies automatic dialect mode executes SQL Server TOP syntax on the shared runtime pipeline.
    /// PT-br: Verifica se o modo automatico de dialeto executa sintaxe TOP do SQL Server no pipeline compartilhado de runtime.
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
        reader.Read().Should().BeTrue();
        reader.GetString(0).Should().Be("Ana");
    }

    /// <summary>
    /// EN: Verifies automatic dialect mode executes ANSI FETCH FIRST syntax on the shared runtime pipeline.
    /// PT-br: Verifica se o modo automatico de dialeto executa sintaxe ANSI FETCH FIRST no pipeline compartilhado de runtime.
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
        reader.Read().Should().BeTrue();
        reader.GetString(0).Should().Be("Ana");
    }

    /// <summary>
    /// EN: Verifies changing to the current database keeps the select-plan cache generation unchanged.
    /// PT-br: Verifica se mudar para o database atual mantem inalterada a geracao do cache de plano de select.
    /// </summary>
    [Fact]
    [Trait("Category", "SqliteMock")]
    public void ChangeDatabase_WithSameValue_ShouldKeepSelectPlanCacheGeneration()
    {
        using (var seed = new SqliteCommandMock(_connection))
        {
            seed.CommandText = "INSERT INTO Users (Id, Name, Email) VALUES (1, 'Ana', NULL)";
            seed.ExecuteNonQuery();
        }

        using var warmup = new SqliteCommandMock(_connection)
        {
            CommandText = "SELECT Name FROM Users WHERE Id = 1"
        };

        warmup.ExecuteScalar().Should().Be("Ana");

        var generationBefore = _connection.GetSelectPlanCacheGeneration();
        _connection.ChangeDatabase(_connection.Database);
        var generationAfter = _connection.GetSelectPlanCacheGeneration();

        generationAfter.Should().Be(generationBefore);
    }

    /// <summary>
    /// EN: Verifies automatic dialect mode executes INSERT SELECT with TOP through the shared non-query pipeline.
    /// PT-br: Verifica se o modo automatico de dialeto executa INSERT SELECT com TOP pelo pipeline compartilhado de non-query.
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

        rowsAffected.Should().Be(1);
        _connection.GetTable("users").Count.Should().Be(3);
        _connection.GetTable("users")[2][1].Should().Be("Ana");
    }

    /// <summary>
    /// EN: Verifies automatic dialect mode supports equivalent pagination syntaxes inside the same reader batch.
    /// PT-br: Verifica se o modo automatico de dialeto suporta sintaxes equivalentes de paginacao no mesmo batch de leitura.
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
        reader.Read().Should().BeTrue();
        reader.GetString(0).Should().Be("Ana");
        reader.NextResult().Should().BeTrue();
        reader.Read().Should().BeTrue();
        reader.GetString(0).Should().Be("Ana");
        reader.NextResult().Should().BeTrue();
        reader.Read().Should().BeTrue();
        reader.GetString(0).Should().Be("Ana");
    }

    /// <summary>
    /// EN: Verifies equivalent pagination syntaxes return the same runtime result when automatic dialect mode is enabled.
    /// PT-br: Verifica se sintaxes equivalentes de paginacao retornam o mesmo resultado em runtime quando o modo automatico de dialeto esta habilitado.
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

        top.Should().Equal(["Ana", "Bia"]);
        limit.Should().Equal(top);
        fetch.Should().Equal(top);
        rownum.Should().Equal(top);
    }

    /// <summary>
    /// EN: Verifies automatic dialect mode executes shared sequence DDL and expression families through the SQLite runtime pipeline.
    /// PT-br: Verifica se o modo automatico de dialeto executa DDL compartilhado de sequence e suas familias de expressoes pelo pipeline de runtime do SQLite.
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
        command.ExecuteNonQuery().Should().Be(0);

        command.CommandText = "SELECT NEXT VALUE FOR seq_users";
        Convert.ToInt64(command.ExecuteScalar()).Should().Be(10L);

        command.CommandText = "SELECT seq_users.NEXTVAL";
        Convert.ToInt64(command.ExecuteScalar()).Should().Be(12L);

        command.CommandText = "SELECT CURRVAL('seq_users')";
        Convert.ToInt64(command.ExecuteScalar()).Should().Be(12L);

        command.CommandText = "SELECT LASTVAL()";
        Convert.ToInt64(command.ExecuteScalar()).Should().Be(12L);
    }

    /// <summary>
    /// EN: Verifies automatic dialect mode executes sequence expressions inside shared DML paths.
    /// PT-br: Verifica se o modo automatico de dialeto executa expressoes de sequence dentro de caminhos compartilhados de DML.
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

        command.ExecuteNonQuery().Should().Be(3);

        var users = _connection.GetTable("users");
        users.Count.Should().Be(3);
        Convert.ToInt32(users[0][0]).Should().Be(20);
        Convert.ToInt32(users[1][0]).Should().Be(25);
        Convert.ToInt32(users[2][0]).Should().Be(30);
    }

    /// <summary>
    /// EN: Verifies automatic dialect mode preserves session-scoped sequence state and shared DROP SEQUENCE behavior.
    /// PT-br: Verifica se o modo automatico de dialeto preserva o estado de sequence por sessao e o comportamento compartilhado de DROP SEQUENCE.
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
        command.ExecuteNonQuery().Should().Be(0);

        command.CommandText = "SELECT NEXT VALUE FOR seq_runtime";
        Convert.ToInt64(command.ExecuteScalar()).Should().Be(7L);

        command.CommandText = "SELECT PREVIOUS VALUE FOR seq_runtime";
        Convert.ToInt64(command.ExecuteScalar()).Should().Be(7L);

        command.CommandText = "DROP SEQUENCE IF EXISTS seq_runtime";
        command.ExecuteNonQuery().Should().Be(0);
        _connection.TryGetSequence("seq_runtime", out _).Should().BeFalse();
    }

    /// <summary>
    /// EN: Verifies SQLite TRUNC truncates numeric values toward zero and rejects the scale form.
    /// PT-br: Verifica se TRUNC do SQLite trunca valores numericos em direcao ao zero e rejeita a forma com escala.
    /// </summary>
    [Fact]
    [Trait("Category", "SqliteMock")]
    public void ExecuteScalar_ShouldSupportTruncWithoutScale()
    {
        using var command = new SqliteCommandMock(_connection)
        {
            CommandText = "SELECT TRUNC(1.9)"
        };

        Convert.ToDecimal(command.ExecuteScalar()).Should().Be(1m);

        command.CommandText = "SELECT TRUNC(-1.9)";
        Convert.ToDecimal(command.ExecuteScalar()).Should().Be(-1m);

        command.CommandText = "SELECT TRUNC(1.987, 2)";
        command.Invoking(c => c.ExecuteScalar()).Should().Throw<NotSupportedException>();
    }

    /// <summary>
    /// EN: Verifies SQLite LOG2 returns the expected base-2 logarithm value.
    /// PT-br: Verifica se LOG2 do SQLite retorna o valor esperado do logaritmo de base 2.
    /// </summary>
    [Fact]
    [Trait("Category", "SqliteMock")]
    public void ExecuteScalar_ShouldSupportLog2()
    {
        using var command = new SqliteCommandMock(_connection)
        {
            CommandText = "SELECT LOG2(8)"
        };

        Convert.ToDouble(command.ExecuteScalar()).Should().BeApproximately(3d, 12);
    }

    /// <summary>
    /// EN: Verifies SQLite hyperbolic math functions return the expected values from the bundled math extension.
    /// PT-br: Verifica se as funcoes matematicas hiperbolicas do SQLite retornam os valores esperados da extensao matematica embutida.
    /// </summary>
    [Fact]
    [Trait("Category", "SqliteMock")]
    public void ExecuteScalar_ShouldSupportHyperbolicMathFunctions()
    {
        using var command = new SqliteCommandMock(_connection);

        command.CommandText = "SELECT ACOSH(1)";
        Convert.ToDouble(command.ExecuteScalar()).Should().BeApproximately(0d, 12);

        command.CommandText = "SELECT ASINH(0)";
        Convert.ToDouble(command.ExecuteScalar()).Should().BeApproximately(0d, 12);

        command.CommandText = "SELECT ATANH(0)";
        Convert.ToDouble(command.ExecuteScalar()).Should().BeApproximately(0d, 12);

        command.CommandText = "SELECT COSH(0)";
        Convert.ToDouble(command.ExecuteScalar()).Should().BeApproximately(1d, 12);

        command.CommandText = "SELECT SINH(0)";
        Convert.ToDouble(command.ExecuteScalar()).Should().BeApproximately(0d, 12);

        command.CommandText = "SELECT TANH(0)";
        Convert.ToDouble(command.ExecuteScalar()).Should().BeApproximately(0d, 12);
    }

    /// <summary>
    /// EN: Verifies automatic dialect mode executes the shared ALTER TABLE ... ADD COLUMN subset and backfills existing rows.
    /// PT-br: Verifica se o modo automatico de dialeto executa o subset compartilhado de ALTER TABLE ... ADD COLUMN e preenche linhas existentes.
    /// </summary>
    [Fact]
    [Trait("Category", "SqliteMock")]
    public void ExecuteNonQuery_WithAutoSqlDialect_ShouldAcceptAlterTableAddColumn()
    {
        _connection.UseAutoSqlDialect = true;

        using var command = new SqliteCommandMock(_connection)
        {
            CommandText = "INSERT INTO Users (Id, Name, Email) VALUES (1, 'Ana', NULL)"
        };
        command.ExecuteNonQuery();

        command.CommandText = "ALTER TABLE Users ADD COLUMN NickName VARCHAR(30) DEFAULT 'guest'";
        command.ExecuteNonQuery().Should().Be(0);

        var users = _connection.GetTable("users");
        users.Columns.ContainsKey("nickname").Should().BeTrue();
        users[0][users.Columns["nickname"].Index].Should().Be("guest");
    }

    /// <summary>
    /// EN: Verifies a cached SELECT * plan is invalidated after ALTER TABLE ... ADD COLUMN changes the shape.
    /// PT-br: Verifica se um plano cacheado de SELECT * e invalidado depois que ALTER TABLE ... ADD COLUMN altera o shape.
    /// </summary>
    [Fact]
    [Trait("Category", "SqliteMock")]
    public void ExecuteReader_WithAutoSqlDialect_ShouldInvalidateSelectPlanAfterAlterTableAddColumn()
    {
        _connection.UseAutoSqlDialect = true;

        using (var seed = new SqliteCommandMock(_connection))
        {
            seed.CommandText = "INSERT INTO Users (Id, Name, Email) VALUES (1, 'Ana', NULL)";
            seed.ExecuteNonQuery();
        }

        using (var warmup = new SqliteCommandMock(_connection))
        {
            warmup.CommandText = "SELECT * FROM Users ORDER BY Id";
            using var warmReader = warmup.ExecuteReader();
            warmReader.Read().Should().BeTrue();
            warmReader.FieldCount.Should().Be(3);
        }

        using (var alter = new SqliteCommandMock(_connection))
        {
            alter.CommandText = "ALTER TABLE Users ADD COLUMN NickName VARCHAR(30) DEFAULT 'guest'";
            alter.ExecuteNonQuery().Should().Be(0);
        }

        using var command = new SqliteCommandMock(_connection)
        {
            CommandText = "SELECT * FROM Users ORDER BY Id"
        };

        using var reader = command.ExecuteReader();

        reader.Read().Should().BeTrue();
        reader.FieldCount.Should().Be(4);
        Convert.ToString(reader.GetValue(3)).Should().Be("guest");
        reader.Read().Should().BeFalse();
    }

    /// <summary>
    /// EN: Verifies automatic dialect mode rejects NOT NULL ALTER TABLE additions without a default when rows already exist.
    /// PT-br: Verifica se o modo automatico de dialeto rejeita adicoes NOT NULL via ALTER TABLE sem default quando ja existem linhas.
    /// </summary>
    [Fact]
    [Trait("Category", "SqliteMock")]
    public void ExecuteNonQuery_WithAutoSqlDialect_ShouldRejectNotNullColumnWithoutDefaultOnPopulatedTable()
    {
        _connection.UseAutoSqlDialect = true;

        using var command = new SqliteCommandMock(_connection)
        {
            CommandText = "INSERT INTO Users (Id, Name, Email) VALUES (1, 'Ana', NULL)"
        };
        command.ExecuteNonQuery();

        var ex = FluentActions.Invoking(() =>
        {
            command.CommandText = "ALTER TABLE Users ADD COLUMN Status VARCHAR(20) NOT NULL";
            command.ExecuteNonQuery();
        }).Should().Throw<Exception>().Which;

        ex.Message.Contains("status", StringComparison.OrdinalIgnoreCase).Should().BeTrue();
        _connection.GetTable("users").Columns.ContainsKey("status").Should().BeFalse();
    }

    /// <summary>
    /// EN: Verifies automatic dialect mode executes shared JSON arrow operators through the SQLite runtime pipeline.
    /// PT-br: Verifica se o modo automatico de dialeto executa operadores JSON compartilhados pelo pipeline de runtime do SQLite.
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

        Convert.ToString(command.ExecuteScalar()).Should().Be("acme");

        command.CommandText = "SELECT '{\"tenant\":\"acme\",\"region\":\"us\"}'->>'$.region'";
        Convert.ToString(command.ExecuteScalar()).Should().Be("us");
    }

    /// <summary>
    /// EN: Verifies automatic dialect mode executes shared JSON_EXTRACT and JSON_VALUE functions through the SQLite runtime pipeline.
    /// PT-br: Verifica se o modo automatico de dialeto executa funcoes compartilhadas JSON_EXTRACT e JSON_VALUE pelo pipeline de runtime do SQLite.
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

        Convert.ToString(command.ExecuteScalar()).Should().Be("acme");

        command.CommandText = "SELECT JSON_VALUE('{\"tenant\":\"acme\",\"region\":\"us\"}', '$.region')";
        Convert.ToString(command.ExecuteScalar()).Should().Be("us");

        command.CommandText = "SELECT JSON_VALUE('{\"tenantId\":42}', '$.tenantId' RETURNING NUMBER)";
        Convert.ToDecimal(command.ExecuteScalar()).Should().Be(42m);
    }

    /// <summary>
    /// EN: Verifies automatic dialect mode executes shared temporal aliases through the SQLite runtime pipeline.
    /// PT-br: Verifica se o modo automatico de dialeto executa aliases temporais compartilhados pelo pipeline de runtime do SQLite.
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

        command.ExecuteScalar().Should().BeOfType<DateTime>();

        command.CommandText = "SELECT GETDATE()";
        command.ExecuteScalar().Should().BeOfType<DateTime>();

        command.CommandText = "SELECT CURRENT_DATE";
        command.ExecuteScalar().Should().BeOfType<DateTime>();

        command.CommandText = "SELECT SYSTEMDATE";
        command.ExecuteScalar().Should().BeOfType<DateTime>();
    }

    /// <summary>
    /// EN: Verifies automatic dialect mode executes shared date-add function families through the SQLite runtime pipeline.
    /// PT-br: Verifica se o modo automatico de dialeto executa familias compartilhadas de funcoes de adicao temporal pelo pipeline de runtime do SQLite.
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

        Convert.ToDateTime(command.ExecuteScalar()).Should().Be(new DateTime(2024, 1, 12));

        command.CommandText = "SELECT DATEADD(DAY, 2, '2024-01-10')";
        Convert.ToDateTime(command.ExecuteScalar()).Should().Be(new DateTime(2024, 1, 12));

        command.CommandText = "SELECT TIMESTAMPADD(DAY, 2, '2024-01-10')";
        Convert.ToDateTime(command.ExecuteScalar()).Should().Be(new DateTime(2024, 1, 12));
    }

    /// <summary>
    /// EN: Verifies automatic dialect mode executes shared string-aggregate families through the SQLite runtime pipeline.
    /// PT-br: Verifica se o modo automatico de dialeto executa familias compartilhadas de agregacao textual pelo pipeline de runtime do SQLite.
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

        Convert.ToString(command.ExecuteScalar()).Should().Be("Ana|Bia|Caio");

        command.CommandText = "SELECT GROUP_CONCAT(DISTINCT Name ORDER BY Name SEPARATOR '|') FROM Users";
        Convert.ToString(command.ExecuteScalar()).Should().Be("Ana|Bia|Caio");

        command.CommandText = "SELECT STRING_AGG(Name, '|') FROM Users";
        Convert.ToString(command.ExecuteScalar()).Should().Be("Ana|Bia|Caio");

        command.CommandText = "SELECT LISTAGG(Name, '|') WITHIN GROUP (ORDER BY Name DESC) FROM Users";
        Convert.ToString(command.ExecuteScalar()).Should().Be("Caio|Bia|Ana");
    }

    /// <summary>
    /// EN: Verifies COUNT(*) over UNION ALL uses the simplified runtime count path.
    /// PT-br: Verifica se COUNT(*) sobre UNION ALL usa o caminho simplificado de contagem em runtime.
    /// </summary>
    [Fact]
    [Trait("Category", "SqliteMock")]
    public void ExecuteScalar_ShouldCountUnionAllRows()
    {
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
                SELECT COUNT(*)
                FROM (
                    SELECT Id FROM Users
                    UNION ALL
                    SELECT Id FROM Users
                ) t
                """
        };

        Convert.ToInt64(command.ExecuteScalar()).Should().Be(6L);
    }

    /// <summary>
    /// EN: Verifies COUNT(*) over UNION ALL still works when the subquery has ORDER BY.
    /// PT-br: Verifica se COUNT(*) sobre UNION ALL continua funcionando quando a subquery tem ORDER BY.
    /// </summary>
    [Fact]
    [Trait("Category", "SqliteMock")]
    public void ExecuteScalar_ShouldCountUnionAllRowsWithOrderBy()
    {
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
                SELECT COUNT(*)
                FROM (
                    SELECT Id FROM Users
                    UNION ALL
                    SELECT Id FROM Users
                    ORDER BY Id DESC
                ) t
                """
        };

        Convert.ToInt64(command.ExecuteScalar()).Should().Be(6L);
    }

    /// <summary>
    /// EN: Verifies COUNT(*) over UNION ALL still works when the outer query applies LIMIT.
    /// PT-br: Verifica se COUNT(*) sobre UNION ALL continua funcionando quando a query externa aplica LIMIT.
    /// </summary>
    [Fact]
    [Trait("Category", "SqliteMock")]
    public void ExecuteScalar_ShouldCountUnionAllRowsWithLimit()
    {
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
                SELECT COUNT(*)
                FROM (
                    SELECT Id FROM Users
                    UNION ALL
                    SELECT Id FROM Users
                ) t
                LIMIT 1
                """
        };

        Convert.ToInt64(command.ExecuteScalar()).Should().Be(6L);
    }

    /// <summary>
    /// EN: Verifies UNION ALL projection preserves rows from both inputs.
    /// PT-br: Verifica se a projeção UNION ALL preserva linhas das duas entradas.
    /// </summary>
    [Fact]
    [Trait("Category", "SqliteMock")]
    public void ExecuteReader_ShouldReturnUnionAllProjectionRows()
    {
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
                SELECT Name
                FROM Users
                WHERE Id = 1
                UNION ALL
                SELECT Name
                FROM Users
                WHERE Id = 2
                """
        };

        using var reader = command.ExecuteReader();
        var values = new List<string>();
        while (reader.Read())
            values.Add(reader.GetString(0));

        values.Should().Equal(["Ana", "Bia"]);
    }

    /// <summary>
    /// EN: Verifies UNION ALL projection still applies ORDER BY and LIMIT after the fast path.
    /// PT-br: Verifica se a projeção UNION ALL ainda aplica ORDER BY e LIMIT após o caminho rapido.
    /// </summary>
    [Fact]
    [Trait("Category", "SqliteMock")]
    public void ExecuteReader_ShouldReturnUnionAllProjectionRowsWithOrderByAndLimit()
    {
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
                SELECT Name
                FROM Users
                WHERE Id = 1
                UNION ALL
                SELECT Name
                FROM Users
                WHERE Id = 2
                ORDER BY Name DESC
                LIMIT 1
                """
        };

        using var reader = command.ExecuteReader();
        var values = new List<string>();
        while (reader.Read())
            values.Add(reader.GetString(0));

        values.Should().Equal(["Bia"]);
    }

    /// <summary>
    /// EN: Verifies automatic dialect mode executes shared rowcount helpers through the SQLite runtime pipeline.
    /// PT-br: Verifica se o modo automatico de dialeto executa helpers compartilhados de rowcount pelo pipeline de runtime do SQLite.
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
        Convert.ToInt64(command.ExecuteScalar()).Should().Be(0L);

        command.CommandText = "SELECT ROW_COUNT()";
        Convert.ToInt64(command.ExecuteScalar()).Should().Be(0L);

        command.CommandText = "SELECT FOUND_ROWS()";
        Convert.ToInt64(command.ExecuteScalar()).Should().Be(0L);

        command.CommandText = "SELECT ROWCOUNT()";
        Convert.ToInt64(command.ExecuteScalar()).Should().Be(0L);

        command.CommandText = "SELECT @@ROWCOUNT";
        Convert.ToInt64(command.ExecuteScalar()).Should().Be(0L);
    }

    /// <summary>
    /// EN: Verifies automatic dialect mode exposes the full row count after SQL_CALC_FOUND_ROWS and FOUND_ROWS.
    /// PT-br: Verifica se o modo automatico de dialeto expõe a contagem total apos SQL_CALC_FOUND_ROWS e FOUND_ROWS.
    /// </summary>
    [Fact]
    [Trait("Category", "SqliteMock")]
    public void ExecuteReader_WithAutoSqlDialect_ShouldExposeSqlCalcFoundRowsCount()
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

        reader.Read().Should().BeTrue();
        reader.GetString(0).Should().Be("Ana");

        reader.NextResult().Should().BeTrue();
        reader.Read().Should().BeTrue();
        Convert.ToInt64(reader.GetValue(0), CultureInfo.InvariantCulture).Should().Be(3L);
    }

    /// <summary>
    /// EN: Verifies automatic dialect mode executes null-safe equality through the shared runtime pipeline.
    /// PT-br: Verifica se o modo automatico de dialeto executa igualdade null-safe pelo pipeline compartilhado de runtime.
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

        Convert.ToBoolean(command.ExecuteScalar()).Should().BeTrue();

        command.CommandText = "SELECT NULL <=> 1";
        Convert.ToBoolean(command.ExecuteScalar()).Should().BeFalse();
    }

    /// <summary>
    /// EN: Verifies automatic dialect mode executes ILIKE through the shared runtime pipeline.
    /// PT-br: Verifica se o modo automatico de dialeto executa ILIKE pelo pipeline compartilhado de runtime.
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

        Convert.ToBoolean(command.ExecuteScalar()).Should().BeTrue();

        command.CommandText = "SELECT 'John' ILIKE 'ma%'";
        Convert.ToBoolean(command.ExecuteScalar()).Should().BeFalse();
    }

    /// <summary>
    /// EN: Verifies automatic dialect mode executes MATCH ... AGAINST through the shared runtime pipeline.
    /// PT-br: Verifica se o modo automatico de dialeto executa MATCH ... AGAINST pelo pipeline compartilhado de runtime.
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

        Convert.ToInt32(command.ExecuteScalar()).Should().Be(1);

        command.CommandText = "SELECT MATCH('john doe', 'john') AGAINST ('+maria -john' IN BOOLEAN MODE)";
        Convert.ToInt32(command.ExecuteScalar()).Should().Be(0);
    }

    /// <summary>
    /// EN: Verifies automatic dialect mode executes shared conditional and null-substitute helpers.
    /// PT-br: Verifica se o modo automatico de dialeto executa helpers compartilhados condicionais e de substituicao de nulos.
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

        Convert.ToString(command.ExecuteScalar()).Should().Be("yes");

        command.CommandText = "SELECT IIF(1 = 0, 'yes', 'no')";
        Convert.ToString(command.ExecuteScalar()).Should().Be("no");

        command.CommandText = "SELECT IFNULL(NULL, 'fallback')";
        Convert.ToString(command.ExecuteScalar()).Should().Be("fallback");

        command.CommandText = "SELECT ISNULL(NULL, 'fallback')";
        Convert.ToString(command.ExecuteScalar()).Should().Be("fallback");

        command.CommandText = "SELECT NVL(NULL, 'fallback')";
        Convert.ToString(command.ExecuteScalar()).Should().Be("fallback");

        command.CommandText = "SELECT COALESCE(NULL, 'fallback')";
        Convert.ToString(command.ExecuteScalar()).Should().Be("fallback");

        command.CommandText = "SELECT NULLIF('same', 'same')";
        command.ExecuteScalar().Should().Be(DBNull.Value);
    }

    /// <summary>
    /// EN: Verifies automatic dialect mode executes the shared OPENJSON scalar subset.
    /// PT-br: Verifica se o modo automatico de dialeto executa o subset escalar compartilhado de OPENJSON.
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

        Convert.ToString(command.ExecuteScalar()).Should().Be("""{"tenant":"acme","region":"sa"}""");

        command.CommandText = "SELECT OPENJSON(NULL)";
        command.ExecuteScalar().Should().Be(DBNull.Value);
    }

    /// <summary>
    /// EN: Verifies automatic dialect mode executes shared window functions through the shared runtime pipeline.
    /// PT-br: Verifica se o modo automatico de dialeto executa funcoes de janela compartilhadas pelo pipeline compartilhado de runtime.
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

        reader.Read().Should().BeTrue();
        reader.GetInt32(0).Should().Be(1);
        Convert.ToInt64(reader.GetValue(1)).Should().Be(1L);
        Convert.ToInt64(reader.GetValue(2)).Should().Be(0L);

        reader.Read().Should().BeTrue();
        reader.GetInt32(0).Should().Be(2);
        Convert.ToInt64(reader.GetValue(1)).Should().Be(2L);
        Convert.ToInt32(reader.GetValue(2)).Should().Be(1);

        reader.Read().Should().BeTrue();
        reader.GetInt32(0).Should().Be(3);
        Convert.ToInt64(reader.GetValue(1)).Should().Be(3L);
        Convert.ToInt32(reader.GetValue(2)).Should().Be(2);

        reader.Read().Should().BeFalse();
    }

    /// <summary>
    /// EN: Verifies LAG with zero offset returns the current row for each ordered row.
    /// PT-br: Verifica se LAG com offset zero retorna a linha atual para cada linha ordenada.
    /// </summary>
    [Fact]
    [Trait("Category", "SqliteMock")]
    public void ExecuteReader_WithAutoSqlDialect_ShouldSupportLagZeroOffset()
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
                    LAG(OrderId, 0, -1) OVER (ORDER BY OrderId) AS lag0
                FROM Orders
                ORDER BY OrderId
                """
        };

        using var reader = command.ExecuteReader();

        reader.Read().Should().BeTrue();
        reader.GetInt32(0).Should().Be(1);
        Convert.ToInt32(reader.GetValue(1)).Should().Be(1);

        reader.Read().Should().BeTrue();
        reader.GetInt32(0).Should().Be(2);
        Convert.ToInt32(reader.GetValue(1)).Should().Be(2);

        reader.Read().Should().BeTrue();
        reader.GetInt32(0).Should().Be(3);
        Convert.ToInt32(reader.GetValue(1)).Should().Be(3);

        reader.Read().Should().BeFalse();
    }

    /// <summary>
    /// EN: Verifies FIRST_VALUE, LAST_VALUE and NTH_VALUE keep returning the expected ordered values.
    /// PT-br: Verifica se FIRST_VALUE, LAST_VALUE e NTH_VALUE continuam retornando os valores ordenados esperados.
    /// </summary>
    [Fact]
    [Trait("Category", "SqliteMock")]
    public void ExecuteReader_WithAutoSqlDialect_ShouldSupportFirstLastAndNthValue()
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
                    FIRST_VALUE(OrderId) OVER (ORDER BY OrderId) AS first_id,
                    LAST_VALUE(OrderId) OVER (ORDER BY OrderId) AS last_id,
                    NTH_VALUE(OrderId, 2) OVER (ORDER BY OrderId) AS second_id
                FROM Orders
                ORDER BY OrderId
                """
        };

        using var reader = command.ExecuteReader();

        reader.Read().Should().BeTrue();
        reader.GetInt32(0).Should().Be(1);
        Convert.ToInt32(reader.GetValue(1)).Should().Be(1);
        Convert.ToInt32(reader.GetValue(2)).Should().Be(1);
        reader.GetValue(3).Should().Be(DBNull.Value);

        reader.Read().Should().BeTrue();
        reader.GetInt32(0).Should().Be(2);
        Convert.ToInt32(reader.GetValue(1)).Should().Be(1);
        Convert.ToInt32(reader.GetValue(2)).Should().Be(2);
        Convert.ToInt32(reader.GetValue(3)).Should().Be(2);

        reader.Read().Should().BeTrue();
        reader.GetInt32(0).Should().Be(3);
        Convert.ToInt32(reader.GetValue(1)).Should().Be(1);
        Convert.ToInt32(reader.GetValue(2)).Should().Be(3);
        Convert.ToInt32(reader.GetValue(3)).Should().Be(2);

        reader.Read().Should().BeFalse();
    }

    /// <summary>
    /// EN: Verifies a scalar COUNT subquery returns the expected total for matching rows.
    /// PT-br: Verifica se uma subquery escalar COUNT retorna o total esperado para as linhas correspondentes.
    /// </summary>
    [Fact]
    [Trait("Category", "SqliteMock")]
    public void ExecuteScalar_WithAutoSqlDialect_ShouldSupportScalarCountSubquery()
    {
        _connection.UseAutoSqlDialect = true;

        using (var seed = new SqliteCommandMock(_connection))
        {
            seed.CommandText = """
                INSERT INTO Orders (OrderId, UserId, Amount) VALUES (1, 1, 10.00);
                INSERT INTO Orders (OrderId, UserId, Amount) VALUES (2, 1, 15.00);
                INSERT INTO Orders (OrderId, UserId, Amount) VALUES (3, 2, 8.00);
                """;
            seed.ExecuteNonQuery();
        }

        using var command = new SqliteCommandMock(_connection)
        {
            CommandText = """
                SELECT (SELECT COUNT(*) FROM Orders o WHERE o.UserId = 1)
                """
        };

        Convert.ToInt32(command.ExecuteScalar()).Should().Be(2);
    }

    /// <summary>
    /// EN: Verifies EXISTS subqueries return true when the filtered source has at least one matching row.
    /// PT-br: Verifica se subqueries EXISTS retornam true quando a fonte filtrada possui ao menos uma linha correspondente.
    /// </summary>
    [Fact]
    [Trait("Category", "SqliteMock")]
    public void ExecuteScalar_WithAutoSqlDialect_ShouldSupportExistsSubquery()
    {
        _connection.UseAutoSqlDialect = true;

        using (var seed = new SqliteCommandMock(_connection))
        {
            seed.CommandText = """
                INSERT INTO Orders (OrderId, UserId, Amount) VALUES (1, 1, 10.00);
                INSERT INTO Orders (OrderId, UserId, Amount) VALUES (2, 1, 15.00);
                INSERT INTO Orders (OrderId, UserId, Amount) VALUES (3, 2, 8.00);
                INSERT INTO Users (Id, Name, Email) VALUES (1, 'Ana', NULL);
                INSERT INTO Users (Id, Name, Email) VALUES (2, 'Bia', NULL);
                INSERT INTO Users (Id, Name, Email) VALUES (3, 'Caio', NULL);
                """;
            seed.ExecuteNonQuery();
        }

        using var command = new SqliteCommandMock(_connection)
        {
            CommandText = """
                SELECT COUNT(*) FROM Users WHERE EXISTS (SELECT 1 FROM Orders WHERE UserId = 1)
                """
        };

        Convert.ToInt32(command.ExecuteScalar()).Should().Be(3);
    }

    /// <summary>
    /// EN: Verifies COUNT(column) still ignores NULL values inside a scalar subquery.
    /// PT-br: Verifica se COUNT(coluna) continua ignorando valores NULL dentro de uma subquery escalar.
    /// </summary>
    [Fact]
    [Trait("Category", "SqliteMock")]
    public void ExecuteScalar_WithAutoSqlDialect_ShouldRespectCountColumnNullSemantics()
    {
        _connection.UseAutoSqlDialect = true;

        using (var seed = new SqliteCommandMock(_connection))
        {
            seed.CommandText = """
                INSERT INTO Users (Id, Name, Email) VALUES (1, 'Ana', 'ana@site.test');
                INSERT INTO Users (Id, Name, Email) VALUES (2, 'Bia', NULL);
                INSERT INTO Users (Id, Name, Email) VALUES (3, 'Caio', NULL);
                """;
            seed.ExecuteNonQuery();
        }

        using var command = new SqliteCommandMock(_connection)
        {
            CommandText = """
                SELECT (SELECT COUNT(Email) FROM Users WHERE Id <= 3)
                """
        };

        Convert.ToInt32(command.ExecuteScalar()).Should().Be(1);
    }

    /// <summary>
    /// EN: Verifies IN subqueries match rows using the expected membership set.
    /// PT-br: Verifica se subqueries IN combinam linhas usando o conjunto de pertencimento esperado.
    /// </summary>
    [Fact]
    [Trait("Category", "SqliteMock")]
    public void ExecuteScalar_WithAutoSqlDialect_ShouldSupportInSubquery()
    {
        _connection.UseAutoSqlDialect = true;

        using (var seed = new SqliteCommandMock(_connection))
        {
            seed.CommandText = """
                INSERT INTO Users (Id, Name, Email) VALUES (1, 'Ana', NULL);
                INSERT INTO Users (Id, Name, Email) VALUES (2, 'Bia', NULL);
                INSERT INTO Users (Id, Name, Email) VALUES (3, 'Caio', NULL);
                INSERT INTO Orders (OrderId, UserId, Amount) VALUES (1, 1, 10.00);
                INSERT INTO Orders (OrderId, UserId, Amount) VALUES (2, 1, 15.00);
                INSERT INTO Orders (OrderId, UserId, Amount) VALUES (3, 2, 8.00);
                """;
            seed.ExecuteNonQuery();
        }

        using var command = new SqliteCommandMock(_connection)
        {
            CommandText = """
                SELECT COUNT(*) FROM Users WHERE Id IN (SELECT UserId FROM Orders)
                """
        };

        Convert.ToInt32(command.ExecuteScalar()).Should().Be(2);
    }

    /// <summary>
    /// EN: Verifies window-function plans can be reused safely across repeated executions.
    /// PT-br: Verifica se planos com funcoes de janela podem ser reutilizados com seguranca em execucoes repetidas.
    /// </summary>
    [Fact]
    [Trait("Category", "SqliteMock")]
    public void ExecuteReader_WithAutoSqlDialect_ShouldReuseWindowFunctionPlanSafely()
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

        List<(int OrderId, long RowNumber, int PrevId)> ReadRows()
        {
            using var reader = command.ExecuteReader();
            var rows = new List<(int, long, int)>();
            while (reader.Read())
            {
                rows.Add((
                    reader.GetInt32(0),
                    Convert.ToInt64(reader.GetValue(1)),
                    Convert.ToInt32(reader.GetValue(2))));
            }

            return rows;
        }

        var firstPass = ReadRows();
        var secondPass = ReadRows();

        secondPass.Should().Equal(firstPass);
        firstPass.Should().Equal([(1, 1L, 0), (2, 2L, 1), (3, 3L, 2)]);
    }

    /// <summary>
    /// EN: Verifies window partitions reset row numbers for each partition key.
    /// PT-br: Verifica se as particoes de janela reiniciam a numeracao de linhas para cada chave de particao.
    /// </summary>
    [Fact]
    [Trait("Category", "SqliteMock")]
    public void ExecuteReader_WithAutoSqlDialect_ShouldPartitionWindowFunctionsByKey()
    {
        _connection.UseAutoSqlDialect = true;

        using (var seed = new SqliteCommandMock(_connection))
        {
            seed.CommandText = """
                INSERT INTO Orders (OrderId, UserId, Amount) VALUES (1, 10, 10.00);
                INSERT INTO Orders (OrderId, UserId, Amount) VALUES (2, 10, 15.00);
                INSERT INTO Orders (OrderId, UserId, Amount) VALUES (3, 20, 8.00);
                INSERT INTO Orders (OrderId, UserId, Amount) VALUES (4, 20, 12.00);
                """;
            seed.ExecuteNonQuery();
        }

        using var command = new SqliteCommandMock(_connection)
        {
            CommandText = """
                SELECT
                    OrderId,
                    UserId,
                    ROW_NUMBER() OVER (PARTITION BY UserId ORDER BY OrderId) AS rn
                FROM Orders
                ORDER BY OrderId
                """
        };

        using var reader = command.ExecuteReader();

        reader.Read().Should().BeTrue();
        reader.GetInt32(0).Should().Be(1);
        reader.GetInt32(1).Should().Be(10);
        Convert.ToInt64(reader.GetValue(2)).Should().Be(1L);

        reader.Read().Should().BeTrue();
        reader.GetInt32(0).Should().Be(2);
        reader.GetInt32(1).Should().Be(10);
        Convert.ToInt64(reader.GetValue(2)).Should().Be(2L);

        reader.Read().Should().BeTrue();
        reader.GetInt32(0).Should().Be(3);
        reader.GetInt32(1).Should().Be(20);
        Convert.ToInt64(reader.GetValue(2)).Should().Be(1L);

        reader.Read().Should().BeTrue();
        reader.GetInt32(0).Should().Be(4);
        reader.GetInt32(1).Should().Be(20);
        Convert.ToInt64(reader.GetValue(2)).Should().Be(2L);

        reader.Read().Should().BeFalse();
    }

    /// <summary>
    /// EN: Verifies automatic dialect mode executes the shared PIVOT subset through the shared runtime pipeline.
    /// PT-br: Verifica se o modo automatico de dialeto executa o subset compartilhado de PIVOT pelo pipeline compartilhado de runtime.
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
        reader.Read().Should().BeTrue();
        reader.GetInt32(reader.GetOrdinal("u10")).Should().Be(2);
        reader.GetInt32(reader.GetOrdinal("u20")).Should().Be(1);
        reader.Read().Should().BeFalse();
    }

    /// <summary>
    /// EN: Verifies automatic dialect mode executes shared WITH/CTE syntax through the shared runtime pipeline.
    /// PT-br: Verifica se o modo automatico de dialeto executa sintaxe compartilhada de WITH/CTE pelo pipeline compartilhado de runtime.
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
        reader.Read().Should().BeTrue();
        reader.GetString(0).Should().Be("Ana");
        reader.Read().Should().BeTrue();
        reader.GetString(0).Should().Be("Bia");
        reader.Read().Should().BeFalse();
    }

    /// <summary>
    /// EN: Verifies automatic dialect mode executes shared DML RETURNING syntax through the SQLite runtime pipeline.
    /// PT-br: Verifica se o modo automatico de dialeto executa sintaxe compartilhada de RETURNING em DML pelo pipeline de runtime do SQLite.
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
            reader.Read().Should().BeTrue();
            reader.GetInt32(0).Should().Be(701);
            reader.GetString(1).Should().Be("Returning Auto");
            reader.Read().Should().BeFalse();
        }

        command.CommandText = "UPDATE Users SET Name = 'Returning Updated' WHERE Id = 701 RETURNING Id, Name";
        using (var reader = command.ExecuteReader())
        {
            reader.Read().Should().BeTrue();
            reader.GetInt32(0).Should().Be(701);
            reader.GetString(1).Should().Be("Returning Updated");
            reader.Read().Should().BeFalse();
        }

        command.CommandText = "DELETE FROM Users WHERE Id = 701 RETURNING Id";
        using (var reader = command.ExecuteReader())
        {
            reader.Read().Should().BeTrue();
            reader.GetInt32(0).Should().Be(701);
            reader.Read().Should().BeFalse();
        }
    }

    /// <summary>
    /// EN: Verifies automatic dialect mode executes shared ORDER BY NULLS FIRST/LAST semantics through the shared runtime pipeline.
    /// PT-br: Verifica se o modo automatico de dialeto executa a semantica compartilhada de ORDER BY NULLS FIRST/LAST pelo pipeline compartilhado de runtime.
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
            reader.Read().Should().BeTrue();
            reader.GetString(0).Should().Be("Ana");
            reader.Read().Should().BeTrue();
            reader.GetString(0).Should().Be("Caio");
            reader.Read().Should().BeTrue();
            reader.GetString(0).Should().Be("Bia");
            reader.Read().Should().BeFalse();
        }

        command.CommandText = "SELECT Name FROM Users ORDER BY Email NULLS LAST, Id";
        using (var reader = command.ExecuteReader())
        {
            reader.Read().Should().BeTrue();
            reader.GetString(0).Should().Be("Bia");
            reader.Read().Should().BeTrue();
            reader.GetString(0).Should().Be("Ana");
            reader.Read().Should().BeTrue();
            reader.GetString(0).Should().Be("Caio");
            reader.Read().Should().BeFalse();
        }
    }

    /// <summary>
    /// EN: Verifies multi-statement INSERT scripts execute through ExecuteNonQuery.
    /// PT-br: Verifica se scripts INSERT com varias instrucoes executam via ExecuteNonQuery.
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

        rowsAffected.Should().Be(3);
        var users = _connection.GetTable("users");
        users.Should().HaveCount(3);
        users[0][1].Should().Be("Ana");
        users[1][1].Should().Be("Bia");
        users[2][1].Should().Be("Caio");
    }

    /// <summary>
    /// EN: Verifies UPDATE statements modify the expected SQLite rows.
    /// PT-br: Verifica se comandos UPDATE modificam as linhas esperadas no SQLite.
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
        rowsAffected.Should().Be(1);
        _connection.GetTable("users")[0][1].Should().Be("Jane Doe");
    }

    /// <summary>
    /// EN: Verifies DELETE statements remove the expected SQLite rows.
    /// PT-br: Verifica se comandos DELETE removem as linhas esperadas no SQLite.
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
        rowsAffected.Should().Be(1);
        _connection.GetTable("users").Should().BeEmpty();
    }

    /// <summary>
    /// EN: Verifies committed transactions persist SQLite changes.
    /// PT-br: Verifica se transacoes confirmadas persistem as alteracoes no SQLite.
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
        users.Should().ContainSingle();
    }

    /// <summary>
    /// EN: Verifies insert and update work inside a committed transaction.
    /// PT-br: Verifica se insert e update funcionam dentro de uma transacao confirmada.
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

        name.Should().Be("Bob");
    }

    /// <summary>
    /// EN: Verifies rolled back transactions discard SQLite changes.
    /// PT-br: Verifica se transacoes revertidas descartam as alteracoes no SQLite.
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
        users.Should().BeEmpty();
    }

    /// <summary>
    /// EN: Disposes test resources.
    /// PT-br: Descarta os recursos do teste.
    /// </summary>
    /// <param name="disposing">EN: True to dispose managed resources. PT-br: True para descartar recursos gerenciados.</param>
    protected override void Dispose(bool disposing)
    {
        _connection.Dispose();
        base.Dispose(disposing);
    }

    /// <summary>
    /// EN: Verifies SQLite rejects FOUND_ROWS because the provider exposes CHANGES for row-count inspection.
    /// PT-br: Verifica que o SQLite rejeita FOUND_ROWS porque o provider expoe CHANGES para inspecao de contagem de linhas.
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
        var act = () => command.ExecuteReader();
        var ex = act.Should().Throw<NotSupportedException>().Which;

        ex.Message.Should().Contain("FOUND_ROWS");
    }


    /// <summary>
    /// EN: Verifies CHANGES returns affected rows for the last UPDATE statement.
    /// PT-br: Verifica que CHANGES retorna as linhas afetadas pelo último UPDATE.
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

        reader.Read().Should().BeTrue();
        Convert.ToInt64(reader.GetValue(0)).Should().Be(1L);
    }


    /// <summary>
    /// EN: Verifies CHANGES returns zero immediately after beginning a transaction.
    /// PT-br: Verifica que CHANGES retorna zero imediatamente após iniciar uma transação.
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
        Convert.ToInt64(command.ExecuteScalar()).Should().Be(0L);
    }



    /// <summary>
    /// EN: Verifies a BEGIN TRANSACTION followed by CHANGES returns zero in batch execution.
    /// PT-br: Verifica que BEGIN TRANSACTION seguido de CHANGES retorna zero em execução em batch.
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

        reader.Read().Should().BeTrue();
        Convert.ToInt64(reader.GetValue(0)).Should().Be(0L);
    }

    /// <summary>
    /// EN: Verifies CALL followed by CHANGES returns zero when no DML affected rows.
    /// PT-br: Verifica que CALL seguido de CHANGES retorna zero quando nenhum DML afetou linhas.
    /// </summary>
    [Fact]
    [Trait("Category", "SqliteMock")]
    public void TestBatch_CallThenChanges_ShouldReturnZero()
    {
        _connection.AddProdecure(new ProcedureDef("sp_ping", [], [], [], null));

        using var command = new SqliteCommandMock(_connection)
        {
            CommandText = "CALL sp_ping(); SELECT CHANGES();"
        };

        using var reader = command.ExecuteReader();

        reader.Read().Should().BeTrue();
        Convert.ToInt64(reader.GetValue(0)).Should().Be(0L);
    }

    /// <summary>
    /// EN: Verifies CHANGES returns zero after COMMIT in a batch that previously updated rows.
    /// PT-br: Verifica que CHANGES retorna zero após COMMIT em um batch que atualizou linhas anteriormente.
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

        reader.Read().Should().BeTrue();
        Convert.ToInt64(reader.GetValue(0)).Should().Be(0L);
    }


    /// <summary>
    /// EN: Verifies CHANGES returns zero after rolling back to a savepoint in batch execution.
    /// PT-br: Verifica que CHANGES retorna zero após rollback para savepoint em execução em batch.
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

        reader.Read().Should().BeTrue();
        Convert.ToInt64(reader.GetValue(0)).Should().Be(0L);
    }

    /// <summary>
    /// EN: Verifies CHANGES returns zero after releasing a savepoint in batch execution.
    /// PT-br: Verifica que CHANGES retorna zero após liberar um savepoint em execução em batch.
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

        reader.Read().Should().BeTrue();
        Convert.ToInt64(reader.GetValue(0)).Should().Be(0L);
    }


    /// <summary>
    /// EN: Verifies a mixed batch reports the last DML change count.
    /// PT-br: Verifica se um batch misto reporta a ultima contagem de alteracoes de DML.
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

        reader.Read().Should().BeTrue();
        reader.NextResult().Should().BeTrue();
        reader.Read().Should().BeTrue();
        Convert.ToInt64(reader.GetValue(0)).Should().Be(1L);
    }


    /// <summary>
    /// EN: Verifies COMMIT resets the change count in a mixed batch.
    /// PT-br: Verifica se COMMIT zera a contagem de alteracoes em um batch misto.
    /// </summary>
    [Fact]
    [Trait("Category", "SqliteMock")]
    public void TestBatch_CallUpdateCommitThenChanges_ShouldReturnZeroAfterCommit()
    {
        _connection.AddProdecure(new ProcedureDef("sp_ping", [], [], [], null));

        using var command = new SqliteCommandMock(_connection)
        {
            CommandText = "CALL sp_ping(); UPDATE Users SET Name = 'Call Dml User' WHERE Id = 1; COMMIT; SELECT CHANGES();"
        };

        using var reader = command.ExecuteReader();

        reader.Read().Should().BeTrue();
        Convert.ToInt64(reader.GetValue(0)).Should().Be(0L);
    }


    /// <summary>
    /// EN: Verifies the last DML in a batch determines the reported change count.
    /// PT-br: Verifica se o ultimo DML de um batch determina a contagem de alteracoes reportada.
    /// </summary>
    [Fact]
    [Trait("Category", "SqliteMock")]
    public void TestBatch_UpdateThenSelectThenChanges_ShouldReflectLastDml()
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
        rows.Should().Be(2);

        reader.NextResult().Should().BeTrue();
        reader.Read().Should().BeTrue();
        Convert.ToInt64(reader.GetValue(0)).Should().Be(1L);
    }

    /// <summary>
    /// EN: Verifies INSERT RETURNING yields the inserted rows.
    /// PT-br: Verifica se INSERT RETURNING devolve as linhas inseridas.
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

        reader.Read().Should().BeTrue();
        reader.GetInt32(reader.GetOrdinal("Id")).Should().Be(601);
        reader.GetString(reader.GetOrdinal("user_name")).Should().Be("Returning Insert");
        reader.Read().Should().BeFalse();
    }

    /// <summary>
    /// EN: Verifies UPDATE RETURNING yields the updated projection.
    /// PT-br: Verifica se UPDATE RETURNING devolve a projeção atualizada.
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

        reader.Read().Should().BeTrue();
        reader.GetInt32(reader.GetOrdinal("Id")).Should().Be(602);
        reader.GetString(reader.GetOrdinal("Name")).Should().Be("After Update");
        reader.Read().Should().BeFalse();
    }

    /// <summary>
    /// EN: Verifies DELETE RETURNING yields the deleted row snapshot.
    /// PT-br: Verifica se DELETE RETURNING devolve o snapshot da linha removida.
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

        reader.Read().Should().BeTrue();
        reader.GetInt32(reader.GetOrdinal("Id")).Should().Be(603);
        reader.GetString(reader.GetOrdinal("Name")).Should().Be("To Delete");
        reader.Read().Should().BeFalse();
        _connection.GetTable("users").Should().NotContain(r => Convert.ToInt32(r[0]) == 603);
    }

    /// <summary>
    /// EN: Verifies INSERT SELECT RETURNING yields every inserted row.
    /// PT-br: Verifica se INSERT SELECT RETURNING devolve cada linha inserida.
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

        reader.Read().Should().BeTrue();
        reader.GetInt32(0).Should().Be(1611);
        reader.Read().Should().BeTrue();
        reader.GetInt32(0).Should().Be(1612);
        reader.Read().Should().BeFalse();
    }

    /// <summary>
    /// EN: Verifies qualified wildcard RETURNING includes all updated columns.
    /// PT-br: Verifica se RETURNING com curinga qualificado inclui todas as colunas atualizadas.
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

        reader.Read().Should().BeTrue();
        reader.GetInt32(reader.GetOrdinal("Id")).Should().Be(613);
        reader.GetString(reader.GetOrdinal("Name")).Should().Be("After");
        reader.GetString(reader.GetOrdinal("Email")).Should().Be("before613@test.local");
        reader.Read().Should().BeFalse();
    }

}
