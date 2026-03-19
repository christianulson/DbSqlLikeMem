namespace DbSqlLikeMem.MariaDb.Test;

/// <summary>
/// EN: Covers MariaDB runtime behavior built on top of the shared MySQL provider family.
/// PT: Cobre o comportamento de runtime do MariaDB construido sobre a familia compartilhada do provider MySQL.
/// </summary>
public sealed class MariaDbMockTests : XUnitTestBase
{
    /// <summary>
    /// EN: Initializes the MariaDB runtime test fixture.
    /// PT: Inicializa a fixture de testes de runtime do MariaDB.
    /// </summary>
    public MariaDbMockTests(ITestOutputHelper helper)
        : base(helper)
    {
    }

    /// <summary>
    /// EN: Ensures INSERT ... RETURNING returns the inserted projection once the MariaDB version gate is enabled.
    /// PT: Garante que INSERT ... RETURNING retorne a projecao inserida quando o gate de versao do MariaDB estiver habilitado.
    /// </summary>
    [Fact]
    [Trait("Category", "MariaDbMock")]
    public void ExecuteReader_InsertReturning_ShouldReturnInsertedProjection()
    {
        using var connection = CreateOpenConnection(MariaDbDbVersions.Version10_5);
        using var command = new MySqlCommandMock(connection)
        {
            CommandText = "INSERT INTO Users (Id, Name, Email) VALUES (901, 'Returning Insert', 'insert@maria.test') RETURNING Id, Name AS user_name"
        };

        using var reader = command.ExecuteReader();

        Assert.True(reader.Read());
        Assert.Equal(901, reader.GetInt32(reader.GetOrdinal("Id")));
        Assert.Equal("Returning Insert", reader.GetString(reader.GetOrdinal("user_name")));
        Assert.False(reader.Read());
    }

    /// <summary>
    /// EN: Ensures DELETE ... RETURNING returns the deleted row snapshot once the MariaDB version gate is enabled.
    /// PT: Garante que DELETE ... RETURNING retorne o snapshot da linha excluida quando o gate de versao do MariaDB estiver habilitado.
    /// </summary>
    [Fact]
    [Trait("Category", "MariaDbMock")]
    public void ExecuteReader_DeleteReturning_ShouldReturnDeletedRowSnapshot()
    {
        using var connection = CreateOpenConnection(MariaDbDbVersions.Version10_5);
        using (var setup = new MySqlCommandMock(connection)
        {
            CommandText = "INSERT INTO Users (Id, Name, Email) VALUES (902, 'To Delete', 'delete@maria.test')"
        })
        {
            setup.ExecuteNonQuery();
        }

        using var command = new MySqlCommandMock(connection)
        {
            CommandText = "DELETE FROM Users WHERE Id = 902 RETURNING Id, Name"
        };

        using var reader = command.ExecuteReader();

        Assert.True(reader.Read());
        Assert.Equal(902, reader.GetInt32(reader.GetOrdinal("Id")));
        Assert.Equal("To Delete", reader.GetString(reader.GetOrdinal("Name")));
        Assert.False(reader.Read());
        Assert.DoesNotContain(connection.GetTable("Users"), row => Convert.ToInt32(row[0], CultureInfo.InvariantCulture) == 902);
    }

    /// <summary>
    /// EN: Ensures REPLACE ... RETURNING replaces an existing MariaDB row and returns the inserted projection.
    /// PT: Garante que REPLACE ... RETURNING substitua uma linha existente do MariaDB e retorne a projecao inserida.
    /// </summary>
    [Fact]
    [Trait("Category", "MariaDbMock")]
    public void ExecuteReader_ReplaceReturning_ShouldReplaceExistingRow()
    {
        using var connection = CreateOpenConnection(MariaDbDbVersions.Version10_5);
        using (var setup = new MySqlCommandMock(connection)
        {
            CommandText = "INSERT INTO Users (Id, Name, Email) VALUES (904, 'Original', 'orig@maria.test')"
        })
        {
            setup.ExecuteNonQuery();
        }

        using var command = new MySqlCommandMock(connection)
        {
            CommandText = "REPLACE INTO Users (Id, Name, Email) VALUES (904, 'Replacement', 'repl@maria.test') RETURNING Id, Name"
        };

        using var reader = command.ExecuteReader();

        Assert.True(reader.Read());
        Assert.Equal(904, reader.GetInt32(reader.GetOrdinal("Id")));
        Assert.Equal("Replacement", reader.GetString(reader.GetOrdinal("Name")));
        Assert.False(reader.Read());
        Assert.Single(connection.GetTable("Users"));
        Assert.Equal("Replacement", Convert.ToString(connection.GetTable("Users")[0][1], CultureInfo.InvariantCulture));
    }

    /// <summary>
    /// EN: Ensures MariaDB executes INSERT with VALUE, LOW_PRIORITY, and PARTITION syntax through the shared runtime path.
    /// PT: Garante que o MariaDB execute INSERT com sintaxe VALUE, LOW_PRIORITY e PARTITION pelo caminho compartilhado de runtime.
    /// </summary>
    [Fact]
    [Trait("Category", "MariaDbMock")]
    public void ExecuteReader_InsertValuePartition_ShouldReturnInsertedProjection()
    {
        using var connection = CreateOpenConnection(MariaDbDbVersions.Version10_5);
        using var command = new MySqlCommandMock(connection)
        {
            CommandText = "INSERT LOW_PRIORITY INTO Users PARTITION (p0) VALUE (905, 'Partition Insert', 'partition@maria.test') RETURNING Id, Name"
        };

        using var reader = command.ExecuteReader();

        Assert.True(reader.Read());
        Assert.Equal(905, reader.GetInt32(reader.GetOrdinal("Id")));
        Assert.Equal("Partition Insert", reader.GetString(reader.GetOrdinal("Name")));
        Assert.False(reader.Read());
        Assert.Single(connection.GetTable("Users"));
    }

    /// <summary>
    /// EN: Ensures MariaDB executes INSERT ... SET through the shared runtime path and returns the inserted projection.
    /// PT: Garante que o MariaDB execute INSERT ... SET pelo caminho compartilhado de runtime e retorne a projecao inserida.
    /// </summary>
    [Fact]
    [Trait("Category", "MariaDbMock")]
    public void ExecuteReader_InsertSet_ShouldReturnInsertedProjection()
    {
        using var connection = CreateOpenConnection(MariaDbDbVersions.Version10_5);
        using var command = new MySqlCommandMock(connection)
        {
            CommandText = "INSERT INTO Users SET Id = 908, Name = 'Set Insert', Email = 'set@maria.test' RETURNING Id, Name"
        };

        using var reader = command.ExecuteReader();

        Assert.True(reader.Read());
        Assert.Equal(908, reader.GetInt32(reader.GetOrdinal("Id")));
        Assert.Equal("Set Insert", reader.GetString(reader.GetOrdinal("Name")));
        Assert.False(reader.Read());
        Assert.Single(connection.GetTable("Users"));
    }

    /// <summary>
    /// EN: Ensures MariaDB executes REPLACE with VALUE, LOW_PRIORITY, and PARTITION syntax through the shared runtime path.
    /// PT: Garante que o MariaDB execute REPLACE com sintaxe VALUE, LOW_PRIORITY e PARTITION pelo caminho compartilhado de runtime.
    /// </summary>
    [Fact]
    [Trait("Category", "MariaDbMock")]
    public void ExecuteReader_ReplaceValuePartition_ShouldReplaceExistingRow()
    {
        using var connection = CreateOpenConnection(MariaDbDbVersions.Version10_5);
        using (var setup = new MySqlCommandMock(connection)
        {
            CommandText = "INSERT INTO Users (Id, Name, Email) VALUES (906, 'Original', 'orig2@maria.test')"
        })
        {
            setup.ExecuteNonQuery();
        }

        using var command = new MySqlCommandMock(connection)
        {
            CommandText = "REPLACE LOW_PRIORITY INTO Users PARTITION (p0) VALUE (906, 'Replacement', 'repl2@maria.test') RETURNING Id, Name"
        };

        using var reader = command.ExecuteReader();

        Assert.True(reader.Read());
        Assert.Equal(906, reader.GetInt32(reader.GetOrdinal("Id")));
        Assert.Equal("Replacement", reader.GetString(reader.GetOrdinal("Name")));
        Assert.False(reader.Read());
        Assert.Single(connection.GetTable("Users"));
        Assert.Equal("Replacement", Convert.ToString(connection.GetTable("Users")[0][1], CultureInfo.InvariantCulture));
    }

    /// <summary>
    /// EN: Ensures MariaDB rejects aggregate functions inside RETURNING at execution time.
    /// PT: Garante que o MariaDB rejeite funcoes de agregacao dentro de RETURNING no tempo de execucao.
    /// </summary>
    [Fact]
    [Trait("Category", "MariaDbMock")]
    public void ExecuteReader_InsertReturning_Aggregate_ShouldThrow()
    {
        using var connection = CreateOpenConnection(MariaDbDbVersions.Version10_5);
        using var command = new MySqlCommandMock(connection)
        {
            CommandText = "INSERT INTO Users (Id, Name, Email) VALUES (907, 'Agg', 'agg@maria.test') RETURNING COUNT(*)"
        };

        var ex = Assert.Throws<InvalidOperationException>(() => command.ExecuteReader());

        Assert.Contains("aggregate", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures MariaDB still rejects RETURNING before the version gate is enabled.
    /// PT: Garante que o MariaDB ainda rejeite RETURNING antes de o gate de versao estar habilitado.
    /// </summary>
    [Fact]
    [Trait("Category", "MariaDbMock")]
    public void ExecuteReader_InsertReturning_BeforeGate_ShouldThrow()
    {
        using var connection = CreateOpenConnection(MariaDbDbVersions.Version10_3);
        using var command = new MySqlCommandMock(connection)
        {
            CommandText = "INSERT INTO Users (Id, Name, Email) VALUES (903, 'Blocked', NULL) RETURNING Id"
        };

        var ex = Assert.Throws<NotSupportedException>(() => command.ExecuteReader());

        Assert.Contains("RETURNING", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures MariaDB executes sequence DDL plus NEXT/PREVIOUS VALUE FOR through the shared runtime path.
    /// PT: Garante que o MariaDB execute DDL de sequence e NEXT/PREVIOUS VALUE FOR pelo caminho compartilhado de runtime.
    /// </summary>
    [Fact]
    [Trait("Category", "MariaDbMock")]
    public void ExecuteScalar_SequenceFamilies_ShouldWork()
    {
        using var connection = CreateOpenConnection(MariaDbDbVersions.Version10_3);
        using var command = new MySqlCommandMock(connection)
        {
            CommandText = "CREATE SEQUENCE seq_users START WITH 10 INCREMENT BY 2"
        };
        Assert.Equal(0, command.ExecuteNonQuery());

        command.CommandText = "SELECT NEXT VALUE FOR seq_users";
        Assert.Equal(10L, Convert.ToInt64(command.ExecuteScalar(), CultureInfo.InvariantCulture));

        command.CommandText = "SELECT PREVIOUS VALUE FOR seq_users";
        Assert.Equal(10L, Convert.ToInt64(command.ExecuteScalar(), CultureInfo.InvariantCulture));
    }

    /// <summary>
    /// EN: Ensures MariaDB can consume sequence expressions inside INSERT statements through the shared runtime path.
    /// PT: Garante que o MariaDB consiga consumir expressoes de sequence dentro de INSERTs pelo caminho compartilhado de runtime.
    /// </summary>
    [Fact]
    [Trait("Category", "MariaDbMock")]
    public void ExecuteNonQuery_InsertWithSequenceExpressions_ShouldWork()
    {
        using var connection = CreateOpenConnection(MariaDbDbVersions.Version10_3);
        using var command = new MySqlCommandMock(connection)
        {
            CommandText = """
                CREATE SEQUENCE seq_users START WITH 20 INCREMENT BY 5;
                INSERT INTO Users (Id, Name, Email) VALUES (NEXT VALUE FOR seq_users, 'Ana', NULL);
                INSERT INTO Users (Id, Name, Email) VALUES (NEXT VALUE FOR seq_users, 'Bia', NULL);
                """
        };

        Assert.Equal(2, command.ExecuteNonQuery());

        var users = connection.GetTable("Users");
        Assert.Equal(2, users.Count);
        Assert.Equal(20, Convert.ToInt32(users[0][0], CultureInfo.InvariantCulture));
        Assert.Equal(25, Convert.ToInt32(users[1][0], CultureInfo.InvariantCulture));
    }

    /// <summary>
    /// EN: Ensures MariaDB evaluates SOUNDS LIKE through the shared runtime path.
    /// PT: Garante que o MariaDB avalie SOUNDS LIKE pelo caminho compartilhado de runtime.
    /// </summary>
    [Fact]
    [Trait("Category", "MariaDbMock")]
    public void ExecuteScalar_SoundsLike_ShouldUseSoundexComparison()
    {
        using var connection = CreateOpenConnection(MariaDbDbVersions.Version10_5);
        using var command = new MySqlCommandMock(connection)
        {
            CommandText = "SELECT CASE WHEN 'Robert' SOUNDS LIKE 'Rupert' THEN 1 ELSE 0 END"
        };

        Assert.Equal(1, Convert.ToInt32(command.ExecuteScalar(), CultureInfo.InvariantCulture));
    }

    /// <summary>
    /// EN: Ensures MariaDB exposes _ROWID as the single-column primary key in the shared runtime.
    /// PT: Garante que o MariaDB exponha _ROWID como a chave primaria de coluna unica no runtime compartilhado.
    /// </summary>
    [Fact]
    [Trait("Category", "MariaDbMock")]
    public void ExecuteScalar_RowId_ShouldResolveToPrimaryKey()
    {
        using var connection = CreateOpenConnection(MariaDbDbVersions.Version10_5);
        using (var setup = new MySqlCommandMock(connection)
        {
            CommandText = "INSERT INTO Users (Id, Name, Email) VALUES (909, 'RowId', 'rowid@maria.test')"
        })
        {
            setup.ExecuteNonQuery();
        }

        using var command = new MySqlCommandMock(connection)
        {
            CommandText = "SELECT _ROWID FROM Users WHERE Id = 909"
        };

        Assert.Equal(909, Convert.ToInt32(command.ExecuteScalar(), CultureInfo.InvariantCulture));
    }

    /// <summary>
    /// EN: Ensures MariaDB can materialize JSON_TABLE rows with ordinality and typed PATH columns in the shared runtime.
    /// PT: Garante que o MariaDB consiga materializar linhas de JSON_TABLE com ordinality e colunas PATH tipadas no runtime compartilhado.
    /// </summary>
    [Fact]
    [Trait("Category", "MariaDbMock")]
    public void ExecuteReader_JsonTable_ShouldProjectRows()
    {
        using var connection = CreateOpenConnection(MariaDbDbVersions.Version10_6);
        using var command = new MySqlCommandMock(connection)
        {
            CommandText = """
                SELECT jt.ord, jt.Id, jt.Name
                FROM JSON_TABLE(
                    '[{"id":1,"name":"Ana"},{"id":2,"name":"Bia"}]',
                    '$[*]' COLUMNS(
                        ord FOR ORDINALITY,
                        Id INT PATH '$.id',
                        Name VARCHAR(50) PATH '$.name'
                    )
                ) jt
                ORDER BY jt.ord
                """
        };

        using var reader = command.ExecuteReader();

        Assert.True(reader.Read());
        Assert.Equal(1L, reader.GetInt64(reader.GetOrdinal("ord")));
        Assert.Equal(1, reader.GetInt32(reader.GetOrdinal("Id")));
        Assert.Equal("Ana", reader.GetString(reader.GetOrdinal("Name")));

        Assert.True(reader.Read());
        Assert.Equal(2L, reader.GetInt64(reader.GetOrdinal("ord")));
        Assert.Equal(2, reader.GetInt32(reader.GetOrdinal("Id")));
        Assert.Equal("Bia", reader.GetString(reader.GetOrdinal("Name")));
        Assert.False(reader.Read());
    }

    /// <summary>
    /// EN: Ensures MariaDB JSON_TABLE supports EXISTS PATH columns with 1/0 semantics in the shared runtime.
    /// PT: Garante que JSON_TABLE do MariaDB suporte colunas EXISTS PATH com semantica 1/0 no runtime compartilhado.
    /// </summary>
    [Fact]
    [Trait("Category", "MariaDbMock")]
    public void ExecuteReader_JsonTable_WithExistsPath_ShouldReturnFlags()
    {
        using var connection = CreateOpenConnection(MariaDbDbVersions.Version10_6);
        using var command = new MySqlCommandMock(connection)
        {
            CommandText = """
                SELECT jt.Id, jt.HasEmail
                FROM JSON_TABLE(
                    '[{"id":1,"email":"ana@test.dev"},{"id":2}]',
                    '$[*]' COLUMNS(
                        Id INT PATH '$.id',
                        HasEmail INT EXISTS PATH '$.email'
                    )
                ) jt
                ORDER BY jt.Id
                """
        };

        using var reader = command.ExecuteReader();

        Assert.True(reader.Read());
        Assert.Equal(1, reader.GetInt32(reader.GetOrdinal("Id")));
        Assert.Equal(1, reader.GetInt32(reader.GetOrdinal("HasEmail")));

        Assert.True(reader.Read());
        Assert.Equal(2, reader.GetInt32(reader.GetOrdinal("Id")));
        Assert.Equal(0, reader.GetInt32(reader.GetOrdinal("HasEmail")));
        Assert.False(reader.Read());
    }

    /// <summary>
    /// EN: Ensures MariaDB JSON_TABLE applies ON EMPTY defaults for missing PATH values in the shared runtime.
    /// PT: Garante que JSON_TABLE do MariaDB aplique defaults de ON EMPTY para valores PATH ausentes no runtime compartilhado.
    /// </summary>
    [Fact]
    [Trait("Category", "MariaDbMock")]
    public void ExecuteReader_JsonTable_WithDefaultOnEmpty_ShouldUseFallback()
    {
        using var connection = CreateOpenConnection(MariaDbDbVersions.Version10_6);
        using var command = new MySqlCommandMock(connection)
        {
            CommandText = """
                SELECT jt.Id, jt.Title
                FROM JSON_TABLE(
                    '[{"id":1,"title":"Ana"},{"id":2}]',
                    '$[*]' COLUMNS(
                        Id INT PATH '$.id',
                        Title VARCHAR(30) PATH '$.title' DEFAULT 'fallback' ON EMPTY
                    )
                ) jt
                ORDER BY jt.Id
                """
        };

        using var reader = command.ExecuteReader();

        Assert.True(reader.Read());
        Assert.Equal(1, reader.GetInt32(reader.GetOrdinal("Id")));
        Assert.Equal("Ana", reader.GetString(reader.GetOrdinal("Title")));

        Assert.True(reader.Read());
        Assert.Equal(2, reader.GetInt32(reader.GetOrdinal("Id")));
        Assert.Equal("fallback", reader.GetString(reader.GetOrdinal("Title")));
        Assert.False(reader.Read());
    }

    /// <summary>
    /// EN: Ensures MariaDB JSON_TABLE applies ON ERROR defaults when a PATH resolves to a non-scalar JSON value.
    /// PT: Garante que JSON_TABLE do MariaDB aplique defaults de ON ERROR quando um PATH resolve para um valor JSON nao escalar.
    /// </summary>
    [Fact]
    [Trait("Category", "MariaDbMock")]
    public void ExecuteReader_JsonTable_WithDefaultOnError_ShouldUseFallback()
    {
        using var connection = CreateOpenConnection(MariaDbDbVersions.Version10_6);
        using var command = new MySqlCommandMock(connection)
        {
            CommandText = """
                SELECT jt.Id, jt.TagValue
                FROM JSON_TABLE(
                    '[{"id":1,"tag":{"name":"vip"}},{"id":2,"tag":42}]',
                    '$[*]' COLUMNS(
                        Id INT PATH '$.id',
                        TagValue INT PATH '$.tag' DEFAULT '99' ON ERROR
                    )
                ) jt
                ORDER BY jt.Id
                """
        };

        using var reader = command.ExecuteReader();

        Assert.True(reader.Read());
        Assert.Equal(1, reader.GetInt32(reader.GetOrdinal("Id")));
        Assert.Equal(99, reader.GetInt32(reader.GetOrdinal("TagValue")));

        Assert.True(reader.Read());
        Assert.Equal(2, reader.GetInt32(reader.GetOrdinal("Id")));
        Assert.Equal(42, reader.GetInt32(reader.GetOrdinal("TagValue")));
        Assert.False(reader.Read());
    }

    /// <summary>
    /// EN: Ensures MariaDB JSON_TABLE raises an error when ON EMPTY is configured to fail on a missing PATH.
    /// PT: Garante que JSON_TABLE do MariaDB lance erro quando ON EMPTY estiver configurado para falhar em PATH ausente.
    /// </summary>
    [Fact]
    [Trait("Category", "MariaDbMock")]
    public void ExecuteReader_JsonTable_WithErrorOnEmpty_ShouldThrow()
    {
        using var connection = CreateOpenConnection(MariaDbDbVersions.Version10_6);
        using var command = new MySqlCommandMock(connection)
        {
            CommandText = """
                SELECT jt.Id, jt.Title
                FROM JSON_TABLE(
                    '[{"id":1,"title":"Ana"},{"id":2}]',
                    '$[*]' COLUMNS(
                        Id INT PATH '$.id',
                        Title VARCHAR(30) PATH '$.title' ERROR ON EMPTY
                    )
                ) jt
                ORDER BY jt.Id
                """
        };

        var ex = Assert.Throws<InvalidOperationException>(() => command.ExecuteReader());

        Assert.Contains("Title", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures MariaDB JSON_TABLE can expand NESTED PATH rows while preserving parent columns in the shared runtime.
    /// PT: Garante que JSON_TABLE do MariaDB consiga expandir linhas de NESTED PATH preservando as colunas pai no runtime compartilhado.
    /// </summary>
    [Fact]
    [Trait("Category", "MariaDbMock")]
    public void ExecuteReader_JsonTable_WithNestedPath_ShouldProjectNestedRows()
    {
        using var connection = CreateOpenConnection(MariaDbDbVersions.Version10_6);
        using var command = new MySqlCommandMock(connection)
        {
            CommandText = """
                SELECT jt.Id, jt.TagOrd, jt.TagName
                FROM JSON_TABLE(
                    '[{"id":1,"tags":[{"name":"vip"},{"name":"new"}]},{"id":2,"tags":[{"name":"beta"}]}]',
                    '$[*]' COLUMNS(
                        Id INT PATH '$.id',
                        NESTED PATH '$.tags[*]' COLUMNS(
                            TagOrd FOR ORDINALITY,
                            TagName VARCHAR(30) PATH '$.name'
                        )
                    )
                ) jt
                ORDER BY jt.Id, jt.TagOrd
                """
        };

        using var reader = command.ExecuteReader();

        Assert.True(reader.Read());
        Assert.Equal(1, reader.GetInt32(reader.GetOrdinal("Id")));
        Assert.Equal(1L, reader.GetInt64(reader.GetOrdinal("TagOrd")));
        Assert.Equal("vip", reader.GetString(reader.GetOrdinal("TagName")));

        Assert.True(reader.Read());
        Assert.Equal(1, reader.GetInt32(reader.GetOrdinal("Id")));
        Assert.Equal(2L, reader.GetInt64(reader.GetOrdinal("TagOrd")));
        Assert.Equal("new", reader.GetString(reader.GetOrdinal("TagName")));

        Assert.True(reader.Read());
        Assert.Equal(2, reader.GetInt32(reader.GetOrdinal("Id")));
        Assert.Equal(1L, reader.GetInt64(reader.GetOrdinal("TagOrd")));
        Assert.Equal("beta", reader.GetString(reader.GetOrdinal("TagName")));
        Assert.False(reader.Read());
    }

    /// <summary>
    /// EN: Ensures MariaDB JSON_TABLE returns null-complemented rows when a nested path has no matches.
    /// PT: Garante que JSON_TABLE do MariaDB retorne linhas com complemento nulo quando um nested path nao encontra correspondencias.
    /// </summary>
    [Fact]
    [Trait("Category", "MariaDbMock")]
    public void ExecuteReader_JsonTable_WithMissingNestedPath_ShouldReturnNullComplementedRows()
    {
        using var connection = CreateOpenConnection(MariaDbDbVersions.Version10_6);
        using var command = new MySqlCommandMock(connection)
        {
            CommandText = """
                SELECT jt.Id, jt.TagName
                FROM JSON_TABLE(
                    '[{"id":1,"tags":[{"name":"vip"}]},{"id":2}]',
                    '$[*]' COLUMNS(
                        Id INT PATH '$.id',
                        NESTED PATH '$.tags[*]' COLUMNS(
                            TagName VARCHAR(30) PATH '$.name'
                        )
                    )
                ) jt
                ORDER BY jt.Id
                """
        };

        using var reader = command.ExecuteReader();

        Assert.True(reader.Read());
        Assert.Equal(1, reader.GetInt32(reader.GetOrdinal("Id")));
        Assert.Equal("vip", reader.GetString(reader.GetOrdinal("TagName")));

        Assert.True(reader.Read());
        Assert.Equal(2, reader.GetInt32(reader.GetOrdinal("Id")));
        Assert.True(reader.IsDBNull(reader.GetOrdinal("TagName")));
        Assert.False(reader.Read());
    }

    /// <summary>
    /// EN: Ensures MariaDB JSON_TABLE keeps sibling nested paths independent and emits nulls for the missing sibling side.
    /// PT: Garante que JSON_TABLE do MariaDB mantenha nested paths irmaos independentes e emita nulos no lado irmao ausente.
    /// </summary>
    [Fact]
    [Trait("Category", "MariaDbMock")]
    public void ExecuteReader_JsonTable_WithSiblingNestedPaths_ShouldProjectIndependentRows()
    {
        using var connection = CreateOpenConnection(MariaDbDbVersions.Version10_6);
        using var command = new MySqlCommandMock(connection)
        {
            CommandText = """
                SELECT jt.Size, jt.Color
                FROM JSON_TABLE(
                    '{
                        "sizes":[{"size":"small"},{"size":"medium"}],
                        "colors":[{"color":"red"},{"color":"blue"}]
                    }',
                    '$' COLUMNS(
                        NESTED PATH '$.sizes[*]' COLUMNS(
                            Size VARCHAR(20) PATH '$.size'
                        ),
                        NESTED PATH '$.colors[*]' COLUMNS(
                            Color VARCHAR(20) PATH '$.color'
                        )
                    )
                ) jt
                """
        };

        using var reader = command.ExecuteReader();

        Assert.True(reader.Read());
        Assert.Equal("small", reader.GetString(reader.GetOrdinal("Size")));
        Assert.True(reader.IsDBNull(reader.GetOrdinal("Color")));

        Assert.True(reader.Read());
        Assert.Equal("medium", reader.GetString(reader.GetOrdinal("Size")));
        Assert.True(reader.IsDBNull(reader.GetOrdinal("Color")));

        Assert.True(reader.Read());
        Assert.True(reader.IsDBNull(reader.GetOrdinal("Size")));
        Assert.Equal("red", reader.GetString(reader.GetOrdinal("Color")));

        Assert.True(reader.Read());
        Assert.True(reader.IsDBNull(reader.GetOrdinal("Size")));
        Assert.Equal("blue", reader.GetString(reader.GetOrdinal("Color")));
        Assert.False(reader.Read());
    }

    /// <summary>
    /// EN: Ensures MariaDB JSON_TABLE evaluates EXISTS PATH columns inside nested branches with 1/0 semantics.
    /// PT: Garante que JSON_TABLE do MariaDB avalie colunas EXISTS PATH dentro de ramos nested com semantica 1/0.
    /// </summary>
    [Fact]
    [Trait("Category", "MariaDbMock")]
    public void ExecuteReader_JsonTable_WithNestedExistsPath_ShouldReturnFlags()
    {
        using var connection = CreateOpenConnection(MariaDbDbVersions.Version10_6);
        using var command = new MySqlCommandMock(connection)
        {
            CommandText = """
                SELECT jt.Id, jt.HasTag
                FROM JSON_TABLE(
                    '[{"id":1,"tags":[{"name":"vip"}]},{"id":2,"tags":[{}]}]',
                    '$[*]' COLUMNS(
                        Id INT PATH '$.id',
                        NESTED PATH '$.tags[*]' COLUMNS(
                            HasTag INT EXISTS PATH '$.name'
                        )
                    )
                ) jt
                ORDER BY jt.Id
                """
        };

        using var reader = command.ExecuteReader();

        Assert.True(reader.Read());
        Assert.Equal(1, reader.GetInt32(reader.GetOrdinal("Id")));
        Assert.Equal(1, reader.GetInt32(reader.GetOrdinal("HasTag")));

        Assert.True(reader.Read());
        Assert.Equal(2, reader.GetInt32(reader.GetOrdinal("Id")));
        Assert.Equal(0, reader.GetInt32(reader.GetOrdinal("HasTag")));
        Assert.False(reader.Read());
    }

    private static MariaDbConnectionMock CreateOpenConnection(int version)
    {
        var db = new MariaDbDbMock(version);
        db.AddTable("Users", [
            new("Id", DbType.Int32, false),
            new("Name", DbType.String, false),
            new("Email", DbType.String, true)
        ]);

        var connection = new MariaDbConnectionMock(db);
        connection.Open();
        return connection;
    }
}
