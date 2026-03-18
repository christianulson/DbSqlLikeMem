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
