using FluentAssertions;

namespace DbSqlLikeMem.MySql.Test.Strategy;

/// <summary>
/// EN: Covers UPDATE execution scenarios in the MySql mock.
/// PT: Cobre cenarios de execucao de UPDATE no mock MySql.
/// </summary>
public sealed class MySqlUpdateStrategyTests(
    ITestOutputHelper helper
) : XUnitTestBase(helper)
{
    /// <summary>
    /// EN: Verifies that UPDATE modifies an existing row.
    /// PT: Verifica se UPDATE modifica uma linha existente.
    /// </summary>
    [Fact]
    [Trait("Category", "Strategy")]
    public void UpdateTableShouldModifyExistingRow()
    {
        // Arrange
        var db = new MySqlDbMock();
        var table = NewUsersTable_Min(db);
        table.Add(new Dictionary<int, object?> { { 0, 1 }, { 1, "John Doe" } });

        using var connection = NewConn(threadSafe: false, db);
        using var command = new MySqlCommandMock(connection)
        {
            CommandText = "UPDATE users SET name = 'Jane Doe' WHERE id = 1"
        };

        // Act
        var rowsAffected = command.ExecuteNonQuery();

        // Assert
        rowsAffected.Should().Be(1);
        table.Should().ContainSingle();
        table[0][1].Should().Be("Jane Doe");
        connection.Metrics.Updates.Should().Be(1);
    }

    /// <summary>
    /// EN: Verifies that UPDATE returns zero when no rows match the predicate.
    /// PT: Verifica se UPDATE retorna zero quando nenhuma linha atende ao predicado.
    /// </summary>
    [Fact]
    [Trait("Category", "Strategy")]
    public void Update_ShouldReturnZero_WhenNoRowsMatchWhere()
    {
        var db = new MySqlDbMock();
        var table = NewUsersTable_Min(db);
        table.Add(new Dictionary<int, object?> { { 0, 1 }, { 1, "John Doe" } });

        using var connection = NewConn(threadSafe: false, db);
        using var command = new MySqlCommandMock(connection)
        {
            CommandText = "UPDATE users SET name = 'X' WHERE id = 999"
        };

        var rowsAffected = command.ExecuteNonQuery();

        rowsAffected.Should().Be(0);
        table.Should().ContainSingle();
        table[0][1].Should().Be("John Doe");
        connection.Metrics.Updates.Should().Be(0);
    }

    /// <summary>
    /// EN: Verifies that UPDATE changes every row matched by the predicate.
    /// PT: Verifica se UPDATE altera todas as linhas batidas pelo predicado.
    /// </summary>
    [Fact]
    [Trait("Category", "Strategy")]
    public void Update_ShouldUpdateMultipleRows_WhenWhereMatchesMultiple()
    {
        var db = new MySqlDbMock();
        var table = NewUsersTable_Min(db);
        table.Add(new Dictionary<int, object?> { { 0, 1 }, { 1, "A" } });
        table.Add(new Dictionary<int, object?> { { 0, 1 }, { 1, "B" } });
        table.Add(new Dictionary<int, object?> { { 0, 2 }, { 1, "C" } });

        using var connection = NewConn(threadSafe: false, db);
        using var command = new MySqlCommandMock(connection)
        {
            CommandText = "UPDATE users SET name = 'Z' WHERE id = 1"
        };

        var rowsAffected = command.ExecuteNonQuery();

        rowsAffected.Should().Be(2);
        table[0][1].Should().Be("Z");
        table[1][1].Should().Be("Z");
        table[2][1].Should().Be("C");
        connection.Metrics.Updates.Should().Be(2);
    }

    /// <summary>
    /// EN: Verifies that UPDATE handles AND in the WHERE clause case-insensitively.
    /// PT: Verifica se UPDATE trata AND no WHERE de forma case-insensitive.
    /// </summary>
    [Fact]
    [Trait("Category", "Strategy")]
    public void Update_ShouldHandleWhereWithAnd_CaseInsensitive()
    {
        var db = new MySqlDbMock();
        var table = NewUsersTable_WithEmail(db);
        table.Add(new Dictionary<int, object?> { { 0, 1 }, { 1, "John" }, { 2, "a@a.com" } });
        table.Add(new Dictionary<int, object?> { { 0, 0 }, { 1, "John" }, { 2, "b@a.com" } });
        table.Add(new Dictionary<int, object?> { { 0, 1 }, { 1, "Bob" }, { 2, "c@a.com" } });

        using var connection = NewConn(threadSafe: false, db);
        using var command = new MySqlCommandMock(connection)
        {
            CommandText = "UPDATE users SET email = 'ok@ok.com' WHERE id = 1 aNd name = 'John'"
        };

        var rowsAffected = command.ExecuteNonQuery();

        rowsAffected.Should().Be(1);
        table[0][2].Should().Be("ok@ok.com");
        table[1][2].Should().Be("b@a.com");
        table[2][2].Should().Be("c@a.com");
        connection.Metrics.Updates.Should().Be(1);
    }

    /// <summary>
    /// EN: Verifies that UPDATE applies multiple SET assignments.
    /// PT: Verifica se UPDATE aplica multiplas atribuicoes SET.
    /// </summary>
    [Fact]
    [Trait("Category", "Strategy")]
    public void Update_ShouldUpdateMultipleSetPairs()
    {
        var db = new MySqlDbMock();
        var table = NewUsersTable_WithEmail(db);
        table.Add(new Dictionary<int, object?> { { 0, 1 }, { 1, "John" }, { 2, "a@a.com" } });

        using var connection = NewConn(threadSafe: false, db);
        using var command = new MySqlCommandMock(connection)
        {
            CommandText = "UPDATE users SET name = 'X', email = 'x@x.com' WHERE id = 1"
        };

        var rowsAffected = command.ExecuteNonQuery();

        rowsAffected.Should().Be(1);
        table[0][1].Should().Be("X");
        table[0][2].Should().Be("x@x.com");
        connection.Metrics.Updates.Should().Be(1);
    }

    /// <summary>
    /// EN: Verifies that UPDATE keywords are parsed case-insensitively.
    /// PT: Verifica se as palavras-chave de UPDATE sao parsed de forma case-insensitive.
    /// </summary>
    [Fact]
    [Trait("Category", "Strategy")]
    public void Update_ShouldBeCaseInsensitive_ForUpdateSetWhereKeywords()
    {
        var db = new MySqlDbMock();
        var table = NewUsersTable_Min(db);
        table.Add(new Dictionary<int, object?> { { 0, 1 }, { 1, "John" } });

        using var connection = NewConn(threadSafe: false, db);
        using var command = new MySqlCommandMock(connection)
        {
            CommandText = "uPdAtE users sEt name = 'Z' wHeRe id = 1"
        };

        var rowsAffected = command.ExecuteNonQuery();

        rowsAffected.Should().Be(1);
        table[0][1].Should().Be("Z");
        connection.Metrics.Updates.Should().Be(1);
    }

    /// <summary>
    /// EN: Verifies that UPDATE works with both thread-safe modes.
    /// PT: Verifica se UPDATE funciona com ambos os modos thread-safe.
    /// </summary>
    [Theory]
    [Trait("Category", "Strategy")]
    [InlineData(false)]
    [InlineData(true)]
    public void Update_ShouldWork_WithThreadSafeTrueOrFalse(bool threadSafe)
    {
        var db = new MySqlDbMock();
        var table = NewUsersTable_Min(db);
        table.Add(new Dictionary<int, object?> { { 0, 1 }, { 1, "John" } });

        using var connection = NewConn(threadSafe, db);
        using var command = new MySqlCommandMock(connection)
        {
            CommandText = "UPDATE users SET name = 'Z' WHERE id = 1"
        };

        var rowsAffected = command.ExecuteNonQuery();

        rowsAffected.Should().Be(1);
        table[0][1].Should().Be("Z");
        connection.Metrics.Updates.Should().Be(1);
    }

    /// <summary>
    /// EN: Verifies that UPDATE fails when the target table does not exist.
    /// PT: Verifica se UPDATE falha quando a tabela alvo nao existe.
    /// </summary>
    [Fact]
    [Trait("Category", "Strategy")]
    public void Update_ShouldThrow_WhenTableDoesNotExist()
    {
        var db = new MySqlDbMock();
        using var connection = NewConn(threadSafe: false, db);
        using var command = new MySqlCommandMock(connection)
        {
            CommandText = "UPDATE users SET name = 'Z' WHERE id = 1"
        };

        Action act = () => command.ExecuteNonQuery();
        act.Should().Throw<Exception>()
            .Which.Message.Should().Contain("does not exist");
    }

    /// <summary>
    /// EN: Verifies that invalid UPDATE SQL without the UPDATE token fails.
    /// PT: Verifica se SQL de UPDATE invalido sem o token UPDATE falha.
    /// </summary>
    [Fact]
    [Trait("Category", "Strategy")]
    public void Update_ShouldThrow_WhenSqlIsInvalid_NoUpdateToken()
    {
        var db = new MySqlDbMock();
        var table = NewUsersTable_Min(db);
        table.Add(new Dictionary<int, object?> { { 0, 1 }, { 1, "John" } });

        using var connection = NewConn(threadSafe: false, db);
        using var command = new MySqlCommandMock(connection)
        {
            CommandText = "UPD users SET name = 'Z' WHERE id = 1"
        };

        Action act = () => command.ExecuteNonQuery();
        act.Should().Throw<Exception>()
            .Which.Message.Should().Contain("UPD");
    }

    /// <summary>
    /// EN: Verifies that generated columns are not changed when the generated value is not null.
    /// PT: Verifica se colunas geradas nao mudam quando o valor gerado nao e nulo.
    /// </summary>
    [Fact]
    [Trait("Category", "Strategy")]
    public void Update_ShouldNotChangeGeneratedColumn_WhenGetGenValueIsNotNull()
    {
        var db = new MySqlDbMock();
        var table = NewGenTable(db);
        table.Add(new Dictionary<int, object?> { { 0, 1 }, { 1, 10 }, { 2, 999 } }); // gen já tem algo

        using var connection = NewConn(threadSafe: false, db);
        using var command = new MySqlCommandMock(connection)
        {
            CommandText = "UPDATE gen SET gen = 123, base = 20 WHERE id = 1"
        };

        var rowsAffected = command.ExecuteNonQuery();

        rowsAffected.Should().Be(1);
        table[0][1].Should().Be(20);    // base atualiza
        table[0][2].Should().Be(999);   // gen ignora (GetGenValue != null)
        connection.Metrics.Updates.Should().Be(1);
    }

    /// <summary>
    /// EN: Verifies that UPDATE supports parameters when the SQL value helper does.
    /// PT: Verifica se UPDATE suporta parametros quando o helper de valores SQL suporta.
    /// </summary>
    [Fact]
    [Trait("Category", "Strategy")]
    public void Update_ShouldSupportParameter_IfSqlValueHelperSupports()
    {
        var db = new MySqlDbMock();
        var table = NewUsersTable_Min(db);
        table.Add(new Dictionary<int, object?> { { 0, 1 }, { 1, "John" } });
        table.Add(new Dictionary<int, object?> { { 0, 2 }, { 1, "Mary" } });

        using var connection = NewConn(threadSafe: false, db);
        using var command = new MySqlCommandMock(connection)
        {
            CommandText = "UPDATE users SET name = 'Z' WHERE id = @id"
        };

        // depende de como seu MySqlCommandMock implementa Parameters.
        // Se Parameters for IDataParameterCollection, isso deve funcionar:
        command.Parameters.Add(new MySqlParameter("@id", 2));

        var rowsAffected = command.ExecuteNonQuery();

        rowsAffected.Should().Be(1);
        table[0][1].Should().Be("John");
        table[1][1].Should().Be("Z");
        connection.Metrics.Updates.Should().Be(1);
    }

    // ============================================================
    // Opcional: testar ValidateUnique (preciso do IndexDef real)
    // ============================================================
    //
    /// <summary>
    /// EN: Verifies that UPDATE fails when a unique index collision occurs.
    /// PT: Verifica se UPDATE falha quando ocorre colisao em indice unico.
    /// </summary>
    [Fact]
    [Trait("Category", "Strategy")]
    public void Update_ShouldThrowDuplicateKey_WhenUniqueIndexCollides()
    {
        var db = new MySqlDbMock();
        var table = NewUsersTable_WithEmail(db);

        table.CreateIndex("Teste", ["name", "email"], unique: true);


        table.Add(new Dictionary<int, object?> { { 0, 1 }, { 1, "John" }, { 2, "a@a.com" } });
        table.Add(new Dictionary<int, object?> { { 0, 2 }, { 1, "Mary" }, { 2, "b@a.com" } });

        using var connection = NewConn(threadSafe: false, db);
        using var command = new MySqlCommandMock(connection)
        {
            CommandText = "UPDATE users SET email = 'a@a.com', name = 'John' WHERE id = 2"
        };

        Action act = () => command.ExecuteNonQuery();
        act.Should().Throw<MySqlMockException>()
            .Which.Message.Should().Contain(SqlExceptionMessages.DuplicateKey(string.Empty, string.Empty).Split('\'')[0].Trim());
    }


    /// <summary>
    /// EN: Recomputes persisted generated columns during update and preserves unique index consistency.
    /// PT: Recalcula colunas geradas persistidas durante update e preserva a consistência de índices únicos.
    /// </summary>
    [Fact]
    [Trait("Category", "Strategy")]
    public void Update_ShouldRecomputePersistedGeneratedColumn_AndAllowUniqueIndex()
    {
        var db = new MySqlDbMock();
        var table = db.AddTable("gen_persisted");

        table.AddColumn("id", DbType.Int32, false);
        table.AddColumn("base", DbType.Int32, false);
        var c = table.AddColumn("gen", DbType.Int32, false);
        c.GetGenValue = (row, _) => ((int?)row[1] ?? 0) * 2;
        c.PersistComputedValue = true;

        table.CreateIndex("ux_gen", ["gen"], unique: true);
        table.Add(new Dictionary<int, object?> { { 0, 1 }, { 1, 10 } });

        table[0][2].Should().Be(20);

        using var connection = NewConn(threadSafe: false, db);
        using var command = new MySqlCommandMock(connection)
        {
            CommandText = "UPDATE gen_persisted SET base = 15 WHERE id = 1"
        };

        var rowsAffected = command.ExecuteNonQuery();

        rowsAffected.Should().Be(1);
        table[0][1].Should().Be(15);
        table[0][2].Should().Be(30);

        Action addDuplicate = () => table.Add(new Dictionary<int, object?> { { 0, 2 }, { 1, 15 } });
        addDuplicate.Should().Throw<MySqlMockException>()
            .Which.Message.Should().Contain(SqlExceptionMessages.DuplicateKey(string.Empty, string.Empty).Split('\'')[0].Trim());
    }

    // ---------------- helpers ----------------

    private static MySqlConnectionMock NewConn(bool threadSafe, MySqlDbMock db)
    {
        db.ThreadSafe = threadSafe;
        return new MySqlConnectionMock(db);
    }

    private static ITableMock NewUsersTable_Min(MySqlDbMock db)
    {
        var table = db.AddTable("users");
        table.AddColumn("id", DbType.Int32, false);
        table.AddColumn("name", DbType.String, false);
        return table;
    }

    private static ITableMock NewUsersTable_WithEmail(MySqlDbMock db)
    {
        var table = db.AddTable("users");
        table.AddColumn("id", DbType.Int32, false);
        table.AddColumn("name", DbType.String, false);
        table.AddColumn("email", DbType.String, false);
        return table;
    }

    private static ITableMock NewGenTable(MySqlDbMock db)
    {
        var table = db.AddTable("gen");
        table.AddColumn("id", DbType.Int32, false);
        table.AddColumn("base", DbType.Int32, false);

        table.AddColumn("gen", DbType.Int32, false)
            .GetGenValue = (row, t) => ((int?)row[1] ?? 0) * 2;

        return table;
    }
}
