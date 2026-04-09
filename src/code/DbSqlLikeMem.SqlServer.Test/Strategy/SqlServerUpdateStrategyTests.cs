namespace DbSqlLikeMem.SqlServer.Test.Strategy;

/// <summary>
/// EN: Covers UPDATE statements in the SqlServer mock.
/// PT: Cobre instrucoes UPDATE no mock SqlServer.
/// </summary>
public sealed class SqlServerUpdateStrategyTests(
    ITestOutputHelper helper
) : XUnitTestBase(helper)
{
    /// <summary>
    /// EN: Verifies UPDATE modifies an existing row.
    /// PT: Verifica se UPDATE modifica uma linha existente.
    /// </summary>
    [Fact]
    [Trait("Category", "Strategy")]
    public void UpdateTableShouldModifyExistingRow()
    {
        // Arrange
        var db = new SqlServerDbMock();
        var table = NewUsersTable_Min(db);
        table.Add(new Dictionary<int, object?> { { 0, 1 }, { 1, "John Doe" } });

        using var connection = NewConn(threadSafe: false, db);
        using var command = new SqlServerCommandMock(connection)
        {
            CommandText = "UPDATE users SET name = 'Jane Doe' WHERE id = 1"
        };

        // Act
        var rowsAffected = command.ExecuteNonQuery();

        // Assert
        Assert.Equal(1, rowsAffected);
        Assert.Single(table);
        Assert.Equal("Jane Doe", table[0][1]);
        Assert.Equal(1, connection.Metrics.Updates);
    }

    /// <summary>
    /// EN: Verifies UPDATE returns zero when no rows match the WHERE clause.
    /// PT: Verifica se UPDATE retorna zero quando nenhuma linha corresponde ao WHERE.
    /// </summary>
    [Fact]
    [Trait("Category", "Strategy")]
    public void Update_ShouldReturnZero_WhenNoRowsMatchWhere()
    {
        var db = new SqlServerDbMock();
        var table = NewUsersTable_Min(db);
        table.Add(new Dictionary<int, object?> { { 0, 1 }, { 1, "John Doe" } });

        using var connection = NewConn(threadSafe: false, db);
        using var command = new SqlServerCommandMock(connection)
        {
            CommandText = "UPDATE users SET name = 'X' WHERE id = 999"
        };

        var rowsAffected = command.ExecuteNonQuery();

        Assert.Equal(0, rowsAffected);
        Assert.Single(table);
        Assert.Equal("John Doe", table[0][1]);
        Assert.Equal(0, connection.Metrics.Updates);
    }

    /// <summary>
    /// EN: Verifies UPDATE affects every matching row.
    /// PT: Verifica se UPDATE afeta todas as linhas correspondentes.
    /// </summary>
    [Fact]
    [Trait("Category", "Strategy")]
    public void Update_ShouldUpdateMultipleRows_WhenWhereMatchesMultiple()
    {
        var db = new SqlServerDbMock();
        var table = NewUsersTable_Min(db);
        table.Add(new Dictionary<int, object?> { { 0, 1 }, { 1, "A" } });
        table.Add(new Dictionary<int, object?> { { 0, 1 }, { 1, "B" } });
        table.Add(new Dictionary<int, object?> { { 0, 2 }, { 1, "C" } });

        using var connection = NewConn(threadSafe: false, db);
        using var command = new SqlServerCommandMock(connection)
        {
            CommandText = "UPDATE users SET name = 'Z' WHERE id = 1"
        };

        var rowsAffected = command.ExecuteNonQuery();

        Assert.Equal(2, rowsAffected);
        Assert.Equal("Z", table[0][1]);
        Assert.Equal("Z", table[1][1]);
        Assert.Equal("C", table[2][1]);
        Assert.Equal(2, connection.Metrics.Updates);
    }

    /// <summary>
    /// EN: Verifies UPDATE matches case-insensitive AND conditions.
    /// PT: Verifica se UPDATE combina condicoes AND sem diferenciar maiusculas e minusculas.
    /// </summary>
    [Fact]
    [Trait("Category", "Strategy")]
    public void Update_ShouldHandleWhereWithAnd_CaseInsensitive()
    {
        var db = new SqlServerDbMock();
        var table = NewUsersTable_WithEmail(db);
        table.Add(new Dictionary<int, object?> { { 0, 1 }, { 1, "John" }, { 2, "a@a.com" } });
        table.Add(new Dictionary<int, object?> { { 0, 0 }, { 1, "John" }, { 2, "b@a.com" } });
        table.Add(new Dictionary<int, object?> { { 0, 1 }, { 1, "Bob" }, { 2, "c@a.com" } });

        using var connection = NewConn(threadSafe: false, db);
        using var command = new SqlServerCommandMock(connection)
        {
            CommandText = "UPDATE users SET email = 'ok@ok.com' WHERE id = 1 aNd name = 'John'"
        };

        var rowsAffected = command.ExecuteNonQuery();

        Assert.Equal(1, rowsAffected);
        Assert.Equal("ok@ok.com", table[0][2]);
        Assert.Equal("b@a.com", table[1][2]);
        Assert.Equal("c@a.com", table[2][2]);
        Assert.Equal(1, connection.Metrics.Updates);
    }

    /// <summary>
    /// EN: Verifies UPDATE applies multiple SET assignments.
    /// PT: Verifica se UPDATE aplica multiplas atribuicoes SET.
    /// </summary>
    [Fact]
    [Trait("Category", "Strategy")]
    public void Update_ShouldUpdateMultipleSetPairs()
    {
        var db = new SqlServerDbMock();
        var table = NewUsersTable_WithEmail(db);
        table.Add(new Dictionary<int, object?> { { 0, 1 }, { 1, "John" }, { 2, "a@a.com" } });

        using var connection = NewConn(threadSafe: false, db);
        using var command = new SqlServerCommandMock(connection)
        {
            CommandText = "UPDATE users SET name = 'X', email = 'x@x.com' WHERE id = 1"
        };

        var rowsAffected = command.ExecuteNonQuery();

        Assert.Equal(1, rowsAffected);
        Assert.Equal("X", table[0][1]);
        Assert.Equal("x@x.com", table[0][2]);
        Assert.Equal(1, connection.Metrics.Updates);
    }

    /// <summary>
    /// EN: Verifies UPDATE parsing is case-insensitive for UPDATE, SET, and WHERE keywords.
    /// PT: Verifica se o parsing de UPDATE nao diferencia maiusculas e minusculas para as palavras UPDATE, SET e WHERE.
    /// </summary>
    [Fact]
    [Trait("Category", "Strategy")]
    public void Update_ShouldBeCaseInsensitive_ForUpdateSetWhereKeywords()
    {
        var db = new SqlServerDbMock();
        var table = NewUsersTable_Min(db);
        table.Add(new Dictionary<int, object?> { { 0, 1 }, { 1, "John" } });

        using var connection = NewConn(threadSafe: false, db);
        using var command = new SqlServerCommandMock(connection)
        {
            CommandText = "uPdAtE users sEt name = 'Z' wHeRe id = 1"
        };

        var rowsAffected = command.ExecuteNonQuery();

        Assert.Equal(1, rowsAffected);
        Assert.Equal("Z", table[0][1]);
        Assert.Equal(1, connection.Metrics.Updates);
    }

    /// <summary>
    /// EN: Verifies UPDATE works in thread-safe and non-thread-safe mode.
    /// PT: Verifica se UPDATE funciona em modo thread-safe e nao thread-safe.
    /// </summary>
    [Theory]
    [Trait("Category", "Strategy")]
    [InlineData(false)]
    [InlineData(true)]
    public void Update_ShouldWork_WithThreadSafeTrueOrFalse(bool threadSafe)
    {
        var db = new SqlServerDbMock();
        var table = NewUsersTable_Min(db);
        table.Add(new Dictionary<int, object?> { { 0, 1 }, { 1, "John" } });

        using var connection = NewConn(threadSafe, db);
        using var command = new SqlServerCommandMock(connection)
        {
            CommandText = "UPDATE users SET name = 'Z' WHERE id = 1"
        };

        var rowsAffected = command.ExecuteNonQuery();

        Assert.Equal(1, rowsAffected);
        Assert.Equal("Z", table[0][1]);
        Assert.Equal(1, connection.Metrics.Updates);
    }

    /// <summary>
    /// EN: Verifies UPDATE throws when the table does not exist.
    /// PT: Verifica se UPDATE dispara erro quando a tabela nao existe.
    /// </summary>
    [Fact]
    [Trait("Category", "Strategy")]
    public void Update_ShouldThrow_WhenTableDoesNotExist()
    {
        var db = new SqlServerDbMock();
        using var connection = NewConn(threadSafe: false, db);
        using var command = new SqlServerCommandMock(connection)
        {
            CommandText = "UPDATE users SET name = 'Z' WHERE id = 1"
        };

        var ex = Assert.ThrowsAny<Exception>(() => command.ExecuteNonQuery());
        Assert.Contains("does not exist", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Verifies UPDATE throws when the statement does not contain UPDATE.
    /// PT: Verifica se UPDATE dispara erro quando a instrucoes nao contem UPDATE.
    /// </summary>
    [Fact]
    [Trait("Category", "Strategy")]
    public void Update_ShouldThrow_WhenSqlIsInvalid_NoUpdateToken()
    {
        var db = new SqlServerDbMock();
        var table = NewUsersTable_Min(db);
        table.Add(new Dictionary<int, object?> { { 0, 1 }, { 1, "John" } });

        using var connection = NewConn(threadSafe: false, db);
        using var command = new SqlServerCommandMock(connection)
        {
            CommandText = "UPD users SET name = 'Z' WHERE id = 1"
        };

        var ex = Assert.ThrowsAny<Exception>(() => command.ExecuteNonQuery());
        Assert.Contains("upd", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Verifies UPDATE preserves non-overridable generated columns.
    /// PT: Verifica se UPDATE preserva colunas geradas que nao podem ser sobrescritas.
    /// </summary>
    [Fact]
    [Trait("Category", "Strategy")]
    public void Update_ShouldNotChangeGeneratedColumn_WhenGetGenValueIsNotNull()
    {
        var db = new SqlServerDbMock();
        var table = NewGenTable(db);
        table.Add(new Dictionary<int, object?> { { 0, 1 }, { 1, 10 }, { 2, 999 } }); // gen já tem algo

        using var connection = NewConn(threadSafe: false, db);
        using var command = new SqlServerCommandMock(connection)
        {
            CommandText = "UPDATE gen SET gen = 123, base = 20 WHERE id = 1"
        };

        var rowsAffected = command.ExecuteNonQuery();

        Assert.Equal(1, rowsAffected);
        Assert.Equal(20, table[0][1]);    // base atualiza
        Assert.Equal(999, table[0][2]);   // gen ignora (GetGenValue != null)
        Assert.Equal(1, connection.Metrics.Updates);
    }

    /// <summary>
    /// EN: Verifies UPDATE resolves parameters when the SQL value helper supports them.
    /// PT: Verifica se UPDATE resolve parametros quando o helper de valores SQL oferece suporte.
    /// </summary>
    [Fact]
    [Trait("Category", "Strategy")]
    public void Update_ShouldSupportParameter_IfSqlValueHelperSupports()
    {
        var db = new SqlServerDbMock();
        var table = NewUsersTable_Min(db);
        table.Add(new Dictionary<int, object?> { { 0, 1 }, { 1, "John" } });
        table.Add(new Dictionary<int, object?> { { 0, 2 }, { 1, "Mary" } });

        using var connection = NewConn(threadSafe: false, db);
        using var command = new SqlServerCommandMock(connection)
        {
            CommandText = "UPDATE users SET name = 'Z' WHERE id = @id"
        };

        // depende de como seu SqlServerCommandMock implementa Parameters.
        // Se Parameters for IDataParameterCollection, isso deve funcionar:
        command.Parameters.Add(new SqlParameter("@id", 2));

        var rowsAffected = command.ExecuteNonQuery();

        Assert.Equal(1, rowsAffected);
        Assert.Equal("John", table[0][1]);
        Assert.Equal("Z", table[1][1]);
        Assert.Equal(1, connection.Metrics.Updates);
    }

    // ============================================================
    // Opcional: testar ValidateUnique (preciso do IndexDef real)
    // ============================================================
    //
    /// <summary>
    /// EN: Verifies UPDATE throws when a unique index would be violated.
    /// PT: Verifica se UPDATE dispara erro quando um indice unico seria violado.
    /// </summary>
    [Fact]
    [Trait("Category", "Strategy")]
    public void Update_ShouldThrowDuplicateKey_WhenUniqueIndexCollides()
    {
        var db = new SqlServerDbMock();
        var table = NewUsersTable_WithEmail(db);

        table.CreateIndex("Teste", ["name", "email"], unique: true);


        table.Add(new Dictionary<int, object?> { { 0, 1 }, { 1, "John" }, { 2, "a@a.com" } });
        table.Add(new Dictionary<int, object?> { { 0, 2 }, { 1, "Mary" }, { 2, "b@a.com" } });

        using var connection = NewConn(threadSafe: false, db);
        using var command = new SqlServerCommandMock(connection)
        {
            CommandText = "UPDATE users SET email = 'a@a.com', name = 'John' WHERE id = 2"
        };

        var ex = Assert.ThrowsAny<SqlServerMockException>(() => command.ExecuteNonQuery());
        Assert.Contains(SqlExceptionMessages.DuplicateKey(string.Empty, string.Empty).Split('\'')[0].Trim(), ex.Message, StringComparison.OrdinalIgnoreCase);
    }


    /// <summary>
    /// EN: Recomputes persisted generated columns during update and preserves unique index consistency.
    /// PT: Recalcula colunas geradas persistidas durante update e preserva a consistência de índices únicos.
    /// </summary>
    [Fact]
    [Trait("Category", "Strategy")]
    public void Update_ShouldRecomputePersistedGeneratedColumn_AndAllowUniqueIndex()
    {
        var db = new SqlServerDbMock();
        var table = db.AddTable("gen_persisted");

        table.AddColumn("id", DbType.Int32, false);
        table.AddColumn("base", DbType.Int32, false);
        var c = table.AddColumn("gen", DbType.Int32, false);
        c.GetGenValue = (row, _) => ((int?)row[1] ?? 0) * 2;
        c.PersistComputedValue = true;

        table.CreateIndex("ux_gen", ["gen"], unique: true);
        table.Add(new Dictionary<int, object?> { { 0, 1 }, { 1, 10 } });

        Assert.Equal(20, table[0][2]);

        using var connection = NewConn(threadSafe: false, db);
        using var command = new SqlServerCommandMock(connection)
        {
            CommandText = "UPDATE gen_persisted SET base = 15 WHERE id = 1"
        };

        var rowsAffected = command.ExecuteNonQuery();

        Assert.Equal(1, rowsAffected);
        Assert.Equal(15, table[0][1]);
        Assert.Equal(30, table[0][2]);

        var duplicate = Assert.ThrowsAny<SqlServerMockException>(() =>
            table.Add(new Dictionary<int, object?> { { 0, 2 }, { 1, 15 } }));
        Assert.Contains(SqlExceptionMessages.DuplicateKey(string.Empty, string.Empty).Split('\'')[0].Trim(), duplicate.Message, StringComparison.OrdinalIgnoreCase);
    }

    // ---------------- helpers ----------------

    private static SqlServerConnectionMock NewConn(bool threadSafe, SqlServerDbMock db)
    {
        db.ThreadSafe = threadSafe;
        return new SqlServerConnectionMock(db);
    }

    private static ITableMock NewUsersTable_Min(SqlServerDbMock db)
    {
        var table = db.AddTable("users");
        table.AddColumn("id", DbType.Int32, false);
        table.AddColumn("name", DbType.String, false);
        return table;
    }

    private static ITableMock NewUsersTable_WithEmail(SqlServerDbMock db)
    {
        var table = db.AddTable("users");
        table.AddColumn("id", DbType.Int32, false);
        table.AddColumn("name", DbType.String, false);
        table.AddColumn("email", DbType.String, false);
        return table;
    }

    private static ITableMock NewGenTable(SqlServerDbMock db)
    {
        var table = db.AddTable("gen");
        table.AddColumn("id", DbType.Int32, false);
        table.AddColumn("base", DbType.Int32, false);

        table.AddColumn("gen", DbType.Int32, false)
        .GetGenValue = (row, t) => ((int?)row[1] ?? 0) * 2;

        return table;
    }
}
