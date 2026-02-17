namespace DbSqlLikeMem.Sqlite.Test.Strategy;

/// <summary>
/// Auto-generated summary.
/// </summary>
public sealed class SqliteUpdateStrategyTests(
    ITestOutputHelper helper
) : XUnitTestBase(helper)
{
    /// <summary>
    /// EN: Tests UpdateTableShouldModifyExistingRow behavior.
    /// PT: Testa o comportamento de UpdateTableShouldModifyExistingRow.
    /// </summary>
    [Fact]
    public void UpdateTableShouldModifyExistingRow()
    {
        // Arrange
        var db = new SqliteDbMock();
        var table = NewUsersTable_Min(db);
        table.Add(new Dictionary<int, object?> { { 0, 1 }, { 1, "John Doe" } });

        using var connection = NewConn(threadSafe: false, db);
        using var command = new SqliteCommandMock(connection)
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
    /// EN: Tests Update_ShouldReturnZero_WhenNoRowsMatchWhere behavior.
    /// PT: Testa o comportamento de Update_ShouldReturnZero_WhenNoRowsMatchWhere.
    /// </summary>
    [Fact]
    public void Update_ShouldReturnZero_WhenNoRowsMatchWhere()
    {
        var db = new SqliteDbMock();
        var table = NewUsersTable_Min(db);
        table.Add(new Dictionary<int, object?> { { 0, 1 }, { 1, "John Doe" } });

        using var connection = NewConn(threadSafe: false, db);
        using var command = new SqliteCommandMock(connection)
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
    /// EN: Tests Update_ShouldUpdateMultipleRows_WhenWhereMatchesMultiple behavior.
    /// PT: Testa o comportamento de Update_ShouldUpdateMultipleRows_WhenWhereMatchesMultiple.
    /// </summary>
    [Fact]
    public void Update_ShouldUpdateMultipleRows_WhenWhereMatchesMultiple()
    {
        var db = new SqliteDbMock();
        var table = NewUsersTable_Min(db);
        table.Add(new Dictionary<int, object?> { { 0, 1 }, { 1, "A" } });
        table.Add(new Dictionary<int, object?> { { 0, 1 }, { 1, "B" } });
        table.Add(new Dictionary<int, object?> { { 0, 2 }, { 1, "C" } });

        using var connection = NewConn(threadSafe: false, db);
        using var command = new SqliteCommandMock(connection)
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
    /// EN: Tests Update_ShouldHandleWhereWithAnd_CaseInsensitive behavior.
    /// PT: Testa o comportamento de Update_ShouldHandleWhereWithAnd_CaseInsensitive.
    /// </summary>
    [Fact]
    public void Update_ShouldHandleWhereWithAnd_CaseInsensitive()
    {
        var db = new SqliteDbMock();
        var table = NewUsersTable_WithEmail(db);
        table.Add(new Dictionary<int, object?> { { 0, 1 }, { 1, "John" }, { 2, "a@a.com" } });
        table.Add(new Dictionary<int, object?> { { 0, 0 }, { 1, "John" }, { 2, "b@a.com" } });
        table.Add(new Dictionary<int, object?> { { 0, 1 }, { 1, "Bob" }, { 2, "c@a.com" } });

        using var connection = NewConn(threadSafe: false, db);
        using var command = new SqliteCommandMock(connection)
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
    /// EN: Tests Update_ShouldUpdateMultipleSetPairs behavior.
    /// PT: Testa o comportamento de Update_ShouldUpdateMultipleSetPairs.
    /// </summary>
    [Fact]
    public void Update_ShouldUpdateMultipleSetPairs()
    {
        var db = new SqliteDbMock();
        var table = NewUsersTable_WithEmail(db);
        table.Add(new Dictionary<int, object?> { { 0, 1 }, { 1, "John" }, { 2, "a@a.com" } });

        using var connection = NewConn(threadSafe: false, db);
        using var command = new SqliteCommandMock(connection)
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
    /// EN: Tests Update_ShouldBeCaseInsensitive_ForUpdateSetWhereKeywords behavior.
    /// PT: Testa o comportamento de Update_ShouldBeCaseInsensitive_ForUpdateSetWhereKeywords.
    /// </summary>
    [Fact]
    public void Update_ShouldBeCaseInsensitive_ForUpdateSetWhereKeywords()
    {
        var db = new SqliteDbMock();
        var table = NewUsersTable_Min(db);
        table.Add(new Dictionary<int, object?> { { 0, 1 }, { 1, "John" } });

        using var connection = NewConn(threadSafe: false, db);
        using var command = new SqliteCommandMock(connection)
        {
            CommandText = "uPdAtE users sEt name = 'Z' wHeRe id = 1"
        };

        var rowsAffected = command.ExecuteNonQuery();

        Assert.Equal(1, rowsAffected);
        Assert.Equal("Z", table[0][1]);
        Assert.Equal(1, connection.Metrics.Updates);
    }

    /// <summary>
    /// EN: Tests Update_ShouldWork_WithThreadSafeTrueOrFalse behavior.
    /// PT: Testa o comportamento de Update_ShouldWork_WithThreadSafeTrueOrFalse.
    /// </summary>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void Update_ShouldWork_WithThreadSafeTrueOrFalse(bool threadSafe)
    {
        var db = new SqliteDbMock();
        var table = NewUsersTable_Min(db);
        table.Add(new Dictionary<int, object?> { { 0, 1 }, { 1, "John" } });

        using var connection = NewConn(threadSafe, db);
        using var command = new SqliteCommandMock(connection)
        {
            CommandText = "UPDATE users SET name = 'Z' WHERE id = 1"
        };

        var rowsAffected = command.ExecuteNonQuery();

        Assert.Equal(1, rowsAffected);
        Assert.Equal("Z", table[0][1]);
        Assert.Equal(1, connection.Metrics.Updates);
    }

    /// <summary>
    /// EN: Tests Update_ShouldThrow_WhenTableDoesNotExist behavior.
    /// PT: Testa o comportamento de Update_ShouldThrow_WhenTableDoesNotExist.
    /// </summary>
    [Fact]
    public void Update_ShouldThrow_WhenTableDoesNotExist()
    {
        var db = new SqliteDbMock();
        using var connection = NewConn(threadSafe: false, db);
        using var command = new SqliteCommandMock(connection)
        {
            CommandText = "UPDATE users SET name = 'Z' WHERE id = 1"
        };

        var ex = Assert.ThrowsAny<Exception>(() => command.ExecuteNonQuery());
        Assert.Contains("does not exist", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Tests Update_ShouldThrow_WhenSqlIsInvalid_NoUpdateToken behavior.
    /// PT: Testa o comportamento de Update_ShouldThrow_WhenSqlIsInvalid_NoUpdateToken.
    /// </summary>
    [Fact]
    public void Update_ShouldThrow_WhenSqlIsInvalid_NoUpdateToken()
    {
        var db = new SqliteDbMock();
        var table = NewUsersTable_Min(db);
        table.Add(new Dictionary<int, object?> { { 0, 1 }, { 1, "John" } });

        using var connection = NewConn(threadSafe: false, db);
        using var command = new SqliteCommandMock(connection)
        {
            CommandText = "UPD users SET name = 'Z' WHERE id = 1"
        };

        var ex = Assert.ThrowsAny<Exception>(() => command.ExecuteNonQuery());
        Assert.Contains("upd", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Tests Update_ShouldNotChangeGeneratedColumn_WhenGetGenValueIsNotNull behavior.
    /// PT: Testa o comportamento de Update_ShouldNotChangeGeneratedColumn_WhenGetGenValueIsNotNull.
    /// </summary>
    [Fact]
    public void Update_ShouldNotChangeGeneratedColumn_WhenGetGenValueIsNotNull()
    {
        var db = new SqliteDbMock();
        var table = NewGenTable(db);
        table.Add(new Dictionary<int, object?> { { 0, 1 }, { 1, 10 }, { 2, 999 } }); // gen já tem algo

        using var connection = NewConn(threadSafe: false, db);
        using var command = new SqliteCommandMock(connection)
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
    /// EN: Tests Update_ShouldSupportParameter_IfSqlValueHelperSupports behavior.
    /// PT: Testa o comportamento de Update_ShouldSupportParameter_IfSqlValueHelperSupports.
    /// </summary>
    [Fact]
    public void Update_ShouldSupportParameter_IfSqlValueHelperSupports()
    {
        var db = new SqliteDbMock();
        var table = NewUsersTable_Min(db);
        table.Add(new Dictionary<int, object?> { { 0, 1 }, { 1, "John" } });
        table.Add(new Dictionary<int, object?> { { 0, 2 }, { 1, "Mary" } });

        using var connection = NewConn(threadSafe: false, db);
        using var command = new SqliteCommandMock(connection)
        {
            CommandText = "UPDATE users SET name = 'Z' WHERE id = @id"
        };

        // depende de como seu SqliteCommandMock implementa Parameters.
        // Se Parameters for IDataParameterCollection, isso deve funcionar:
        command.Parameters.Add(new SqliteParameter("@id", 2));

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
    /// EN: Tests Update_ShouldThrowDuplicateKey_WhenUniqueIndexCollides behavior.
    /// PT: Testa o comportamento de Update_ShouldThrowDuplicateKey_WhenUniqueIndexCollides.
    /// </summary>
    [Fact]
    public void Update_ShouldThrowDuplicateKey_WhenUniqueIndexCollides()
    {
        var db = new SqliteDbMock();
        var table = NewUsersTable_WithEmail(db);

        table.CreateIndex("Teste", ["name", "email"], unique: true);


        table.Add(new Dictionary<int, object?> { { 0, 1 }, { 1, "John" }, { 2, "a@a.com" } });
        table.Add(new Dictionary<int, object?> { { 0, 2 }, { 1, "Mary" }, { 2, "b@a.com" } });

        using var connection = NewConn(threadSafe: false, db);
        using var command = new SqliteCommandMock(connection)
        {
            CommandText = "UPDATE users SET email = 'a@a.com', name = 'John' WHERE id = 2"
        };

        var ex = Assert.ThrowsAny<SqliteMockException>(() => command.ExecuteNonQuery());
        Assert.Contains(SqlExceptionMessages.DuplicateKey(string.Empty, string.Empty).Split('\'')[0].Trim(), ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    // ---------------- helpers ----------------

    private static SqliteConnectionMock NewConn(bool threadSafe, SqliteDbMock db)
    {
        db.ThreadSafe = threadSafe;
        return new SqliteConnectionMock(db);
    }

    private static ITableMock NewUsersTable_Min(SqliteDbMock db)
    {
        var table = db.AddTable("users");
        table.AddColumn("id", DbType.Int32, false);
        table.AddColumn("name", DbType.String, false);
        return table;
    }

    private static ITableMock NewUsersTable_WithEmail(SqliteDbMock db)
    {
        var table = db.AddTable("users");
        table.AddColumn("id", DbType.Int32, false);
        table.AddColumn("name", DbType.String, false);
        table.AddColumn("email", DbType.String, false);
        return table;
    }

    private static ITableMock NewGenTable(SqliteDbMock db)
    {
        var table = db.AddTable("gen");
        table.AddColumn("id", DbType.Int32, false);
        table.AddColumn("base", DbType.Int32, false);

        table.AddColumn("gen", DbType.Int32, false)
            .GetGenValue = (row, t) => ((int?)row[1] ?? 0) * 2;

        return table;
    }
}
