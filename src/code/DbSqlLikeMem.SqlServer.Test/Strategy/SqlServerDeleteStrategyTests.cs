namespace DbSqlLikeMem.SqlServer.Test.Strategy;

/// <summary>
/// EN: Covers DELETE statements in the SqlServer mock.
/// PT: Cobre instrucoes DELETE no mock SqlServer.
/// </summary>
public sealed class SqlServerCommandDeleteTests(
        ITestOutputHelper helper
    ) : XUnitTestBase(helper)
{
    /// <summary>
    /// EN: Verifies DELETE removes one matching row.
    /// PT: Verifica se DELETE remove uma linha correspondente.
    /// </summary>
    [Fact]
    [Trait("Category", "Strategy")]
    public void ExecuteNonQuery_DELETE_remove_1_linha()
    {
        var db = new SqlServerDbMock();
        var table = NewUsersTable(db);
        table.Add(RowUsers(id: 1, name: "John"));
        table.Add(RowUsers(id: 2, name: "Mary"));

        using var conn = NewConn(threadSafe: false, db);
        using var cmd = new SqlServerCommandMock(conn) { CommandText = "DELETE FROM users WHERE id = 1" };

        var affected = cmd.ExecuteNonQuery();

        Assert.Equal(1, affected);
        Assert.Single(table);
        Assert.Equal(2, table[0][0]); // id
        Assert.Equal(1, conn.Metrics.Deletes);
    }

    /// <summary>
    /// EN: Verifies DELETE removes every matching row.
    /// PT: Verifica se DELETE remove todas as linhas correspondentes.
    /// </summary>
    [Fact]
    [Trait("Category", "Strategy")]
    public void ExecuteNonQuery_DELETE_remove_varias_linhas()
    {
        var db = new SqlServerDbMock();
        var table = NewUsersTable(db);
        table.Add(RowUsers(2, "A"));
        table.Add(RowUsers(2, "B"));
        table.Add(RowUsers(1, "C"));

        using var conn = NewConn(threadSafe: false, db);
        using var cmd = new SqlServerCommandMock(conn) { CommandText = "DELETE FROM users WHERE id = 2" };

        var affected = cmd.ExecuteNonQuery();

        Assert.Equal(2, affected);
        Assert.Single(table);
        Assert.Equal(1, table[0][0]);
        Assert.Equal(2, conn.Metrics.Deletes);
    }

    /// <summary>
    /// EN: Verifies DELETE returns zero when no row matches.
    /// PT: Verifica se DELETE retorna zero quando nenhuma linha corresponde.
    /// </summary>
    [Fact]
    [Trait("Category", "Strategy")]
    public void ExecuteNonQuery_DELETE_quando_nao_acha_retorna_0()
    {
        var db = new SqlServerDbMock();
        var table = NewUsersTable(db);
        table.Add(RowUsers(1, "John"));

        using var conn = NewConn(threadSafe: false, db);
        using var cmd = new SqlServerCommandMock(conn) { CommandText = "DELETE FROM users WHERE id = 999" };

        var affected = cmd.ExecuteNonQuery();

        Assert.Equal(0, affected);
        Assert.Single(table);
        Assert.Equal(0, conn.Metrics.Deletes);
    }

    /// <summary>
    /// EN: Verifies DELETE throws when the target table does not exist.
    /// PT: Verifica se DELETE dispara erro quando a tabela alvo nao existe.
    /// </summary>
    [Fact]
    [Trait("Category", "Strategy")]
    public void ExecuteNonQuery_DELETE_tabela_inexistente_dispara()
    {
        var db = new SqlServerDbMock();
        using var conn = NewConn(threadSafe: false, db);
        using var cmd = new SqlServerCommandMock(conn) { CommandText = "DELETE FROM users WHERE id = 1" };

        var ex = Assert.ThrowsAny<Exception>(() => cmd.ExecuteNonQuery());
        Assert.Contains("does not exist", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Verifies DELETE without FROM still removes one matching row.
    /// PT: Verifica se DELETE sem FROM ainda remove uma linha correspondente.
    /// </summary>
    [Fact]
    [Trait("Category", "Strategy")]
    public void ExecuteNonQuery_DELETE_sem_FROM_remove_1_linha()
    {
        var db = new SqlServerDbMock();
        var table = NewUsersTable(db);
        table.Add(RowUsers(1, "John"));
        table.Add(RowUsers(2, "Mary"));

        using var conn = NewConn(threadSafe: false, db);
        using var cmd = new SqlServerCommandMock(conn) { CommandText = "DELETE users WHERE id = 1" };

        var affected = cmd.ExecuteNonQuery();

        Assert.Equal(1, affected);
        Assert.Single(table);
        Assert.Equal(2, table[0][0]);
        Assert.Equal(1, conn.Metrics.Deletes);
    }

    /// <summary>
    /// EN: Verifies DELETE is blocked when a foreign key references the target row.
    /// PT: Verifica se DELETE e bloqueado quando uma chave estrangeira referencia a linha alvo.
    /// </summary>
    [Fact]
    [Trait("Category", "Strategy")]
    public void ExecuteNonQuery_DELETE_bloqueia_quando_fk_referencia()
    {
        var db = new SqlServerDbMock();
        var parent = NewParentTable(db);
        parent.Add(RowParent(id: 10));

        var child = NewChildTable(db);
        // FK: child.parent_id -> parent.id
        child.CreateForeignKey("ix_parent_id", parent.TableName, [("parent_id", "id")]);
        child.Add(RowChild(id: 1, parentId: 10));

        using var conn = NewConn(threadSafe: false, db);
        using var cmd = new SqlServerCommandMock(conn) { CommandText = "DELETE FROM parent WHERE id = 10" };

        var ex = Assert.ThrowsAny<Exception>(() => cmd.ExecuteNonQuery());
        Assert.Contains("parent", ex.Message, StringComparison.OrdinalIgnoreCase);

        Assert.Single(parent);              // não deletou
        Assert.Equal(0, conn.Metrics.Deletes);
    }

    /// <summary>
    /// EN: Verifies a thread-safe delete with multiple parent rows stops when one row is still referenced.
    /// PT: Verifica se um delete thread-safe com varias linhas pai para quando uma linha ainda esta referenciada.
    /// </summary>
    [Fact]
    [Trait("Category", "Strategy")]
    public void ExecuteNonQuery_DELETE_multiplas_linhas_com_fk_referencia_em_thread_safe()
    {
        var db = new SqlServerDbMock
        {
            ThreadSafe = true
        };
        var parent = NewParentTable(db);
        parent.Add(RowParent(id: 10));
        parent.Add(RowParent(id: 11));

        var child = NewChildTable(db);
        child.CreateForeignKey("ix_parent_id", parent.TableName, [("parent_id", "id")]);
        child.Add(RowChild(id: 1, parentId: 10));

        using var conn = NewConn(threadSafe: true, db);
        using var cmd = new SqlServerCommandMock(conn) { CommandText = "DELETE FROM parent WHERE id >= 10" };

        var ex = Assert.ThrowsAny<Exception>(() => cmd.ExecuteNonQuery());

        Assert.Contains("parent", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Equal(2, parent.Count);
        Assert.Equal(0, conn.Metrics.Deletes);
    }

    /// <summary>
    /// EN: Verifies DELETE works in thread-safe and non-thread-safe mode.
    /// PT: Verifica se DELETE funciona em modo thread-safe e nao thread-safe.
    /// </summary>
    [Theory]
    [Trait("Category", "Strategy")]
    [InlineData(false)]
    [InlineData(true)]
    public void ExecuteNonQuery_DELETE_funciona_com_ThreadSafe_true_ou_false(bool threadSafe)
    {
        var db = new SqlServerDbMock();
        var table = NewUsersTable(db);
        table.Add(RowUsers(1, "John"));

        using var conn = NewConn(threadSafe, db);
        using var cmd = new SqlServerCommandMock(conn) { CommandText = "DELETE FROM users WHERE id = 1" };

        var affected = cmd.ExecuteNonQuery();

        Assert.Equal(1, affected);
        Assert.Empty(table);
    }

    /// <summary>
    /// EN: Verifies DELETE parsing is case-insensitive.
    /// PT: Verifica se o parsing de DELETE nao diferencia maiusculas e minusculas.
    /// </summary>
    [Fact]
    [Trait("Category", "Strategy")]
    public void ExecuteNonQuery_DELETE_case_insensitive()
    {
        var db = new SqlServerDbMock();
        var table = NewUsersTable(db);
        table.Add(RowUsers(1, "John"));

        using var conn = NewConn(threadSafe: false, db);
        using var cmd = new SqlServerCommandMock(conn) { CommandText = "delete FrOm users wHeRe id = 1" };

        var affected = cmd.ExecuteNonQuery();

        Assert.Equal(1, affected);
        Assert.Empty(table);
    }

    /// <summary>
    /// EN: Verifies DELETE accepts a parameter when the mock supports it.
    /// PT: Verifica se DELETE aceita parametro quando o mock oferece suporte.
    /// </summary>
    [Fact]
    [Trait("Category", "Strategy")]
    public void ExecuteNonQuery_DELETE_com_parametro_se_suportado()
    {
        var db = new SqlServerDbMock();
        var table = NewUsersTable(db);
        table.Add(RowUsers(1, "A"));
        table.Add(RowUsers(2, "B"));

        using var conn = NewConn(threadSafe: false, db);
        using var cmd = new SqlServerCommandMock(conn)
        {
            CommandText = "DELETE FROM users WHERE id = @id"
        };

        cmd.Parameters.Add(new SqlParameter("@id", 2));

        var affected = cmd.ExecuteNonQuery();

        Assert.Equal(1, affected);
        Assert.Single(table);
        Assert.Equal(1, table[0][0]);
    }

    // ---------------- helpers ----------------

    private static SqlServerConnectionMock NewConn(bool threadSafe, SqlServerDbMock db)
    {
        db.ThreadSafe = threadSafe;

        var conn = new SqlServerConnectionMock(db);
        return conn;
    }

    private static ITableMock NewUsersTable(SqlServerDbMock db)
    {
        var t = db.AddTable("users");
        t.AddColumn("id", DbType.Int32, false);
        t.AddColumn("name", DbType.String, false);
        return t;
    }

    private static ITableMock NewParentTable(SqlServerDbMock db)
    {
        var t = db.AddTable("parent");
        t.AddColumn("id", DbType.Int32, false);
        return t;
    }

    private static ITableMock NewChildTable(SqlServerDbMock db)
    {
        var t = db.AddTable("child");
        t.AddColumn("id", DbType.Int32, false);
        t.AddColumn("parent_id", DbType.Int32, false);
        return t;
    }

    private static Dictionary<int, object?> RowUsers(int id, string name)
        => new() { [0] = id, [1] = name };

    private static Dictionary<int, object?> RowParent(int id)
        => new() { [0] = id };

    private static Dictionary<int, object?> RowChild(int id, int parentId)
        => new() { [0] = id, [1] = parentId };
}
