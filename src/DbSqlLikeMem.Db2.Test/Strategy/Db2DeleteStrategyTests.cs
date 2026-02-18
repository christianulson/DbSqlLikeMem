namespace DbSqlLikeMem.Db2.Test.Strategy;

/// <summary>
/// Auto-generated summary.
/// </summary>
public sealed class Db2CommandDeleteTests(
        ITestOutputHelper helper
    ) : XUnitTestBase(helper)
{
    /// <summary>
    /// EN: Tests ExecuteNonQuery_DELETE_remove_1_linha behavior.
    /// PT: Testa o comportamento de ExecuteNonQuery_DELETE_remove_1_linha.
    /// </summary>
    [Fact]
    [Trait("Category", "Strategy")]
    public void ExecuteNonQuery_DELETE_remove_1_linha()
    {
        var db = new Db2DbMock();
        var table = NewUsersTable(db);
        table.Add(RowUsers(id: 1, name: "John"));
        table.Add(RowUsers(id: 2, name: "Mary"));

        using var conn = NewConn(threadSafe: false, db);
        using var cmd = new Db2CommandMock(conn) { CommandText = "DELETE FROM users WHERE id = 1" };

        var affected = cmd.ExecuteNonQuery();

        Assert.Equal(1, affected);
        Assert.Single(table);
        Assert.Equal(2, table[0][0]); // id
        Assert.Equal(1, conn.Metrics.Deletes);
    }

    /// <summary>
    /// EN: Tests ExecuteNonQuery_DELETE_remove_varias_linhas behavior.
    /// PT: Testa o comportamento de ExecuteNonQuery_DELETE_remove_varias_linhas.
    /// </summary>
    [Fact]
    [Trait("Category", "Strategy")]
    public void ExecuteNonQuery_DELETE_remove_varias_linhas()
    {
        var db = new Db2DbMock();
        var table = NewUsersTable(db);
        table.Add(RowUsers(2, "A"));
        table.Add(RowUsers(2, "B"));
        table.Add(RowUsers(1, "C"));

        using var conn = NewConn(threadSafe: false, db);
        using var cmd = new Db2CommandMock(conn) { CommandText = "DELETE FROM users WHERE id = 2" };

        var affected = cmd.ExecuteNonQuery();

        Assert.Equal(2, affected);
        Assert.Single(table);
        Assert.Equal(1, table[0][0]);
        Assert.Equal(2, conn.Metrics.Deletes);
    }

    /// <summary>
    /// EN: Tests ExecuteNonQuery_DELETE_quando_nao_acha_retorna_0 behavior.
    /// PT: Testa o comportamento de ExecuteNonQuery_DELETE_quando_nao_acha_retorna_0.
    /// </summary>
    [Fact]
    [Trait("Category", "Strategy")]
    public void ExecuteNonQuery_DELETE_quando_nao_acha_retorna_0()
    {
        var db = new Db2DbMock();
        var table = NewUsersTable(db);
        table.Add(RowUsers(1, "John"));

        using var conn = NewConn(threadSafe: false, db);
        using var cmd = new Db2CommandMock(conn) { CommandText = "DELETE FROM users WHERE id = 999" };

        var affected = cmd.ExecuteNonQuery();

        Assert.Equal(0, affected);
        Assert.Single(table);
        Assert.Equal(0, conn.Metrics.Deletes);
    }

    /// <summary>
    /// EN: Tests ExecuteNonQuery_DELETE_tabela_inexistente_dispara behavior.
    /// PT: Testa o comportamento de ExecuteNonQuery_DELETE_tabela_inexistente_dispara.
    /// </summary>
    [Fact]
    [Trait("Category", "Strategy")]
    public void ExecuteNonQuery_DELETE_tabela_inexistente_dispara()
    {
        var db = new Db2DbMock();
        using var conn = NewConn(threadSafe: false, db);
        using var cmd = new Db2CommandMock(conn) { CommandText = "DELETE FROM users WHERE id = 1" };

        var ex = Assert.ThrowsAny<Exception>(() => cmd.ExecuteNonQuery());
        Assert.Contains("does not exist", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Tests ExecuteNonQuery_DELETE_sql_invalido_sem_FROM_dispara behavior.
    /// PT: Testa o comportamento de ExecuteNonQuery_DELETE_sql_invalido_sem_FROM_dispara.
    /// </summary>
    [Fact]
    [Trait("Category", "Strategy")]
    public void ExecuteNonQuery_DELETE_sql_invalido_sem_FROM_dispara()
    {
        var db = new Db2DbMock();
        var table = NewUsersTable(db);
        table.Add(RowUsers(1, "John"));

        using var conn = NewConn(threadSafe: false, db);
        using var cmd = new Db2CommandMock(conn) { CommandText = "DELETE users WHERE id = 1" };

        // Pode ser InvalidOperationException ou outra, depende do seu pipeline.
        var ex = Assert.ThrowsAny<Exception>(() => cmd.ExecuteNonQuery());
        Assert.Contains("delete", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Tests ExecuteNonQuery_DELETE_bloqueia_quando_fk_referencia behavior.
    /// PT: Testa o comportamento de ExecuteNonQuery_DELETE_bloqueia_quando_fk_referencia.
    /// </summary>
    [Fact]
    [Trait("Category", "Strategy")]
    public void ExecuteNonQuery_DELETE_bloqueia_quando_fk_referencia()
    {
        var db = new Db2DbMock();
        var parent = NewParentTable(db);
        parent.Add(RowParent(id: 10));

        var child = NewChildTable(db);
        // FK: child.parent_id -> parent.id
        child.CreateForeignKey("ix_parent_id", "parent", [("parent_id", "id")]);
        child.Add(RowChild(id: 1, parentId: 10));

        using var conn = NewConn(threadSafe: false, db);
        using var cmd = new Db2CommandMock(conn) { CommandText = "DELETE FROM parent WHERE id = 10" };

        var ex = Assert.ThrowsAny<Exception>(() => cmd.ExecuteNonQuery());
        Assert.Contains("parent", ex.Message, StringComparison.OrdinalIgnoreCase);

        Assert.Single(parent);              // não deletou
        Assert.Equal(0, conn.Metrics.Deletes);
    }

    /// <summary>
    /// EN: Tests ExecuteNonQuery_DELETE_funciona_com_ThreadSafe_true_ou_false behavior.
    /// PT: Testa o comportamento de ExecuteNonQuery_DELETE_funciona_com_ThreadSafe_true_ou_false.
    /// </summary>
    [Theory]
    [Trait("Category", "Strategy")]
    [InlineData(false)]
    [InlineData(true)]
    public void ExecuteNonQuery_DELETE_funciona_com_ThreadSafe_true_ou_false(bool threadSafe)
    {
        var db = new Db2DbMock();
        var table = NewUsersTable(db);
        table.Add(RowUsers(1, "John"));

        using var conn = NewConn(threadSafe, db);
        using var cmd = new Db2CommandMock(conn) { CommandText = "DELETE FROM users WHERE id = 1" };

        var affected = cmd.ExecuteNonQuery();

        Assert.Equal(1, affected);
        Assert.Empty(table);
    }

    /// <summary>
    /// EN: Tests ExecuteNonQuery_DELETE_case_insensitive behavior.
    /// PT: Testa o comportamento de ExecuteNonQuery_DELETE_case_insensitive.
    /// </summary>
    [Fact]
    [Trait("Category", "Strategy")]
    public void ExecuteNonQuery_DELETE_case_insensitive()
    {
        var db = new Db2DbMock();
        var table = NewUsersTable(db);
        table.Add(RowUsers(1, "John"));

        using var conn = NewConn(threadSafe: false, db);
        using var cmd = new Db2CommandMock(conn) { CommandText = "delete FrOm users wHeRe id = 1" };

        var affected = cmd.ExecuteNonQuery();

        Assert.Equal(1, affected);
        Assert.Empty(table);
    }

    /// <summary>
    /// EN: Tests ExecuteNonQuery_DELETE_com_parametro_se_suportado behavior.
    /// PT: Testa o comportamento de ExecuteNonQuery_DELETE_com_parametro_se_suportado.
    /// </summary>
    [Fact]
    [Trait("Category", "Strategy")]
    public void ExecuteNonQuery_DELETE_com_parametro_se_suportado()
    {
        var db = new Db2DbMock();
        var table = NewUsersTable(db);
        table.Add(RowUsers(1, "A"));
        table.Add(RowUsers(2, "B"));

        using var conn = NewConn(threadSafe: false, db);
        using var cmd = new Db2CommandMock(conn)
        {
            CommandText = "DELETE FROM users WHERE id = @id"
        };

        cmd.Parameters.Add(new DB2Parameter("@id", 2));

        var affected = cmd.ExecuteNonQuery();

        Assert.Equal(1, affected);
        Assert.Single(table);
        Assert.Equal(1, table[0][0]);
    }

    // ---------------- helpers ----------------

    private static Db2ConnectionMock NewConn(bool threadSafe, Db2DbMock db)
    {
        db.ThreadSafe = threadSafe;
        return new Db2ConnectionMock(db);
    }

    private static ITableMock NewUsersTable(Db2DbMock db)
    {
        var t = db.AddTable("users");
        t.AddColumn("id", DbType.Int32, false);
        t.AddColumn("name", DbType.String, false);
        return t;
    }

    private static ITableMock NewParentTable(Db2DbMock db)
    {
        var t = db.AddTable("parent");
        t.AddColumn("id", DbType.Int32, false);
        return t;
    }

    private static ITableMock NewChildTable(Db2DbMock db)
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
