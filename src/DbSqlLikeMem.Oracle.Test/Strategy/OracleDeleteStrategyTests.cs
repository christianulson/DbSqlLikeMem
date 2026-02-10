namespace DbSqlLikeMem.Oracle.Test.Strategy;

public sealed class OracleCommandDeleteTests(
        ITestOutputHelper helper
    ) : XUnitTestBase(helper)
{
    /// <summary>
    /// EN: Tests ExecuteNonQuery_DELETE_remove_1_linha behavior.
    /// PT: Testa o comportamento de ExecuteNonQuery_DELETE_remove_1_linha.
    /// </summary>
    [Fact]
    public void ExecuteNonQuery_DELETE_remove_1_linha()
    {
        var db = new OracleDbMock();
        var table = NewUsersTable(db);
        table.Add(RowUsers(id: 1, name: "John"));
        table.Add(RowUsers(id: 2, name: "Mary"));

        using var conn = NewConn(threadSafe: false, db);
        using var cmd = new OracleCommandMock(conn) { CommandText = "DELETE FROM users WHERE id = 1" };

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
    public void ExecuteNonQuery_DELETE_remove_varias_linhas()
    {
        var db = new OracleDbMock();
        var table = NewUsersTable(db);
        table.Add(RowUsers(2, "A"));
        table.Add(RowUsers(2, "B"));
        table.Add(RowUsers(1, "C"));

        using var conn = NewConn(threadSafe: false, db);
        using var cmd = new OracleCommandMock(conn) { CommandText = "DELETE FROM users WHERE id = 2" };

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
    public void ExecuteNonQuery_DELETE_quando_nao_acha_retorna_0()
    {
        var db = new OracleDbMock();
        var table = NewUsersTable(db);
        table.Add(RowUsers(1, "John"));

        using var conn = NewConn(threadSafe: false, db);
        using var cmd = new OracleCommandMock(conn) { CommandText = "DELETE FROM users WHERE id = 999" };

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
    public void ExecuteNonQuery_DELETE_tabela_inexistente_dispara()
    {
        var db = new OracleDbMock();
        using var conn = NewConn(threadSafe: false, db);
        using var cmd = new OracleCommandMock(conn) { CommandText = "DELETE FROM users WHERE id = 1" };

        var ex = Assert.ThrowsAny<Exception>(() => cmd.ExecuteNonQuery());
        Assert.Contains("does not exist", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Tests ExecuteNonQuery_DELETE_sql_invalido_sem_FROM_dispara behavior.
    /// PT: Testa o comportamento de ExecuteNonQuery_DELETE_sql_invalido_sem_FROM_dispara.
    /// </summary>
    [Fact(Skip = "Isso é valido no Oracle")]
    public void ExecuteNonQuery_DELETE_sql_invalido_sem_FROM_dispara()
    {
        var db = new OracleDbMock();
        var table = NewUsersTable(db);
        table.Add(RowUsers(1, "John"));

        using var conn = NewConn(threadSafe: false, db);
        using var cmd = new OracleCommandMock(conn) { CommandText = "DELETE FROM users WHERE id = 1" };

        // Pode ser InvalidOperationException ou outra, depende do seu pipeline.
        var ex = Assert.ThrowsAny<Exception>(() => cmd.ExecuteNonQuery());
        Assert.Contains("delete", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Tests ExecuteNonQuery_DELETE_bloqueia_quando_fk_referencia behavior.
    /// PT: Testa o comportamento de ExecuteNonQuery_DELETE_bloqueia_quando_fk_referencia.
    /// </summary>
    [Fact]
    public void ExecuteNonQuery_DELETE_bloqueia_quando_fk_referencia()
    {
        var db = new OracleDbMock();
        var parent = NewParentTable(db);
        parent.Add(RowParent(id: 10));

        var child = NewChildTable(db);
        // FK: child.parent_id -> parent.id
        child.CreateForeignKey("parent_id", "parent", "id");
        child.Add(RowChild(id: 1, parentId: 10));

        using var conn = NewConn(threadSafe: false, db);
        using var cmd = new OracleCommandMock(conn) { CommandText = "DELETE FROM parent WHERE id = 10" };

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
    [InlineData(false)]
    [InlineData(true)]
    public void ExecuteNonQuery_DELETE_funciona_com_ThreadSafe_true_ou_false(bool threadSafe)
    {
        var db = new OracleDbMock();
        var table = NewUsersTable(db);
        table.Add(RowUsers(1, "John"));

        using var conn = NewConn(threadSafe, db);
        using var cmd = new OracleCommandMock(conn) { CommandText = "DELETE FROM users WHERE id = 1" };

        var affected = cmd.ExecuteNonQuery();

        Assert.Equal(1, affected);
        Assert.Empty(table);
    }

    /// <summary>
    /// EN: Tests ExecuteNonQuery_DELETE_case_insensitive behavior.
    /// PT: Testa o comportamento de ExecuteNonQuery_DELETE_case_insensitive.
    /// </summary>
    [Fact]
    public void ExecuteNonQuery_DELETE_case_insensitive()
    {
        var db = new OracleDbMock();
        var table = NewUsersTable(db);
        table.Add(RowUsers(1, "John"));

        using var conn = NewConn(threadSafe: false, db);
        using var cmd = new OracleCommandMock(conn) { CommandText = "delete FrOm users wHeRe id = 1" };

        var affected = cmd.ExecuteNonQuery();

        Assert.Equal(1, affected);
        Assert.Empty(table);
    }

    /// <summary>
    /// EN: Tests ExecuteNonQuery_DELETE_com_parametro_se_suportado behavior.
    /// PT: Testa o comportamento de ExecuteNonQuery_DELETE_com_parametro_se_suportado.
    /// </summary>
    [Fact]
    public void ExecuteNonQuery_DELETE_com_parametro_se_suportado()
    {
        var db = new OracleDbMock();
        var table = NewUsersTable(db);
        table.Add(RowUsers(1, "A"));
        table.Add(RowUsers(2, "B"));

        using var conn = NewConn(threadSafe: false, db);
        using var cmd = new OracleCommandMock(conn)
        {
            CommandText = "DELETE FROM users WHERE id = @id"
        };

        cmd.Parameters.Add(new OracleParameter("@id", 2));

        var affected = cmd.ExecuteNonQuery();

        Assert.Equal(1, affected);
        Assert.Single(table);
        Assert.Equal(1, table[0][0]);
    }

    // ---------------- helpers ----------------

    private static OracleConnectionMock NewConn(bool threadSafe, OracleDbMock db)
    {
        db.ThreadSafe = threadSafe;

        var conn = new OracleConnectionMock(db);
        return conn;
    }

    private static ITableMock NewUsersTable(OracleDbMock db)
    {
        var t = db.AddTable("users");
        t.Columns["id"] = new ColumnDef(0, DbType.Int32, false);
        t.Columns["name"] = new ColumnDef(1, DbType.String, false);
        return t;
    }

    private static ITableMock NewParentTable(OracleDbMock db)
    {
        var t = db.AddTable("parent");
        t.Columns["id"] = new ColumnDef(0, DbType.Int32, false);
        return t;
    }

    private static ITableMock NewChildTable(OracleDbMock db)
    {
        var t = db.AddTable("child");
        t.Columns["id"] = new ColumnDef(0, DbType.Int32, false);
        t.Columns["parent_id"] = new ColumnDef(1, DbType.Int32, false);
        return t;
    }

    private static Dictionary<int, object?> RowUsers(int id, string name)
        => new() { [0] = id, [1] = name };

    private static Dictionary<int, object?> RowParent(int id)
        => new() { [0] = id };

    private static Dictionary<int, object?> RowChild(int id, int parentId)
        => new() { [0] = id, [1] = parentId };
}
