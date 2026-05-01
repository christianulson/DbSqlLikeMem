namespace DbSqlLikeMem.MySql.Test.Strategy;

/// <summary>
/// EN: Covers DELETE execution scenarios in the MySql mock.
/// PT-br: Cobre cenarios de execucao de DELETE no mock MySql.
/// </summary>
public sealed class MySqlCommandDeleteTests(
        ITestOutputHelper helper
    ) : XUnitTestBase(helper)
{
    /// <summary>
    /// EN: Verifies that DELETE removes a single matching row.
    /// PT-br: Verifica se DELETE remove uma unica linha correspondente.
    /// </summary>
    [Fact]
    [Trait("Category", "Strategy")]
    public void ExecuteNonQuery_DELETE_remove_1_linha()
    {
        var db = new MySqlDbMock();
        var table = NewUsersTable(db);
        table.Add(RowUsers(id: 1, name: "John"));
        table.Add(RowUsers(id: 2, name: "Mary"));

        using var conn = NewConn(threadSafe: false, db);
        using var cmd = new MySqlCommandMock(conn) { CommandText = "DELETE FROM users WHERE id = 1" };

        var affected = cmd.ExecuteNonQuery();

        affected.Should().Be(1);
        table.Should().ContainSingle();
        table[0][0].Should().Be(2); // id
        conn.Metrics.Deletes.Should().Be(1);
    }

    /// <summary>
    /// EN: Verifies that DELETE removes every matching row.
    /// PT-br: Verifica se DELETE remove todas as linhas correspondentes.
    /// </summary>
    [Fact]
    [Trait("Category", "Strategy")]
    public void ExecuteNonQuery_DELETE_remove_varias_linhas()
    {
        var db = new MySqlDbMock();
        var table = NewUsersTable(db);
        table.Add(RowUsers(2, "A"));
        table.Add(RowUsers(2, "B"));
        table.Add(RowUsers(1, "C"));

        using var conn = NewConn(threadSafe: false, db);
        using var cmd = new MySqlCommandMock(conn) { CommandText = "DELETE FROM users WHERE id = 2" };

        var affected = cmd.ExecuteNonQuery();

        affected.Should().Be(2);
        table.Should().ContainSingle();
        table[0][0].Should().Be(1);
        conn.Metrics.Deletes.Should().Be(2);
    }

    /// <summary>
    /// EN: Verifies that DELETE returns zero when no rows match.
    /// PT-br: Verifica se DELETE retorna zero quando nenhuma linha corresponde.
    /// </summary>
    [Fact]
    [Trait("Category", "Strategy")]
    public void ExecuteNonQuery_DELETE_quando_nao_acha_retorna_0()
    {
        var db = new MySqlDbMock();
        var table = NewUsersTable(db);
        table.Add(RowUsers(1, "John"));

        using var conn = NewConn(threadSafe: false, db);
        using var cmd = new MySqlCommandMock(conn) { CommandText = "DELETE FROM users WHERE id = 999" };

        var affected = cmd.ExecuteNonQuery();

        affected.Should().Be(0);
        table.Should().ContainSingle();
        conn.Metrics.Deletes.Should().Be(0);
    }

    /// <summary>
    /// EN: Verifies that DELETE fails when the target table does not exist.
    /// PT-br: Verifica se DELETE falha quando a tabela alvo nao existe.
    /// </summary>
    [Fact]
    [Trait("Category", "Strategy")]
    public void ExecuteNonQuery_DELETE_tabela_inexistente_dispara()
    {
        var db = new MySqlDbMock();
        using var conn = NewConn(threadSafe: false, db);
        using var cmd = new MySqlCommandMock(conn) { CommandText = "DELETE FROM users WHERE id = 1" };

        Action act = () => cmd.ExecuteNonQuery();
        act.Should().Throw<Exception>()
            .Which.Message.Should().Contain("does not exist");
    }

    /// <summary>
    /// EN: Verifies that DELETE without FROM still removes the matching row.
    /// PT-br: Verifica se DELETE sem FROM ainda remove a linha correspondente.
    /// </summary>
    [Fact]
    [Trait("Category", "Strategy")]
    public void ExecuteNonQuery_DELETE_sem_FROM_remove_1_linha()
    {
        var db = new MySqlDbMock();
        var table = NewUsersTable(db);
        table.Add(RowUsers(1, "John"));
        table.Add(RowUsers(2, "Mary"));

        using var conn = NewConn(threadSafe: false, db);
        using var cmd = new MySqlCommandMock(conn) { CommandText = "DELETE users WHERE id = 1" };

        var affected = cmd.ExecuteNonQuery();

        affected.Should().Be(1);
        table.Should().ContainSingle();
        table[0][0].Should().Be(2);
        conn.Metrics.Deletes.Should().Be(1);
    }

    /// <summary>
    /// EN: Verifies that DELETE is blocked when a foreign key references the row.
    /// PT-br: Verifica se DELETE e bloqueado quando uma FK referencia a linha.
    /// </summary>
    [Fact]
    [Trait("Category", "Strategy")]
    public void ExecuteNonQuery_DELETE_bloqueia_quando_fk_referencia()
    {
        var db = new MySqlDbMock();
        var parent = NewParentTable(db);
        parent.Add(RowParent(id: 10));

        var child = NewChildTable(db);
        // FK: child.parent_id -> parent.id
        child.CreateForeignKey("ix_parent_id", parent.TableName, [("parent_id", "id")]);
        child.Add(RowChild(id: 1, parentId: 10));

        using var conn = NewConn(threadSafe: false, db);
        using var cmd = new MySqlCommandMock(conn) { CommandText = "DELETE FROM parent WHERE id = 10" };

        Action act = () => cmd.ExecuteNonQuery();
        act.Should().Throw<Exception>()
            .Which.Message.Should().Contain("parent");

        parent.Should().ContainSingle();              // não deletou
        conn.Metrics.Deletes.Should().Be(0);
    }

    /// <summary>
    /// EN: Verifies a thread-safe delete with multiple parent rows stops when one row is still referenced.
    /// PT-br: Verifica se um delete thread-safe com varias linhas pai para quando uma linha ainda esta referenciada.
    /// </summary>
    [Fact]
    [Trait("Category", "Strategy")]
    public void ExecuteNonQuery_DELETE_multiplas_linhas_com_fk_referencia_em_thread_safe()
    {
        var db = new MySqlDbMock
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
        using var cmd = new MySqlCommandMock(conn) { CommandText = "DELETE FROM parent WHERE id >= 10" };

        Action act = () => cmd.ExecuteNonQuery();
        act.Should().Throw<Exception>()
            .Which.Message.Should().Contain("parent");

        parent.Count.Should().Be(2);
        conn.Metrics.Deletes.Should().Be(0);
    }

    /// <summary>
    /// EN: Verifies that DELETE works with both thread-safe modes.
    /// PT-br: Verifica se DELETE funciona com ambos os modos thread-safe.
    /// </summary>
    [Theory]
    [Trait("Category", "Strategy")]
    [InlineData(false)]
    [InlineData(true)]
    public void ExecuteNonQuery_DELETE_funciona_com_ThreadSafe_true_ou_false(bool threadSafe)
    {
        var db = new MySqlDbMock();
        var table = NewUsersTable(db);
        table.Add(RowUsers(1, "John"));

        using var conn = NewConn(threadSafe, db);
        using var cmd = new MySqlCommandMock(conn) { CommandText = "DELETE FROM users WHERE id = 1" };

        var affected = cmd.ExecuteNonQuery();

        affected.Should().Be(1);
        table.Should().BeEmpty();
    }

    /// <summary>
    /// EN: Verifies that DELETE parsing is case-insensitive.
    /// PT-br: Verifica se o parsing de DELETE e case-insensitive.
    /// </summary>
    [Fact]
    [Trait("Category", "Strategy")]
    public void ExecuteNonQuery_DELETE_case_insensitive()
    {
        var db = new MySqlDbMock();
        var table = NewUsersTable(db);
        table.Add(RowUsers(1, "John"));

        using var conn = NewConn(threadSafe: false, db);
        using var cmd = new MySqlCommandMock(conn) { CommandText = "delete FrOm users wHeRe id = 1" };

        var affected = cmd.ExecuteNonQuery();

        affected.Should().Be(1);
        table.Should().BeEmpty();
    }

    /// <summary>
    /// EN: Verifies that DELETE supports parameters when the helper does.
    /// PT-br: Verifica se DELETE suporta parametros quando o helper suporta.
    /// </summary>
    [Fact]
    [Trait("Category", "Strategy")]
    public void ExecuteNonQuery_DELETE_com_parametro_se_suportado()
    {
        var db = new MySqlDbMock();
        var table = NewUsersTable(db);
        table.Add(RowUsers(1, "A"));
        table.Add(RowUsers(2, "B"));

        using var conn = NewConn(threadSafe: false, db);
        using var cmd = new MySqlCommandMock(conn)
        {
            CommandText = "DELETE FROM users WHERE id = @id"
        };

        cmd.Parameters.Add(new MySqlParameter("@id", 2));

        var affected = cmd.ExecuteNonQuery();

        affected.Should().Be(1);
        table.Should().ContainSingle();
        table[0][0].Should().Be(1);
    }

    // ---------------- helpers ----------------

    private static MySqlConnectionMock NewConn(bool threadSafe, MySqlDbMock db)
    {
        db.ThreadSafe = threadSafe;
        return new MySqlConnectionMock(db);
    }

    private static ITableMock NewUsersTable(MySqlDbMock db)
    {
        var t = db.AddTable("users");
        t.AddColumn("id", DbType.Int32, false);
        t.AddColumn("name", DbType.String, false);
        return t;
    }

    private static ITableMock NewParentTable(MySqlDbMock db)
    {
        var t = db.AddTable("parent");
        t.AddColumn("id", DbType.Int32, false);
        return t;
    }

    private static ITableMock NewChildTable(MySqlDbMock db)
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
