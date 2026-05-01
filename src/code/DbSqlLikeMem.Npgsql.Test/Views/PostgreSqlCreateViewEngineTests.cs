namespace DbSqlLikeMem.Npgsql.Test.Views;

/// <summary>
/// EN: Covers view execution scenarios in the Npgsql mock.
/// PT-br: Cobre cenarios de execucao de view no mock Npgsql.
/// </summary>
public sealed class PostgreSqlCreateViewEngineTests : XUnitTestBase
{
    private readonly NpgsqlConnectionMock _cnn;
    private readonly ITableMock _users;
    private readonly ITableMock _orders;

    /// <summary>
    /// EN: Creates the users and orders tables used by the view tests.
    /// PT-br: Cria as tabelas users e orders usadas pelos testes de view.
    /// </summary>
    public PostgreSqlCreateViewEngineTests(ITestOutputHelper helper) : base(helper)
    {
        var db = new NpgsqlDbMock();
        _users = db.AddTable("users");
        _users.AddColumn("id", DbType.Int32, false, identity: false);
        _users.AddColumn("name", DbType.String, false);
        _users.AddColumn("tenantid", DbType.Int32, false);
        _users.Add(new Dictionary<int, object?> { [0] = 1, [1] = "John", [2] = 10 });
        _users.Add(new Dictionary<int, object?> { [0] = 2, [1] = "Bob", [2] = 10 });
        _users.Add(new Dictionary<int, object?> { [0] = 3, [1] = "Jane", [2] = 20 });

        _orders = db.AddTable("orders");
        _orders.AddColumn("userid", DbType.Int32, false);
        _orders.AddColumn("amount", DbType.Decimal, false, decimalPlaces: 2);
        _orders.Add(new Dictionary<int, object?> { [0] = 1, [1] = 10m });
        _orders.Add(new Dictionary<int, object?> { [0] = 1, [1] = 5m });
        _orders.Add(new Dictionary<int, object?> { [0] = 2, [1] = 7m });

        _cnn = new NpgsqlConnectionMock(db);
        _cnn.Open();
    }

    /// <summary>
    /// EN: Verifies that a view returns rows projected from the base table.
    /// PT-br: Verifica se a view retorna linhas projetadas a partir da tabela base.
    /// </summary>
    [Fact]
    [Trait("Category", "Views")]
    public void CreateView_ThenSelectFromView_ShouldReturnExpectedRows()
    {
        _cnn.ExecNonQuery(@"
CREATE VIEW v10 AS
SELECT id, name FROM users WHERE tenantid = 10;
");

        var rows = _cnn.QueryRows("SELECT id, name FROM v10 ORDER BY id");
        Assert.Equal([1, 2], [.. rows.Select(r => (int)r["id"]!)]);
        Assert.Equal(["John", "Bob"], [.. rows.Select(r => (string)r["name"]!)]);
    }

    /// <summary>
    /// EN: Verifies that view reads reflect later base table changes.
    /// PT-br: Verifica se leituras da view refletem alteracoes posteriores na tabela base.
    /// </summary>
    [Fact]
    [Trait("Category", "Views")]
    public void View_IsNotMaterialized_ShouldReflectBaseTableChanges()
    {
        _cnn.ExecNonQuery("CREATE VIEW v_all AS SELECT id FROM users ORDER BY id;");

        // altera tabela base após criar view
        _users.Add(new Dictionary<int, object?> { [0] = 4, [1] = "Zoe", [2] = 10 });

        var rows = _cnn.QueryRows("SELECT id FROM v_all ORDER BY id");
        Assert.Equal([1, 2, 3, 4], [.. rows.Select(r => (int)r["id"]!)]);
    }

    /// <summary>
    /// EN: Verifies that CREATE OR REPLACE VIEW replaces the stored definition.
    /// PT-br: Verifica se CREATE OR REPLACE VIEW substitui a definicao armazenada.
    /// </summary>
    [Fact]
    [Trait("Category", "Views")]
    public void CreateOrReplaceView_ShouldChangeDefinition()
    {
        _cnn.ExecNonQuery("CREATE VIEW v AS SELECT id FROM users WHERE tenantid = 10;");
        var r1 = _cnn.QueryRows("SELECT id FROM v ORDER BY id");
        Assert.Equal([1, 2], [.. r1.Select(x => (int)x["id"]!)]);

        _cnn.ExecNonQuery("CREATE OR REPLACE VIEW v AS SELECT id FROM users WHERE tenantid = 20;");
        var r2 = _cnn.QueryRows("SELECT id FROM v ORDER BY id");
        Assert.Equal([3], [.. r2.Select(x => (int)x["id"]!)]);
    }

    /// <summary>
    /// EN: Verifies that a view name shadows a table with the same name.
    /// PT-br: Verifica se o nome da view sobrescreve uma tabela com o mesmo nome.
    /// </summary>
    [Fact]
    [Trait("Category", "Views")]
    public void View_NameShouldShadowTable_WhenSameName()
    {
        var db = new NpgsqlDbMock();
        // cria uma tabela física chamada vshadow, com dados diferentes
        var vshadow = db.AddTable("vshadow");
        vshadow.AddColumn("id", DbType.Int32, false);
        vshadow.Add(new Dictionary<int, object?> { [0] = 999 });

        // cria view com o mesmo nome
        _cnn.ExecNonQuery("CREATE VIEW vshadow AS SELECT id FROM users WHERE id = 1;");

        var rows = _cnn.QueryRows("SELECT id FROM vshadow");
        Assert.Single(rows);
        Assert.Equal(1, (int)rows[0]["id"]!);
    }

    /// <summary>
    /// EN: Verifies that one view can reference another view.
    /// PT-br: Verifica se uma view pode referenciar outra view.
    /// </summary>
    [Fact]
    [Trait("Category", "Views")]
    public void View_CanReferenceAnotherView()
    {
        _cnn.ExecNonQuery("CREATE VIEW v1 AS SELECT id FROM users WHERE tenantid = 10;");
        _cnn.ExecNonQuery("CREATE VIEW v2 AS SELECT id FROM v1 WHERE id > 1;");

        var rows = _cnn.QueryRows("SELECT id FROM v2 ORDER BY id");
        Assert.Equal([2], [.. rows.Select(r => (int)r["id"]!)]);
    }

    /// <summary>
    /// EN: Verifies that a view can project joins and aggregations.
    /// PT-br: Verifica se uma view pode projetar joins e agregacoes.
    /// </summary>
    [Fact]
    [Trait("Category", "Views")]
    public void View_WithJoinAndAggregation_ShouldWork()
    {
        _cnn.ExecNonQuery(@"
CREATE VIEW user_totals AS
SELECT u.id, SUM(o.amount) AS total
FROM users u
LEFT JOIN orders o ON o.userid = u.id
GROUP BY u.id;
");

        var rows = _cnn.QueryRows("SELECT id, total FROM user_totals ORDER BY id");
        Assert.Equal([1, 2, 3], [.. rows.Select(r => (int)r["id"]!)]);

        // user 1: 15, user 2: 7, user 3: sem orders -> NULL (MySQL)
        Assert.Equal(15m, (decimal)rows[0]["total"]!);
        Assert.Equal(7m, (decimal)rows[1]["total"]!);
        Assert.True(rows[2]["total"] is null);
    }

    /// <summary>
    /// EN: Verifies that creating a view with an existing name without OR REPLACE fails.
    /// PT-br: Verifica se criar uma view com nome existente sem OR REPLACE falha.
    /// </summary>
    [Fact]
    [Trait("Category", "Views")]
    public void CreateView_ExistingNameWithoutOrReplace_ShouldThrow()
    {
        _cnn.ExecNonQuery("CREATE VIEW vdup AS SELECT 1 AS x ;");
        Assert.ThrowsAny<Exception>(() => _cnn.ExecNonQuery("CREATE VIEW vdup AS SELECT 2 AS x ;"));
    }

    /// <summary>
    /// EN: Verifies that dropping a view removes its definition.
    /// PT-br: Verifica se remover uma view exclui sua definicao.
    /// </summary>
    [Fact]
    [Trait("Category", "Views")]
    public void DropView_ShouldRemoveDefinition()
    {
        _cnn.ExecNonQuery("CREATE VIEW vdrop AS SELECT id FROM users;");
        _cnn.ExecNonQuery("DROP VIEW vdrop;");
        Assert.ThrowsAny<Exception>(() => _cnn.QueryRows("SELECT * FROM vdrop"));
    }

    /// <summary>
    /// EN: Disposes test resources.
    /// PT-br: Descarta os recursos do teste.
    /// </summary>
    /// <param name="disposing">EN: True to dispose managed resources. PT-br: True para descartar recursos gerenciados.</param>
    protected override void Dispose(bool disposing)
    {
        _cnn.Dispose();
        base.Dispose(disposing);
    }
}
