namespace DbSqlLikeMem.Npgsql.Test.Views;

public sealed class PostgreSqlCreateViewEngineTests : XUnitTestBase
{
    private readonly NpgsqlConnectionMock _cnn;
    private readonly ITableMock _users;
    private readonly ITableMock _orders;

    public PostgreSqlCreateViewEngineTests(ITestOutputHelper helper): base(helper)
    {
        var db = new NpgsqlDbMock();
        _users = db.AddTable("users");
        _users.Columns["id"] = new(0, DbType.Int32, false) { Identity = false };
        _users.Columns["name"] = new(1, DbType.String, false);
        _users.Columns["tenantid"] = new(2, DbType.Int32, false);
        _users.Add(new Dictionary<int, object?> { [0] = 1, [1] = "John", [2] = 10 });
        _users.Add(new Dictionary<int, object?> { [0] = 2, [1] = "Bob",  [2] = 10 });
        _users.Add(new Dictionary<int, object?> { [0] = 3, [1] = "Jane", [2] = 20 });

        _orders = db.AddTable("orders");
        _orders.Columns["userid"] = new(0, DbType.Int32, false);
        _orders.Columns["amount"] = new(1, DbType.Decimal, false);
        _orders.Add(new Dictionary<int, object?> { [0] = 1, [1] = 10m });
        _orders.Add(new Dictionary<int, object?> { [0] = 1, [1] = 5m });
        _orders.Add(new Dictionary<int, object?> { [0] = 2, [1] = 7m });

        _cnn = new NpgsqlConnectionMock(db);
        _cnn.Open();
    }

    [Fact]
    public void CreateView_ThenSelectFromView_ShouldReturnExpectedRows()
    {
        _cnn.ExecNonQuery(@"
CREATE VIEW v10 AS
SELECT id, name FROM users WHERE tenantid = 10;
");

        var rows = _cnn.QueryRows("SELECT id, name FROM v10 ORDER BY id");
        Assert.Equal([1, 2], rows.Select(r => (int)r["id"]!).ToArray());
        Assert.Equal(["John", "Bob" ], rows.Select(r => (string)r["name"]!).ToArray());
    }

    [Fact]
    public void View_IsNotMaterialized_ShouldReflectBaseTableChanges()
    {
        _cnn.ExecNonQuery("CREATE VIEW v_all AS SELECT id FROM users ORDER BY id;");

        // altera tabela base após criar view
        _users.Add(new Dictionary<int, object?> { [0] = 4, [1] = "Zoe", [2] = 10 });

        var rows = _cnn.QueryRows("SELECT id FROM v_all ORDER BY id");
        Assert.Equal([1, 2, 3, 4], rows.Select(r => (int)r["id"]!).ToArray());
    }

    [Fact]
    public void CreateOrReplaceView_ShouldChangeDefinition()
    {
        _cnn.ExecNonQuery("CREATE VIEW v AS SELECT id FROM users WHERE tenantid = 10;");
        var r1 = _cnn.QueryRows("SELECT id FROM v ORDER BY id");
        Assert.Equal([1, 2], r1.Select(x => (int)x["id"]!).ToArray());

        _cnn.ExecNonQuery("CREATE OR REPLACE VIEW v AS SELECT id FROM users WHERE tenantid = 20;");
        var r2 = _cnn.QueryRows("SELECT id FROM v ORDER BY id");
        Assert.Equal([3], r2.Select(x => (int)x["id"]!).ToArray());
    }

    [Fact]
    public void View_NameShouldShadowTable_WhenSameName()
    {
        var db = new NpgsqlDbMock();
        // cria uma tabela física chamada vshadow, com dados diferentes
        var vshadow = db.AddTable("vshadow");
        vshadow.Columns["id"] = new(0, DbType.Int32, false);
        vshadow.Add(new Dictionary<int, object?> { [0] = 999 });

        // cria view com o mesmo nome
        _cnn.ExecNonQuery("CREATE VIEW vshadow AS SELECT id FROM users WHERE id = 1;");

        var rows = _cnn.QueryRows("SELECT id FROM vshadow");
        Assert.Single(rows);
        Assert.Equal(1, (int)rows[0]["id"]!);
    }

    [Fact]
    public void View_CanReferenceAnotherView()
    {
        _cnn.ExecNonQuery("CREATE VIEW v1 AS SELECT id FROM users WHERE tenantid = 10;");
        _cnn.ExecNonQuery("CREATE VIEW v2 AS SELECT id FROM v1 WHERE id > 1;");

        var rows = _cnn.QueryRows("SELECT id FROM v2 ORDER BY id");
        Assert.Equal([2], rows.Select(r => (int)r["id"]!).ToArray());
    }

    [Fact]
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
        Assert.Equal([1, 2, 3], rows.Select(r => (int)r["id"]!).ToArray());

        // user 1: 15, user 2: 7, user 3: sem orders -> NULL (MySQL)
        Assert.Equal(15m, (decimal)rows[0]["total"]!);
        Assert.Equal(7m, (decimal)rows[1]["total"]!);
        Assert.True(rows[2]["total"] is null);
    }

    [Fact]
    public void CreateView_ExistingNameWithoutOrReplace_ShouldThrow()
    {
        _cnn.ExecNonQuery("CREATE VIEW vdup AS SELECT 1 AS x ;");
        Assert.ThrowsAny<Exception>(() => _cnn.ExecNonQuery("CREATE VIEW vdup AS SELECT 2 AS x ;"));
    }

    [Fact(Skip = "MySQL: DROP VIEW faz parte do ciclo de vida. Implementar depois.")]
    public void DropView_ShouldRemoveDefinition()
    {
        _cnn.ExecNonQuery("CREATE VIEW vdrop AS SELECT id FROM users;");
        _cnn.ExecNonQuery("DROP VIEW vdrop;");
        Assert.ThrowsAny<Exception>(() => _cnn.QueryRows("SELECT * FROM vdrop"));
    }

    /// <summary>
    /// EN: Disposes test resources.
    /// PT: Descarta os recursos do teste.
    /// </summary>
    /// <param name="disposing">EN: True to dispose managed resources. PT: True para descartar recursos gerenciados.</param>
    protected override void Dispose(bool disposing)
    {
        _cnn.Dispose();
        base.Dispose(disposing);
    }
}
