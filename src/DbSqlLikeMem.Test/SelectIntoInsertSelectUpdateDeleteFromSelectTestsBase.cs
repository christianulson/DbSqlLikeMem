namespace DbSqlLikeMem.Test;

/// <summary>
/// EN: Shared tests for CREATE TABLE AS SELECT, INSERT SELECT, UPDATE with derived select and DELETE from select.
/// PT: Testes compartilhados para CREATE TABLE AS SELECT, INSERT SELECT, UPDATE com subselect derivado e DELETE via select.
/// </summary>
public abstract class SelectIntoInsertSelectUpdateDeleteFromSelectTestsBase<TDbMock>(
        ITestOutputHelper helper
    ) : XUnitTestBase(helper)
    where TDbMock : DbMock
{
    /// <summary>
    /// EN: Creates a provider-specific database mock used by shared select/insert/update/delete tests.
    /// PT: Cria um simulado de banco específico do provedor usado pelos testes compartilhados de select/insert/update/delete.
    /// </summary>
    protected abstract TDbMock CreateDb();

    /// <summary>
    /// EN: Executes a provider-specific non-query SQL command against the supplied mock database.
    /// PT: Executa um comando SQL sem retorno específico do provedor no banco simulado informado.
    /// </summary>
    protected abstract int ExecuteNonQuery(
        TDbMock db,
        string sql);

    /// <summary>
    /// EN: Gets the SQL used to delete rows based on a derived select expression.
    /// PT: Obtém o SQL usado para excluir linhas com base em uma expressão de subselect derivado.
    /// </summary>
    protected virtual string UpdateJoinDerivedSelectSql
        => @"
UPDATE users u
JOIN (SELECT userid, SUM(amount) AS total FROM orders GROUP BY userid) s ON s.userid = u.id
SET u.total = s.total
WHERE u.tenantid = 10";

    /// <summary>
    /// EN: Gets the SQL used to delete rows through a derived select-based join strategy.
    /// PT: Obtém o SQL usado para excluir linhas por meio de uma estratégia de join baseada em select derivado.
    /// </summary>
    protected virtual string DeleteJoinDerivedSelectSql
        => "DELETE u FROM users u JOIN (SELECT id FROM users WHERE tenantid = 10) s ON s.id = u.id";

    /// <summary>
    /// EN: Indicates whether this provider should execute UPDATE/DELETE JOIN runtime paths.
    /// PT: Indica se este provedor deve executar fluxos de runtime de UPDATE/DELETE com JOIN.
    /// </summary>
    protected virtual bool SupportsUpdateDeleteJoinRuntime => false;

    /// <summary>
    /// EN: Tests CreateTableAsSelect_ShouldCreateNewTableWithRows behavior.
    /// PT: Testa o comportamento de CreateTableAsSelect_ShouldCreateNewTableWithRows.
    /// </summary>
    [Fact]
    [Trait("Category", "SelectIntoInsertSelectUpdateDeleteFromSelect")]
    public void CreateTableAsSelect_ShouldCreateNewTableWithRows()
    {
        var db = CreateDb();
        var users = db.AddTable("users");
        users.AddColumn("id", DbType.Int32, false);
        users.AddColumn("name", DbType.String, false);
        users.AddColumn("tenantid", DbType.Int32, false);
        users.Add(new Dictionary<int, object?> { { 0, 1 }, { 1, "A" }, { 2, 10 } });
        users.Add(new Dictionary<int, object?> { { 0, 2 }, { 1, "B" }, { 2, 20 } });
        users.Add(new Dictionary<int, object?> { { 0, 3 }, { 1, "C" }, { 2, 10 } });

        const string sql = "CREATE TABLE active_users AS SELECT id, name FROM users WHERE tenantid = 10";

        var affected = ExecuteNonQuery(db, sql);

        Assert.Equal(0, affected);
        Assert.True(db.TryGetTable("active_users", out var active));
        Assert.NotNull(active);
        Assert.Equal(2, active!.Count);
        Assert.True(active.Columns.ContainsKey("id"));
        Assert.True(active.Columns.ContainsKey("name"));
        Assert.Equal(1, (int)active[0][0]!);
        Assert.Equal("A", active[0][1]);
    }

    /// <summary>
    /// EN: Tests InsertIntoSelect_ShouldInsertRowsFromQuery behavior.
    /// PT: Testa o comportamento de InsertIntoSelect_ShouldInsertRowsFromQuery.
    /// </summary>
    [Fact]
    [Trait("Category", "SelectIntoInsertSelectUpdateDeleteFromSelect")]
    public void InsertIntoSelect_ShouldInsertRowsFromQuery()
    {
        var db = CreateDb();
        var users = db.AddTable("users");
        users.AddColumn("id", DbType.Int32, false);
        users.AddColumn("name", DbType.String, false);
        users.AddColumn("tenantid", DbType.Int32, false);
        users.Add(new Dictionary<int, object?> { { 0, 1 }, { 1, "A" }, { 2, 10 } });
        users.Add(new Dictionary<int, object?> { { 0, 2 }, { 1, "B" }, { 2, 20 } });

        var audit = db.AddTable("audit_users");
        audit.AddColumn("userid", DbType.Int32, false);
        audit.AddColumn("username", DbType.String, false);

        const string sql = "INSERT INTO audit_users (userid, username) SELECT id, name FROM users WHERE tenantid = 10";

        var inserted = ExecuteNonQuery(db, sql);

        Assert.Equal(1, inserted);
        Assert.Single(audit);
        Assert.Equal(1, (int)audit[0][0]!);
        Assert.Equal("A", audit[0][1]);
    }

    /// <summary>
    /// EN: Tests UpdateJoinDerivedSelect_ShouldUpdateRows behavior.
    /// PT: Testa o comportamento de UpdateJoinDerivedSelect_ShouldUpdateRows.
    /// </summary>
    [Fact]
    [Trait("Category", "SelectIntoInsertSelectUpdateDeleteFromSelect")]
    public void UpdateJoinDerivedSelect_ShouldUpdateRows()
    {
        var db = CreateDb();
        var users = db.AddTable("users");
        users.AddColumn("id", DbType.Int32, false);
        users.AddColumn("tenantid", DbType.Int32, false);
        users.AddColumn("total", DbType.Decimal, true, decimalPlaces: 2);
        users.Add(new Dictionary<int, object?> { { 0, 1 }, { 1, 10 }, { 2, null } });
        users.Add(new Dictionary<int, object?> { { 0, 2 }, { 1, 10 }, { 2, null } });
        users.Add(new Dictionary<int, object?> { { 0, 3 }, { 1, 20 }, { 2, null } });

        var orders = db.AddTable("orders");
        orders.AddColumn("userid", DbType.Int32, false);
        orders.AddColumn("amount", DbType.Decimal, false, decimalPlaces: 2);
        orders.Add(new Dictionary<int, object?> { { 0, 1 }, { 1, 10m } });
        orders.Add(new Dictionary<int, object?> { { 0, 1 }, { 1, 5m } });
        orders.Add(new Dictionary<int, object?> { { 0, 2 }, { 1, 7m } });

        var sql = UpdateJoinDerivedSelectSql;

        if (!SupportsUpdateDeleteJoinRuntime)
        {
            var ex = Assert.Throws<NotSupportedException>(() => ExecuteNonQuery(db, sql));
            Assert.Contains("SQL não suportado para dialeto", ex.Message, StringComparison.OrdinalIgnoreCase);
            return;
        }

        var updated = ExecuteNonQuery(db, sql);

        Assert.Equal(2, updated);
        Assert.Equal(15m, users[0][2]);
        Assert.Equal(7m, users[1][2]);
        Assert.Null(users[2][2]);
    }

    /// <summary>
    /// EN: Tests DeleteJoinDerivedSelect_ShouldDeleteRows behavior.
    /// PT: Testa o comportamento de DeleteJoinDerivedSelect_ShouldDeleteRows.
    /// </summary>
    [Fact]
    [Trait("Category", "SelectIntoInsertSelectUpdateDeleteFromSelect")]
    public void DeleteJoinDerivedSelect_ShouldDeleteRows()
    {
        var db = CreateDb();
        var users = db.AddTable("users");
        users.AddColumn("id", DbType.Int32, false);
        users.AddColumn("tenantid", DbType.Int32, false);
        users.Add(new Dictionary<int, object?> { { 0, 1 }, { 1, 10 } });
        users.Add(new Dictionary<int, object?> { { 0, 2 }, { 1, 10 } });
        users.Add(new Dictionary<int, object?> { { 0, 3 }, { 1, 20 } });

        if (!SupportsUpdateDeleteJoinRuntime)
        {
            var ex = Assert.Throws<NotSupportedException>(() => ExecuteNonQuery(db, DeleteJoinDerivedSelectSql));
            Assert.Contains("SQL não suportado para dialeto", ex.Message, StringComparison.OrdinalIgnoreCase);
            return;
        }

        var deleted = ExecuteNonQuery(db, DeleteJoinDerivedSelectSql);

        Assert.Equal(2, deleted);
        Assert.Single(users);
        Assert.Equal(3, (int)users[0][0]!);
    }
}
