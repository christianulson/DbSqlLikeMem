namespace DbSqlLikeMem.MySql.Test;

/// <summary>
/// EN: Exercises select-into, insert-select, update, and delete-from-select flows for MySQL.
/// PT: Exercita fluxos de select-into, insert-select, update e delete-from-select para MySQL.
/// </summary>
/// <param name="helper">
/// EN: Output helper used by the test base.
/// PT: Helper de saída usado pela base de testes.
/// </param>
public sealed class SelectIntoInsertSelectUpdateDeleteFromSelectTests(
        ITestOutputHelper helper
    ) : SelectIntoInsertSelectUpdateDeleteFromSelectTestsBase<MySqlDbMock>(helper)
{
    /// <summary>
    /// EN: Creates a new MySQL mock database for each scenario.
    /// PT: Cria um novo banco simulado de MySQL para cada cenário.
    /// </summary>
    protected override MySqlDbMock CreateDb() => [];

    protected override bool SupportsUpdateDeleteJoinRuntime => true;

    /// <summary>
    /// EN: Executes a non-query command using a MySQL mock connection.
    /// PT: Executa um comando sem retorno usando uma conexão simulada de MySQL.
    /// </summary>
    protected override int ExecuteNonQuery(
        MySqlDbMock db,
        string sql)
    {
        using var c = new MySqlConnectionMock(db);
        using var cmd = new MySqlCommandMock(c) { CommandText = sql };
        return cmd.ExecuteNonQuery();
    }

    /// <summary>
    /// EN: Gets the MySQL-specific SQL used to delete from a join with a derived select.
    /// PT: Obtém o SQL específico de MySQL usado para deletar de um join com select derivado.
    /// </summary>
    protected override string DeleteJoinDerivedSelectSql
        => "DELETE u FROM users u JOIN (SELECT id FROM users WHERE tenantid = 10) s ON s.id = u.id";

    [Fact]
    [Trait("Category", "SelectIntoInsertSelectUpdateDeleteFromSelect")]
    public void UpdateFromJoinSyntax_ShouldThrowNotSupported_ForMySql()
    {
        var db = CreateDb();
        var users = db.AddTable("users");
        users.AddColumn("id", DbType.Int32, false);
        users.AddColumn("total", DbType.Decimal, true, decimalPlaces: 2);
        users.Add(new Dictionary<int, object?> { { 0, 1 }, { 1, null } });

        var orders = db.AddTable("orders");
        orders.AddColumn("userid", DbType.Int32, false);
        orders.AddColumn("amount", DbType.Decimal, false, decimalPlaces: 2);
        orders.Add(new Dictionary<int, object?> { { 0, 1 }, { 1, 10m } });

        const string sql = @"
UPDATE u
SET u.total = s.total
FROM users u
JOIN (SELECT userid, SUM(amount) AS total FROM orders GROUP BY userid) s ON s.userid = u.id";

        var ex = Assert.Throws<NotSupportedException>(() => ExecuteNonQuery(db, sql));
        Assert.Contains("SQL não suportado para dialeto", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("UPDATE ... FROM ... JOIN", ex.Message, StringComparison.OrdinalIgnoreCase);
    }
}
