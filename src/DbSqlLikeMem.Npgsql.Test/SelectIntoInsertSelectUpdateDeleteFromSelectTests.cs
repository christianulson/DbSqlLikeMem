namespace DbSqlLikeMem.Npgsql.Test;

/// <summary>
/// EN: Exercises select-into, insert-select, update, and delete-from-select flows for PostgreSQL.
/// PT: Exercita fluxos de select-into, insert-select, update e delete-from-select para PostgreSQL.
/// </summary>
/// <param name="helper">
/// EN: Output helper used by the test base.
/// PT: Helper de saída usado pela base de testes.
/// </param>
public sealed class SelectIntoInsertSelectUpdateDeleteFromSelectTests(
        ITestOutputHelper helper
    ) : SelectIntoInsertSelectUpdateDeleteFromSelectTestsBase<NpgsqlDbMock>(helper)
{
    /// <summary>
    /// EN: Creates a new PostgreSQL mock database for each scenario.
    /// PT: Cria um novo banco simulado de PostgreSQL para cada cenário.
    /// </summary>
    protected override NpgsqlDbMock CreateDb() => [];

    protected override bool SupportsUpdateDeleteJoinRuntime => true;

    protected override string UpdateJoinDerivedSelectSql
        => @"
UPDATE u
SET u.total = s.total
FROM users u
JOIN (SELECT userid, SUM(amount) AS total FROM orders GROUP BY userid) s ON s.userid = u.id
WHERE u.tenantid = 10";

    protected override string DeleteJoinDerivedSelectSql
        => "DELETE FROM users u USING (SELECT id FROM users WHERE tenantid = 10) s WHERE s.id = u.id";

    /// <summary>
    /// EN: Executes a non-query command using a PostgreSQL mock connection.
    /// PT: Executa um comando sem retorno usando uma conexão simulada de PostgreSQL.
    /// </summary>
    protected override int ExecuteNonQuery(
        NpgsqlDbMock db,
        string sql)
    {
        using var c = new NpgsqlConnectionMock(db);
        using var cmd = new NpgsqlCommandMock(c) { CommandText = sql };
        return cmd.ExecuteNonQuery();
    }

    [Fact]
    [Trait("Category", "SelectIntoInsertSelectUpdateDeleteFromSelect")]
    public void DeleteUsing_WithJoinConditionAndExtraFilter_ShouldDeleteOnlyFilteredRows()
    {
        var db = CreateDb();
        var users = db.AddTable("users");
        users.AddColumn("id", DbType.Int32, false);
        users.AddColumn("tenantid", DbType.Int32, false);
        users.Add(new Dictionary<int, object?> { { 0, 1 }, { 1, 10 } });
        users.Add(new Dictionary<int, object?> { { 0, 2 }, { 1, 20 } });

        const string sql = "DELETE FROM users u USING (SELECT id FROM users) s WHERE (s.id = u.id) AND u.tenantid = 10";

        var deleted = ExecuteNonQuery(db, sql);

        Assert.Equal(1, deleted);
        Assert.Single(users);
        Assert.Equal(2, (int)users[0][0]!);
    }



    [Fact]
    [Trait("Category", "SelectIntoInsertSelectUpdateDeleteFromSelect")]
    public void DeleteUsing_WithNestedParenthesizedJoinCondition_ShouldDeleteRows()
    {
        var db = CreateDb();
        var users = db.AddTable("users");
        users.AddColumn("id", DbType.Int32, false);
        users.AddColumn("tenantid", DbType.Int32, false);
        users.Add(new Dictionary<int, object?> { { 0, 1 }, { 1, 10 } });
        users.Add(new Dictionary<int, object?> { { 0, 2 }, { 1, 20 } });

        const string sql = "DELETE FROM users u USING (SELECT id FROM users WHERE tenantid = 10) s WHERE ((s.id = u.id))";

        var deleted = ExecuteNonQuery(db, sql);

        Assert.Equal(1, deleted);
        Assert.Single(users);
        Assert.Equal(2, (int)users[0][0]!);
    }

    [Fact]
    [Trait("Category", "SelectIntoInsertSelectUpdateDeleteFromSelect")]
    public void DeleteUsing_WithoutJoinCondition_ShouldThrowActionableMessage()
    {
        var db = CreateDb();
        var users = db.AddTable("users");
        users.AddColumn("id", DbType.Int32, false);
        users.AddColumn("tenantid", DbType.Int32, false);
        users.Add(new Dictionary<int, object?> { { 0, 1 }, { 1, 10 } });

        const string sql = "DELETE FROM users u USING (SELECT id FROM users) s WHERE u.tenantid = 10";

        var ex = Assert.Throws<InvalidOperationException>(() => ExecuteNonQuery(db, sql));
        Assert.Contains("WHERE deve conter uma condição de junção", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

}
